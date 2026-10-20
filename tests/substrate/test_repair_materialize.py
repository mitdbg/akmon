"""Repair materialization witnesses — the recorded selection (r*) over the
resolved claim state.

PACT ``consistency-repair-objective``, task.repair.materialize
(req.repair.executed-cleaned-state). These are the PURE / interpreted witnesses
(no DB): the projection's kept rows come from ``compute_rows_interpreted`` (the
reference interpreter over in-memory surfaces), and the selection rows come from
``compute_repair_selection`` (the pure selection layer + cross-check). The live
publish witness (published status + kept-row / selection-row counts) is
``tests/substrate/consistency/test_claim_plane_wire.py``.

Proven here:

* a decisively-resolved bucket records ``selected`` and its distrusted set is
  EXACTLY that bucket's superseded ids (the kept ``bucket_resolution`` row) — the
  selection inherits the projection's resolution, never re-ranking; a contested
  bucket records ``unresolved`` with reason contested;
* source preservation: the loser claims stay present in the resolved projection
  (ranked, superseded — never deleted); the selection only RECORDS the distrust;
* order-independence: shuffling the input claim order produces a byte-identical
  whole-selection digest (footprint-disjoint corrections commute).

Input shapes follow tests/substrate/transform/test_claim_projection_ir.py.
"""
from __future__ import annotations

import json
import random
from typing import Any

from okg.substrate.consistency.claim_model.precedence import OUTCOME_CONTESTED
from okg.substrate.consistency.claim_model.applicability import (
    Applicability,
    TimeBounds,
)
from okg.substrate.consistency.claim_model.views import (
    POLARITY_NEGATED,
    ClaimSemantics,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    ROW_KIND_RESOLUTION,
    bucket_row_key,
    compute_rows_interpreted,
)
from okg.substrate.derived_artifacts.repair_materialization import (
    OUTCOME_SELECTED,
    OUTCOME_UNRESOLVED,
    compute_dirty_selection,
    compute_repair_selection,
    selection_digest_from_rows,
)

_PRED = "preferred_target"
_CONSTRAINT = "unique_preferred"


def _eff(cid: str, *, key: str, value: str, source: str) -> dict[str, Any]:
    return {
        "claim_id": cid, "predicate": _PRED, "normalized_key": key,
        "value": value, "scope": "A > B", "scope_state": "known",
        "source_id": source,
    }


def _claim_node(cid: str, **attrs: Any) -> dict[str, Any]:
    return {"node_id": cid, "attrs": attrs}


_FUNCTIONAL = [{
    "node_id": "pol:f",
    "attrs": {
        "policy_id": _CONSTRAINT, "policy_kind": "functional",
        "predicate": _PRED, "enabled": "true",
    },
}]


# k1: a (authority 10) beats b (authority 1) — DECISIVE, b superseded.
# k2: c and d tie on every signal — CONTESTED.
def _decisive_and_contested() -> tuple[list, list]:
    effective = [
        _eff("claim:a", key="k1", value="X", source="page:1"),
        _eff("claim:b", key="k1", value="Y", source="page:2"),
        _eff("claim:c", key="k2", value="P", source="page:3"),
        _eff("claim:d", key="k2", value="Q", source="page:4"),
    ]
    claims = [
        _claim_node("claim:a", source_authority="10"),
        _claim_node("claim:b", source_authority="1"),
        _claim_node("claim:c"),
        _claim_node("claim:d"),
    ]
    return effective, claims


def _rows_by_bucket(sel_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {r["bucket_key"]: r for r in sel_rows}


def test_selection_records_decisive_and_contested_buckets() -> None:
    effective, claims = _decisive_and_contested()
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    sel_rows, digest = compute_repair_selection(
        effective, claims, _FUNCTIONAL, [], kept
    )
    by_bucket = _rows_by_bucket(sel_rows)
    rk1 = bucket_row_key(_PRED, "k1")
    rk2 = bucket_row_key(_PRED, "k2")
    assert set(by_bucket) == {rk1, rk2}

    # Decisive bucket k1: selected, precedence-retraction, winner a, distrust b.
    k1 = by_bucket[rk1]
    assert k1["outcome"] == OUTCOME_SELECTED
    assert k1["generator_id"] == "precedence-retraction"
    assert k1["winner_claim_id"] == "claim:a"
    assert k1["distrusted_claim_ids"] == ["claim:b"]
    assert k1["asserted_claim_ids"] == []
    assert k1["unresolved_reason"] is None
    assert k1["basis"]  # a decisive basis (source_authority), non-empty

    # The distrusted set is EXACTLY the kept bucket_resolution superseded set —
    # inherited from the projection, never re-derived.
    kept_k1 = kept[(ROW_KIND_RESOLUTION, rk1)]
    assert k1["distrusted_claim_ids"] == json.loads(kept_k1["superseded_claim_ids"])
    assert k1["basis"] == kept_k1["basis"]

    # Contested bucket k2: unresolved, empty repair, reason contested.
    k2 = by_bucket[rk2]
    assert k2["outcome"] == OUTCOME_UNRESOLVED
    assert k2["generator_id"] == "empty-repair"
    assert k2["winner_claim_id"] is None
    assert k2["distrusted_claim_ids"] == []
    assert k2["unresolved_reason"] == OUTCOME_CONTESTED

    assert isinstance(digest, str) and len(digest) == 64


def test_typed_callback_conflict_flows_into_repair_cross_check() -> None:
    semantics = ClaimSemantics(
        logical_subject="entity:shared",
        activity="installation",
        polarity=POLARITY_NEGATED,
        applicability=Applicability(
            heading_path="A > B",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01"),
        ),
    ).to_attr()
    effective = [
        {**_eff("claim:a", key="typed", value="same", source="page:1"),
         "claim_semantics": semantics},
        {**_eff("claim:b", key="typed", value="same", source="page:2"),
         "claim_semantics": semantics},
    ]
    claims = [
        _claim_node("claim:a", source_authority="10"),
        _claim_node("claim:b", source_authority="1"),
    ]
    calls: list[tuple[str, str]] = []

    def incompatible(left, right):
        calls.append((left.node_id, right.node_id))
        return True

    kept = compute_rows_interpreted(
        effective,
        claims,
        _FUNCTIONAL,
        typed_incompatible=incompatible,
    )
    selection, _digest = compute_repair_selection(
        effective,
        claims,
        _FUNCTIONAL,
        [],
        kept,
        typed_incompatible=incompatible,
    )
    assert len(selection) == 1
    assert selection[0]["outcome"] == OUTCOME_SELECTED
    assert selection[0]["distrusted_claim_ids"] == ["claim:b"]
    assert calls == [("claim:a", "claim:b")] * 2


def test_superseded_loser_remains_present_in_projection() -> None:
    """Source preservation: the distrusted loser stays present (ranked,
    superseded) in the resolved projection — the selection records the distrust,
    the projection never deletes the claim."""
    effective, claims = _decisive_and_contested()
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    sel_rows, _ = compute_repair_selection(
        effective, claims, _FUNCTIONAL, [], kept
    )
    rk1 = bucket_row_key(_PRED, "k1")
    k1_sel = _rows_by_bucket(sel_rows)[rk1]
    assert k1_sel["distrusted_claim_ids"] == ["claim:b"]

    kept_k1 = kept[(ROW_KIND_RESOLUTION, rk1)]
    ranked = json.loads(kept_k1["ranked_claim_ids"])
    # The distrusted loser is STILL a member of the resolved bucket (ranked),
    # and the effective input row for it is untouched — source-preserving.
    assert "claim:b" in ranked
    assert any(r["claim_id"] == "claim:b" for r in effective)


def test_shuffling_inputs_gives_identical_selection_digest() -> None:
    """Footprint-disjoint corrections commute: the whole-selection digest is
    invariant under input claim-order shuffling."""
    effective, claims = _decisive_and_contested()
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    _, digest0 = compute_repair_selection(effective, claims, _FUNCTIONAL, [], kept)

    rng = random.Random(1234)
    for _ in range(8):
        eff_s = list(effective)
        claims_s = list(claims)
        rng.shuffle(eff_s)
        rng.shuffle(claims_s)
        kept_s = compute_rows_interpreted(eff_s, claims_s, _FUNCTIONAL, [])
        _, digest_s = compute_repair_selection(
            eff_s, claims_s, _FUNCTIONAL, [], kept_s
        )
        assert digest_s == digest0


def test_no_conflicts_records_empty_selection() -> None:
    """A generation with no conflicted buckets records an empty, valid
    selection (zero rows) — the complete selection set is empty, not missing."""
    effective = [_eff("claim:solo", key="k9", value="Z", source="page:9")]
    claims = [_claim_node("claim:solo")]
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    sel_rows, digest = compute_repair_selection(
        effective, claims, _FUNCTIONAL, [], kept
    )
    assert sel_rows == []
    assert isinstance(digest, str) and len(digest) == 64


def test_selection_digest_from_rows_matches_pure_layer() -> None:
    """The rows-based whole-generation digest is byte-equal to the pure
    ``SelectedRepair`` digest (task.repair.differential): the differential carry
    path stamps the digest from ASSEMBLED rows (it cannot rebuild the pure
    SelectedRepair without re-ranking the untouched buckets), so this LOCKS the
    row-reconstruction inverse to the forward shaping. Any drift makes the
    incremental and from-scratch digests diverge — the parity arbiter's guard."""
    effective, claims = _decisive_and_contested()
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    sel_rows, digest = compute_repair_selection(
        effective, claims, _FUNCTIONAL, [], kept
    )
    assert selection_digest_from_rows(sel_rows) == digest
    # empty selection: still byte-equal.
    solo = [_eff("claim:solo", key="k9", value="Z", source="page:9")]
    solo_kept = compute_rows_interpreted(solo, [_claim_node("claim:solo")],
                                         _FUNCTIONAL, [])
    empty_rows, empty_digest = compute_repair_selection(
        solo, [_claim_node("claim:solo")], _FUNCTIONAL, [], solo_kept
    )
    assert selection_digest_from_rows(empty_rows) == empty_digest


def test_dirty_selection_reselects_only_dirty_buckets() -> None:
    """The pure differential half (task.repair.differential): re-selecting a
    subset of buckets equals the from-scratch selection restricted to that
    subset — bucket-locality — and dirty-fresh + carried reproduces the
    whole-generation digest a full recompute stamps."""
    # k1 decisive (a>b), k2 contested (c,d tie).
    effective, claims = _decisive_and_contested()
    kept = compute_rows_interpreted(effective, claims, _FUNCTIONAL, [])
    full_rows, full_digest = compute_repair_selection(
        effective, claims, _FUNCTIONAL, [], kept
    )
    full_by = _rows_by_bucket(full_rows)
    rk1 = bucket_row_key(_PRED, "k1")
    rk2 = bucket_row_key(_PRED, "k2")

    # Touch only k1: the dirty selection re-selects k1 alone, byte-matching the
    # from-scratch k1 row (bucket-locality).
    dirty_rows = compute_dirty_selection(
        effective, claims, _FUNCTIONAL, [], kept, dirty_bucket_keys={rk1}
    )
    assert [r["bucket_key"] for r in dirty_rows] == [rk1]
    fields = ("outcome", "winner_claim_id", "generator_id", "generator_version",
              "basis", "distrusted_claim_ids", "asserted_claim_ids",
              "unresolved_reason")
    assert {k: dirty_rows[0][k] for k in fields} == {
        k: full_by[rk1][k] for k in fields
    }

    # dirty-fresh (k1) + carried (k2 from the prior full set) reproduces the
    # whole-generation digest a from-scratch recompute stamps.
    carried = [{**full_by[rk2], "carried_from_generation": "PRIOR"}]
    assembled = sorted([*dirty_rows, *carried], key=lambda r: r["bucket_key"])
    assert selection_digest_from_rows(assembled) == full_digest
