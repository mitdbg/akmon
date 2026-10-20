"""Parity tests for the scoped-precedents ≻-resolution core. Pure, DB-free.

Ported from the cern-twiki reference implementation's test_scoped_precedents.py
(okg-deployments main) under PACT consistency-claim-model-core
task.core.extract-library. Covers the fixed lexicographic order, the
decisive-vs-ranking-only distinction (recency never settles alone), operator
precedents (scope match + last-writer-wins), determinism, and ranked output.
"""
from __future__ import annotations

from okg.substrate.consistency.claim_model import (
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_EFFECTIVE_DATE,
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
    SIGNAL_SUPERSESSION,
    ClaimSignals,
    Precedent,
    resolve_bucket,
)


def sig(cid, **kw):
    return ClaimSignals(claim_id=cid, **kw)


# --------------------------------------------------------------------------- #
# Degenerate                                                                  #
# --------------------------------------------------------------------------- #
def test_sole_claim_is_canonical():
    r = resolve_bucket([sig("a")])
    assert r.outcome == OUTCOME_CANONICAL and r.canonical_claim_id == "a"


def test_empty_bucket_contested():
    assert resolve_bucket([]).outcome == OUTCOME_CONTESTED


# --------------------------------------------------------------------------- #
# The fixed lexicographic order (decisive signals settle)                     #
# --------------------------------------------------------------------------- #
def test_supersession_settles():
    r = resolve_bucket([sig("old", is_superseded=True), sig("new", is_superseded=False)])
    assert r.outcome == OUTCOME_CANONICAL and r.canonical_claim_id == "new"
    assert r.basis == SIGNAL_SUPERSESSION


def test_effective_date_settles_when_supersession_tied():
    r = resolve_bucket([
        sig("a", effective_date="2023-01-01"),
        sig("b", effective_date="2024-06-01"),
    ])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_EFFECTIVE_DATE


def test_source_authority_settles():
    r = resolve_bucket([sig("a", source_authority=1), sig("b", source_authority=5)])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY


def test_independent_support_settles():
    r = resolve_bucket([sig("a", independent_support=1), sig("b", independent_support=3)])
    assert r.canonical_claim_id == "b"


# --------------------------------------------------------------------------- #
# Ranking-only signals never settle (recency / maintenance)                   #
# --------------------------------------------------------------------------- #
def test_recency_alone_is_contested():
    # everything decisive is tied; only document recency differs -> contested
    r = resolve_bucket([
        sig("a", document_recency="2020-01-01"),
        sig("b", document_recency="2026-06-01"),
    ])
    assert r.outcome == OUTCOME_CONTESTED
    assert r.basis == SIGNAL_DOCUMENT_RECENCY
    # but it still RANKS (recency-descending)
    assert r.ranked_claim_ids == ("b", "a")


def test_maintenance_alone_is_contested():
    r = resolve_bucket([sig("a", maintenance_rank=0), sig("b", maintenance_rank=9)])
    assert r.outcome == OUTCOME_CONTESTED


def test_full_tie_is_contested():
    r = resolve_bucket([sig("a", source_authority=2), sig("b", source_authority=2)])
    assert r.outcome == OUTCOME_CONTESTED and r.basis == "tie"


def test_decisive_lead_wins_even_with_recency_against():
    # b leads on a decisive signal (authority) though a is more recent -> b canonical
    r = resolve_bucket([
        sig("a", source_authority=1, document_recency="2026-06-01"),
        sig("b", source_authority=9, document_recency="2010-01-01"),
    ])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY


# --------------------------------------------------------------------------- #
# Operator precedents (the ≻-update)                                          #
# --------------------------------------------------------------------------- #
def test_precedent_overrides_the_fixed_order():
    # by the fixed order 'b' (later date) would win; an operator precedent prefers
    # source class 'dpg' (which 'a' has) -> 'a' canonical on the precedent signal.
    claims = [
        sig("a", source_class="dpg", effective_date="2020-01-01"),
        sig("b", source_class="user", effective_date="2025-01-01"),
    ]
    r = resolve_bucket(claims, [Precedent(prefer_source="dpg")])
    assert r.canonical_claim_id == "a" and r.basis == SIGNAL_PRECEDENT


def test_precedent_applies_only_within_overlapping_scope():
    claims = [sig("a", source_class="dpg", scope="Reco > HCAL"),
              sig("b", source_class="user", scope="Reco > HCAL", source_authority=5)]
    # precedent scoped to a DIFFERENT branch -> does not apply, authority settles for b
    r = resolve_bucket(claims, [Precedent(prefer_source="dpg", scope="Calibration")])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY
    # precedent scoped to an ANCESTOR ('Reco') -> applies (subsumption), a wins
    r2 = resolve_bucket(claims, [Precedent(prefer_source="dpg", scope="Reco")])
    assert r2.canonical_claim_id == "a" and r2.basis == SIGNAL_PRECEDENT


def test_precedent_scoped_to_descendant_applies_to_ancestor_claim():
    # the REVERSE subsumption direction (_scope_applies matches "either way"):
    # a precedent recorded at a NARROWER heading path ("Reco > HCAL") also
    # governs a claim scoped to its ANCESTOR ("Reco") — the coarser claim
    # covers the precedent's subtree, so exempting it would let it evade the
    # operator's instruction inside that subtree.
    claims = [sig("a", source_class="dpg", scope="Reco"),
              sig("b", source_class="user", scope="Reco", source_authority=5)]
    r = resolve_bucket(claims, [Precedent(prefer_source="dpg", scope="Reco > HCAL")])
    assert r.canonical_claim_id == "a" and r.basis == SIGNAL_PRECEDENT


def test_competing_precedents_last_writer_wins():
    claims = [sig("a", source_class="dpg"), sig("b", source_class="ops")]
    precedents = [
        Precedent(prefer_source="dpg", sequence=1),
        Precedent(prefer_source="ops", sequence=2),  # later -> wins
    ]
    r = resolve_bucket(claims, precedents)
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_PRECEDENT


def test_precedent_for_absent_source_has_no_effect():
    claims = [sig("a", source_authority=1), sig("b", source_authority=2)]
    r = resolve_bucket(claims, [Precedent(prefer_source="nobody")])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY


# --------------------------------------------------------------------------- #
# Determinism + ranking                                                       #
# --------------------------------------------------------------------------- #
def test_deterministic_under_shuffled_input():
    claims = [
        sig("a", source_authority=3),
        sig("b", source_authority=1),
        sig("c", source_authority=5),
    ]
    r1 = resolve_bucket(claims)
    r2 = resolve_bucket(list(reversed(claims)))
    assert r1.canonical_claim_id == r2.canonical_claim_id == "c"
    assert r1.ranked_claim_ids == r2.ranked_claim_ids == ("c", "a", "b")
