"""Parity tests for the Σ-parametric conflict engine + claim identity. Pure, DB-free.

Ported from the cern-twiki reference implementation's test_currency_check.py
(okg-deployments main) under PACT consistency-claim-model-core
task.core.extract-library. The twiki-specific Σ (its three constraints and one
cross-predicate policy) is reconstructed here as a fixture ConflictRules — the
engine mechanics are what these tests pin: bucketing, scope gating (the killer
case: distinct heading scopes are NOT a contradiction), definite/possible
downgrade, not-comparable keys, locality, cross-predicate policies, key
canonicalization injection, determinism.
"""
from __future__ import annotations

from okg.substrate.consistency.claim_model import (
    CONSTRAINT_AMBIGUOUS,
    LOCALITY_CROSS_DOC,
    LOCALITY_INTERNAL,
    OUTCOME_DEFINITE,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_POSSIBLE,
    ClaimView,
    ConflictRules,
    CrossPredicatePolicy,
    claim_key_slug,
    claim_logical_id,
    derive_conflict_findings,
)

PREDICATE_PREFERRED_TARGET = "preferred_target"
PREDICATE_LIFECYCLE_STATUS = "lifecycle_status"
PREDICATE_SUPERSEDES = "supersedes"

CONSTRAINT_PREFERRED_TARGET = "preferred_target_single_valued"
CONSTRAINT_LIFECYCLE = "lifecycle_status_exclusive"
CONSTRAINT_RECOMMEND_DEPRECATED = "no_recommend_deprecated_target"

_DEPRECATED_OR_RETIRED = frozenset({"deprecated", "retired"})
ACTIVITY_UNKNOWN = "unknown"


def _incompatible(predicate: str, va: str, vb: str) -> bool:
    if predicate == PREDICATE_PREFERRED_TARGET:
        return va != vb  # single-valued: distinct targets conflict
    if predicate == PREDICATE_LIFECYCLE_STATUS:
        pair = {va, vb}
        return "current" in pair and bool(_DEPRECATED_OR_RETIRED & pair)
    return False


def _constraint_for(predicate: str) -> str:
    return (
        CONSTRAINT_PREFERRED_TARGET
        if predicate == PREDICATE_PREFERRED_TARGET
        else CONSTRAINT_LIFECYCLE
    )


def _normalize_entity_value(value: str) -> str:
    return claim_key_slug(value)


RECOMMEND_DEPRECATED = CrossPredicatePolicy(
    constraint=CONSTRAINT_RECOMMEND_DEPRECATED,
    source_predicate=PREDICATE_PREFERRED_TARGET,
    target_predicate=PREDICATE_LIFECYCLE_STATUS,
    link_key=lambda c: _normalize_entity_value(c.value),
    violates=lambda c, t: t.value in _DEPRECATED_OR_RETIRED,
    detail=lambda c, t: f"recommends {c.value!r} which is {t.value} per {t.source_id}",
)

RULES = ConflictRules(
    conflict_predicates=frozenset({PREDICATE_PREFERRED_TARGET, PREDICATE_LIFECYCLE_STATUS}),
    incompatible=_incompatible,
    constraint_for=_constraint_for,
    unresolved_keys=frozenset({"", ACTIVITY_UNKNOWN}),
    cross_policies=(RECOMMEND_DEPRECATED,),
)


def _check(claims):
    return derive_conflict_findings(claims, RULES)


def _pt(node_id, source, value, scope, *, key="db_setup", state="known"):
    return ClaimView(
        node_id=node_id, source_id=source, predicate=PREDICATE_PREFERRED_TARGET,
        normalized_key=key, value=value, scope=scope, scope_state=state,
    )


def _life(node_id, source, key, value, scope="root", *, state="known"):
    return ClaimView(
        node_id=node_id, source_id=source, predicate=PREDICATE_LIFECYCLE_STATUS,
        normalized_key=key, value=value, scope=scope, scope_state=state,
    )


# ---------------------------------------------------------------------------
# The killer case: distinct scopes are NOT a contradiction.
# ---------------------------------------------------------------------------
def test_three_recommendations_under_distinct_scopes_is_not_a_conflict():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_9_X", "HCAL DB > Setup for 9X"),
        _pt("claim:b", "HcalDB", "CMSSW_8_X", "HCAL DB > Setup for 2016 (8X)"),
        _pt("claim:c", "HcalDB", "CMSSW_7_5_X", "HCAL DB > Setup for 2015 (75X)"),
    ]
    assert _check(claims) == []


def test_same_scope_distinct_preferred_targets_is_definite():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "HCAL DB > Current Setup"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "HCAL DB > Current Setup"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_PREFERRED_TARGET
    assert findings[0].outcome == OUTCOME_DEFINITE
    assert findings[0].locality == LOCALITY_INTERNAL
    assert findings[0].member_claim_ids == ("claim:a", "claim:b")


def test_unknown_scope_downgrades_to_possible():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "HCAL DB > Setup"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "", state="unknown"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].outcome == OUTCOME_POSSIBLE


def test_subsumed_heading_path_overlaps_and_is_definite():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "HCAL DB"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "HCAL DB > Subsection"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].outcome == OUTCOME_DEFINITE


def test_cross_document_locality():
    claims = [
        _pt("claim:a", "PageOne", "CMSSW_14_0_0", "Setup"),
        _pt("claim:b", "PageTwo", "CMSSW_12_4_0", "Setup"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].locality == LOCALITY_CROSS_DOC


# ---------------------------------------------------------------------------
# lifecycle_status exclusivity (the fixture Σ's second constraint).
# ---------------------------------------------------------------------------
def test_lifecycle_current_vs_deprecated_is_definite():
    claims = [
        _life("claim:a", "PageOne", "cmssw-12-4-0", "current", "Status"),
        _life("claim:b", "PageTwo", "cmssw-12-4-0", "deprecated", "Status"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_LIFECYCLE
    assert findings[0].outcome == OUTCOME_DEFINITE
    assert findings[0].locality == LOCALITY_CROSS_DOC


def test_lifecycle_deprecated_vs_retired_is_compatible():
    claims = [
        _life("claim:a", "PageOne", "cmssw-12-4-0", "deprecated", "Status"),
        _life("claim:b", "PageTwo", "cmssw-12-4-0", "retired", "Status"),
    ]
    assert _check(claims) == []


# ---------------------------------------------------------------------------
# Cross-predicate policy: recommending a deprecated/retired target.
# ---------------------------------------------------------------------------
def test_recommending_a_deprecated_target_is_flagged():
    claims = [
        _pt("claim:rec", "HcalDB", "CMSSW_12_4_0", "Setup"),
        _life("claim:dep", "ReleaseNotes", "cmssw-12-4-0", "deprecated", "Setup"),
    ]
    findings = _check(claims)
    rec = [f for f in findings if f.constraint == CONSTRAINT_RECOMMEND_DEPRECATED]
    assert len(rec) == 1
    assert rec[0].outcome == OUTCOME_DEFINITE
    assert set(rec[0].member_claim_ids) == {"claim:rec", "claim:dep"}
    assert "deprecated" in rec[0].detail


def test_recommending_a_current_target_is_not_flagged():
    claims = [
        _pt("claim:rec", "HcalDB", "CMSSW_14_0_0", "Setup"),
        _life("claim:cur", "ReleaseNotes", "cmssw-14-0-0", "current", "Setup"),
    ]
    assert not [
        f for f in _check(claims)
        if f.constraint == CONSTRAINT_RECOMMEND_DEPRECATED
    ]


# ---------------------------------------------------------------------------
# Not-comparable + ignored predicates.
# ---------------------------------------------------------------------------
def test_unknown_key_is_not_comparable_not_a_forced_conflict():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "Setup", key="unknown"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "Setup", key="unknown"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_AMBIGUOUS
    assert findings[0].outcome == OUTCOME_NOT_COMPARABLE


def test_non_conflict_predicate_is_ignored():
    claims = [
        ClaimView("claim:a", "P", PREDICATE_SUPERSEDES, "cmssw-14-0-0", "CMSSW_12_4_0",
                  scope="S", scope_state="known"),
        ClaimView("claim:b", "P", PREDICATE_SUPERSEDES, "cmssw-14-0-0", "CMSSW_13_0_0",
                  scope="S", scope_state="known"),
    ]
    assert _check(claims) == []


def test_identical_value_same_scope_is_no_conflict():
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "Setup"),
        _pt("claim:b", "HcalDB", "CMSSW_14_0_0", "Setup"),
    ]
    assert _check(claims) == []


# ---------------------------------------------------------------------------
# Determinism + ordering.
# ---------------------------------------------------------------------------
def test_output_is_deterministic_and_sorted():
    claims = [
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "Setup"),
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "Setup"),
        _life("claim:d", "P2", "cmssw-1-0", "deprecated", "Setup"),
        _life("claim:c", "P1", "cmssw-1-0", "current", "Setup"),
    ]
    f1 = _check(claims)
    f2 = _check(list(reversed(claims)))
    assert [f.node_id for f in f1] == [f.node_id for f in f2]
    assert [f.node_id for f in f1] == sorted(f.node_id for f in f1)


# ---------------------------------------------------------------------------
# Key canonicalization injection (subject phrasings bucket together).
# ---------------------------------------------------------------------------
_CANON = {"GLIBv2 firmware": "glibv2", "GLIBv2": "glibv2"}


def _canon_rules():
    return ConflictRules(
        conflict_predicates=frozenset({PREDICATE_PREFERRED_TARGET}),
        incompatible=_incompatible,
        constraint_for=_constraint_for,
        canonical_key=lambda predicate, key: _CANON.get(key, key),
        unresolved_keys=frozenset({"", ACTIVITY_UNKNOWN}),
    )


def test_subject_phrasings_canonicalize_into_one_bucket():
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "Firmware", key="GLIBv2 firmware"),
        _pt("claim:b", "PageB", "03.02.00", "Firmware", key="GLIBv2"),
    ]
    pt = [f for f in derive_conflict_findings(claims, _canon_rules())
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert len(pt) == 1
    assert pt[0].outcome == OUTCOME_DEFINITE  # same scope -> overlap


def test_canonicalized_subject_disjoint_scope_is_not_a_conflict():
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "Firmware > HF", key="GLIBv2 firmware"),
        _pt("claim:b", "PageB", "03.02.00", "Boards > HE", key="GLIBv2"),
    ]
    pt = [f for f in derive_conflict_findings(claims, _canon_rules())
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert pt == []


def test_distinct_subjects_do_not_bucket_together():
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "S", key="HE/HB half speed firmware"),
        _pt("claim:b", "PageB", "03.02.00", "S", key="HF full speed firmware"),
    ]
    pt = [f for f in derive_conflict_findings(claims, _canon_rules())
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert pt == []


# ---------------------------------------------------------------------------
# Claim-identity discipline (v1-currency-spec §3).
# ---------------------------------------------------------------------------
def test_claim_logical_id_excludes_value_and_is_stable():
    a = claim_logical_id("PageA", "preferred_target", "HCAL DB setup", 0)
    b = claim_logical_id("PageA", "preferred_target", "HCAL DB setup", 0)
    assert a == b == "claim:preferred_target:hcal-db-setup:PageA:0"


def test_claim_logical_id_occurrence_discriminates_same_key():
    a = claim_logical_id("PageA", "preferred_target", "k", 0)
    b = claim_logical_id("PageA", "preferred_target", "k", 1)
    assert a != b


def test_claim_logical_id_anchor_discriminates_regions():
    a = claim_logical_id("PageA", "preferred_target", "k", 0, anchor="h1")
    b = claim_logical_id("PageA", "preferred_target", "k", 0, anchor="h2")
    assert a != b
    assert claim_logical_id("PageA", "preferred_target", "k", 0) != a


def test_claim_key_slug_normalizes():
    assert claim_key_slug("  HCAL/DB Setup (9X)! ") == "hcal-db-setup-9x"
    assert claim_key_slug("!!!") == "_"
