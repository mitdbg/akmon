"""Parity tests for the effective-claim override-merge (C_eff = derived ⊕ corrections).

Ported from the cern-twiki reference implementation's test_effective_claims.py
(okg-deployments main) under PACT consistency-claim-model-core
task.core.extract-library: same cases, expressed against the okg core API.
Pure, DB-free, no LLM. Covers the three correction actions, last-writer-wins
ordering, expired vs malformed corrections, determinism, source-preservation,
and the integration contract: the merged C_eff feeds a Σ-parametric conflict
check and an operator correction changes the outcome.
"""
from __future__ import annotations

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    OUTCOME_DEFINITE,
    PROV_ASSERTED,
    PROV_CORRECTED,
    PROV_DERIVED,
    ClaimView,
    ConflictRules,
    Correction,
    EffectiveClaimError,
    derive_conflict_findings,
    merge_effective_claims,
)

PREDICATE_PREFERRED_TARGET = "preferred_target"
PREDICATE_LIFECYCLE_STATUS = "lifecycle_status"

# A minimal fixture Σ: preferred_target is single-valued (distinct values conflict).
RULES = ConflictRules(
    conflict_predicates=frozenset({PREDICATE_PREFERRED_TARGET}),
    incompatible=lambda predicate, va, vb: va != vb,
    constraint_for=lambda predicate: f"{predicate}_single_valued",
)


def claim(node_id, *, source="PageA", predicate=PREDICATE_PREFERRED_TARGET,
          key="reconstruction", value="CMSSW_14_0", scope="Reco", scope_state="known"):
    return ClaimView(
        node_id=node_id, source_id=source, predicate=predicate, normalized_key=key,
        value=value, scope=scope, scope_state=scope_state,
    )


# --------------------------------------------------------------------------- #
# Baseline / no-op                                                            #
# --------------------------------------------------------------------------- #
def test_no_corrections_is_identity_sorted():
    c1, c2 = claim("claim:b"), claim("claim:a")
    res = merge_effective_claims([c1, c2], [])
    assert [c.node_id for c in res.effective] == ["claim:a", "claim:b"]
    assert res.provenance == {"claim:a": PROV_DERIVED, "claim:b": PROV_DERIVED}
    assert res.retracted == res.corrected == res.asserted == res.expired == ()


# --------------------------------------------------------------------------- #
# RETRACT                                                                     #
# --------------------------------------------------------------------------- #
def test_retract_removes_claim_by_id():
    c1, c2 = claim("claim:a"), claim("claim:b")
    res = merge_effective_claims([c1, c2], [Correction("claim:a", ACTION_RETRACT)])
    assert [c.node_id for c in res.effective] == ["claim:b"]
    assert res.retracted == ("claim:a",)
    assert "claim:a" not in res.provenance


# --------------------------------------------------------------------------- #
# OVERRIDE (revalue / rescope / relink)                                       #
# --------------------------------------------------------------------------- #
def test_override_revalue():
    c = claim("claim:a", value="CMSSW_13_2")
    res = merge_effective_claims([c], [Correction("claim:a", ACTION_OVERRIDE, value="CMSSW_14_0")])
    (eff,) = res.effective
    assert eff.value == "CMSSW_14_0"
    assert res.provenance["claim:a"] == PROV_CORRECTED
    assert res.corrected == ("claim:a",)


def test_override_rescope_sets_both_fields():
    c = claim("claim:a", scope="", scope_state="unknown")
    res = merge_effective_claims(
        [c], [Correction("claim:a", ACTION_OVERRIDE, scope="Reco > HCAL", scope_state="known")]
    )
    (eff,) = res.effective
    assert eff.scope == "Reco > HCAL" and eff.scope_state == "known"


def test_override_relink_key():
    c = claim("claim:a", key="reco")
    res = merge_effective_claims(
        [c], [Correction("claim:a", ACTION_OVERRIDE, normalized_key="reconstruction")]
    )
    assert res.effective[0].normalized_key == "reconstruction"


def test_override_absent_claim_is_expired_not_error():
    c = claim("claim:a")
    res = merge_effective_claims([c], [Correction("claim:ghost", ACTION_OVERRIDE, value="X")])
    assert res.expired == ("claim:ghost",)
    assert [x.node_id for x in res.effective] == ["claim:a"]
    assert res.effective[0].value == "CMSSW_14_0"  # untouched


# --------------------------------------------------------------------------- #
# ASSERT                                                                       #
# --------------------------------------------------------------------------- #
def test_assert_adds_operator_authored_claim():
    res = merge_effective_claims(
        [claim("claim:a")],
        [Correction(
            "claim:op:1", ACTION_ASSERT, source_id="operator",
            predicate=PREDICATE_LIFECYCLE_STATUS,
            normalized_key="cmssw_13_2", value="deprecated", scope="Reco", scope_state="known",
        )],
    )
    ids = {c.node_id: c for c in res.effective}
    assert "claim:op:1" in ids
    assert ids["claim:op:1"].value == "deprecated"
    assert res.asserted == ("claim:op:1",)
    assert res.provenance["claim:op:1"] == PROV_ASSERTED


def test_assert_of_present_claim_raises():
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims(
            [claim("claim:a")],
            [Correction("claim:a", ACTION_ASSERT, source_id="p",
                        predicate=PREDICATE_PREFERRED_TARGET,
                        normalized_key="k", value="v")],
        )


# --------------------------------------------------------------------------- #
# Ordering: last-writer-wins; retract/assert interplay                        #
# --------------------------------------------------------------------------- #
def test_last_writer_wins_by_sequence():
    c = claim("claim:a", value="v0")
    corrections = [
        Correction("claim:a", ACTION_OVERRIDE, sequence=2, value="v2"),
        Correction("claim:a", ACTION_OVERRIDE, sequence=1, value="v1"),
    ]
    res = merge_effective_claims([c], corrections)
    assert res.effective[0].value == "v2"


def test_retract_then_later_override_is_expired():
    c = claim("claim:a")
    res = merge_effective_claims([c], [
        Correction("claim:a", ACTION_RETRACT, sequence=1),
        Correction("claim:a", ACTION_OVERRIDE, sequence=2, value="X"),
    ])
    assert res.effective == ()
    assert res.retracted == ("claim:a",)
    assert res.expired == ("claim:a",)


def test_retract_then_assert_resurrects_as_operator_authored():
    c = claim("claim:a", value="derived")
    res = merge_effective_claims([c], [
        Correction("claim:a", ACTION_RETRACT, sequence=1),
        Correction("claim:a", ACTION_ASSERT, sequence=2, source_id="operator",
                   predicate=PREDICATE_PREFERRED_TARGET, normalized_key="reconstruction",
                   value="CMSSW_14_2", scope="Reco", scope_state="known"),
    ])
    (eff,) = res.effective
    assert eff.value == "CMSSW_14_2"
    assert res.provenance["claim:a"] == PROV_ASSERTED
    assert res.retracted == ()  # resurrected, no longer reported retracted


def test_override_of_absent_claim_expires_before_malformed_check():
    # expired-beats-malformed: an OVERRIDE naming an absent claim is
    # recorded as expired even when the override itself is malformed (here:
    # no fields) — the absence check precedes payload validation. Discovered
    # by the property suite (test_claim_model_properties.py) 2026-07-06.
    res = merge_effective_claims(
        [claim("claim:a")], [Correction("claim:ghost", ACTION_OVERRIDE)]
    )
    assert res.expired == ("claim:ghost",)
    assert [c.node_id for c in res.effective] == ["claim:a"]


def test_retract_of_never_existing_claim_is_expired():
    # symmetric with test_override_absent_claim_is_expired_not_error: a
    # retract naming a claim id that never existed is EXPIRED (recorded,
    # never applied) — not an error, and not reported as retracted.
    res = merge_effective_claims(
        [claim("claim:a")], [Correction("claim:ghost", ACTION_RETRACT)]
    )
    assert res.expired == ("claim:ghost",)
    assert res.retracted == ()
    assert [c.node_id for c in res.effective] == ["claim:a"]


def test_equal_sequence_actions_apply_retract_override_assert():
    # at EQUAL sequence for one claim id the action order is deterministic:
    # retract first, then override (now targeting an absent claim -> expired),
    # then assert (resurrects the id as operator-authored) — independent of
    # the input order of the corrections.
    corrections = [
        Correction("claim:a", ACTION_ASSERT, sequence=7, source_id="operator",
                   predicate=PREDICATE_PREFERRED_TARGET, normalized_key="reconstruction",
                   value="CMSSW_15_0", scope="Reco", scope_state="known"),
        Correction("claim:a", ACTION_OVERRIDE, sequence=7, value="never-lands"),
        Correction("claim:a", ACTION_RETRACT, sequence=7),
    ]
    res = merge_effective_claims([claim("claim:a", value="derived")], corrections)
    (eff,) = res.effective
    assert eff.value == "CMSSW_15_0"
    assert res.provenance["claim:a"] == PROV_ASSERTED
    assert res.expired == ("claim:a",)   # the override, applied while retracted
    assert res.retracted == ()           # resurrected by the assert
    res2 = merge_effective_claims(
        [claim("claim:a", value="derived")], list(reversed(corrections))
    )
    assert res2 == res


# --------------------------------------------------------------------------- #
# Malformed corrections fail LOUD                                             #
# --------------------------------------------------------------------------- #
def test_unknown_action_raises():
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims([claim("claim:a")], [Correction("claim:a", "frobnicate")])


def test_override_with_no_fields_raises():
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims([claim("claim:a")], [Correction("claim:a", ACTION_OVERRIDE)])


def test_half_specified_scope_raises():
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims(
            [claim("claim:a")], [Correction("claim:a", ACTION_OVERRIDE, scope="Reco")]
        )


def test_assert_missing_coordinates_raises():
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims([], [Correction("claim:op", ACTION_ASSERT, value="v")])


# --------------------------------------------------------------------------- #
# Determinism + source-preservation                                          #
# --------------------------------------------------------------------------- #
def test_determinism_under_shuffled_input():
    derived = [claim(f"claim:{i}", value=f"v{i}") for i in range(5)]
    corrections = [
        Correction("claim:0", ACTION_RETRACT, sequence=0),
        Correction("claim:1", ACTION_OVERRIDE, sequence=1, value="x"),
        Correction("claim:9", ACTION_ASSERT, sequence=2, source_id="op",
                   predicate=PREDICATE_PREFERRED_TARGET, normalized_key="k", value="z"),
    ]
    a = merge_effective_claims(derived, corrections)
    b = merge_effective_claims(list(reversed(derived)), list(reversed(corrections)))
    assert [c.node_id for c in a.effective] == [c.node_id for c in b.effective]
    assert a.provenance == b.provenance
    assert (a.retracted, a.corrected, a.asserted, a.expired) == \
           (b.retracted, b.corrected, b.asserted, b.expired)


def test_source_claims_not_mutated():
    c = claim("claim:a", value="original")
    merge_effective_claims([c], [Correction("claim:a", ACTION_OVERRIDE, value="changed")])
    assert c.value == "original"  # frozen + replace -> original untouched


# --------------------------------------------------------------------------- #
# Integration: a correction changes the conflict-check outcome                 #
# --------------------------------------------------------------------------- #
def _conflicting_pair():
    a = claim("claim:a", source="PageA", value="CMSSW_14_0")
    b = claim("claim:b", source="PageB", value="CMSSW_13_2")  # distinct target, same scope
    return a, b


def test_retract_resolves_a_definite_conflict():
    a, b = _conflicting_pair()
    before = derive_conflict_findings([a, b], RULES)
    assert any(f.outcome == OUTCOME_DEFINITE for f in before)

    res = merge_effective_claims([a, b], [Correction("claim:b", ACTION_RETRACT)])
    after = derive_conflict_findings(res.effective, RULES)
    assert after == []  # the operator retracted the misread claim -> no conflict


def test_override_aligns_values_and_resolves_conflict():
    a, b = _conflicting_pair()
    assert derive_conflict_findings([a, b], RULES)  # conflict exists
    res = merge_effective_claims(
        [a, b], [Correction("claim:b", ACTION_OVERRIDE, value="CMSSW_14_0")]
    )
    assert derive_conflict_findings(res.effective, RULES) == []  # values now agree
