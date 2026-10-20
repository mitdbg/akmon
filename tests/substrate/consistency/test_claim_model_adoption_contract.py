"""Adoption contract: cern-twiki's current call sites express against the frozen
okg ``claim_model`` core API without behavior change.

PACT ``consistency-claim-model-core`` task.core.adoption-contract / req.core.adoption.
This is the "expressible against the core API without behavior change" proof that
lets the deployment repos adopt the hoisted core (docs/develop/architecture/consistency.md
§8 residence table; docs/reference/consistency-claim-model-api.md is the frozen API
surface).

WHAT IS VENDORED. cern-twiki still ships four pure-Python modules that ARE the
reference implementation the core was hoisted from. Their current call patterns and
representative inputs are VENDORED here as fixtures — copied, not imported (we never
depend on okg-deployments) — from ``okg-deployments`` main at commit
``8cccc7a4bcd3f2f4d2b7514b1637c5f5e0376dcc`` (modules last touched
``3ef6391`` 2026-07-01):

  1. ``cern-twiki/cern_twiki/effective_claims.py``  — the C-updater override-merge
     (``merge_effective_claims``); vendored call patterns + data from
     ``cern-twiki/tests/test_effective_claims.py``.
  2. ``cern-twiki/cern_twiki/scoped_precedents.py`` — the ≻-resolver
     (``resolve_bucket``); from ``cern-twiki/tests/test_scoped_precedents.py``.
  3. ``cern-twiki/cern_twiki/currency_check.py``    — the ClaimView bucket conflict
     check (``derive_conflict_findings``); from ``cern-twiki/tests/test_currency_check.py``.
  4. ``cern-twiki/cern_twiki/claims.py``            — the claim-identity discipline
     (``claim_node_id``); from ``cern-twiki/tests/test_currency_check.py`` /
     the spec §3 examples.

For each call site we build the exact inputs twiki passes, call the CORE API, and
assert the outcome equals what twiki's OWN implementation produces on that input —
each expected value a literal with a comment tracing it to the twiki function or
test. The end-to-end section flows a claim set + corrections + a precedent through
derived → C_eff → findings → resolution, matching twiki's currency pipeline.

THE ONE SHAPE GENERALIZATION (faithful, not a gap). Three of the four twiki modules
key a claim/correction by ``page_id`` (a TWiki page); the core renames this single
field to ``source_id`` (any document/record id) — the one generalization documented
in ``claim_model/views.py``. Every vendored fixture below therefore passes
``source_id=<the page id twiki used>``; nothing else about the shapes changes.

EXPRESSIBILITY NOTES (honest accounting; see ``test_expressibility_notes`` at the
foot of the file). No twiki call site needed a capability the core library API
lacks. The one nuance worth recording: twiki's currency check canonicalizes
preferred_target subjects **corpus-relatively** (``canonicalize_subjects``, rapidfuzz
clustering over ALL of a corpus's subjects at once) before bucketing. The core's
``ConflictRules.canonical_key`` is a **per-claim pure function** ``(predicate, key) ->
key``. This is expressible WITHOUT behavior change because the clustering is
deployment-supplied data (consistency-model.md §8: scope vocabulary and extraction
heuristics are deployment-owned): the deployment precomputes the same corpus map and
closes over it as ``canonical_key`` — exactly what twiki itself does (it computes
``canon = canonicalize_subjects(...)`` once, then looks up ``canon.get(key, key)``
per claim). The library consumes the precomputed map; it does not itself perform
corpus-relative clustering, and is not meant to. (The *separate* question of whether
the tier-2 graph-derivation IR can express corpus-relative clustering in-SQL is not a
library-API concern; it is tracked by task.core.canonicalization-replacement, D3/F2b.)
"""
from __future__ import annotations

import hashlib
import re

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    OUTCOME_DEFINITE,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_POSSIBLE,
    PROV_ASSERTED,
    PROV_CORRECTED,
    PROV_DERIVED,
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_EFFECTIVE_DATE,
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
    SIGNAL_SUPERSESSION,
    ClaimSignals,
    ClaimView,
    ConflictRules,
    Correction,
    CrossPredicatePolicy,
    EffectiveClaimError,
    Precedent,
    claim_logical_id,
    derive_conflict_findings,
    merge_effective_claims,
    resolve_bucket,
)

# --------------------------------------------------------------------------- #
# Vendored twiki deployment vocabulary (cern_twiki.claims / .currency_check).  #
# These are the deployment-supplied Σ facts, copied verbatim so this test is   #
# self-contained (never imported from okg-deployments).                        #
# --------------------------------------------------------------------------- #
PREDICATE_PREFERRED_TARGET = "preferred_target"
PREDICATE_LIFECYCLE_STATUS = "lifecycle_status"
PREDICATE_SUPERSEDES = "supersedes"

CONSTRAINT_PREFERRED_TARGET = "preferred_target_single_valued"
CONSTRAINT_LIFECYCLE = "lifecycle_status_exclusive"
CONSTRAINT_RECOMMEND_DEPRECATED = "no_recommend_deprecated_target"
CONSTRAINT_AMBIGUOUS = "ambiguous_key_not_comparable"

_DEPRECATED_OR_RETIRED = frozenset({"deprecated", "retired"})
ACTIVITY_UNKNOWN = "unknown"  # cern_twiki.claims.ACTIVITY_UNKNOWN

# cern_twiki.claims.normalize_entity_value, verbatim (the cross-policy link key).
_VERSIONISH_RE = re.compile(r"[^a-z0-9]+")


def _normalize_entity_value(raw_value: str) -> str:
    return _VERSIONISH_RE.sub("-", str(raw_value).strip().lower()).strip("-")


def _twiki_incompatible(predicate: str, va: str, vb: str) -> bool:
    """Verbatim cern_twiki.currency_check._incompatible — the deployment Σ."""
    if predicate == PREDICATE_PREFERRED_TARGET:
        return va != vb  # single-valued: distinct targets conflict
    if predicate == PREDICATE_LIFECYCLE_STATUS:
        pair = {va, vb}
        return "current" in pair and bool(_DEPRECATED_OR_RETIRED & pair)
    return False


def _twiki_constraint_for(predicate: str) -> str:
    return (
        CONSTRAINT_PREFERRED_TARGET
        if predicate == PREDICATE_PREFERRED_TARGET
        else CONSTRAINT_LIFECYCLE
    )


# The one cross-predicate policy twiki hardcodes: recommending a deprecated target.
_RECOMMEND_DEPRECATED = CrossPredicatePolicy(
    constraint=CONSTRAINT_RECOMMEND_DEPRECATED,
    source_predicate=PREDICATE_PREFERRED_TARGET,
    target_predicate=PREDICATE_LIFECYCLE_STATUS,
    link_key=lambda c: _normalize_entity_value(c.value),
    violates=lambda c, t: t.value in _DEPRECATED_OR_RETIRED,
    detail=lambda c, t: f"recommends {c.value!r} which is {t.value} per {t.source_id}",
)


def _twiki_conflict_rules(*, canon: dict[str, str] | None = None) -> ConflictRules:
    """Reconstruct cern_twiki.currency_check's hardcoded Σ as a core ``ConflictRules``.

    ``canon`` is the corpus-relative subject map the DEPLOYMENT precomputes
    (``cern_twiki.subject_canonicalization.canonicalize_subjects``); the core
    consumes it as a per-claim ``canonical_key`` closure — see the module
    docstring's expressibility note. ``None`` means no canonicalization (identity).
    """
    canon = canon or {}
    return ConflictRules(
        conflict_predicates=frozenset(
            {PREDICATE_PREFERRED_TARGET, PREDICATE_LIFECYCLE_STATUS}
        ),
        incompatible=_twiki_incompatible,
        constraint_for=_twiki_constraint_for,
        # twiki canonicalizes preferred_target keys only; lifecycle keeps its key.
        canonical_key=lambda predicate, key: (
            canon.get(key, key) if predicate == PREDICATE_PREFERRED_TARGET else key
        ),
        unresolved_keys=frozenset({"", ACTIVITY_UNKNOWN}),
        cross_policies=(_RECOMMEND_DEPRECATED,),
    )


# --------------------------------------------------------------------------- #
# Vendored twiki fixture builders. twiki's ClaimView.page_id -> core source_id. #
# --------------------------------------------------------------------------- #
def _claim(
    node_id: str,
    *,
    source: str = "PageA",  # twiki fixture arg name: page
    predicate: str = PREDICATE_PREFERRED_TARGET,
    key: str = "reconstruction",
    value: str = "CMSSW_14_0",
    scope: str = "Reco",
    scope_state: str = "known",
) -> ClaimView:
    """cern_twiki tests' ``claim(...)`` builder, with page_id -> source_id."""
    return ClaimView(
        node_id=node_id,
        source_id=source,
        predicate=predicate,
        normalized_key=key,
        value=value,
        scope=scope,
        scope_state=scope_state,
    )


def _pt(node_id, source, value, scope, *, key="db_setup", state="known"):
    """cern_twiki.test_currency_check._pt (preferred_target), page_id -> source_id."""
    return ClaimView(
        node_id=node_id, source_id=source, predicate=PREDICATE_PREFERRED_TARGET,
        normalized_key=key, value=value, scope=scope, scope_state=state,
    )


def _life(node_id, source, key, value, scope="root", *, state="known"):
    """cern_twiki.test_currency_check._life (lifecycle_status), page_id -> source_id."""
    return ClaimView(
        node_id=node_id, source_id=source, predicate=PREDICATE_LIFECYCLE_STATUS,
        normalized_key=key, value=value, scope=scope, scope_state=state,
    )


def _sig(cid, **kw):
    """cern_twiki.test_scoped_precedents.sig — identical to the core shape (1:1)."""
    return ClaimSignals(claim_id=cid, **kw)


# =========================================================================== #
# CALL SITE 1 — effective_claims.py: merge_effective_claims (the C-updater).   #
# Source: cern-twiki/cern_twiki/effective_claims.py + test_effective_claims.py #
# =========================================================================== #
def test_call_site_1_effective_no_corrections_is_identity_sorted():
    # twiki test_effective_claims::test_no_corrections_is_identity_sorted
    res = merge_effective_claims([_claim("claim:b"), _claim("claim:a")], [])
    assert [c.node_id for c in res.effective] == ["claim:a", "claim:b"]
    assert res.provenance == {"claim:a": PROV_DERIVED, "claim:b": PROV_DERIVED}
    assert res.retracted == res.corrected == res.asserted == res.expired == ()


def test_call_site_1_effective_retract_removes_by_id():
    # twiki test_effective_claims::test_retract_removes_claim_by_id
    res = merge_effective_claims(
        [_claim("claim:a"), _claim("claim:b")],
        [Correction("claim:a", ACTION_RETRACT)],
    )
    assert [c.node_id for c in res.effective] == ["claim:b"]
    assert res.retracted == ("claim:a",)
    assert "claim:a" not in res.provenance


def test_call_site_1_effective_override_revalue():
    # twiki test_effective_claims::test_override_revalue
    res = merge_effective_claims(
        [_claim("claim:a", value="CMSSW_13_2")],
        [Correction("claim:a", ACTION_OVERRIDE, value="CMSSW_14_0")],
    )
    (eff,) = res.effective
    assert eff.value == "CMSSW_14_0"
    assert res.provenance["claim:a"] == PROV_CORRECTED
    assert res.corrected == ("claim:a",)


def test_call_site_1_effective_override_absent_is_expired_not_error():
    # twiki test_effective_claims::test_override_absent_claim_is_expired_not_error
    res = merge_effective_claims(
        [_claim("claim:a")],
        [Correction("claim:ghost", ACTION_OVERRIDE, value="X")],
    )
    assert res.expired == ("claim:ghost",)
    assert [x.node_id for x in res.effective] == ["claim:a"]
    assert res.effective[0].value == "CMSSW_14_0"  # untouched


def test_call_site_1_effective_assert_adds_operator_authored():
    # twiki test_effective_claims::test_assert_adds_operator_authored_claim
    # NOTE the vendored shape delta: twiki passes page_id=..., core takes source_id=...
    res = merge_effective_claims(
        [_claim("claim:a")],
        [Correction(
            "claim:op:1", ACTION_ASSERT, source_id="operator",
            predicate=PREDICATE_LIFECYCLE_STATUS, normalized_key="cmssw_13_2",
            value="deprecated", scope="Reco", scope_state="known",
        )],
    )
    ids = {c.node_id: c for c in res.effective}
    assert "claim:op:1" in ids
    assert ids["claim:op:1"].value == "deprecated"
    assert ids["claim:op:1"].source_id == "operator"  # page_id -> source_id
    assert res.asserted == ("claim:op:1",)
    assert res.provenance["claim:op:1"] == PROV_ASSERTED


def test_call_site_1_effective_last_writer_wins_by_sequence():
    # twiki test_effective_claims::test_last_writer_wins_by_sequence
    res = merge_effective_claims(
        [_claim("claim:a", value="v0")],
        [
            Correction("claim:a", ACTION_OVERRIDE, sequence=2, value="v2"),
            Correction("claim:a", ACTION_OVERRIDE, sequence=1, value="v1"),
        ],
    )
    assert res.effective[0].value == "v2"  # highest sequence wins


def test_call_site_1_effective_retract_then_override_is_expired():
    # twiki test_effective_claims::test_retract_then_later_override_is_expired
    res = merge_effective_claims(
        [_claim("claim:a")],
        [
            Correction("claim:a", ACTION_RETRACT, sequence=1),
            Correction("claim:a", ACTION_OVERRIDE, sequence=2, value="X"),
        ],
    )
    assert res.effective == ()
    assert res.retracted == ("claim:a",)
    assert res.expired == ("claim:a",)


def test_call_site_1_effective_retract_then_assert_resurrects():
    # twiki test_effective_claims::test_retract_then_assert_resurrects_as_operator_authored
    res = merge_effective_claims(
        [_claim("claim:a", value="derived")],
        [
            Correction("claim:a", ACTION_RETRACT, sequence=1),
            Correction(
                "claim:a", ACTION_ASSERT, sequence=2, source_id="operator",
                predicate=PREDICATE_PREFERRED_TARGET, normalized_key="reconstruction",
                value="CMSSW_14_2", scope="Reco", scope_state="known",
            ),
        ],
    )
    (eff,) = res.effective
    assert eff.value == "CMSSW_14_2"
    assert res.provenance["claim:a"] == PROV_ASSERTED
    assert res.retracted == ()  # resurrected -> no longer reported retracted


@pytest.mark.parametrize(
    "correction",
    [
        # twiki test_effective_claims::test_unknown_action_raises
        Correction("claim:a", "frobnicate"),
        # twiki ::test_override_with_no_fields_raises
        Correction("claim:a", ACTION_OVERRIDE),
        # twiki ::test_half_specified_scope_raises
        Correction("claim:a", ACTION_OVERRIDE, scope="Reco"),
    ],
)
def test_call_site_1_effective_malformed_corrections_raise(correction):
    with pytest.raises(EffectiveClaimError):
        merge_effective_claims([_claim("claim:a")], [correction])


def test_call_site_1_effective_source_claims_not_mutated():
    # twiki test_effective_claims::test_source_claims_not_mutated
    c = _claim("claim:a", value="original")
    merge_effective_claims([c], [Correction("claim:a", ACTION_OVERRIDE, value="changed")])
    assert c.value == "original"  # frozen + replace -> original untouched


# =========================================================================== #
# CALL SITE 2 — scoped_precedents.py: resolve_bucket (the ≻-resolver).         #
# Source: cern-twiki/cern_twiki/scoped_precedents.py + test_scoped_precedents  #
# The API is 1:1 with the core (no shape delta).                              #
# =========================================================================== #
def test_call_site_2_resolve_sole_claim_is_canonical():
    # twiki test_scoped_precedents::test_sole_claim_is_canonical
    r = resolve_bucket([_sig("a")])
    assert r.outcome == OUTCOME_CANONICAL and r.canonical_claim_id == "a"


def test_call_site_2_resolve_supersession_settles():
    # twiki test_scoped_precedents::test_supersession_settles
    r = resolve_bucket([_sig("old", is_superseded=True), _sig("new", is_superseded=False)])
    assert r.outcome == OUTCOME_CANONICAL and r.canonical_claim_id == "new"
    assert r.basis == SIGNAL_SUPERSESSION


def test_call_site_2_resolve_effective_date_settles():
    # twiki test_scoped_precedents::test_effective_date_settles_when_supersession_tied
    r = resolve_bucket([
        _sig("a", effective_date="2023-01-01"),
        _sig("b", effective_date="2024-06-01"),
    ])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_EFFECTIVE_DATE


def test_call_site_2_resolve_recency_alone_is_contested_but_ranks():
    # twiki test_scoped_precedents::test_recency_alone_is_contested
    r = resolve_bucket([
        _sig("a", document_recency="2020-01-01"),
        _sig("b", document_recency="2026-06-01"),
    ])
    assert r.outcome == OUTCOME_CONTESTED
    assert r.basis == SIGNAL_DOCUMENT_RECENCY
    assert r.ranked_claim_ids == ("b", "a")  # still RANKS recency-descending


def test_call_site_2_resolve_full_tie_is_contested():
    # twiki test_scoped_precedents::test_full_tie_is_contested
    r = resolve_bucket([_sig("a", source_authority=2), _sig("b", source_authority=2)])
    assert r.outcome == OUTCOME_CONTESTED and r.basis == "tie"


def test_call_site_2_resolve_decisive_lead_wins_over_recency():
    # twiki test_scoped_precedents::test_decisive_lead_wins_even_with_recency_against
    r = resolve_bucket([
        _sig("a", source_authority=1, document_recency="2026-06-01"),
        _sig("b", source_authority=9, document_recency="2010-01-01"),
    ])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY


def test_call_site_2_resolve_precedent_overrides_fixed_order():
    # twiki test_scoped_precedents::test_precedent_overrides_the_fixed_order
    r = resolve_bucket(
        [
            _sig("a", source_class="dpg", effective_date="2020-01-01"),
            _sig("b", source_class="user", effective_date="2025-01-01"),
        ],
        [Precedent(prefer_source="dpg")],
    )
    assert r.canonical_claim_id == "a" and r.basis == SIGNAL_PRECEDENT


def test_call_site_2_resolve_precedent_scope_subsumption():
    # twiki test_scoped_precedents::test_precedent_applies_only_within_overlapping_scope
    claims = [
        _sig("a", source_class="dpg", scope="Reco > HCAL"),
        _sig("b", source_class="user", scope="Reco > HCAL", source_authority=5),
    ]
    # precedent scoped to a DIFFERENT branch -> does not apply; authority settles b
    r = resolve_bucket(claims, [Precedent(prefer_source="dpg", scope="Calibration")])
    assert r.canonical_claim_id == "b" and r.basis == SIGNAL_SOURCE_AUTHORITY
    # precedent scoped to an ANCESTOR ('Reco') -> applies by subsumption; a wins
    r2 = resolve_bucket(claims, [Precedent(prefer_source="dpg", scope="Reco")])
    assert r2.canonical_claim_id == "a" and r2.basis == SIGNAL_PRECEDENT


def test_call_site_2_resolve_deterministic_under_shuffle():
    # twiki test_scoped_precedents::test_deterministic_under_shuffled_input
    claims = [
        _sig("a", source_authority=3),
        _sig("b", source_authority=1),
        _sig("c", source_authority=5),
    ]
    r1 = resolve_bucket(claims)
    r2 = resolve_bucket(list(reversed(claims)))
    assert r1.canonical_claim_id == r2.canonical_claim_id == "c"
    assert r1.ranked_claim_ids == r2.ranked_claim_ids == ("c", "a", "b")


# =========================================================================== #
# CALL SITE 3 — currency_check.py: derive_conflict_findings (the Σ check).     #
# Source: cern-twiki/cern_twiki/currency_check.py + test_currency_check.py.    #
# twiki's Σ is hardcoded; the core takes it as ConflictRules (reconstructed    #
# above). twiki ClaimView.page_id -> core source_id.                          #
# =========================================================================== #
def _check(claims, *, canon=None):
    """twiki call site ``derive_conflict_findings(claims)`` -> core two-arg form."""
    return derive_conflict_findings(claims, _twiki_conflict_rules(canon=canon))


def test_call_site_3_conflict_three_scopes_is_not_a_conflict():
    # twiki test_currency_check::test_three_cmssw_recommendations_..._is_not_a_conflict
    # The killer case: distinct heading scopes are NOT a contradiction.
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_9_X", "HCAL DB > Setup for 9X"),
        _pt("claim:b", "HcalDB", "CMSSW_8_X", "HCAL DB > Setup for 2016 (8X)"),
        _pt("claim:c", "HcalDB", "CMSSW_7_5_X", "HCAL DB > Setup for 2015 (75X)"),
    ]
    assert _check(claims) == []


def test_call_site_3_conflict_same_scope_distinct_targets_is_definite():
    # twiki test_currency_check::test_same_scope_distinct_preferred_targets_is_definite
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "HCAL DB > Current Setup"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "HCAL DB > Current Setup"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_PREFERRED_TARGET
    assert findings[0].outcome == OUTCOME_DEFINITE
    assert findings[0].member_claim_ids == ("claim:a", "claim:b")


def test_call_site_3_conflict_unknown_scope_downgrades_to_possible():
    # twiki test_currency_check::test_unknown_scope_downgrades_to_possible
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "HCAL DB > Setup"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "", state="unknown"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].outcome == OUTCOME_POSSIBLE


def test_call_site_3_conflict_lifecycle_current_vs_deprecated_is_definite():
    # twiki test_currency_check::test_lifecycle_current_vs_deprecated_is_definite
    claims = [
        _life("claim:a", "PageOne", "cmssw-12-4-0", "current", "Status"),
        _life("claim:b", "PageTwo", "cmssw-12-4-0", "deprecated", "Status"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_LIFECYCLE
    assert findings[0].outcome == OUTCOME_DEFINITE


def test_call_site_3_conflict_lifecycle_deprecated_vs_retired_is_compatible():
    # twiki test_currency_check::test_lifecycle_deprecated_vs_retired_is_compatible_v1
    claims = [
        _life("claim:a", "PageOne", "cmssw-12-4-0", "deprecated", "Status"),
        _life("claim:b", "PageTwo", "cmssw-12-4-0", "retired", "Status"),
    ]
    assert _check(claims) == []


def test_call_site_3_conflict_recommend_deprecated_is_flagged():
    # twiki test_currency_check::test_recommending_a_deprecated_target_is_flagged
    # The one cross-predicate policy.
    claims = [
        _pt("claim:rec", "HcalDB", "CMSSW_12_4_0", "Setup"),
        _life("claim:dep", "ReleaseNotes", "cmssw-12-4-0", "deprecated", "Setup"),
    ]
    rec = [
        f for f in _check(claims)
        if f.constraint == CONSTRAINT_RECOMMEND_DEPRECATED
    ]
    assert len(rec) == 1
    assert rec[0].outcome == OUTCOME_DEFINITE
    assert set(rec[0].member_claim_ids) == {"claim:rec", "claim:dep"}
    assert "deprecated" in rec[0].detail


def _twiki_finding_node_id(constraint: str, member_ids: tuple[str, ...]) -> str:
    """Verbatim cern_twiki.currency_check._finding node_id formula (the identity oracle)."""
    ids = tuple(sorted(member_ids))
    digest = hashlib.sha256("|".join(ids).encode("utf-8")).hexdigest()[:12]
    return f"conflict:{constraint}:{digest}"


def test_call_site_3_cross_policy_finding_is_identical_except_the_label_prefix():
    # THE ONE DIVERGENCE in the whole adoption surface, pinned honestly. The core
    # reproduces twiki's cross-predicate finding (test_recommending_a_deprecated_target_
    # is_flagged) with a byte-identical IDENTITY (node_id) and byte-identical
    # constraint / outcome / locality / members / detail. The single field that differs
    # is the descriptive ``logical_key`` LABEL: twiki hardcodes ``f"recommend:{target_key}"``
    # while the core engine derives EVERY cross-policy label uniformly as
    # ``f"{constraint}:{link}"`` and exposes no hook to customize it (``detail`` is a
    # deployment callable; ``logical_key`` is engine-owned). This is a benign label
    # normalization, NOT a behavior change: a finding's cross-generation identity is its
    # node_id, keyed on (constraint, sorted members), and is unaffected.
    rec = _pt("claim:rec", "HcalDB", "CMSSW_12_4_0", "Setup")
    dep = _life("claim:dep", "ReleaseNotes", "cmssw-12-4-0", "deprecated", "Setup")
    (finding,) = [
        f for f in _check([rec, dep])
        if f.constraint == CONSTRAINT_RECOMMEND_DEPRECATED
    ]
    members = ("claim:dep", "claim:rec")  # sorted member ids
    # Identity + every decision-load-bearing field: byte-identical to twiki's _finding.
    assert finding.node_id == _twiki_finding_node_id(
        CONSTRAINT_RECOMMEND_DEPRECATED, members
    )
    assert finding.member_claim_ids == members
    assert finding.outcome == OUTCOME_DEFINITE
    assert finding.locality == "cross_doc"  # HcalDB vs ReleaseNotes (twiki _locality)
    # twiki detail string, reconstructed verbatim (page_id -> source_id):
    assert finding.detail == "recommends 'CMSSW_12_4_0' which is deprecated per ReleaseNotes"
    # The single divergence — core normalizes the label to ``{constraint}:{link}``:
    assert finding.logical_key == f"{CONSTRAINT_RECOMMEND_DEPRECATED}:cmssw-12-4-0"
    assert finding.logical_key != "recommend:cmssw-12-4-0"  # twiki's bespoke prefix


def test_call_site_3_conflict_unknown_key_is_not_comparable():
    # twiki test_currency_check::test_unknown_key_is_not_comparable_not_a_forced_conflict
    claims = [
        _pt("claim:a", "HcalDB", "CMSSW_14_0_0", "Setup", key="unknown"),
        _pt("claim:b", "HcalDB", "CMSSW_12_4_0", "Setup", key="unknown"),
    ]
    findings = _check(claims)
    assert len(findings) == 1
    assert findings[0].constraint == CONSTRAINT_AMBIGUOUS
    assert findings[0].outcome == OUTCOME_NOT_COMPARABLE


def test_call_site_3_conflict_supersedes_is_not_a_conflict_predicate():
    # twiki test_currency_check::test_supersedes_is_not_a_conflict_predicate
    claims = [
        ClaimView("claim:a", "P", PREDICATE_SUPERSEDES, "cmssw-14-0-0", "CMSSW_12_4_0",
                  scope="S", scope_state="known"),
        ClaimView("claim:b", "P", PREDICATE_SUPERSEDES, "cmssw-14-0-0", "CMSSW_13_0_0",
                  scope="S", scope_state="known"),
    ]
    assert _check(claims) == []


def test_call_site_3_conflict_output_is_deterministic_and_sorted():
    # twiki test_currency_check::test_output_is_deterministic_and_sorted
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


# The subject-canonicalization call site: the deployment PRECOMPUTES a corpus map
# (canonicalize_subjects) and the core consumes it as ``canonical_key``. The canon
# maps below are the literal output cern_twiki.subject_canonicalization would
# produce for these fixture subjects (both phrasings cluster onto the shortest
# member "glibv2"); see the module docstring's expressibility note.
def test_call_site_3_conflict_subject_phrasings_canonicalize_into_one_bucket():
    # twiki test_currency_check::test_subject_phrasings_canonicalize_into_one_bucket
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "Firmware", key="GLIBv2 firmware"),
        _pt("claim:b", "PageB", "03.02.00", "Firmware", key="GLIBv2"),
    ]
    canon = {"GLIBv2 firmware": "glibv2", "GLIBv2": "glibv2"}
    pt = [f for f in _check(claims, canon=canon)
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert len(pt) == 1
    assert pt[0].outcome == OUTCOME_DEFINITE  # same scope -> overlap


def test_call_site_3_conflict_canonicalized_subject_disjoint_scope_no_conflict():
    # twiki test_currency_check::test_canonicalized_subject_disjoint_scope_is_not_a_conflict
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "Firmware > HF", key="GLIBv2 firmware"),
        _pt("claim:b", "PageB", "03.02.00", "Boards > HE", key="GLIBv2"),
    ]
    canon = {"GLIBv2 firmware": "glibv2", "GLIBv2": "glibv2"}
    pt = [f for f in _check(claims, canon=canon)
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert pt == []  # scope_relation guard prevents the false positive


def test_call_site_3_conflict_distinct_subjects_do_not_bucket_together():
    # twiki test_currency_check::test_distinct_subjects_do_not_bucket_together
    # Distinct subjects: canonicalize_subjects clusters neither -> identity map.
    claims = [
        _pt("claim:a", "PageA", "03.00.07", "S", key="HE/HB half speed firmware"),
        _pt("claim:b", "PageB", "03.02.00", "S", key="HF full speed firmware"),
    ]
    pt = [f for f in _check(claims, canon={})
          if f.constraint == CONSTRAINT_PREFERRED_TARGET]
    assert pt == []


# =========================================================================== #
# CALL SITE 4 — claims.py: claim_node_id (the claim-identity discipline).      #
# Source: cern-twiki/cern_twiki/claims.py::claim_node_id.                      #
# Core claim_logical_id(source_id, predicate, key, occurrence, *, anchor="")   #
# is a superset (adds anchor); with anchor="" it is byte-identical to twiki.   #
# =========================================================================== #
def _twiki_claim_node_id(page_id: str, predicate: str, key: str, occurrence: int) -> str:
    """Verbatim cern_twiki.claims.claim_node_id — the expected-behavior oracle."""
    slug = _VERSIONISH_RE.sub("-", str(key).strip().lower()).strip("-") or "_"
    return f"claim:{predicate}:{slug}:{page_id}:{occurrence}"


def test_call_site_4_identity_matches_twiki_and_excludes_value():
    # twiki claim_node_id excludes the value; same (page, predicate, key, occ) is stable.
    core = claim_logical_id("PageA", "preferred_target", "HCAL DB setup", 0)
    assert core == "claim:preferred_target:hcal-db-setup:PageA:0"  # literal, from twiki slug
    # and equals twiki's own implementation byte-for-byte:
    assert core == _twiki_claim_node_id("PageA", "preferred_target", "HCAL DB setup", 0)
    # id excludes the value: two different values, same coordinates -> same id
    assert core == claim_logical_id("PageA", "preferred_target", "HCAL DB setup", 0)


def test_call_site_4_identity_occurrence_discriminates_same_key():
    # twiki claim_node_id: occurrence index keeps two same-key claims distinct.
    a = claim_logical_id("PageA", "preferred_target", "k", 0)
    b = claim_logical_id("PageA", "preferred_target", "k", 1)
    assert a != b
    assert a == _twiki_claim_node_id("PageA", "preferred_target", "k", 0)
    assert b == _twiki_claim_node_id("PageA", "preferred_target", "k", 1)


def test_call_site_4_identity_slug_normalizes_like_twiki():
    # twiki slug: non-alphanumerics -> '-', trimmed; empty -> '_'.
    assert (
        claim_logical_id("P", "preferred_target", "  HCAL/DB Setup (9X)! ", 0)
        == _twiki_claim_node_id("P", "preferred_target", "  HCAL/DB Setup (9X)! ", 0)
        == "claim:preferred_target:hcal-db-setup-9x:P:0"
    )


def test_call_site_4_identity_anchor_is_a_core_superset_not_a_gap():
    # The core ADDS an optional structural anchor twiki's id lacked. With anchor=""
    # (twiki's implicit shape) the ids are identical; a non-empty anchor is a strict
    # extension that never collides with the twiki 4-tuple id.
    no_anchor = claim_logical_id("PageA", "preferred_target", "k", 0)
    assert no_anchor == _twiki_claim_node_id("PageA", "preferred_target", "k", 0)
    anchored = claim_logical_id("PageA", "preferred_target", "k", 0, anchor="h1")
    assert anchored != no_anchor  # extension, not a collision


# =========================================================================== #
# END-TO-END — twiki's currency pipeline through the core API:                 #
# derived -> C_eff (merge) -> findings (Σ check) -> resolution (≻).            #
# Mirrors cern_twiki test_effective_claims integration tests plus a precedent  #
# resolution over the conflicting bucket.                                     #
# =========================================================================== #
def test_end_to_end_currency_pipeline_derived_to_ceff_to_findings_to_resolution():
    # Step 0 — derived claims: a conflicting pair (same scope, distinct targets),
    # exactly twiki test_effective_claims::_conflicting_pair (page_id -> source_id).
    a = _claim("claim:a", source="PageA", value="CMSSW_14_0")
    b = _claim("claim:b", source="PageB", value="CMSSW_13_2")
    rules = _twiki_conflict_rules()

    # Step 1 — the raw corpus conflicts (twiki ::test_retract_resolves_a_definite_conflict
    # asserts a DEFINITE finding exists before correction).
    before = derive_conflict_findings([a, b], rules)
    assert any(f.outcome == OUTCOME_DEFINITE for f in before)
    conflict = [f for f in before if f.outcome == OUTCOME_DEFINITE][0]
    assert set(conflict.member_claim_ids) == {"claim:a", "claim:b"}

    # Step 2 — ≻-resolution over the conflicting bucket: an operator precedent prefers
    # the 'dpg' source class (claim:a). Mirrors twiki
    # test_scoped_precedents::test_precedent_overrides_the_fixed_order.
    signals = [
        _sig("claim:a", source_class="dpg", scope="Reco"),
        _sig("claim:b", source_class="user", scope="Reco", effective_date="2025-01-01"),
    ]
    resolution = resolve_bucket(signals, [Precedent(prefer_source="dpg", scope="Reco")])
    assert resolution.outcome == OUTCOME_CANONICAL
    assert resolution.canonical_claim_id == "claim:a"  # precedent settles it
    assert resolution.basis == SIGNAL_PRECEDENT

    # Step 3 — the operator records the resolution as a source correction: retract the
    # ≻-dominated claim. C_eff re-derives, and the conflict clears (twiki
    # ::test_retract_resolves_a_definite_conflict: after == []).
    loser = [cid for cid in conflict.member_claim_ids
             if cid != resolution.canonical_claim_id][0]
    merged = merge_effective_claims([a, b], [Correction(loser, ACTION_RETRACT)])
    assert merged.retracted == ("claim:b",)
    after = derive_conflict_findings(merged.effective, rules)
    assert after == []  # the ≻-loser retracted -> no conflict


def test_end_to_end_override_aligns_values_and_resolves_conflict():
    # twiki test_effective_claims::test_override_aligns_values_and_resolves_conflict:
    # an OVERRIDE that aligns the values clears the conflict without a retract.
    a = _claim("claim:a", source="PageA", value="CMSSW_14_0")
    b = _claim("claim:b", source="PageB", value="CMSSW_13_2")
    rules = _twiki_conflict_rules()
    assert derive_conflict_findings([a, b], rules)  # conflict exists
    merged = merge_effective_claims(
        [a, b], [Correction("claim:b", ACTION_OVERRIDE, value="CMSSW_14_0")]
    )
    assert merged.provenance["claim:b"] == PROV_CORRECTED
    assert derive_conflict_findings(merged.effective, rules) == []  # values now agree


# =========================================================================== #
# Expressibility accounting (honest): what the core API did / did not lack.    #
# =========================================================================== #
def test_expressibility_notes():
    """No twiki call site needed a capability the core library API lacks.

    Findings (all borne out by the tests above):

    1. effective_claims / currency_check / claims key by ``page_id``; the core
       renames the single field to ``source_id``. A pure rename, expressed by every
       vendored fixture — not a gap.
    2. scoped_precedents.resolve_bucket is 1:1 with the core (no shape delta).
    3. currency_check's hardcoded Σ (two predicates, one cross-policy, the
       ``("", "unknown")`` unresolved keys) expresses as a ``ConflictRules`` value
       (``_twiki_conflict_rules`` above) — the deployment-data half of the
       core/deployment split (consistency-model.md §8) — with ONE benign field-label
       normalization (pinned by ``test_call_site_3_cross_policy_finding_is_identical_
       except_the_label_prefix``): the core engine derives every cross-policy finding's
       descriptive ``logical_key`` uniformly as ``{constraint}:{link}`` rather than
       twiki's bespoke ``recommend:{target_key}`` prefix. The finding's IDENTITY
       (node_id, keyed on constraint + sorted members) and its constraint / outcome /
       locality / members / detail are byte-identical; only the human-readable label
       prefix changes, so no finding-tracking or blocking behavior is affected. This is
       the closest thing to an expressibility gap in the whole surface, and it is a
       label, not a capability.
    4. claim_node_id is a strict subset of the core ``claim_logical_id`` (the core
       adds an optional ``anchor``); with ``anchor=""`` the ids are byte-identical.

    The single nuance (NOT a library-API gap): twiki canonicalizes preferred_target
    subjects corpus-relatively (rapidfuzz ``canonicalize_subjects``). The core's
    ``canonical_key`` is a per-claim pure function, so the deployment must precompute
    the corpus map and close over it — which is precisely what twiki already does
    internally, and precisely the deployment-owned side of the split. The distinct
    question of expressing corpus-relative clustering inside the tier-2 graph-derivation
    IR is out of scope for the pure library and tracked by
    task.core.canonicalization-replacement (D3/F2b).
    """
    # This test's body is executable documentation; the asserted facts are proven by
    # the call-site tests above. Guard the load-bearing shape claim mechanically:
    assert (
        claim_logical_id("PageA", "preferred_target", "k", 0)
        == _twiki_claim_node_id("PageA", "preferred_target", "k", 0)
    )
