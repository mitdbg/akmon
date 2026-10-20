"""Witnessed-falsifiable property suite for the claim-model invariants.

consistency-formal-spec task.spec.formal-witness, at the
strengthen-formal-models bar: every invariant is a NAMED executable property,
and each is demonstrably falsifiable — inverting its semantic in src makes it
fail (.scratch/property_falsifiability.sh runs the mutations).

Each property drives the pure claim_model library with seeded random cases
(deterministic, DB-free, no wall clock). The oracles restate the invariant
from the normative spec (docs/develop/architecture/consistency.md §2/§5/§6.2),
independently of the implementation's internals:

- prop_resolution_deterministic: same bucket, any input order -> identical
  Resolution (spec §5 determinism).
- prop_contested_iff_no_unique_decisive_max: canonical exactly when the
  leader's first separation from the runner-up is one of the five decisive
  signals, per a spec-restated lexicographic oracle (spec §5).
- prop_ranking_only_never_settles: buckets tied on every decisive signal are
  always contested, however recency/maintenance vary (spec §5).
- prop_merge_matches_script_oracle: C_eff equals a spec-restated fold of the
  correction script — per-field last-writer, retract terminality,
  assert-resurrection, expired bookkeeping — and is input-order independent
  (spec §2).
- prop_malformed_corrections_raise: an injected malformed correction always
  raises EffectiveClaimError, never partially applies (spec §2).
- prop_scoped_run_never_certifies_universe: a scope-restricted coverage
  report claiming universe completeness is rejected at the write boundary,
  as are non-exact bases and nonzero gaps (spec §6.2).
"""
from __future__ import annotations

import random

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    ClaimSignals,
    ClaimView,
    Correction,
    EffectiveClaimError,
    Precedent,
    merge_effective_claims,
    resolve_bucket,
)
from okg.substrate.consistency.observations import _validate_coverage

N_CASES = 200
_SEP = " > "


# --------------------------------------------------------------------------- #
# Generators (small domains on purpose: ties and collisions must occur)       #
# --------------------------------------------------------------------------- #
_SCOPES = ["", "Reco", "Reco > HCAL", "Reco > HCAL > Calib", "Ops"]
_DATES = ["", "2020-01-01", "2024-06-01"]
_CLASSES = ["", "dpg", "user", "ops"]


def _rand_signals(rng: random.Random, cid: str) -> ClaimSignals:
    return ClaimSignals(
        claim_id=cid,
        source_class=rng.choice(_CLASSES),
        scope=rng.choice(_SCOPES),
        is_superseded=rng.random() < 0.3,
        effective_date=rng.choice(_DATES),
        source_authority=rng.randint(0, 2),
        independent_support=rng.randint(0, 2),
        document_recency=rng.choice(_DATES),
        maintenance_rank=rng.randint(0, 2),
    )


def _rand_bucket(rng: random.Random) -> tuple[list[ClaimSignals], list[Precedent]]:
    claims = [_rand_signals(rng, f"claim:{i}") for i in range(rng.randint(1, 5))]
    precedents = [
        Precedent(
            prefer_source=rng.choice(_CLASSES[1:]),
            scope=rng.choice(_SCOPES),
            sequence=i,
        )
        for i in range(rng.randint(0, 2))
    ]
    return claims, precedents


# Spec-restated ≻ oracle (docs/develop/architecture/consistency.md §5): the
# signal order, either-direction precedent subsumption, latest-precedent-wins,
# and the five-decisive/two-ranking split — written from the spec text.
def _oracle_scope_applies(precedent_scope: str, claim_scope: str) -> bool:
    if precedent_scope == "":
        return True
    if not claim_scope:
        return False
    return (
        precedent_scope == claim_scope
        or claim_scope.startswith(precedent_scope + _SEP)
        or precedent_scope.startswith(claim_scope + _SEP)
    )


def _oracle_key(c: ClaimSignals, precedents: list[Precedent]) -> tuple:
    applicable = [p for p in precedents if _oracle_scope_applies(p.scope, c.scope)]
    if applicable:
        latest = max(applicable, key=lambda p: p.sequence)
        pref = 1 if c.source_class and c.source_class == latest.prefer_source else 0
    else:
        pref = 0
    return (
        pref,
        0 if c.is_superseded else 1,
        c.effective_date,
        c.source_authority,
        c.independent_support,
        c.document_recency,
        c.maintenance_rank,
    )


_N_DECISIVE = 5  # precedent, supersession, effective date, authority, support


def test_prop_resolution_deterministic():
    rng = random.Random(0xC0FFEE)
    for _ in range(N_CASES):
        claims, precedents = _rand_bucket(rng)
        base = resolve_bucket(claims, precedents)
        for _ in range(3):
            shuffled = claims[:]
            rng.shuffle(shuffled)
            assert resolve_bucket(shuffled, precedents) == base


def test_prop_contested_iff_no_unique_decisive_max():
    rng = random.Random(0xD15C0)
    for _ in range(N_CASES):
        claims, precedents = _rand_bucket(rng)
        r = resolve_bucket(claims, precedents)
        if len(claims) == 1:
            assert r.outcome == OUTCOME_CANONICAL
            continue
        ranked = sorted(
            claims, key=lambda c: (_oracle_key(c, precedents), c.claim_id), reverse=True
        )
        top, runner = _oracle_key(ranked[0], precedents), _oracle_key(ranked[1], precedents)
        first_diff = next((i for i, (t, u) in enumerate(zip(top, runner)) if t != u), None)
        expect_canonical = first_diff is not None and first_diff < _N_DECISIVE
        assert (r.outcome == OUTCOME_CANONICAL) == expect_canonical, (
            claims, precedents, r,
        )
        if expect_canonical:
            assert r.canonical_claim_id == ranked[0].claim_id


def test_prop_ranking_only_never_settles():
    rng = random.Random(0xBEEF)
    for _ in range(N_CASES):
        # every decisive signal identical across the bucket; only the
        # ranking-only signals (recency / maintenance) vary
        shared = _rand_signals(rng, "shared")
        claims = []
        for i in range(rng.randint(2, 5)):
            claims.append(
                ClaimSignals(
                    claim_id=f"claim:{i}",
                    source_class=shared.source_class,
                    scope=shared.scope,
                    is_superseded=shared.is_superseded,
                    effective_date=shared.effective_date,
                    source_authority=shared.source_authority,
                    independent_support=shared.independent_support,
                    document_recency=rng.choice(_DATES),
                    maintenance_rank=rng.randint(0, 5),
                )
            )
        r = resolve_bucket(claims, [])
        assert r.outcome == OUTCOME_CONTESTED


# --------------------------------------------------------------------------- #
# Override-merge: script oracle (spec §2 restated as a fold)                  #
# --------------------------------------------------------------------------- #
def _mk_claim(cid: str, value: str) -> ClaimView:
    return ClaimView(
        node_id=cid, source_id="PageA", predicate="preferred_target",
        normalized_key="reconstruction", value=value, scope="Reco",
        scope_state="known",
    )


def _rand_scenario(rng: random.Random):
    """Per-id legal correction scripts with globally unique sequences.

    Returns (derived, corrections, expected) where expected maps claim_id ->
    (present, value_or_None, provenance_kind) computed by folding the script
    per the spec's §2 rules — the oracle, independent of merge internals.
    """
    derived: list[ClaimView] = []
    corrections: list[Correction] = []
    expected: dict[str, tuple[bool, str | None]] = {}
    seq = iter(range(1, 1000))

    for i in range(rng.randint(1, 4)):
        cid = f"claim:{i}"
        is_derived = rng.random() < 0.7
        v0 = f"v{i}.0"
        if is_derived:
            derived.append(_mk_claim(cid, v0))
        pattern = rng.choice(
            ["none", "overrides", "retract", "retract_assert", "retract_override"]
            if is_derived
            else ["none", "ghost_override", "ghost_retract", "assert"]
        )
        if pattern == "none":
            expected[cid] = (is_derived, v0 if is_derived else None)
        elif pattern == "overrides":
            last = v0
            for k in range(rng.randint(1, 3)):
                last = f"v{i}.{k + 1}"
                corrections.append(
                    Correction(cid, ACTION_OVERRIDE, sequence=next(seq), value=last)
                )
            expected[cid] = (True, last)
        elif pattern == "retract":
            corrections.append(Correction(cid, ACTION_RETRACT, sequence=next(seq)))
            expected[cid] = (False, None)
        elif pattern == "retract_assert":
            corrections.append(Correction(cid, ACTION_RETRACT, sequence=next(seq)))
            corrections.append(
                Correction(cid, ACTION_ASSERT, sequence=next(seq), source_id="op",
                           predicate="preferred_target", normalized_key="reconstruction",
                           value=f"op{i}", scope="Reco", scope_state="known")
            )
            expected[cid] = (True, f"op{i}")
        elif pattern == "retract_override":
            corrections.append(Correction(cid, ACTION_RETRACT, sequence=next(seq)))
            corrections.append(
                Correction(cid, ACTION_OVERRIDE, sequence=next(seq), value="never")
            )
            expected[cid] = (False, None)  # retract terminality: override expired
        elif pattern == "ghost_override":
            corrections.append(
                Correction(cid, ACTION_OVERRIDE, sequence=next(seq), value="never")
            )
            expected[cid] = (False, None)
        elif pattern == "ghost_retract":
            corrections.append(Correction(cid, ACTION_RETRACT, sequence=next(seq)))
            expected[cid] = (False, None)
        else:  # assert (ghost id)
            corrections.append(
                Correction(cid, ACTION_ASSERT, sequence=next(seq), source_id="op",
                           predicate="preferred_target", normalized_key="reconstruction",
                           value=f"op{i}", scope="Reco", scope_state="known")
            )
            expected[cid] = (True, f"op{i}")
    return derived, corrections, expected


def test_prop_merge_matches_script_oracle():
    rng = random.Random(0x5EED)
    for _ in range(N_CASES):
        derived, corrections, expected = _rand_scenario(rng)
        res = merge_effective_claims(derived, corrections)
        got = {c.node_id: c.value for c in res.effective}
        for cid, (present, value) in expected.items():
            if present:
                assert got.get(cid) == value, (derived, corrections, res)
            else:
                assert cid not in got, (derived, corrections, res)
        # input-order independence of the correction list
        shuffled = corrections[:]
        rng.shuffle(shuffled)
        assert merge_effective_claims(derived, shuffled) == res
        # source claims never mutated
        for cv in derived:
            assert cv.value.endswith(".0")


def test_prop_malformed_corrections_raise():
    # NOTE the discovered precedence nuance this generator respects: an
    # override's payload is validated only when the target is PRESENT —
    # override-of-absent expires before the malformed check (pinned by
    # test_claim_model_effective.py::
    # test_override_of_absent_claim_expires_before_malformed_check). So the
    # malformed overrides here target an anchor claim that is always derived
    # and never touched by the scenario script.
    rng = random.Random(0xBAD)
    malformed_makers = [
        lambda s: Correction("claim:anchor", "explode", sequence=s),
        lambda s: Correction("claim:anchor", ACTION_OVERRIDE, sequence=s),  # no fields
        lambda s: Correction("claim:anchor", ACTION_OVERRIDE, sequence=s,
                             scope="Reco"),  # half scope pair
        lambda s: Correction("claim:ghost:x", ACTION_ASSERT, sequence=s,
                             value="v"),  # assert missing coordinates
    ]
    for _ in range(N_CASES):
        derived, corrections, _ = _rand_scenario(rng)
        derived.append(_mk_claim("claim:anchor", "anchor.0"))
        bad = rng.choice(malformed_makers)(999)
        corrections.insert(rng.randint(0, len(corrections)), bad)
        with pytest.raises(EffectiveClaimError):
            merge_effective_claims(derived, corrections)


# --------------------------------------------------------------------------- #
# Coverage certification boundary (spec §6.2)                                 #
# --------------------------------------------------------------------------- #
def _coverage_report(rng: random.Random, **over) -> dict:
    n = rng.randint(1, 50)
    report = {
        "eligible_input_count": n,
        "missing_text_count": 0,
        "missing_embedding_count": 0,
        "block_count": 1,
        "candidate_count": 0,
        "largest_block_size": n,
        "all_pairs_upper_bound": n * (n - 1) // 2,
        "evaluator_invocation_count": n,
        "p95_block_size": n,
        "reduction_ratio": 1,
        "coverage_complete": True,
        "coverage_basis": "exact_postgres",
        "scope_restricted": False,
        "candidate_count_by_block": {},
    }
    report.update(over)
    return report


def test_prop_scoped_run_never_certifies_universe():
    rng = random.Random(0xC0DE)
    for _ in range(N_CASES):
        _validate_coverage(_coverage_report(rng))  # honest universe-exact: fine
        _validate_coverage(
            _coverage_report(rng, coverage_complete=False, scope_restricted=True)
        )  # honest scoped run: fine
        with pytest.raises(ValueError):
            _validate_coverage(
                _coverage_report(rng, scope_restricted=True)
            )  # scoped-complete is unrepresentable
        with pytest.raises(ValueError):
            _validate_coverage(
                _coverage_report(rng, coverage_basis="heuristic")
            )  # complete requires an exact basis
        with pytest.raises(ValueError):
            _validate_coverage(
                _coverage_report(rng, missing_text_count=rng.randint(1, 5))
            )  # complete requires zero gaps
