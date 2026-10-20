"""Witnesses for the SECOND repair generator (``claim_model/selection.py``).

PACT ``concord-second-repair-generator``:
``req.repairgen.second-declared-generator`` (a second declared generator
proposes a materially different repair) and
``req.repairgen.selection-stays-deterministic`` (the enlarged candidate set does
not change the character of selection). Pure, DB-free; the published-generation
half lives in ``tests/substrate/consistency/test_repair_selection.py``.

The two failure modes these witnesses exist to prevent:

1. **A second generator that is a re-spelling of the first.** Retraction deletes
   the losing claim; override rewrites its value. If they always landed the same
   residual state there would be nothing to choose between, so
   ``test_retraction_and_override_leave_different_residual_state`` APPLIES both
   (through the real ``merge_effective_claims``) and compares.
2. **Fabricating a value to fill a conflict.** Where the evidence names no
   surviving value the generator must propose NOTHING. All three refusals —
   contested, value-less, typed surrogate — are witnessed.
"""
from __future__ import annotations

import json
import random

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    EMPTY_REPAIR_GENERATOR_ID,
    OVERRIDE_SURVIVING_VALUE_GENERATOR,
    OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
    PRECEDENCE_RETRACTION_GENERATOR,
    PRECEDENCE_RETRACTION_GENERATOR_ID,
    ClaimSignals,
    ConflictBucket,
    Correction,
    RepairCorrection,
    RepairGenerator,
    RepairSelectionError,
    enumerate_candidates,
    merge_effective_claims,
    select_repairs,
)
from okg.substrate.consistency.claim_model.views import ClaimView

_PRED = "preferred_target"


def _sig(cid: str, **kw) -> ClaimSignals:
    return ClaimSignals(claim_id=cid, **kw)


def _view(cid: str, value: str) -> ClaimView:
    return ClaimView(
        node_id=cid, source_id=f"page:{cid}", predicate=_PRED,
        normalized_key="k1", value=value, scope="A > B", scope_state="known",
    )


# k1: a (authority 10, value X) decisively beats b (authority 1, value Y).
def _decisive_valued() -> ConflictBucket:
    return ConflictBucket("k1", (
        _sig("claim:a", source_authority=10, value="X"),
        _sig("claim:b", source_authority=1, value="Y"),
    ))


def _contested() -> ConflictBucket:
    """No decisive winner: c and d tie on every signal."""
    return ConflictBucket("k2", (
        _sig("claim:c", value="P"),
        _sig("claim:d", value="Q"),
    ))


def _valueless() -> ConflictBucket:
    """Decisive, but NO claim in the bucket supplies a value to copy."""
    return ConflictBucket("k3", (
        _sig("claim:e", source_authority=10),
        _sig("claim:f", source_authority=1),
    ))


def _typed_surrogate() -> ConflictBucket:
    """Decisive, but ``value`` is the typed surrogate node_id (the derivation's
    ``value=view.node_id if view.semantics is not None`` peer-grouping key), not
    a value any source asserted."""
    return ConflictBucket("k4", (
        _sig("claim:g", source_authority=10, value="claim:g",
             value_is_surrogate=True),
        _sig("claim:h", source_authority=1, value="claim:h",
             value_is_surrogate=True),
    ))


def _all_buckets() -> list[ConflictBucket]:
    return [_decisive_valued(), _contested(), _valueless(), _typed_surrogate()]


def _by_generator(candidates) -> dict[str, list]:
    out: dict[str, list] = {}
    for c in candidates:
        out.setdefault(c.generator_id, []).append(c)
    return out


# --------------------------------------------------------------------------- #
# req.repairgen.second-declared-generator — a second DECLARED generator        #
# --------------------------------------------------------------------------- #
def test_override_generator_is_declared_and_round_trips():
    g = OVERRIDE_SURVIVING_VALUE_GENERATOR
    assert RepairGenerator.from_json_dict(g.to_json_dict()) == g
    d = g.to_json_dict()
    assert d["generator_id"] == OVERRIDE_SURVIVING_VALUE_GENERATOR_ID
    assert d["version"] == "v1"
    # it declares OVERRIDE and nothing else — a different output form from the
    # retraction generator's, so the two cannot be the same repair.
    assert d["output_form"] == [ACTION_OVERRIDE]
    assert PRECEDENCE_RETRACTION_GENERATOR.output_form == frozenset({ACTION_RETRACT})
    assert g.output_form != PRECEDENCE_RETRACTION_GENERATOR.output_form


def test_candidate_set_offers_more_than_two_repairs_on_a_real_conflict():
    """The candidate set for a decisive, valued conflict has THREE members: do
    nothing, retract the loser, or rewrite the loser to the winner's value."""
    cset = enumerate_candidates(_all_buckets())
    for_k1 = [c for c in cset.candidates
              if c.bucket_key in ("k1", "")]
    got = sorted(
        (c.generator_id,
         tuple((rc.action, rc.claim_id, rc.source_claim_id) for rc in c.corrections))
        for c in for_k1
    )
    assert got == [
        (EMPTY_REPAIR_GENERATOR_ID, ()),
        (OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
         (("override", "claim:b", "claim:a"),)),
        (PRECEDENCE_RETRACTION_GENERATOR_ID, (("retract", "claim:b", None),)),
    ]
    assert len(got) == 3, got


def test_retraction_and_override_leave_different_residual_state():
    """The two repairs are materially different, shown by APPLYING each.

    Retraction removes the losing claim from the effective set; override keeps
    it and rewrites its value. Residual state differs in membership AND in
    content — that difference is the whole point of having two options.
    """
    derived = [_view("claim:a", "X"), _view("claim:b", "Y")]
    cands = _by_generator(enumerate_candidates([_decisive_valued()]).candidates)
    retraction = cands[PRECEDENCE_RETRACTION_GENERATOR_ID][0]
    override = cands[OVERRIDE_SURVIVING_VALUE_GENERATOR_ID][0]
    values = {v.node_id: v.value for v in derived}

    def _apply(candidate):
        """The repair corrections as source corrections. An override reads its
        value off the claim its ``source_claim_id`` names — nothing here (and
        nothing in the generator) can supply a value of its own."""
        corrections = []
        for rc in candidate.corrections:
            if rc.action == ACTION_RETRACT:
                corrections.append(Correction(claim_id=rc.claim_id,
                                              action=ACTION_RETRACT))
            else:
                corrections.append(Correction(
                    claim_id=rc.claim_id, action=ACTION_OVERRIDE,
                    value=values[rc.source_claim_id],
                ))
        merged = merge_effective_claims(derived, corrections)
        return {v.node_id: v.value for v in merged.effective}

    after_retract = _apply(retraction)
    after_override = _apply(override)
    print(f"\n[residual] after retraction={after_retract}")
    print(f"[residual] after override  ={after_override}")

    # retraction: the loser is GONE — the conflict closes by deleting evidence.
    assert after_retract == {"claim:a": "X"}
    # override: the loser SURVIVES, now asserting the winner's value — the
    # conflict closes by correcting the record instead.
    assert after_override == {"claim:a": "X", "claim:b": "X"}
    # measurably different residual state, on both axes.
    assert after_retract != after_override
    assert set(after_retract) != set(after_override)       # membership
    assert after_override["claim:b"] == after_retract["claim:a"]  # content
    # ...and both genuinely close the conflict: one value survives in each.
    assert len(set(after_retract.values())) == 1
    assert len(set(after_override.values())) == 1


def test_override_refuses_when_the_evidence_names_no_value():
    """Where no surviving value is supported the generator proposes NOTHING.

    Three refusals, each a way the evidence names no replacement — and in each
    the RETRACTION generator still proposes, so the refusal is the override
    generator's judgement about evidence, not the bucket being unrepairable.
    """
    proposed = OVERRIDE_SURVIVING_VALUE_GENERATOR.propose(_all_buckets())
    assert [c.bucket_key for c in proposed] == ["k1"], [c.bucket_key for c in proposed]

    retractions = {
        c.bucket_key for c in PRECEDENCE_RETRACTION_GENERATOR.propose(_all_buckets())
    }
    # contested (no decisive winner) — retraction ALSO abstains here.
    assert "k2" not in retractions
    # value-less and typed-surrogate: retraction proposes, override does not.
    assert {"k3", "k4"} <= retractions
    for bucket in (_contested(), _valueless(), _typed_surrogate()):
        assert OVERRIDE_SURVIVING_VALUE_GENERATOR.propose([bucket]) == (), bucket.bucket_key


def test_override_never_authors_a_value_it_can_only_reference():
    """A correction cannot carry a literal value at all: an override names the
    SURVIVING claim to copy from. Fabrication is not expressible."""
    override = _by_generator(
        enumerate_candidates([_decisive_valued()]).candidates
    )[OVERRIDE_SURVIVING_VALUE_GENERATOR_ID][0]
    assert not hasattr(override.corrections[0], "value")
    # the source is a claim IN the bucket that SURVIVES the repair (the winner),
    # never the claim being rewritten.
    src = override.corrections[0].source_claim_id
    assert src == "claim:a"
    assert src != override.corrections[0].claim_id
    assert src in {s.claim_id for s in _decisive_valued().signals}

    # an override with no source, or one sourcing from itself, fails loud.
    with pytest.raises(RepairSelectionError, match="names no source_claim_id"):
        RepairCorrection(action=ACTION_OVERRIDE, claim_id="claim:b")
    with pytest.raises(RepairSelectionError, match="from itself"):
        RepairCorrection(action=ACTION_OVERRIDE, claim_id="claim:b",
                         source_claim_id="claim:b")
    # ...and a retraction may not smuggle one in.
    with pytest.raises(RepairSelectionError, match="carries a source_claim_id"):
        RepairCorrection(action=ACTION_RETRACT, claim_id="claim:b",
                         source_claim_id="claim:a")


def test_declared_output_form_is_enforced_in_both_directions():
    """A generator may emit ONLY what it declares — forced, both ways."""
    # declares OVERRIDE, implementation emits RETRACTIONS -> refused.
    mis_declared_retractor = RepairGenerator(
        PRECEDENCE_RETRACTION_GENERATOR_ID, "v1", frozenset({ACTION_OVERRIDE}))
    with pytest.raises(RepairSelectionError, match="outside its declared output_form"):
        mis_declared_retractor.propose(_all_buckets())
    # declares RETRACT, implementation emits OVERRIDES -> refused.
    mis_declared_overrider = RepairGenerator(
        OVERRIDE_SURVIVING_VALUE_GENERATOR_ID, "v1", frozenset({ACTION_RETRACT}))
    with pytest.raises(RepairSelectionError, match="outside its declared output_form"):
        mis_declared_overrider.propose(_all_buckets())


# --------------------------------------------------------------------------- #
# req.repairgen.selection-stays-deterministic                                  #
# --------------------------------------------------------------------------- #
def test_selection_names_the_producing_generator_over_the_enlarged_set():
    """A reader of the record can tell retraction from override without
    re-deriving: the selected candidate carries the generator identity, and the
    candidate set names a provenance for every option it did not take."""
    sr = select_repairs(_all_buckets())
    by_key = {s.bucket_key: s for s in sr.selected}
    assert by_key["k1"].generator_id == PRECEDENCE_RETRACTION_GENERATOR_ID
    assert by_key["k1"].generator_version == "v1"
    assert by_key["k2"].generator_id == EMPTY_REPAIR_GENERATOR_ID
    # the alternative it did NOT take is recorded, with ITS generator named.
    alternatives = {
        (c.bucket_key, c.generator_id) for c in sr.candidate_set.candidates
    }
    assert ("k1", OVERRIDE_SURVIVING_VALUE_GENERATOR_ID) in alternatives
    # every selected repair is a member of the recorded candidate set.
    present = {
        (c.bucket_key or None, c.generator_id,
         tuple((rc.action, rc.claim_id) for rc in c.corrections))
        for c in sr.candidate_set.candidates
    }
    for s in sr.selected:
        key = (s.bucket_key if s.corrections else None, s.generator_id,
               tuple((rc.action, rc.claim_id) for rc in s.corrections))
        assert key in present, key


def test_declaration_order_is_the_tie_break_and_is_ordering_stable():
    """The tie-break is declaration order, not a comparison of the repairs.

    Both generators propose for k1; the earliest-declared one wins. Nothing about
    the candidates' content is consulted, so the rule cannot be perturbed by the
    inputs — only by editing the declaration.
    """
    from okg.substrate.consistency.claim_model.selection import DEFAULT_GENERATORS

    assert [g.generator_id for g in DEFAULT_GENERATORS] == [
        PRECEDENCE_RETRACTION_GENERATOR_ID,
        OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
    ]
    proposers = [
        g.generator_id for g in DEFAULT_GENERATORS
        if g.propose([_decisive_valued()])
    ]
    assert len(proposers) == 2, proposers  # a genuine tie to break
    selected = {
        s.bucket_key: s for s in select_repairs([_decisive_valued()]).selected
    }
    assert selected["k1"].generator_id == proposers[0]


def test_selection_over_the_enlarged_set_is_invariant_under_shuffling():
    buckets = _all_buckets()
    baseline = json.dumps(select_repairs(buckets).to_json_dict(), sort_keys=True)
    # the enlarged set is genuinely present in what is being held stable.
    assert OVERRIDE_SURVIVING_VALUE_GENERATOR_ID in baseline
    rng = random.Random(20260803)
    for _ in range(12):
        shuffled = [
            ConflictBucket(b.bucket_key, tuple(rng.sample(b.signals, len(b.signals))))
            for b in rng.sample(buckets, len(buckets))
        ]
        assert json.dumps(
            select_repairs(shuffled).to_json_dict(), sort_keys=True
        ) == baseline


def test_contested_conflict_still_abstains_and_retains_its_violation():
    """The second generator is not a back door: a bucket the precedence rules
    refused to settle is still unrepaired and still a retained violation."""
    sr = select_repairs(_all_buckets())
    k2 = {s.bucket_key: s for s in sr.selected}["k2"]
    assert k2.generator_id == EMPTY_REPAIR_GENERATOR_ID
    assert k2.corrections == ()
    # no generator proposed anything at all for the contested bucket.
    assert not [c for c in sr.candidate_set.candidates if c.bucket_key == "k2"]
    # the violation is retained.
    assert [u.bucket_key for u in sr.unresolved] == ["k2"]
    assert sr.unresolved[0].reason == "contested"
    # and nothing from that bucket is distrusted or asserted.
    assert "claim:c" not in sr.distrusted_claim_ids
    assert "claim:d" not in sr.distrusted_claim_ids
    assert sr.asserted_claim_ids == ()


def test_a_generator_that_repairs_a_contested_bucket_is_refused():
    """The back-door guard, FORCED: abstention is not a generator's to overrule.

    Both shipped generators abstain on a contested bucket structurally (a
    contested ``resolve_bucket`` reports no superseded claims, so there is
    nothing to retract or rewrite). This forces the case anyway with a generator
    that DOES propose there, and selection refuses it rather than letting a
    second generator settle what the precedence rules would not.
    """
    from okg.substrate.consistency.claim_model import selection as sel

    def _settles_contested(generator, buckets, precedents):
        return tuple(
            sel.Candidate(
                bucket_key=b.bucket_key, generator_id=generator.generator_id,
                generator_version=generator.version,
                corrections=(RepairCorrection(action=ACTION_RETRACT,
                                              claim_id=b.signals[-1].claim_id),),
            )
            for b in buckets
        )

    rogue = RepairGenerator("rogue-settler", "v1", frozenset({ACTION_RETRACT}))
    sel._PROPOSE_IMPLS["rogue-settler"] = _settles_contested
    original = sel.DEFAULT_GENERATORS
    sel.DEFAULT_GENERATORS = (rogue,)
    try:
        with pytest.raises(RepairSelectionError,
                           match="abstention is not a generator's to overrule"):
            sel.select_repairs([_contested()])
    finally:
        sel.DEFAULT_GENERATORS = original
        del sel._PROPOSE_IMPLS["rogue-settler"]


def test_a_generator_may_not_propose_twice_for_one_bucket():
    """Declaration order can only separate generators, so two candidates from
    ONE generator for one bucket is refused rather than resolved by guess."""
    from okg.substrate.consistency.claim_model import selection as sel

    def _double(generator, buckets, precedents):
        c = sel.Candidate(
            bucket_key="k1", generator_id=generator.generator_id,
            generator_version=generator.version,
            corrections=(RepairCorrection(action=ACTION_RETRACT,
                                          claim_id="claim:b"),),
        )
        return (c, c._replace() if hasattr(c, "_replace") else
                sel.Candidate(bucket_key="k1",
                              generator_id=generator.generator_id,
                              generator_version=generator.version,
                              corrections=()))

    rogue = RepairGenerator("rogue-double", "v1", frozenset({ACTION_RETRACT}))
    sel._PROPOSE_IMPLS["rogue-double"] = _double
    try:
        with pytest.raises(RepairSelectionError, match="more than one candidate"):
            rogue.propose([_decisive_valued()])
    finally:
        del sel._PROPOSE_IMPLS["rogue-double"]
