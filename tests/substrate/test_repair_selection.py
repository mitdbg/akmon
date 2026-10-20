"""Witnesses for the repair-selection layer (``claim_model/selection.py``). Pure, DB-free.

Covers the two ``consistency-repair-objective`` tasks whose scope is the pure
layer: the declared candidate-generator contract (``task.repair.generator-contract``)
and recorded deterministic selection with contested abstention
(``task.repair.selection``). Same style as ``test_claim_model_precedence.py``.

The v1 policy is the precedence full-repair limit (adjudication #9/#10): apply
every decisive-bucket retraction; select the empty repair for every conflict
with no decisive winner and retain its violation as ``contested``. Selection
consults ``resolve_bucket`` and never re-derives, so recency ranks but never
authorizes.
"""
from __future__ import annotations

import json
import random

import pytest

from okg.substrate.consistency.claim_model import (
    ACTION_ASSERT,
    ACTION_RETRACT,
    EMPTY_REPAIR_GENERATOR_ID,
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    PRECEDENCE_RETRACTION_GENERATOR,
    PRECEDENCE_RETRACTION_GENERATOR_ID,
    SELECTION_SCHEMA_VERSION,
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_SOURCE_AUTHORITY,
    SIGNAL_SUPERSESSION,
    ClaimSignals,
    ConflictBucket,
    Precedent,
    RepairGenerator,
    RepairSelectionError,
    enumerate_candidates,
    select_repairs,
)


def sig(cid, **kw):
    return ClaimSignals(claim_id=cid, **kw)


def mixed_fixture() -> list[ConflictBucket]:
    """A bucket of each shape: two decisive (authority, supersession), a
    ranking-only contested (recency), a full tie, and a sole claim."""
    return [
        ConflictBucket("auth", (
            sig("auth-a", source_authority=1),
            sig("auth-b", source_authority=5),  # decisive winner
        )),
        ConflictBucket("supers", (
            sig("sup-x", is_superseded=True),
            sig("sup-y", is_superseded=False),  # decisive winner
        )),
        ConflictBucket("recency", (
            sig("rec-p", document_recency="2020-01-01"),
            sig("rec-q", document_recency="2026-06-01"),  # ranking-only lead
        )),
        ConflictBucket("tie", (
            sig("tie-m", source_authority=2),
            sig("tie-n", source_authority=2),  # fully tied
        )),
        ConflictBucket("sole", (sig("sole-s"),)),  # decisive, nothing to retract
    ]


def _shuffled(seq, rng):
    out = list(seq)
    rng.shuffle(out)
    return out


# --------------------------------------------------------------------------- #
# task.repair.generator-contract — declared, versioned generators             #
# --------------------------------------------------------------------------- #
def test_generator_declaration_round_trips():
    g = PRECEDENCE_RETRACTION_GENERATOR
    assert RepairGenerator.from_json_dict(g.to_json_dict()) == g
    d = g.to_json_dict()
    assert d["generator_id"] == PRECEDENCE_RETRACTION_GENERATOR_ID
    assert d["version"] == "v1"
    assert d["output_form"] == [ACTION_RETRACT]
    # a richer declaration (two actions + the unbounded/run-always markers) also
    # round-trips exactly.
    g2 = RepairGenerator(
        "assert-gen", "v3", frozenset({ACTION_RETRACT, ACTION_ASSERT}),
        unbounded=True, run_always=True,
    )
    assert RepairGenerator.from_json_dict(g2.to_json_dict()) == g2


def test_generator_output_form_rejects_unknown_action():
    with pytest.raises(RepairSelectionError):
        RepairGenerator("bad", "v1", frozenset({"frobnicate"}))


def test_unbounded_generator_only_declarable_run_always():
    # an unbounded generator declared skippable (run_always False) fails loud
    with pytest.raises(RepairSelectionError):
        RepairGenerator("u", "v1", frozenset({ACTION_RETRACT}), unbounded=True)
    # declared run_always -> ok (it can never be skipped)
    g = RepairGenerator("u", "v1", frozenset({ACTION_RETRACT}),
                        unbounded=True, run_always=True)
    assert g.unbounded and g.run_always
    # a bounded generator may be skippable (run_always False) -> ok
    assert RepairGenerator("b", "v1", frozenset({ACTION_RETRACT})).run_always is False


def test_precedence_generator_enumerates_decisive_retractions_plus_empty():
    buckets = mixed_fixture()
    proposed = PRECEDENCE_RETRACTION_GENERATOR.propose(buckets)
    got = {
        (c.bucket_key, tuple((rc.action, rc.claim_id) for rc in c.corrections))
        for c in proposed
    }
    # exactly the two decisive-bucket retractions; contested / tie / sole propose nothing
    assert got == {
        ("auth", (("retract", "auth-a"),)),
        ("supers", (("retract", "sup-x"),)),
    }
    # the candidate set is those retractions PLUS the single always-present empty repair
    cset = enumerate_candidates(buckets)
    assert [(c.bucket_key, c.generator_id) for c in cset.candidates] == [
        ("", EMPTY_REPAIR_GENERATOR_ID),
        ("auth", PRECEDENCE_RETRACTION_GENERATOR_ID),
        ("supers", PRECEDENCE_RETRACTION_GENERATOR_ID),
    ]


def test_candidate_set_recorded_per_evaluation():
    cset = enumerate_candidates(mixed_fixture())
    d = cset.to_json_dict()
    assert d["schema_version"] == SELECTION_SCHEMA_VERSION
    # finite + enumerable: two decisive retractions + one empty repair
    assert len(d["candidates"]) == 3
    # every candidate names a declared generator provenance
    assert {c["generator_id"] for c in d["candidates"]} == {
        EMPTY_REPAIR_GENERATOR_ID, PRECEDENCE_RETRACTION_GENERATOR_ID}


def test_generator_enforces_declared_output_form():
    # a generator whose impl would emit 'retract' but whose declared output form
    # forbids it must fail loud, never silently emit outside the declaration.
    rogue = RepairGenerator(PRECEDENCE_RETRACTION_GENERATOR_ID, "v1", frozenset())
    with pytest.raises(RepairSelectionError):
        rogue.propose(mixed_fixture())


# --------------------------------------------------------------------------- #
# task.repair.selection — recorded deterministic selection with abstention     #
# --------------------------------------------------------------------------- #
def test_selection_record_carries_winner_generator_and_basis():
    by_key = {s.bucket_key: s for s in select_repairs(mixed_fixture()).selected}
    auth = by_key["auth"]
    assert auth.generator_id == PRECEDENCE_RETRACTION_GENERATOR_ID
    assert auth.generator_version == "v1"
    assert auth.basis == SIGNAL_SOURCE_AUTHORITY
    assert auth.outcome == OUTCOME_CANONICAL
    assert tuple((c.action, c.claim_id) for c in auth.corrections) == (("retract", "auth-a"),)
    supers = by_key["supers"]
    assert supers.basis == SIGNAL_SUPERSESSION
    assert tuple((c.action, c.claim_id) for c in supers.corrections) == (("retract", "sup-x"),)


def test_ranking_only_lead_selects_empty_and_is_unresolved_contested():
    sr = select_repairs(mixed_fixture())
    rec = {s.bucket_key: s for s in sr.selected}["recency"]
    # a ranking-only (recency) lead selects the EMPTY repair, not a retraction
    assert rec.generator_id == EMPTY_REPAIR_GENERATOR_ID
    assert rec.corrections == ()
    assert rec.outcome == OUTCOME_CONTESTED
    assert rec.basis == SIGNAL_DOCUMENT_RECENCY
    un = {u.bucket_key: u for u in sr.unresolved}
    # the conflict is retained as unresolved with reason contested (recency never settles)
    assert un["recency"].reason == OUTCOME_CONTESTED
    assert un["recency"].basis == SIGNAL_DOCUMENT_RECENCY
    # the full tie is likewise retained; only the two contested buckets are unresolved
    assert un["tie"].reason == OUTCOME_CONTESTED and un["tie"].basis == "tie"
    assert set(un) == {"recency", "tie"}


def test_distrusted_set_is_exactly_superseded_ids_of_decisive_buckets():
    sr = select_repairs(mixed_fixture())
    # distrusted = the retraction targets = the losers of the two decisive buckets
    assert sr.distrusted_claim_ids == ("auth-a", "sup-x")


def test_asserted_set_is_empty_in_v1():
    assert select_repairs(mixed_fixture()).asserted_claim_ids == ()


def test_selection_selects_from_the_candidate_set():
    sr = select_repairs(mixed_fixture())
    present = {
        (c.generator_id, tuple((rc.action, rc.claim_id) for rc in c.corrections))
        for c in sr.candidate_set.candidates
    }
    for s in sr.selected:
        key = (s.generator_id, tuple((rc.action, rc.claim_id) for rc in s.corrections))
        assert key in present


def test_selection_invariant_under_input_shuffling():
    buckets = mixed_fixture() + [
        ConflictBucket("prec", (
            sig("prec-a", source_class="dpg", source_authority=1),
            sig("prec-b", source_class="user", source_authority=5),
        )),
    ]
    precedents = [
        Precedent(prefer_source="user", sequence=1),
        Precedent(prefer_source="dpg", sequence=2),  # latest by sequence -> prefers dpg
    ]
    baseline = json.dumps(
        select_repairs(buckets, precedents).to_json_dict(), sort_keys=True
    )
    rng = random.Random(20260713)
    for _ in range(10):
        order = _shuffled(buckets, rng)
        shuffled = [
            ConflictBucket(b.bucket_key, tuple(_shuffled(b.signals, rng)))
            for b in order
        ]
        got = json.dumps(
            select_repairs(shuffled, _shuffled(precedents, rng)).to_json_dict(),
            sort_keys=True,
        )
        assert got == baseline


def test_selection_is_bucket_local_under_v1_policy():
    # req.repair.differential-selection's incremental shape as a pure property:
    # selecting over a subset of buckets equals the subset of selecting over all
    # (bucket-local independence — each bucket resolves from its own signals only).
    buckets = mixed_fixture()
    full = select_repairs(buckets)
    full_sel = {s.bucket_key: s for s in full.selected}
    full_un = {u.bucket_key: u for u in full.unresolved}

    subset_keys = {"auth", "recency", "sole"}
    subset = [b for b in buckets if b.bucket_key in subset_keys]
    part = select_repairs(subset)
    part_sel = {s.bucket_key: s for s in part.selected}
    part_un = {u.bucket_key: u for u in part.unresolved}

    for k in subset_keys:
        assert part_sel[k] == full_sel[k]
    assert part_un == {k: full_un[k] for k in part_un}
    assert set(part_un) == {"recency"}
    # distrusted over the subset is exactly the subset's decisive losers
    assert part.distrusted_claim_ids == ("auth-a",)
