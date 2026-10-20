"""task.arms.ordering: prove the ORDER, then falsify the proof (no DB).

For each ADJACENT pair of decisive signals the arm builds a DISAGREEMENT bucket:
the lower-priority signal favours one value, the higher favours another. The
engine's ``resolve_bucket`` — the ≻ spec the published RANK op is parity-GATED
against on every publish — must pick the HIGHER signal's value, with the basis
naming it (checked against the SEALED expected winner). Non-vacuity is by
MUTATION: transposing the two adjacent entries in the precedence key (monkeypatch
the engine's ``_key`` / ``_KEY_SIGNAL``, engine source untouched) must flip the
winner, breaking the witness. A suite that only ever presents one signal at a
time could not detect such a transposition — that is the gap this closes.

The published-generation half of the ordering proof lives in test_precedence_emit
(the same disagreement buckets, checked on real bucket_resolution rows).
"""
from __future__ import annotations

from okg.bench.synth.config import ADMITTABLE_SIGNALS
from okg.substrate.consistency.claim_model import precedence
from okg.substrate.consistency.claim_model.precedence import resolve_bucket
from tests.bench.synth._precedence_arm_support import (
    all_signals_config,
    bucket_claims_of,
    build_arm_manifest,
    signals_for_target,
)


def _disagreement_targets():
    manifest = build_arm_manifest(all_signals_config())
    claims = bucket_claims_of(manifest)
    targets = [
        t
        for t in manifest["precedence_arm"]["plan"]["targets"]
        if t["role"].startswith("disagree:")
    ]
    return targets, claims


def test_every_adjacent_decisive_pair_has_a_disagreement_bucket() -> None:
    """One disagreement bucket per adjacent decisive pair (4 pairs across 5
    signals). Non-vacuity: each bucket names two DIFFERENT winning values, so the
    two signals really do disagree."""
    targets, claims = _disagreement_targets()
    pairs = {t["role"].split(":", 1)[1] for t in targets}
    assert pairs == {
        "operator_precedent_over_explicit_supersession",
        "explicit_supersession_over_claim_local_effective_date",
        "claim_local_effective_date_over_declared_source_authority",
        "declared_source_authority_over_dependency_adjusted_support",
    }
    assert len(pairs) == len(ADMITTABLE_SIGNALS) - 1  # adjacent pairs
    for t in targets:
        assert t["expected_winner_value"] != t["lower_would_prefer_value"], (
            f"{t['role']}: the two signals must favour DIFFERENT values"
        )


def test_higher_priority_signal_wins_each_disagreement() -> None:
    """The un-mutated engine: the higher-priority signal wins with its basis,
    matching the SEALED expected winner."""
    targets, claims = _disagreement_targets()
    for t in targets:
        signals, precedents, value_by_id = signals_for_target(t, claims)
        res = resolve_bucket(signals, precedents)
        assert res.outcome == "canonical", f"{t['role']}: expected a settled bucket"
        assert res.basis == t["expected_basis"], (
            f"{t['role']}: basis {res.basis} != sealed {t['expected_basis']}"
        )
        assert value_by_id[res.canonical_claim_id] == t["expected_winner_value"], (
            f"{t['role']}: winner value != sealed expected"
        )


def test_transposing_an_adjacent_pair_breaks_a_witness(monkeypatch, capsys) -> None:
    """MUTATION non-vacuity: for each disagreement, transpose exactly that pair's
    two adjacent entries in the precedence key and show the winner FLIPS to the
    lower-priority signal's preference. At least one ordering witness must break;
    here all four do, and the observed flips are printed for the evidence."""
    targets, claims = _disagreement_targets()
    key_names = list(precedence._KEY_SIGNAL)
    orig_key = precedence._key

    flips = []
    for t in targets:
        hi, lo = t["role"].split(":", 1)[1].split("_over_")
        i, j = key_names.index(hi), key_names.index(lo)
        assert j == i + 1, f"{t['role']}: {hi},{lo} are not adjacent in _KEY_SIGNAL"

        def transposed_key(c, precedents, _i=i, _orig=orig_key):
            tup = _orig(c, precedents)
            return tup[:_i] + (tup[_i + 1], tup[_i]) + tup[_i + 2:]

        transposed_names = tuple(
            key_names[:i] + [key_names[i + 1], key_names[i]] + key_names[i + 2:]
        )
        monkeypatch.setattr(precedence, "_key", transposed_key)
        monkeypatch.setattr(precedence, "_KEY_SIGNAL", transposed_names)
        try:
            signals, precedents, value_by_id = signals_for_target(t, claims)
            res = resolve_bucket(signals, precedents)
            mutated_winner = value_by_id.get(res.canonical_claim_id)
        finally:
            monkeypatch.setattr(precedence, "_key", orig_key)
            monkeypatch.setattr(precedence, "_KEY_SIGNAL", tuple(key_names))

        assert mutated_winner != t["expected_winner_value"], (
            f"{t['role']}: transposition did NOT change the outcome — the order is "
            "not load-bearing for this pair, so the witness is vacuous"
        )
        assert mutated_winner == t["lower_would_prefer_value"], (
            f"{t['role']}: transposition should hand the win to the lower signal's "
            f"preference {t['lower_would_prefer_value']!r}, got {mutated_winner!r}"
        )
        flips.append(
            f"{t['role']}: unmutated winner {t['expected_winner_value']!r} "
            f"-> mutated winner {mutated_winner!r}"
        )

    print("\nORDERING MUTATION — observed failures under transposition:")
    for line in flips:
        print("  " + line)
    assert len(flips) == 4
