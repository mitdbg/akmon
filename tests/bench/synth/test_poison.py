"""synth-authority-poisoning witnesses.

req.poison.arm — the precedence arm gains a poisoning rate r: over the eligible
conflicted buckets (poisonable = a corrupted claim that is not a strict majority),
a PRF-chosen fraction r gets ``declared_source_authority`` stamped on the CORRUPTED
claim, so the engine settles the bucket canonically on the corrupted value — a FLIP.
The scorecard reports the flip rate (flips / poisoned) and keeps poisoned buckets OUT
of every other repair denominator (the unpoisoned mis-repair reading, the
majority-flavor denominators, capstone axis h). Witnesses: the exclusion shown
load-bearing on a constructed overlap; r=0 reads zero flips; a flip on any basis OTHER
than declared_source_authority fails the by-basis witness.

req.poison.curve — three real deterministic-lane runs at r in {0, 0.2, 0.6} (declared
in presets/poison_curve.yaml), same world and seed, r the ONLY config difference
(digest diff witnessed on the canonical dumps). The flip curve is monotone by
construction; a non-monotone reading fails loudly as an instrument defect.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from okg.bench.synth.config import (
    SIGNAL_DECLARED_SOURCE_AUTHORITY,
    ConfigError,
    PrecedenceArm,
    load_config,
    load_preset,
)
from okg.bench.synth.provenance import FLAVOR_COPY, FLAVOR_INDEPENDENT
from okg.bench.synth.resolution import (
    STATE_MIS_REPAIRED,
    STATE_REPAIRED,
    ArmScopeError,
    BucketResolution,
    PoisonCurveError,
    assert_arm_signal_scope,
    assert_monotone_flip_curve,
    corruption_bucket_map,
    per_flavor_mis_repair,
    poison_flip_report,
    poisoned_corruptions_of,
)
from tests.bench.synth._precedence_arm_support import build_arm_manifest

# The three declared curve rates (presets/poison_curve.yaml): eligible pool = 20, so
# round(0.0*20)=0, round(0.2*20)=4, round(0.6*20)=12 poisoned buckets.
CURVE_RATES = (0.0, 0.2, 0.6)
EXPECTED_POISONED = {0.0: 0, 0.2: 4, 0.6: 12}
EXPECTED_ELIGIBLE = 20


def curve_config(rate: float):
    """The poison_curve preset with r set — r is the ONLY thing that varies, so the
    same preset_name/world/seed are shared across the three points."""
    base = load_preset("poison_curve")
    return base.model_copy(
        update={"precedence_arm": base.precedence_arm.model_copy(
            update={"poison_rate": rate})}
    )


@dataclass
class _Claim:
    """A stand-in consistency_claim fact (.node_id + .attrs) for the scope check."""

    node_id: str
    attrs: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# config: the poisoning rate is a bounded [0,1] field                          #
# --------------------------------------------------------------------------- #
def test_poison_rate_defaults_to_zero_and_is_bounded() -> None:
    assert PrecedenceArm().poison_rate == 0.0
    assert PrecedenceArm(poison_rate=0.5).poison_rate == 0.5
    for bad in (1.5, -0.1):
        with pytest.raises(ConfigError):
            load_config({
                "seed": "x", "world_path": "world_smoke",
                "precedence_arm": {"poison_rate": bad},
            })


# --------------------------------------------------------------------------- #
# the sealed plan: eligible pool + PRF-chosen poisoned prefix (nested)          #
# --------------------------------------------------------------------------- #
def test_plan_seals_eligible_pool_and_poisoned_prefix() -> None:
    """The arm plan seals the eligible pool and the poisoned buckets; each poison
    target stamps declared_source_authority on a corrupted claim; and the poisoned
    set NESTS as r grows (low ⊆ high) so the flip curve is monotone by construction."""
    plans = {r: build_arm_manifest(curve_config(r))["precedence_arm"]["plan"]
             for r in CURVE_RATES}
    for r in CURVE_RATES:
        plan = plans[r]
        assert plan["poison_rate"] == r
        assert len(plan["poison_eligible"]) == EXPECTED_ELIGIBLE
        assert len(plan["poisoned_buckets"]) == EXPECTED_POISONED[r]
        poison_targets = [t for t in plan["targets"] if t.get("poison")]
        assert len(poison_targets) == EXPECTED_POISONED[r]
        for t in poison_targets:
            assert t["expected_basis"] == SIGNAL_DECLARED_SOURCE_AUTHORITY
            assert t["expected_outcome"] == "canonical"
            assert t["stamps"], "a poison target must stamp the corrupted claim"
            for attrs in t["stamps"].values():
                assert attrs == {"source_authority": "9"}
            # the flip is a genuine mis-repair: the corrupted value the poison
            # carries differs from the clean value it would settle on absent the stamp
            assert t["expected_winner_value"] != t["lower_would_prefer_value"]
    # nested: r=0.2's poisoned buckets are a strict subset of r=0.6's
    low = set(plans[0.2]["poisoned_buckets"])
    high = set(plans[0.6]["poisoned_buckets"])
    assert low < high, "the poisoned set must nest as r grows (monotone by construction)"


def test_r0_is_the_negative_control() -> None:
    """r=0 poisons nothing: no target, no poisoned bucket, and the offline flip
    report reads a poisoned count and flip rate of zero."""
    manifest = build_arm_manifest(curve_config(0.0))
    plan = manifest["precedence_arm"]["plan"]
    assert plan["poisoned_buckets"] == []
    assert [t for t in plan["targets"] if t.get("poison")] == []
    assert poisoned_corruptions_of(manifest) == frozenset()
    report = poison_flip_report(manifest, (), {"per_corruption": {}, "rows": []})
    assert report["poisoned"] == 0
    assert report["flipped"] == 0
    assert report["flip_rate"] == 0.0
    assert report["enabled"] is False


def test_poisoned_corruptions_derived_from_sealed_plan() -> None:
    """poisoned_corruptions_of joins the sealed poisoned buckets to the sealed
    corruption→bucket map; one corruption per poisoned bucket here."""
    manifest = build_arm_manifest(curve_config(0.6))
    pc = poisoned_corruptions_of(manifest)
    assert len(pc) == EXPECTED_POISONED[0.6]
    cbm = corruption_bucket_map(manifest)
    poisoned_buckets = set(manifest["precedence_arm"]["plan"]["poisoned_buckets"])
    assert {cbm[cid] for cid in pc} == poisoned_buckets


# --------------------------------------------------------------------------- #
# the scope guard admits the wrong-authority stamp on POISONED claims ONLY      #
# --------------------------------------------------------------------------- #
def test_scope_guard_admits_authority_only_on_poisoned_claims() -> None:
    """A poisoned claim may carry source_authority (the declared adversarial axis);
    a source_authority on any NON-poisoned claim of an arm that did not admit the
    signal still fails loudly — the guard is precise by claim id, never a global
    relaxation."""
    poisoned = [_Claim("claim:p", {"value": "1", "source_authority": "9"})]
    # declared on the poisoned claim → silent
    assert_arm_signal_scope(
        poisoned, admits=[], poisoned_claim_ids=frozenset({"claim:p"})
    )
    # the SAME authority attr on a non-poisoned claim still fails, naming the signal
    leaked = [
        _Claim("claim:p", {"value": "1", "source_authority": "9"}),
        _Claim("claim:q", {"value": "2", "source_authority": "9"}),
    ]
    with pytest.raises(ArmScopeError) as exc:
        assert_arm_signal_scope(
            leaked, admits=[], poisoned_claim_ids=frozenset({"claim:p"})
        )
    assert SIGNAL_DECLARED_SOURCE_AUTHORITY in str(exc.value)
    assert "claim:q" in str(exc.value)


# --------------------------------------------------------------------------- #
# the exclusion is LOAD-BEARING on a constructed overlap                        #
# --------------------------------------------------------------------------- #
def test_exclusion_load_bearing_on_constructed_overlap() -> None:
    """A corruption that is BOTH a provenance-flavor preserved majority AND poisoned:
    without the exclusion it is double-counted into the majority-flavor denominator;
    with it, the poisoned bucket drops out. The count change proves the exclusion is
    load-bearing, not decorative."""
    manifest = {
        "provenance": {"flavor_by_corruption": {"c1": "copy", "c2": "copy"}}
    }
    preserved_mis = {"corrupted": 2, "clean": 1, "other": 0}
    stages = {
        "rows": [
            {"corruption_id": "c1", "state": STATE_MIS_REPAIRED,
             "realized_vote": preserved_mis},
            {"corruption_id": "c2", "state": STATE_MIS_REPAIRED,
             "realized_vote": preserved_mis},
        ]
    }
    # WITHOUT exclusion: both the poisoned (c1) and the organic (c2) count
    contaminated = per_flavor_mis_repair(manifest, stages)
    assert contaminated[FLAVOR_COPY] == {"preserved": 2, "mis_repaired": 2}
    # WITH c1 excluded as poisoned: only the organic c2 remains — the axes are orthogonal
    clean = per_flavor_mis_repair(
        manifest, stages, poisoned_corruptions=frozenset({"c1"})
    )
    assert clean[FLAVOR_COPY] == {"preserved": 1, "mis_repaired": 1}
    assert clean[FLAVOR_INDEPENDENT] == {"preserved": 0, "mis_repaired": 0}


# --------------------------------------------------------------------------- #
# by-basis flip attribution: a non-authority flip FAILS the witness            #
# --------------------------------------------------------------------------- #
def _res(bucket: str, basis: str) -> BucketResolution:
    predicate, key = bucket.split(":", 1)
    return BucketResolution(
        predicate=predicate, bucket_key=key, outcome="canonical", basis=basis,
        winner_claim_id="claim:x", ranked_claim_ids=("claim:x",), winner_value="wrong",
    )


def test_by_basis_attribution_authority_passes_other_fails() -> None:
    """A poisoned bucket that flipped on declared_source_authority is attributed to
    the poison; one that flipped on a DIFFERENT basis fails the by-basis witness (the
    flip must be the poison, not a coincidence)."""
    manifest = build_arm_manifest(curve_config(0.2))
    cbm = corruption_bucket_map(manifest)
    poisoned = poisoned_corruptions_of(manifest)
    buckets = sorted(cbm[cid] for cid in poisoned)
    per_corruption = {cid: STATE_MIS_REPAIRED for cid in poisoned}

    # every poisoned bucket flipped on authority → attributed to the poison
    good = poison_flip_report(
        manifest,
        tuple(_res(b, SIGNAL_DECLARED_SOURCE_AUTHORITY) for b in buckets),
        {"per_corruption": per_corruption, "rows": []},
    )
    assert good["flipped"] == len(buckets)
    assert good["flip_rate"] == 1.0
    assert good["all_flips_on_authority"] is True
    assert set(good["by_basis"]) == {SIGNAL_DECLARED_SOURCE_AUTHORITY}

    # one bucket flipped on corroboration instead → the witness must catch it
    off = [_res(buckets[0], "dependency_adjusted_support")] + [
        _res(b, SIGNAL_DECLARED_SOURCE_AUTHORITY) for b in buckets[1:]
    ]
    bad = poison_flip_report(
        manifest, tuple(off), {"per_corruption": per_corruption, "rows": []}
    )
    assert bad["flipped"] == len(buckets)
    assert bad["all_flips_on_authority"] is False, (
        "a poisoned bucket that flipped on a non-authority basis must fail the witness"
    )
    assert "dependency_adjusted_support" in bad["by_basis"]

    # A poisoned bucket the engine did NOT settle wrongly must NOT count as a
    # flip. Without this case the `flipped == len(buckets)` assertions above only
    # restate the per_corruption map the test itself supplied — every poisoned
    # cid was handed STATE_MIS_REPAIRED, and `flipped` is defined as the count of
    # exactly that, so the flip would be DECLARED by the test rather than
    # observed from the report.
    unflipped_cid = sorted(poisoned)[0]
    unflipped_bucket = cbm[unflipped_cid]
    mixed_states = dict(per_corruption)
    mixed_states[unflipped_cid] = STATE_REPAIRED
    mixed = poison_flip_report(
        manifest,
        tuple(_res(b, SIGNAL_DECLARED_SOURCE_AUTHORITY) for b in buckets),
        {"per_corruption": mixed_states, "rows": []},
    )
    assert mixed["flipped"] == len(buckets) - 1, (
        "a poisoned bucket settled on the CLEAN value is not a flip"
    )
    assert mixed["flip_rate"] == (len(buckets) - 1) / len(buckets)
    assert sum(mixed["by_basis"].values()) == mixed["flipped"]
    by_bucket = {row["bucket"]: row for row in mixed["per_bucket"]}
    assert by_bucket[unflipped_bucket]["flipped"] is False
    assert all(
        row["flipped"] for b, row in by_bucket.items() if b != unflipped_bucket
    )


# --------------------------------------------------------------------------- #
# r is the ONLY config difference (compare canonical dumps, not digests)        #
# --------------------------------------------------------------------------- #
def test_only_the_poison_field_differs_across_the_curve() -> None:
    """The three curve configs differ ONLY in precedence_arm.poison_rate: mask that
    one field in the canonical dumps and the rest is byte-identical. The digests DO
    differ (non-vacuity), which is what forces the golden regen."""
    configs = {r: curve_config(r) for r in CURVE_RATES}
    dumps = {r: c.model_dump(mode="json") for r, c in configs.items()}
    masked = {}
    for r, d in dumps.items():
        d = copy.deepcopy(d)
        d["precedence_arm"]["poison_rate"] = None  # mask the ONE varying field
        masked[r] = d
    assert masked[0.0] == masked[0.2] == masked[0.6], (
        "something other than poison_rate differs across the curve configs"
    )
    # the field really carries the declared rates
    assert [dumps[r]["precedence_arm"]["poison_rate"] for r in CURVE_RATES] == list(
        CURVE_RATES
    )
    # non-vacuity: three distinct config digests, so this is a real sweep
    assert len({configs[r].config_digest() for r in CURVE_RATES}) == 3


# --------------------------------------------------------------------------- #
# the monotonicity guard: an instrument defect fails LOUDLY                     #
# --------------------------------------------------------------------------- #
def test_monotone_flip_curve_guard() -> None:
    ok = [(0.0, 0), (0.2, 4), (0.6, 12)]
    assert assert_monotone_flip_curve(list(reversed(ok))) == ok
    with pytest.raises(PoisonCurveError, match="non-monotone"):
        assert_monotone_flip_curve([(0.0, 0), (0.2, 12), (0.6, 4)])


# --------------------------------------------------------------------------- #
# the DB-backed curve: three published points, flips read from run output      #
# --------------------------------------------------------------------------- #
psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.runner import run_synth_benchmark  # noqa: E402
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, compose_catalog(d))
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


@pytest.mark.postgres
def test_poison_curve_three_published_points(clean_runtime: str, tmp_path: Path) -> None:
    """req.poison.curve: three real deterministic-lane runs at r in {0, 0.2, 0.6}
    publish generations; the flip curve is read from run output. r=0 reads zero flips;
    the flip count is monotone in r; every flip is attributed to declared_source_authority;
    and the poisoned flips stay OUT of the unpoisoned repair reading."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    scorecards: dict[float, dict] = {}
    for r in CURVE_RATES:
        result = run_synth_benchmark(
            dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / f"run_{r}",
            config=curve_config(r),
        )
        assert result["clean"]["status"] == "published"
        assert result["corrupted"]["status"] == "published"
        scorecards[r] = result["scorecard"]

    curve = []
    for r in CURVE_RATES:
        block = scorecards[r]["poisoning"]
        assert block["poison_rate"] == r
        assert block["eligible"] == EXPECTED_ELIGIBLE
        assert block["poisoned"] == EXPECTED_POISONED[r]
        curve.append((r, block["flipped"]))

    # the curve is MONOTONE (instrument-defect guard) and reads exactly the seeded points
    assert_monotone_flip_curve(curve)
    assert curve == [(0.0, 0), (0.2, 4), (0.6, 12)], curve

    # r=0 negative control: zero flips, flip rate zero
    r0 = scorecards[0.0]["poisoning"]
    assert r0["flipped"] == 0 and r0["flip_rate"] == 0.0

    for r in (0.2, 0.6):
        block = scorecards[r]["poisoning"]
        # every poisoned bucket flipped (lossless lane): flip rate 1.0 — one mis-repair
        # per wrong-authority stamp
        assert block["flipped"] == EXPECTED_POISONED[r]
        assert block["flip_rate"] == 1.0
        # by-basis: every flip is the poison (declared_source_authority), none coincidental
        assert block["all_flips_on_authority"] is True, block["by_basis"]
        assert set(block["by_basis"]) == {SIGNAL_DECLARED_SOURCE_AUTHORITY}

    # DENOMINATOR HYGIENE, from run output: at r=0.6 the funnel attributes all 12
    # flips to mis_repaired, but the UNPOISONED repair reading excludes them (0), so
    # the flips live ONLY in the poison block — the axes are orthogonal by construction.
    high = scorecards[0.6]
    assert high["funnel"]["states"]["mis_repaired"] == EXPECTED_POISONED[0.6]
    assert high["repair"]["mis_repaired"] == 0, (
        "poisoned flips leaked into the unpoisoned repair tally: "
        f"{high['repair']}"
    )
    assert high["poisoning"]["flipped"] == EXPECTED_POISONED[0.6]
    # The majority-flavor block reads over an EMPTY set here: the poison preset
    # declares no provenance axis, so no site carries a flavor and mis_repaired
    # is 0 for any engine behaviour, correct or broken. So assert the WHOLE cell
    # — the denominator as well as the count — together with the reason the
    # denominator is empty. A silent fallback that flavored unlabelled majority
    # sites would raise `preserved` and fail here, where `mis_repaired == 0`
    # alone would not have noticed.
    assert curve_config(0.6).provenance.enabled is False
    for flavor in (FLAVOR_COPY, FLAVOR_INDEPENDENT):
        assert high["majority_flavor"][flavor] == {"preserved": 0, "mis_repaired": 0}, (
            f"{flavor}: an undeclared provenance axis must leave the "
            f"majority-flavor block empty, not merely zero-valued: "
            f"{high['majority_flavor'][flavor]}"
        )
