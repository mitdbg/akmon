"""synth-cost-weighted-gold witnesses: cost tables in gold, cost-weighted
scoring, qualification, and the artifact-only sensitivity sweep.

The benchmark grades in counts; the paper's objective is money. These witnesses
pin the translation: that the cost tables are SEALED (so a re-score cannot
re-price a recorded run), that the identity weights reproduce today's counts
byte-for-byte, that remaining error cost partitions by funnel stage fail-closed,
that qualification is reported loudly in BOTH directions on real published runs,
and that the 0.1x / 1x / 10x sweep is a re-score of recorded artifacts with no
model call and no live table read.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from okg.bench.synth.config import (
    ACTION_CLASSES,
    CORRUPTION_CLASS_VOCABULARY,
    MANDATORY_COST,
    ActionPrices,
    ConfigError,
    load_config,
    load_preset,
)
from okg.bench.synth.corruption import CLASS_BACKUP_CHECK, Planner
from okg.bench.synth.cost import (
    FUNNEL_STAGE_ORDER,
    RESOLVED_STATES,
    STAGE_DETECTION,
    STAGE_EXTRACTION,
    STAGE_REPAIR,
    STATE_STAGE,
    CostError,
    assert_sensitivity_invariants,
    assert_stage_sums,
    error_cost_by_corruption,
    qualification,
    remaining_error_cost,
    sealed_cost_model,
    spent_action_cost,
)
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus
from okg.bench.synth.resolution import (
    FUNNEL_STATES,
    STATE_NOT_DETECTED,
    STATE_REMOVED_FROM_SCOPE,
    funnel,
    synth_slug,
)
from okg.bench.synth.runner import (
    ENGINE_READS_FILENAME,
    _deserialize_resolutions,
    _deserialize_state,
    _subset_world,
    action_counts,
    build_synth_scorecard,
    render_synth_scorecard,
    synth_survival_equivalent,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS = Path(__file__).parent
_SYNTH = _REPO_ROOT / "src/okg/bench/synth"


# --------------------------------------------------------------------------- #
# helpers: an offline manifest + a lossless extraction                         #
# --------------------------------------------------------------------------- #
def _offline_manifest(preset: str):
    world_dir = _SYNTH / "world_smoke"
    prov = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov
    )
    config = load_preset(preset)
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered), config


def _full_extraction(manifest) -> dict[str, str]:
    patients = manifest["patients"]
    claims: dict[str, str] = {}
    for row in manifest["trace"]:
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        pred = synth_slug(row["predicate_label"])
        claims[f"claim:{row['doc']}:{pred}:{entity}:0"] = row["surface_value"]
    return claims


def _offline_stages(manifest):
    return funnel(
        manifest=manifest,
        extracted_claims=_full_extraction(manifest),
        resolutions=(),
        findings=(),
        equivalent=synth_survival_equivalent,
    )


class _FakeState:
    """The scorer-visible surface of a PipelineState, for the count witnesses."""

    def __init__(self, claims, candidates=(), findings=()):
        self.claims = dict(claims)
        self.candidates = frozenset(frozenset(m) for m in candidates)
        self.findings = tuple(findings)


# --------------------------------------------------------------------------- #
# the closed vocabularies                                                      #
# --------------------------------------------------------------------------- #
def test_cost_class_vocabulary_is_byte_equal_to_the_corruption_classes() -> None:
    """config cannot import corruption (corruption imports config), so the class
    vocabulary is a literal copy — pinned here so the two can never drift and a
    newly added corruption class cannot be silently unpriceable."""
    assert CORRUPTION_CLASS_VOCABULARY == frozenset(CLASS_BACKUP_CHECK)


def test_every_action_class_is_priced_exactly_once() -> None:
    table = ActionPrices().as_table()
    assert set(table) == set(ACTION_CLASSES)
    assert len(ACTION_CLASSES) == len(set(ACTION_CLASSES)) == 5


def test_every_funnel_state_is_resolved_or_charged_to_a_stage() -> None:
    """Totality of the cost attribution over resolution.FUNNEL_STATES: a terminal
    state that is neither resolved nor staged would drop out of the ledger."""
    assert set(FUNNEL_STATES) == set(STATE_STAGE) | RESOLVED_STATES
    assert not (set(STATE_STAGE) & RESOLVED_STATES)
    assert set(STATE_STAGE.values()) <= set(FUNNEL_STAGE_ORDER)


# --------------------------------------------------------------------------- #
# the config schema: typed rejections                                          #
# --------------------------------------------------------------------------- #
def _cost_config(**cost):
    return load_config({"seed": "x", "world_path": "world_smoke", "cost": cost})


def test_mandatory_class_may_not_also_carry_a_finite_price() -> None:
    with pytest.raises(ConfigError, match="cannot cost both infinity"):
        _cost_config(
            error_costs={"entity_swap": 10.0}, mandatory_classes=["entity_swap"]
        )


def test_unknown_class_in_either_cost_table_is_rejected() -> None:
    with pytest.raises(ConfigError, match="unknown corruption class"):
        _cost_config(error_costs={"not_a_class": 1.0})
    with pytest.raises(ConfigError, match="unknown corruption class"):
        _cost_config(mandatory_classes=["not_a_class"])


def test_duplicate_mandatory_class_is_rejected() -> None:
    with pytest.raises(ConfigError, match="duplicate class"):
        _cost_config(mandatory_classes=["entity_swap", "entity_swap"])


def test_unknown_cost_key_is_rejected() -> None:
    """Tier-3 strictness reaches the cost model too: a misspelt knob is loud."""
    with pytest.raises(ConfigError, match="invalid synth config"):
        _cost_config(error_cost=1.0)


def test_negative_price_and_negative_error_cost_are_rejected() -> None:
    with pytest.raises(Exception):
        ActionPrices(check=-1.0)
    with pytest.raises(ConfigError, match="invalid synth config"):
        load_config(
            {
                "seed": "x",
                "world_path": "world_smoke",
                "cost": {"default_error_cost": -1.0},
            }
        )


def test_cost_model_is_part_of_the_config_digest() -> None:
    """A re-priced config is a DIFFERENT config: the digest moves, so two runs
    priced differently can never be silently compared as one."""
    base = load_config({"seed": "x", "world_path": "world_smoke"})
    priced = load_config(
        {
            "seed": "x",
            "world_path": "world_smoke",
            "cost": {"action_prices": {"check": 1.0}},
        }
    )
    assert base.config_digest() != priced.config_digest()


# --------------------------------------------------------------------------- #
# the seal                                                                     #
# --------------------------------------------------------------------------- #
def test_manifest_seals_both_tables_and_stamps_every_corruption() -> None:
    manifest, config = _offline_manifest("cost_weighted")
    sealed = sealed_cost_model(manifest)
    # the FULL closed vocabulary is priced, not only the planted classes
    assert set(sealed["error_costs"]) == CORRUPTION_CLASS_VOCABULARY
    assert sealed["action_prices"] == config.cost.action_prices.as_table()
    assert sealed["error_costs"]["entity_swap"]["error_cost"] == 500.0
    # a class with no explicit entry falls to the declared default, never to a
    # hidden built-in
    assert sealed["error_costs"]["derived_value"]["error_cost"] == 4.0
    # every planted corruption declares its own cost, drawn from that table
    planted = manifest["gold"]["corruptions"]
    assert planted
    for site in planted:
        assert site["mandatory"] is False
        assert (
            site["error_cost"]
            == sealed["error_costs"][site["corruption_class"]]["error_cost"]
        )


def test_mandatory_preset_plants_one_and_the_smoke_preset_plants_none() -> None:
    """Both directions exist on the GOLD side (req.cost.gold-weights)."""
    mandatory, _ = _offline_manifest("cost_mandatory")
    stamped = [s for s in mandatory["gold"]["corruptions"] if s["mandatory"]]
    assert stamped, "cost_mandatory must plant at least one MANDATORY violation"
    assert all(s["error_cost"] == MANDATORY_COST for s in stamped)

    none, _ = _offline_manifest("smoke")
    assert none["gold"]["corruptions"], "smoke must plant corruptions"
    assert not [s for s in none["gold"]["corruptions"] if s["mandatory"]]
    assert sealed_cost_model(none)["mandatory_classes"] == []


def test_infinity_is_a_json_safe_sentinel_and_the_digest_round_trips(tmp_path) -> None:
    """The infinity trap, closed. A bare float('inf') would serialize as the token
    ``Infinity``, which is NOT valid JSON: no conformant parser could re-read the
    manifest, and manifest_digest would hash bytes nothing else accepts. The
    sentinel keeps the sealed bytes strict-JSON, so the digest round-trips."""
    manifest, _ = _offline_manifest("cost_mandatory")
    # strict JSON: allow_nan=False raises on any Infinity/NaN anywhere in the tree
    strict = json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert "Infinity" not in strict and "NaN" not in strict

    # a float('inf') in the same slot would NOT survive that dump — the control
    # that makes the assertion above non-vacuous
    with pytest.raises(ValueError):
        json.dumps({"error_cost": float("inf")}, allow_nan=False)

    # write and re-load through the real manifest path, then recompute the digest
    # from the re-loaded bytes: it must reproduce the sealed one
    from okg.bench.synth.goldseal import load_manifest, write_manifest

    write_manifest(manifest, tmp_path / "benchmark-manifest.json")
    reloaded = load_manifest(tmp_path / "benchmark-manifest.json")
    import hashlib

    body = {k: v for k, v in reloaded.items() if k != "manifest_digest"}
    assert (
        hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        == manifest["manifest_digest"]
    )
    assert sealed_cost_model(reloaded) == sealed_cost_model(manifest)
    # the sentinel is what carries the infinity, and it agrees with the flag
    costs = error_cost_by_corruption(manifest)
    assert any(v is None for v in costs.values())
    for site in manifest["gold"]["corruptions"]:
        assert site["mandatory"] == (site["error_cost"] == MANDATORY_COST)


def test_a_manifest_without_a_sealed_cost_model_is_fail_closed() -> None:
    manifest, _ = _offline_manifest("smoke")
    del manifest["cost_model"]
    with pytest.raises(CostError, match="no sealed cost_model"):
        sealed_cost_model(manifest)


def test_a_gold_corruption_without_an_error_cost_is_fail_closed() -> None:
    manifest, _ = _offline_manifest("smoke")
    manifest["gold"]["corruptions"][0].pop("error_cost")
    with pytest.raises(CostError, match="declares no error_cost"):
        error_cost_by_corruption(manifest)


# --------------------------------------------------------------------------- #
# the IDENTITY: weight 1, price 0 reproduces the counts                        #
# --------------------------------------------------------------------------- #
def test_smoke_declares_the_identity_cost_model() -> None:
    cost = load_preset("smoke").cost
    assert cost.default_error_cost == 1.0
    assert dict(cost.error_costs) == {}
    assert cost.mandatory_classes == ()
    assert set(cost.action_prices.as_table().values()) == {0.0}


def test_identity_weights_reproduce_the_counts_offline() -> None:
    manifest, _ = _offline_manifest("smoke")
    stages = _offline_stages(manifest)
    unresolved = sum(
        n for state, n in stages["states"].items() if state not in RESOLVED_STATES
    )
    assert unresolved > 0, "the identity witness must not read a zero denominator"
    remaining = remaining_error_cost(manifest, stages)
    assert remaining["total"] == float(unresolved)
    assert remaining["corruptions"] == unresolved
    for stage in FUNNEL_STAGE_ORDER:
        cell = remaining["by_stage"][stage]
        # every weight is 1, so each stage's DOLLARS equal its COUNT exactly
        assert cell["cost"] == float(cell["corruptions"])


def test_golden_smoke_identity_is_byte_for_byte_todays_counts() -> None:
    """The sharp acceptance: with every weight 1 and every price 0, the smoke
    scorecard's count metrics are byte-identical to the pre-change golden, and
    the new cost blocks are exactly those counts re-expressed in dollars.

    ``golden_smoke_scorecard.precost.json`` is the committed pre-change golden.
    If the live golden has not been regenerated on a real publish yet, the
    comparison below is vacuous BY DESIGN; regenerating it makes it the proof.
    """
    precost = json.loads((_TESTS / "golden_smoke_scorecard.precost.json").read_text())
    live = json.loads((_TESTS / "golden_smoke_scorecard.json").read_text())

    declared = {
        # the cost model is part of the canonical config dump, so the digest moves
        # and the manifest (which embeds the config AND now seals the tables) with it
        "config_digest",
        "manifest_digest",
        # the four new blocks, all ADDITIVE
        "qualified",
        "qualification",
        "remaining_error_cost",
        "spent_action_cost",
        "cost_model",
    }
    changed = {
        key for key in set(precost) | set(live) if precost.get(key) != live.get(key)
    }
    assert changed <= declared, (
        "a COUNT block moved when only the declared cost fields should have: "
        f"{sorted(changed - declared)}"
    )
    assert declared <= set(live), "the new cost blocks must be present"

    # the identity, read off the golden itself
    assert live["spent_action_cost"]["total"] == 0.0
    for cell in live["spent_action_cost"]["by_class"].values():
        assert cell["unit_price"] == 0.0 and cell["cost"] == 0.0
        assert cell["count"] >= 0
    unresolved = sum(
        n for state, n in live["funnel"]["states"].items()
        if state not in RESOLVED_STATES
    )
    assert live["remaining_error_cost"]["total"] == float(unresolved)
    assert live["remaining_error_cost"]["corruptions"] == unresolved
    assert live["qualified"] is True
    assert live["qualification"]["mandatory_planted"] == 0
    # non-vacuity: the run really did leave corruptions unresolved
    assert unresolved > 0


# --------------------------------------------------------------------------- #
# weights that are NOT the identity                                            #
# --------------------------------------------------------------------------- #
def test_weighted_costs_differentiate_classes() -> None:
    """A weighted total cannot be mistaken for a count: the priced corpus's
    remaining cost is the per-class sum, not the number of corruptions."""
    manifest, _ = _offline_manifest("cost_weighted")
    stages = _offline_stages(manifest)
    remaining = remaining_error_cost(manifest, stages)
    costs = error_cost_by_corruption(manifest)
    expected = math.fsum(
        costs[cid]
        for cid, state in stages["per_corruption"].items()
        if state not in RESOLVED_STATES
    )
    assert remaining["total"] == pytest.approx(expected)
    assert remaining["total"] != float(remaining["corruptions"])
    assert len({c for c in costs.values()}) > 1, "the classes must be priced apart"


def test_scale_reprices_finite_costs_and_leaves_mandatory_infinite() -> None:
    manifest, _ = _offline_manifest("cost_weighted")
    stages = _offline_stages(manifest)
    base = remaining_error_cost(manifest, stages)
    for scale in (0.1, 10.0):
        point = remaining_error_cost(manifest, stages, scale=scale)
        assert point["total"] == pytest.approx(base["total"] * scale)
        assert point["corruptions"] == base["corruptions"]
        assert point["scale"] == scale

    mandatory, _ = _offline_manifest("cost_mandatory")
    m_stages = _offline_stages(mandatory)
    for scale in (0.1, 1.0, 10.0):
        point = remaining_error_cost(mandatory, m_stages, scale=scale)
        # scaling a finite cost can never create or destroy an infinite one
        assert point["mandatory_unresolved"] == m_stages["states"][STATE_NOT_DETECTED]
        assert point["infinite"] is True


# --------------------------------------------------------------------------- #
# fail-closed: the stage partition                                             #
# --------------------------------------------------------------------------- #
def _real_block():
    manifest, _ = _offline_manifest("cost_weighted")
    return remaining_error_cost(manifest, _offline_stages(manifest))


def test_stage_sums_are_fail_closed_on_a_constructed_gap() -> None:
    """The guard FIRES: hole the stage partition four different ways and watch
    each one be refused. A passing check on an intact block first, so the
    witness is not vacuously green."""
    intact = _real_block()
    assert_stage_sums(intact)  # the control: the real block passes

    # (1) a corruption dropped from a stage's count
    holed = json.loads(json.dumps(intact))
    holed["by_stage"][STAGE_DETECTION]["corruptions"] -= 1
    with pytest.raises(CostError, match="not total by stage"):
        assert_stage_sums(holed)

    # (2) dollars that no longer reconstruct the total
    holed = json.loads(json.dumps(intact))
    holed["by_stage"][STAGE_DETECTION]["cost"] -= 7.5
    with pytest.raises(CostError, match="do not reconstruct the total"):
        assert_stage_sums(holed)

    # (3) a MANDATORY violation lost from the stage split
    holed = json.loads(json.dumps(intact))
    holed["mandatory_unresolved"] += 1
    with pytest.raises(CostError, match="MANDATORY unresolved violations are not total"):
        assert_stage_sums(holed)

    # (4) a stage removed outright
    holed = json.loads(json.dumps(intact))
    holed["by_stage"].pop(STAGE_REPAIR)
    with pytest.raises(CostError, match="stage set is wrong"):
        assert_stage_sums(holed)


def test_an_unstaged_terminal_state_is_fail_closed(monkeypatch) -> None:
    """Remove a state's stage attribution and the builder REFUSES rather than
    silently omitting those corruptions' cost."""
    manifest, _ = _offline_manifest("cost_weighted")
    stages = _offline_stages(manifest)
    assert stages["states"][STATE_NOT_DETECTED] > 0
    patched = {k: v for k, v in STATE_STAGE.items() if k != STATE_NOT_DETECTED}
    monkeypatch.setattr("okg.bench.synth.cost.STATE_STAGE", patched)
    with pytest.raises(CostError, match="neither resolved nor charged"):
        remaining_error_cost(manifest, stages)


def test_spent_action_cost_refuses_a_partial_ledger() -> None:
    prices = ActionPrices().as_table()
    counts = {name: 1 for name in ACTION_CLASSES}
    spent_action_cost(prices, counts)  # the control
    with pytest.raises(CostError, match="action counts cover"):
        spent_action_cost(prices, {k: v for k, v in counts.items() if k != "check"})
    with pytest.raises(CostError, match="action price table covers"):
        spent_action_cost({k: v for k, v in prices.items() if k != "check"}, counts)
    with pytest.raises(CostError, match="is negative"):
        spent_action_cost(prices, {**counts, "check": -1})


# --------------------------------------------------------------------------- #
# the action ledger                                                            #
# --------------------------------------------------------------------------- #
def test_action_counts_are_derived_from_the_run_not_declared() -> None:
    manifest, _ = _offline_manifest("smoke")
    claims = _full_extraction(manifest)
    state = _FakeState(claims, candidates=[["a", "b"], ["c", "d"]])
    counts = action_counts(
        manifest=manifest, clean_state=state, corrupted_state=state
    )
    docs = len(manifest["clean_corpus"])
    assert counts["parse"] == 2 * docs
    assert counts["extract_document"] == 2 * docs  # the lane is lossless
    assert counts["check"] == 4
    assert counts["correction"] == 0
    assert counts["publish"] == 2


def test_extract_document_falls_below_parse_when_a_document_is_lost() -> None:
    """The two counts are NOT definitionally equal: a document lost whole at
    extraction delivers no claims, and the gap between the two action counts is
    exactly that loss."""
    manifest, _ = _offline_manifest("smoke")
    claims = _full_extraction(manifest)
    lost = sorted({cid.split(":")[1] for cid in claims})[0]
    survived = {k: v for k, v in claims.items() if k.split(":")[1] != lost}
    assert len(survived) < len(claims)
    counts = action_counts(
        manifest=manifest,
        clean_state=_FakeState(claims),
        corrupted_state=_FakeState(survived),
    )
    assert counts["extract_document"] == counts["parse"] - 1


def test_corrections_are_billed_in_full_not_only_the_removals() -> None:
    """A corrections plan costs the operator for every action it took, including
    the assert that removes no corruption."""
    manifest, _ = _offline_manifest("smoke")
    state = _FakeState(_full_extraction(manifest))
    removed = frozenset({"corr:a", "corr:b"})
    fallback = action_counts(
        manifest=manifest, clean_state=state, corrupted_state=state,
        removed_corruptions=removed,
    )
    assert fallback["correction"] == 2
    billed = action_counts(
        manifest=manifest, clean_state=state, corrupted_state=state,
        removed_corruptions=removed, corrections_applied=3,
    )
    assert billed["correction"] == 3


# --------------------------------------------------------------------------- #
# provenance: the scorecard prices from the SEAL, never from the config        #
# --------------------------------------------------------------------------- #
def test_scorecard_prices_from_the_sealed_manifest_not_the_passed_config() -> None:
    """A re-score carries its own config object. If the scorer priced from there,
    a re-score could silently change a recorded run's economics — so it prices
    from the manifest, and a config that disagrees changes nothing."""
    manifest, config = _offline_manifest("cost_weighted")
    state = _FakeState(_full_extraction(manifest))
    honest = build_synth_scorecard(
        manifest=manifest, clean_state=state, corrupted_state=state,
        routed=frozenset(), config=config,
    )
    tampered_config = load_config(
        {
            **json.loads(config.canonical_json_bytes().decode("utf-8")),
            "cost": {
                "default_error_cost": 999999.0,
                "action_prices": {"check": 999999.0},
            },
        }
    )
    assert tampered_config.cost.default_error_cost == 999999.0
    tampered = build_synth_scorecard(
        manifest=manifest, clean_state=state, corrupted_state=state,
        routed=frozenset(), config=tampered_config,
    )
    assert tampered["remaining_error_cost"] == honest["remaining_error_cost"]
    assert tampered["spent_action_cost"] == honest["spent_action_cost"]
    assert tampered["cost_model"] == honest["cost_model"] == sealed_cost_model(manifest)


def test_qualification_is_a_loud_top_level_field() -> None:
    manifest, config = _offline_manifest("cost_mandatory")
    state = _FakeState(_full_extraction(manifest))
    core = build_synth_scorecard(
        manifest=manifest, clean_state=state, corrupted_state=state,
        routed=frozenset(), config=config,
    )
    assert core["qualified"] is False
    assert core["qualification"]["mandatory_unresolved"] > 0
    text = render_synth_scorecard(core)
    # the verdict is on the report's SECOND line, above every recall number
    assert "*** NOT QUALIFIED ***" in text.splitlines()[2]
    assert "remaining error cost" in text
    assert "spent action cost" in text


# --------------------------------------------------------------------------- #
# the sensitivity invariants                                                   #
# --------------------------------------------------------------------------- #
def _point(scale: float, total: float, qualified: bool = True):
    return {
        "scale": scale,
        "qualified": qualified,
        "remaining_error_cost": {"total": total},
    }


def test_sensitivity_invariants_fire() -> None:
    good = [_point(0.1, 1.0), _point(1.0, 10.0), _point(10.0, 100.0)]
    assert_sensitivity_invariants(good)  # the control
    with pytest.raises(CostError, match="qualification is not invariant"):
        assert_sensitivity_invariants(
            [_point(0.1, 1.0, True), _point(1.0, 10.0, False)]
        )
    with pytest.raises(CostError, match="must scale linearly"):
        assert_sensitivity_invariants(
            [_point(1.0, 10.0), _point(10.0, 42.0)]
        )
    with pytest.raises(CostError, match="at least one price point"):
        assert_sensitivity_invariants([])


# --------------------------------------------------------------------------- #
# REAL published runs: qualification in both directions, and the sweep         #
# --------------------------------------------------------------------------- #
def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


@pytest.mark.postgres
def test_qualification_both_directions_on_real_published_runs(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The acceptance, on two REAL published generations of the SAME corpus.

    The corrections lane publishes the corrupted corpus twice: generation A with
    no corrections and generation B with them. cost_mandatory declares
    value_perturbation MANDATORY and renders every fact exactly twice with one
    rendering corrupted, so at A every planted violation reaches the engine as a
    conflict corroboration cannot break and stands UNRESOLVED — the result does
    not qualify. At B the operator's retract and override remove both from scope
    and the same corpus qualifies. Neither verdict is arranged by the scorer.
    """
    from okg.bench.synth.runner import run_synth_corrections

    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_corrections(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        preset="cost_mandatory",
    )
    assert result["before"]["status"] == "published"
    assert result["after"]["status"] == "published"

    before, after = result["scorecard_before"], result["scorecard"]
    # the SAME corpus: the two generations are scored against one sealed manifest
    assert before["manifest_digest"] == after["manifest_digest"]
    print("\nqualification, generation A (no corrections):", json.dumps(
        {"qualified": before["qualified"],
         "remaining_error_cost": before["remaining_error_cost"],
         "funnel_states": before["funnel"]["states"],
         "spent_action_cost": before["spent_action_cost"]["total"]},
        sort_keys=True, indent=1))
    print("qualification, generation B (corrected):", json.dumps(
        {"qualified": after["qualified"],
         "remaining_error_cost": after["remaining_error_cost"],
         "funnel_states": after["funnel"]["states"],
         "spent_action_cost": after["spent_action_cost"]["total"]},
        sort_keys=True, indent=1))

    # DIRECTION 1 — the mandatory violation stands: NOT qualified.
    assert before["qualified"] is False
    assert before["qualification"]["mandatory_planted"] > 0
    assert before["qualification"]["mandatory_unresolved"] > 0
    assert before["remaining_error_cost"]["infinite"] is True
    unresolved_ids = before["qualification"]["unresolved_mandatory_corruptions"]
    assert unresolved_ids

    # DIRECTION 2 — the operator resolved it: qualified.
    assert after["qualified"] is True, after["qualification"]["reason"]
    assert after["qualification"]["mandatory_planted"] == (
        before["qualification"]["mandatory_planted"]
    )
    assert after["qualification"]["mandatory_unresolved"] == 0
    assert after["remaining_error_cost"]["infinite"] is False
    # And it flipped for the RIGHT reason: EACH violation that stood at A is
    # individually attributed to the operator action at B, never counted as an
    # engine repair. This has to be read per corruption — the previous form
    # (`for cid in unresolved_ids: assert ...removed_from_scope > 0`) never used
    # the loop variable, so it asserted one aggregate fact N times and would hold
    # if the removals landed on entirely different corruptions.
    #
    # The per-corruption states are deliberately kept out of the golden-pinned
    # scorecard, so the funnel is rebuilt from the run's OWN recorded engine
    # snapshot — no live table, no second model call.
    reads = json.loads(
        (tmp_path / "run" / ENGINE_READS_FILENAME).read_text(encoding="utf-8")
    )
    corrupted_state = _deserialize_state(reads["corrupted_state"])
    stages = funnel(
        manifest=result["manifest"],
        extracted_claims=corrupted_state.claims,
        resolutions=_deserialize_resolutions(reads["resolutions"]),
        findings=corrupted_state.findings,
        equivalent=synth_survival_equivalent,
        removed_corruptions=frozenset(reads.get("removed_corruptions", ())),
    )
    assert stages["states"] == after["funnel"]["states"], (
        "the rebuilt funnel must reproduce the run's own tally"
    )
    for cid in unresolved_ids:
        assert stages["per_corruption"][cid] == STATE_REMOVED_FROM_SCOPE, (
            f"{cid} stood as a mandatory violation at A but terminated as "
            f"{stages['per_corruption'][cid]!r} at B — the qualification flip is "
            "not attributable to the operator's correction"
        )
    assert result["report"]["funnel_states"]["removed_from_scope"] == len(unresolved_ids)

    # the operator PAID for it: the correction actions are billed, and the run's
    # dollars moved from the error side of the ledger to the action side
    assert after["spent_action_cost"]["by_class"]["correction"]["count"] == len(
        result["plan"].specs
    )
    assert (
        after["spent_action_cost"]["by_class"]["correction"]["cost"]
        > before["spent_action_cost"]["by_class"]["correction"]["cost"]
    )


@pytest.mark.postgres
def test_sensitivity_three_points_from_artifacts_alone(
    clean_runtime: str, tmp_path: Path
) -> None:
    """0.1x / 1x / 10x, re-scored from a RECORDED run with the network and the
    database provably untouched: psycopg.connect and socket.socket are both
    replaced with detonators for the duration of the sweep."""
    import socket

    import psycopg

    from okg.bench.synth.runner import (
        SENSITIVITY_FILENAME,
        rescore_from_artifacts,
        run_synth_benchmark,
        sensitivity_from_artifacts,
    )

    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    out_dir = tmp_path / "run"
    run = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=out_dir, preset="cost_weighted"
    )
    assert run["corrupted"]["status"] == "published"
    live = run["scorecard"]
    assert live["remaining_error_cost"]["total"] > 0, (
        "the sweep must not read a zero remaining cost"
    )

    def _no_db(*a, **k):  # pragma: no cover - the detonator
        raise AssertionError("re-scoring touched the database")

    def _no_net(*a, **k):  # pragma: no cover - the detonator
        raise AssertionError("re-scoring touched the network")

    real_connect, real_socket = psycopg.connect, socket.socket
    psycopg.connect, socket.socket = _no_db, _no_net
    try:
        # the detonators are ARMED: proven, so "no exception" below means the
        # sweep really did not reach out, not that the patch missed
        with pytest.raises(AssertionError, match="touched the database"):
            psycopg.connect(dsn)
        with pytest.raises(AssertionError, match="touched the network"):
            socket.socket()
        report = sensitivity_from_artifacts(out_dir)
        rescored = rescore_from_artifacts(out_dir)
    finally:
        psycopg.connect, socket.socket = real_connect, real_socket

    # the 1x point reproduces the run's own scorecard exactly
    assert rescored == live

    scales = [p["scale"] for p in report["points"]]
    assert scales == [0.1, 1.0, 10.0]
    by_scale = {p["scale"]: p for p in report["points"]}
    base = by_scale[1.0]["remaining_error_cost"]["total"]
    assert base == live["remaining_error_cost"]["total"]
    assert by_scale[0.1]["remaining_error_cost"]["total"] == pytest.approx(base * 0.1)
    assert by_scale[10.0]["remaining_error_cost"]["total"] == pytest.approx(base * 10.0)
    # the ACTION prices are not swept: the protocol re-prices error, not work
    assert {p["spent_action_cost"]["total"] for p in report["points"]} == {
        live["spent_action_cost"]["total"]
    }
    # the stage split survives the re-price at every point
    for point in report["points"]:
        assert_stage_sums(point["remaining_error_cost"])
    # and the run recorded the sweep alongside its other artifacts
    on_disk = json.loads((out_dir / SENSITIVITY_FILENAME).read_text())
    assert on_disk == report

    from okg.bench.synth.cost import render_sensitivity

    print("\n" + render_sensitivity(report))


# --------------------------------------------------------------------------- #
# composition: parity's WORK ledger prices with the same table                 #
# --------------------------------------------------------------------------- #
def test_parity_work_ledger_prices_with_the_same_action_table() -> None:
    """The umbrella's controlled-fixture row needs parity's step ledger turned
    into dollars. The shapes compose: parity charges per-document ``claim_source``
    steps, per-bucket checks and per-update publishes, which are exactly the
    ``extract_document`` / ``check`` / ``publish`` action classes. A mode that
    skips source steps bills strictly less than one that runs them all — the
    saving an incremental mechanism claims, now in dollars."""
    from okg.bench.synth.cost import action_counts_from_step_ledger
    from okg.bench.synth.parity import CLAIM_SOURCE_STEP, LedgerRow

    prices = load_preset("cost_weighted").cost.action_prices.as_table()
    docs = ("a.md", "b.md", "c.md", "d.md")

    def _row(run_steps: tuple[str, ...]) -> LedgerRow:
        return LedgerRow(
            mode="fixture", update=1,
            steps_reached=len(docs) + 3, steps_run=len(run_steps),
            skips_by_cause={}, run_causes={}, certificates_written=0,
            certificates_invalidated=0, invalidated_classes=(), terminal="ok",
            fallback_cause=None, published=True, generation_id="g1",
            cone_steps=len(docs) + 3, skips_achievable=0, skips_achieved=0,
            unrun_achievable=0,
        )

    def _bill(run_steps: tuple[str, ...], stated_buckets: int) -> float:
        row = _row(run_steps)
        counts = action_counts_from_step_ledger(
            documents_parsed=len(docs),
            source_steps_run=sum(
                1 for s in run_steps if s.startswith(f"{CLAIM_SOURCE_STEP}:")
            ),
            checks_evaluated=stated_buckets,
            publishes=int(row.published),
        )
        assert set(counts) == set(ACTION_CLASSES)
        return spent_action_cost(prices, counts)["total"]

    full = tuple(f"{CLAIM_SOURCE_STEP}:{d}" for d in docs) + ("claim_producer",)
    incremental = (f"{CLAIM_SOURCE_STEP}:{docs[0]}", "claim_producer")
    assert _bill(incremental, 2) < _bill(full, 8)
    # the parse charge does NOT move with the skipping: the bytes still arrived,
    # which is why parse and extract_document are separately priced classes
    assert _bill(full, 8) - _bill(incremental, 8) == pytest.approx(
        (len(docs) - 1) * prices["extract_document"]
    )


def test_qualification_block_names_the_unresolved_violations() -> None:
    manifest, _ = _offline_manifest("cost_mandatory")
    stages = _offline_stages(manifest)
    remaining = remaining_error_cost(manifest, stages)
    qual = qualification(manifest, remaining)
    assert qual["qualified"] is False
    assert qual["mandatory_planted"] == qual["mandatory_unresolved"] > 0
    for cid in qual["unresolved_mandatory_corruptions"]:
        assert stages["per_corruption"][cid] not in RESOLVED_STATES
        assert cid in qual["reason"]
    # the extraction stage carries none of them here: the deterministic lane is
    # lossless, so the whole charge is downstream
    assert remaining["by_stage"][STAGE_EXTRACTION]["mandatory_unresolved"] == 0
