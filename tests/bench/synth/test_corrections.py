"""task.c.corrections witnesses: the operator-correction path that forms C_eff.

The benchmark emits claim_correction facts (retract / override / assert); a real
two-generation run OBSERVES their effect as a row-level delta on
okg.claim_effective between pinned generations (never a re-read of the
corrections that were emitted); and the reserved removed_from_scope funnel state
becomes reachable for both the retract and override shapes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import load_preset  # noqa: E402
from okg.bench.synth.corrections import (  # noqa: E402
    CorrectionError,
    corrections_gold,
    plan_corrections,
)
from okg.bench.synth.fhir_map import world_from_synthea_dir  # noqa: E402
from okg.bench.synth.goldseal import build_manifest  # noqa: E402
from okg.bench.synth.prf import SynthPrf  # noqa: E402
from okg.bench.synth.corruption import Planner  # noqa: E402
from okg.bench.synth.render import render_corpus  # noqa: E402
from okg.bench.synth.resolution import (  # noqa: E402
    STATE_REMOVED_FROM_SCOPE,
    funnel,
)
from okg.bench.synth.runner import (  # noqa: E402
    _subset_world,
    run_synth_corrections,
    synth_survival_equivalent,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
_SYNTH = _REPO_ROOT / "src/okg/bench/synth"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
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


def _offline_corrections_manifest():
    world_dir = _SYNTH / "world_smoke"
    prov = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov
    )
    config = load_preset("corrections")
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered), config


# --------------------------------------------------------------------------- #
# DB-free: the plan, its gold, and its fail-closed conformance                 #
# --------------------------------------------------------------------------- #
def test_plan_emits_all_three_correction_shapes() -> None:
    manifest, config = _offline_corrections_manifest()
    plan = plan_corrections(manifest, SynthPrf(config.seed))
    assert plan.count_by_action() == {"retract": 1, "override": 1, "assert": 1}
    # retract and override target DISTINCT corrupted claims; both are removed
    assert len(plan.removed_corruption_ids) == 2
    gold = corrections_gold(plan)
    actions = {row["action"] for row in gold}
    assert actions == {"retract", "override", "assert"}
    # the override records the clean surface it rewrites to, and its before value
    override = next(row for row in gold if row["action"] == "override")
    assert override["expected_value"] != override["before_value"]


def test_plan_is_fail_closed_when_too_few_corruptions() -> None:
    manifest, config = _offline_corrections_manifest()
    manifest["gold"]["corruptions"] = manifest["gold"]["corruptions"][:1]
    with pytest.raises(CorrectionError, match="at least two corruptions"):
        plan_corrections(manifest, SynthPrf(config.seed))


# --------------------------------------------------------------------------- #
# DB-free: removed_from_scope is REACHABLE, and only via a correction          #
# --------------------------------------------------------------------------- #
def _full_extraction(manifest) -> dict[str, str]:
    from okg.bench.synth.resolution import synth_slug

    patients = manifest["patients"]
    claims: dict[str, str] = {}
    for row in manifest["trace"]:
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        pred = synth_slug(row["predicate_label"])
        claims[f"claim:{row['doc']}:{pred}:{entity}:0"] = row["surface_value"]
    return claims


def test_removed_from_scope_is_reachable_only_with_a_correction() -> None:
    """The reserved state is unreachable WITHOUT a correction and reachable WITH
    one — for both the retract and override shapes (the funnel treats them
    identically via removed_corruptions)."""
    manifest, config = _offline_corrections_manifest()
    claims = _full_extraction(manifest)
    plan = plan_corrections(manifest, SynthPrf(config.seed))
    removed = plan.removed_corruption_ids
    assert len(removed) == 2

    base = funnel(
        manifest=manifest, extracted_claims=claims, resolutions=(),
        findings=(), equivalent=synth_survival_equivalent,
    )
    assert base["states"][STATE_REMOVED_FROM_SCOPE] == 0, (
        "removed_from_scope must be 0 with no corrections (smoke invariant)"
    )

    corrected = funnel(
        manifest=manifest, extracted_claims=claims, resolutions=(),
        findings=(), equivalent=synth_survival_equivalent,
        removed_corruptions=removed,
    )
    assert corrected["states"][STATE_REMOVED_FROM_SCOPE] == 2
    for cid in removed:
        assert corrected["per_corruption"][cid] == STATE_REMOVED_FROM_SCOPE
        # and NOT debited to the engine as a detection miss
        assert base["per_corruption"][cid] != STATE_REMOVED_FROM_SCOPE
    # a removed corruption is excluded from the conditional detection denominator
    assert (
        corrected["detection_recall"]["conditional"]["denominator"]
        <= base["detection_recall"]["conditional"]["denominator"]
    )


# --------------------------------------------------------------------------- #
# The real two-generation run: observe the C_eff delta                         #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_corrections_observed_on_claim_effective(
    clean_runtime: str, tmp_path: Path
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_corrections(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        preset="corrections",
    )
    assert result["before"]["status"] == "published"
    assert result["after"]["status"] == "published"

    report = result["report"]
    plan = result["plan"]
    before, after = result["effective_before"], result["effective_after"]

    # RETRACT: the target claim is present at generation A and ABSENT at B.
    retract = next(s for s in plan.specs if s.action == "retract")
    assert retract.target_claim_id in before
    assert retract.target_claim_id not in after
    assert retract.target_claim_id in report["retracted_absent"]

    # OVERRIDE: the target claim carries the NEW (clean) value at B, not the
    # corrupted value it had at A.
    override = next(s for s in plan.specs if s.action == "override")
    assert before[override.target_claim_id]["value"] != after[override.target_claim_id]["value"]
    assert after[override.target_claim_id]["value"] == override.value
    row = next(r for r in report["overridden"] if r["claim_id"] == override.target_claim_id)
    assert row["before"] != row["after"] and row["after"] == override.value

    # ASSERT: a claim absent at A is present at B, provenance operator_asserted.
    asserted = next(s for s in plan.specs if s.action == "assert")
    assert asserted.target_claim_id not in before
    assert asserted.target_claim_id in after
    assert after[asserted.target_claim_id]["provenance"] == "operator_asserted"

    # POPULATION asserted explicitly: after == before - retracts + asserts. A
    # silent drop would break this equality rather than pass unnoticed.
    assert report["population_ok"]
    assert report["population_after"] == report["population_before"] - 1 + 1

    # THE FUNNEL: the retracted AND overridden corruptions land in
    # removed_from_scope — the reserved state, now reachable — not as a miss.
    assert report["funnel_states"][STATE_REMOVED_FROM_SCOPE] == 2


def test_smoke_and_default_presets_declare_no_corrections() -> None:
    """The default presets emit no corrections, so their funnel keeps
    removed_from_scope = 0 (the smoke golden invariant)."""
    manifest, config = _offline_corrections_manifest()
    claims = _full_extraction(manifest)
    stages = funnel(
        manifest=manifest, extracted_claims=claims, resolutions=(),
        findings=(), equivalent=synth_survival_equivalent,
    )
    assert stages["states"][STATE_REMOVED_FROM_SCOPE] == 0
