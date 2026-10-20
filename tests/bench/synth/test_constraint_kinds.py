"""task.sigma.kinds witnesses: all FOUR Σ constraint kinds produce findings in a
real published generation, detection recall is reported PER KIND (a zero
denominator is a loud failure), the linked_pair case is cross-bucket, and the
coverage policies are load-bearing (without them the three relational kinds do
not fire).

The proof is read from the FINDING ROWS of a published generation, never from
the policies the deployment declares (req.sigma.kinds)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import (  # noqa: E402
    CONSTRAINT_KIND_VOCABULARY,
    ConfigError,
    load_config,
    load_preset,
)
from okg.bench.synth.corruption import CONSTRAINT_KINDS  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    RunnerError,
    constraint_policy_facts,
    finding_kinds,
    functional_policies,
    ingest_and_publish,
    run_synth_benchmark,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
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
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


# --------------------------------------------------------------------------- #
# DB-free: the config vocabulary is the engine's, and the gate rejects junk    #
# --------------------------------------------------------------------------- #
def test_constraint_kind_vocabulary_matches_corruption() -> None:
    """The config's closed kind vocabulary equals corruption.CONSTRAINT_KINDS
    (literals in the core module, pinned here so they cannot drift) and the
    engine's four policy kinds."""
    assert CONSTRAINT_KIND_VOCABULARY == set(CONSTRAINT_KINDS)
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        _POLICY_KINDS,
    )

    assert CONSTRAINT_KIND_VOCABULARY == set(_POLICY_KINDS)


def test_require_constraint_kinds_rejects_unknown_kind() -> None:
    with pytest.raises(ConfigError, match="unknown kind"):
        load_config({
            "seed": "x", "world_path": "world_smoke",
            "require_constraint_kinds": ["functional", "not_a_kind"],
        })


def test_coverage_preset_requires_all_four_kinds() -> None:
    cfg = load_preset("constraint_kinds")
    assert set(cfg.require_constraint_kinds) == set(CONSTRAINT_KINDS)


# --------------------------------------------------------------------------- #
# DB-free: the two metrics agree under ONE predicate (req.honesty.*)            #
# --------------------------------------------------------------------------- #
_CAPSTONE_RUN = _REPO_ROOT / ".scratch" / "capstone_run"


def test_capstone_artifacts_agree_across_metrics_under_one_predicate() -> None:
    """req.honesty.* ACCEPTANCE, on the REAL capstone artifacts via rescore (no
    model call, no live table read): the two unresolved_key sites read DETECTED in
    BOTH the funnel (detected_only) and per_kind_detection (end-to-end detected);
    the per-kind conditional misses sum EXACTLY to the funnel not_detected count
    (strictly detection losses); funnel exhaustiveness still sums. Read-only over
    .scratch/capstone_run, so it skips when the local artifact dir is absent."""
    from okg.bench.synth.goldseal import load_manifest
    from okg.bench.synth.resolution import (
        _DETECTED_STATES,
        funnel,
        gold_constraint_kind,
    )
    from okg.bench.synth.runner import (
        ENGINE_READS_FILENAME,
        MANIFEST_FILENAME,
        _deserialize_resolutions,
        _deserialize_state,
        rescore_from_artifacts,
        synth_survival_equivalent,
    )

    if not (_CAPSTONE_RUN / MANIFEST_FILENAME).exists() or not (
        _CAPSTONE_RUN / ENGINE_READS_FILENAME
    ).exists():
        pytest.skip(f"no capstone artifacts at {_CAPSTONE_RUN}")
    # A run recorded before synth-cost-weighted-gold has no sealed cost tables,
    # and scoring it would have to INVENT the error costs and action prices —
    # which the scorer refuses (fail-closed). Skip such a dir explicitly rather
    # than let the refusal read as a failure of this per-kind witness; a re-run of
    # the capstone re-seals it and the witness returns.
    if "cost_model" not in load_manifest(_CAPSTONE_RUN / MANIFEST_FILENAME):
        pytest.skip(
            f"capstone artifacts at {_CAPSTONE_RUN} predate the sealed cost model"
        )

    core = rescore_from_artifacts(_CAPSTONE_RUN)
    by_kind = core["per_kind_detection"]["by_kind"]
    states = core["funnel"]["states"]

    # exhaustiveness still sums
    assert sum(states.values()) == core["funnel"]["total"]

    # per-kind conditional misses == funnel not_detected (detection losses only)
    cond_miss = sum(
        c["conditional"]["denominator"] - c["conditional"]["detected"]
        for c in by_kind.values()
    )
    assert cond_miss == states["not_detected"]

    # the funnel side, computed INDEPENDENTLY of per_kind: the two unresolved_key
    # sites land in a detected state (detected_only) — genuine cross-metric
    # agreement, not one metric read twice.
    manifest = load_manifest(_CAPSTONE_RUN / MANIFEST_FILENAME)
    reads = json.loads((_CAPSTONE_RUN / ENGINE_READS_FILENAME).read_text())
    corrupted = _deserialize_state(reads["corrupted_state"])
    fout = funnel(
        manifest=manifest,
        extracted_claims=corrupted.claims,
        resolutions=_deserialize_resolutions(reads["resolutions"]),
        findings=corrupted.findings,
        equivalent=synth_survival_equivalent,
    )
    unresolved = [
        cid
        for site in manifest["gold"]["corruptions"]
        for cid in (site["corruption_id"],)
        if gold_constraint_kind(site) == "unresolved_key"
    ]
    assert unresolved, "the capstone artifacts must carry unresolved_key sites"
    for cid in unresolved:
        assert fout["per_corruption"][cid] in _DETECTED_STATES, (
            f"unresolved_key site {cid} not detected in the funnel: "
            f"{fout['per_corruption'][cid]}"
        )
    # and per_kind reads the SAME two sites detected end-to-end
    assert by_kind["unresolved_key"]["end_to_end"]["detected"] == len(set(unresolved))


def test_deterministic_golden_pins_conditional_equals_end_to_end() -> None:
    """The smoke golden is a lossless-extraction control: nothing dies at
    extraction, so every gold site survived and per-kind conditional == end_to_end
    for every kind that has a site (req.honesty.perkind-conditional). Pins that the
    two denominators coincide exactly when extraction is lossless."""
    golden_path = Path(__file__).parent / "golden_smoke_scorecard.json"
    if not golden_path.exists():
        pytest.skip("smoke golden not yet minted")
    golden = json.loads(golden_path.read_text())
    by_kind = golden["per_kind_detection"]["by_kind"]
    exercised = False
    for kind, cell in by_kind.items():
        if cell["end_to_end"]["denominator"] == 0:
            continue
        exercised = True
        assert cell["conditional"] == cell["end_to_end"], (
            f"{kind}: lossless control must have conditional == end_to_end: {cell}"
        )
    assert exercised, "the smoke golden must exercise at least one kind"


# --------------------------------------------------------------------------- #
# The real published run: all four kinds fire, recall per kind, cross-bucket    #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_all_four_constraint_kinds_fire_in_a_published_generation(
    clean_runtime: str, tmp_path: Path
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        preset="constraint_kinds",
    )
    core = result["scorecard"]

    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"

    # PROVEN FROM FINDING ROWS: the published generation carries findings of all
    # four constraint kinds (read via their constraint_kind, never from config).
    kinds_live = set(
        finding_kinds(dsn, result["corrupted"]["generation_id"]).values()
    )
    assert set(CONSTRAINT_KINDS) <= kinds_live, (
        f"published findings miss a constraint kind: have {sorted(kinds_live)}"
    )

    # Detection recall PER KIND, in BOTH forms (end_to_end + conditional): every
    # one of the four kinds has a NON-ZERO end-to-end denominator and at least one
    # detected — a zero end-to-end detected would be a loud failure (the run's
    # require_constraint_kinds gate keys on end_to_end.detected>=1).
    by_kind = core["per_kind_detection"]["by_kind"]
    assert set(by_kind) == set(CONSTRAINT_KINDS)
    for kind in CONSTRAINT_KINDS:
        e2e = by_kind[kind]["end_to_end"]
        cond = by_kind[kind]["conditional"]
        assert e2e["denominator"] > 0, f"{kind} has a zero end-to-end denominator"
        assert e2e["detected"] > 0, f"{kind} fired zero findings"
        # conditional is a subset of end-to-end (survived conflicts only)
        assert cond["denominator"] <= e2e["denominator"]
        assert cond["detected"] <= e2e["detected"]

    # THE ONE PREDICATE: per-kind and the funnel derive from it, so the per-kind
    # conditional misses (survived-but-not-detected) sum EXACTLY to the funnel's
    # not_detected count — the two metrics can never again disagree the way the
    # channel-vocabulary split used to make them.
    funnel_states = core["funnel"]["states"]
    cond_miss = sum(
        c["conditional"]["denominator"] - c["conditional"]["detected"]
        for c in by_kind.values()
    )
    assert cond_miss == funnel_states["not_detected"], (
        f"per-kind conditional misses {cond_miss} != funnel not_detected "
        f"{funnel_states['not_detected']} — the metrics disagree"
    )
    # the unresolved_key sites, flagged on not_comparable, read DETECTED in the
    # per-kind metric AND (as detected_only) in the funnel — the split is gone.
    assert by_kind["unresolved_key"]["end_to_end"]["detected"] >= 1

    # THE linked_pair CROSS-BUCKET obligation: its two member claims sit in
    # DIFFERENT PREDICATE buckets — the only cross-bucket constraint kind. The
    # planted pairing is record_reference (rewritten to the patient's entity key)
    # linked to birthdate, so the two predicates differ. It is SAME-PATIENT: a
    # linked finding needs the two claims' scopes to overlap, and a synth claim's
    # only applicability axis is its heading path, which always carries the
    # patient's H1 ("Patient X > Patient Details"); two patients' claims are
    # therefore scope-disjoint and never link. So the two member buckets share one
    # entity and differ only in predicate.
    linked = core["per_kind_detection"]["linked_cross_bucket"]
    assert linked, "no linked_pair finding was recorded"
    cross = [row for row in linked if len(row["member_buckets"]) == 2]
    assert cross, "a linked_pair finding must span two buckets, none did"
    for row in cross:
        b0, b1 = row["member_buckets"]
        pred0, entity0 = b0.split(":", 1)
        pred1, entity1 = b1.split(":", 1)
        assert pred0 != pred1, (
            f"linked_pair members must be in different predicate buckets: {row}"
        )
        assert {pred0, pred1} == {"record_reference", "birthdate"}, (
            f"linked_pair members must be record_reference + birthdate: {row}"
        )
        assert entity0 == entity1, (
            f"linked_pair members are same-patient (scope = heading path, so a "
            f"cross-patient link is scope-disjoint and cannot fire): {row}"
        )

    # clean gate: the certified-clean corpus produced zero contested findings
    # (the unresolved-key policy's NOT-COMPARABLE findings are not contested, so
    # the negative control still holds).
    assert core["clean_run_fp"] == 0
    assert core["clean_gate_passed"]

    # the golden pin (deterministic core, byte-comparable across runs)
    golden_path = Path(__file__).parent / "golden_constraint_kinds_scorecard.json"
    if golden_path.exists():
        golden = json.loads(golden_path.read_text())
        assert core == golden, (
            "constraint-kinds scorecard drifted from the pinned golden; if the "
            "change is intentional, regenerate "
            "tests/bench/synth/golden_constraint_kinds_scorecard.json"
        )
    else:
        import os

        assert not os.environ.get("CI"), (
            "golden_constraint_kinds_scorecard.json is missing under CI"
        )
        golden_path.write_text(json.dumps(core, sort_keys=True, indent=1) + "\n")


@pytest.mark.postgres
def test_coverage_policies_are_load_bearing(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Non-vacuity: WITH the coverage policies the three relational kinds fire;
    republishing the SAME corrupted corpus with every predicate functional and
    NO coverage policies makes them vanish — so each policy is what admits its
    kind, not the corruption alone."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        preset="constraint_kinds",
    )
    manifest = result["manifest"]
    with_kinds = set(finding_kinds(dsn, result["corrupted"]["generation_id"]).values())
    relational = {"incompatible_pair", "linked_pair", "unresolved_key"}
    assert relational <= with_kinds, (
        f"the relational kinds did not fire WITH their policies: {sorted(with_kinds)}"
    )

    # Republish the corrupted corpus with EVERY predicate functional and the
    # coverage policies RETRACTED (they persist as graph nodes across
    # generations, so they must be removed, not merely un-minted): the relational
    # kinds can no longer be expressed, so they vanish.
    corrupted_dir = tmp_path / "run" / "corpus_corrupted"
    extra, _excluded = constraint_policy_facts(manifest)
    retractions = [
        NodeFact(f.node_id, "predicate_policy", {}, {"id": f.node_id}, {"gone": "1"}, op="R")
        for f in extra
    ]
    policies = functional_policies(manifest)  # no exclusion -> all functional
    out, _ = ingest_and_publish(
        dsn, corrupted_dir, _DEP_DIR, policies, extra_policy_facts=retractions
    )
    without_kinds = set(finding_kinds(dsn, out["generation_id"]).values())
    assert relational.isdisjoint(without_kinds), (
        f"a relational kind fired WITHOUT its policy: "
        f"{sorted(relational & without_kinds)}"
    )
    # functional conflicts are still present (the corruptions still contradict);
    # only the relational KINDS are gone, which is the load-bearing claim.
    assert "functional" in without_kinds


@pytest.mark.postgres
def test_missing_required_kind_fails_the_run(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The zero-denominator gate is loud: a preset that requires a kind whose
    corruption class is absent FAILS the run rather than reporting a hidden
    zero."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    cfg = load_preset("constraint_kinds")
    # drop the linked class but keep requiring linked_pair -> the run must fail
    classes = {
        name: spec
        for name, spec in cfg.corruption.classes.items()
        if name != "linked_reference"
    }
    broken = cfg.model_copy(update={
        "corruption": cfg.corruption.model_copy(update={"classes": classes}),
    })
    with pytest.raises(RunnerError, match="required constraint kind"):
        run_synth_benchmark(
            dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "broken",
            config=broken,
        )
