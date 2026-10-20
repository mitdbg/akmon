"""task.attrib.survival witnesses: injection survival + the extractor/engine FP split.

Lane-free and DB-free (no network, no Postgres): every witness drives
build_synth_scorecard over a synthesized post-extraction claim set, so the
survival metric and the FP-split predicate are checked on every CI run.

The mocked "extractor" IS the claim set: a lossless extraction is the control
(survival 1.0), and a laundering / missing / hallucinating extractor is that set
with one site's claim corrected, dropped, or invented. One witness also
monkeypatches LlmExtractor._complete directly, proving the corrupted value
survives from an actual (stubbed) model response into the published claim.

Non-vacuity is in BOTH directions the PACT demands: the lossless control pins
survival at 1.0 for every class, and a deliberately correcting extractor lowers
it and names the site. The FP split is TOTAL over claims: a hallucinated claim
with no trace row attributes its bucket's finding to the extractor, never to the
engine, while a faithfully-extracted conflict stays engine noise.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from okg.bench.synth.config import load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest, write_manifest
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus
from okg.bench.synth.resolution import (
    STATE_LAUNDERED_AT_EXTRACTION,
    STATE_MISSED_AT_EXTRACTION,
    corruption_bucket_map,
    funnel,
    synth_slug,
)
from okg.bench.synth.runner import (
    ENGINE_READS_FILENAME,
    MANIFEST_FILENAME,
    _attribute_unexpected_findings,
    _subset_world,
    _survival_key,
    _write_engine_reads,
    build_synth_scorecard,
    report_from_artifacts,
    rescore_from_artifacts,
    synth_survival_equivalent,
)
from okg.bench.injection.scoring import FindingView, PipelineState

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SYNTH = _REPO_ROOT / "src/okg/bench/synth"


# --------------------------------------------------------------------------- #
# offline smoke manifest + a lossless extraction (the mocked "extractor")     #
# --------------------------------------------------------------------------- #
def _smoke_manifest():
    world_dir = _SYNTH / "world_smoke"
    prov = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov
    )
    config = load_preset("smoke")
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered), config


def _lossless_claims(manifest) -> dict[str, str]:
    """Every trace row present as an occurrence-0 claim valued at its rendered
    (corrupted-corpus) surface — a lossless extractor's output."""
    patients = manifest["patients"]
    claims: dict[str, str] = {}
    for row in manifest["trace"]:
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        pred = synth_slug(row["predicate_label"])
        claims[f"claim:{row['doc']}:{pred}:{entity}:0"] = row["surface_value"]
    return claims


def _score(manifest, config, claims, *, findings=()):
    corrupted = PipelineState(
        claims=claims, candidates=frozenset(), findings=tuple(findings)
    )
    clean = PipelineState(claims={}, candidates=frozenset(), findings=())
    return build_synth_scorecard(
        manifest=manifest,
        clean_state=clean,
        corrupted_state=corrupted,
        routed=frozenset(),
        config=config,
        resolutions=(),
    )


def _a_cross_doc_site(manifest):
    """A corruption site with a surviving clean counterpart (cross_doc), and its
    gold bucket — the site type a laundering / missing extractor can knock out."""
    buckets = corruption_bucket_map(manifest)
    for site in manifest["gold"]["corruptions"]:
        if site["partition"] == "cross_doc":
            return site, buckets[site["corruption_id"]]
    raise AssertionError("smoke manifest has no cross_doc corruption site")


def _a_non_gold_trace_bucket(manifest) -> str:
    """A bucket that carries real trace rows but is neither a corruption nor a
    twin — an engine-noise finding lands here with every claim matching trace."""
    patients = manifest["patients"]
    trace = manifest["trace"]
    gold = set(corruption_bucket_map(manifest).values())
    twin_buckets = set()
    for twin in manifest["gold"]["twins"]:
        fid = twin["fact_id"]
        rows = [r for r in trace if r["fact_id"] == fid]
        if rows:
            pred = synth_slug(rows[0]["predicate_label"])
            ent = patients[fid.split(":")[1]]["entity"]
            twin_buckets.add(f"{pred}:{ent}")
    for row in trace:
        pred = synth_slug(row["predicate_label"])
        ent = patients[row["fact_id"].split(":")[1]]["entity"]
        bucket = f"{pred}:{ent}"
        if bucket not in gold and bucket not in twin_buckets:
            return bucket
    raise AssertionError("no non-gold trace bucket in the smoke manifest")


# --------------------------------------------------------------------------- #
# the declared equivalence has ONE implementation                             #
# --------------------------------------------------------------------------- #
def test_survival_key_is_the_declared_equivalence() -> None:
    """_survival_key is the KEY form of synth_survival_equivalent, not a divergent
    copy: two values share a key IFF they are survival-equivalent. This is what
    lets the FP split join by membership and still judge laundering identically to
    the funnel.

    Each pair carries its EXPECTED answer. Agreement alone is not enough: two
    functions that both collapse everything to one key, or both answer True,
    agree perfectly and would pass a test that only compares them to each other.
    """
    pairs = [
        # (a, b, survival-equivalent?)
        ("82000 g", "82.0 kg", True),        # unit rescale, g -> kg
        ("77.2 kg", " 77.2   KG ", True),    # whitespace + case
        ("1750 mm", "175.0 cm", True),       # unit rescale, mm -> cm
        ("180.0 kg", "82.0 kg", False),      # genuinely different magnitudes
        ("denied", "present", False),        # opposite categoricals
    ]
    for a, b, expected in pairs:
        assert synth_survival_equivalent(a, b) is expected, (a, b)
        assert (_survival_key(a) == _survival_key(b)) is expected, (a, b)


# --------------------------------------------------------------------------- #
# survive: the lossless control pins survival at 1.0 (non-vacuity direction b) #
# --------------------------------------------------------------------------- #
def test_lossless_extraction_pins_survival_at_one() -> None:
    """A lossless extractor carries every corrupted value into its gold bucket,
    so survival is 1.0 for every class and no site is non-surviving. This is the
    deterministic lane's control condition."""
    manifest, config = _smoke_manifest()
    core = _score(manifest, config, _lossless_claims(manifest))
    survival = core["per_class_survival"]
    assert survival["non_surviving_sites"] == []
    assert survival["by_class"], "the smoke world must express >=1 corruption class"
    for cls, cell in survival["by_class"].items():
        assert cell["survived"] == cell["sites"], f"{cls}: {cell}"
    # the deterministic control has no unexpected findings at all
    assert core["corrupted_unexpected_findings"] == {
        "extractor_attributed": [],
        "engine_attributed": [],
    }


def test_mocked_extractor_carries_corrupted_value_into_claim(monkeypatch) -> None:
    """The literal survive path: a stubbed model response returning the corrupted
    value verbatim yields a claim carrying that value, survival-equivalent to the
    injected surface — so it lands in the gold bucket and survives."""
    from okg.bench.synth.llm_extract import LlmExtractor

    ex = LlmExtractor(labels=("Body Weight",))
    doc = "# Patient Ada Lovelace\n\n## Measurements\n\nBody Weight: 180.0 kg\n"

    def responder(_text: str) -> str:
        return (
            '[{"attribute": "Body Weight", "value": "180.0 kg", '
            '"quote": "Body Weight: 180.0 kg"}]'
        )

    monkeypatch.setattr(ex, "_complete", responder)
    claims = list(ex.claims("note.md", doc))
    assert len(claims) == 1
    assert claims[0].attrs["value"] == "180.0 kg"
    assert synth_survival_equivalent(claims[0].attrs["value"], "180.0 kg")


# --------------------------------------------------------------------------- #
# launder: the mutation witness — a correcting extractor lowers survival        #
# --------------------------------------------------------------------------- #
def test_correcting_extractor_lowers_survival_and_agrees_with_funnel() -> None:
    """A deliberately CORRECTING extractor returns the CLEAN value at a corruption
    site. Survival for that class drops below 1.0, the site is named non-surviving,
    and the funnel calls it laundered_at_extraction — survival and the funnel AGREE
    because the survival block is lifted verbatim from the funnel's rows."""
    manifest, config = _smoke_manifest()
    site, bucket = _a_cross_doc_site(manifest)
    cls = site["corruption_class"]
    pred, entity = bucket.split(":", 1)
    claim_id = f"claim:{site['doc']}:{pred}:{entity}:0"

    claims = _lossless_claims(manifest)
    assert synth_survival_equivalent(claims[claim_id], site["post_text"]), (
        "baseline: the corrupted value is present before the extractor corrects it"
    )
    # the correcting extractor: clean value in place of the injected one
    claims[claim_id] = site["pre_text"]

    core = _score(manifest, config, claims)
    cell = core["per_class_survival"]["by_class"][cls]
    assert cell["survived"] == cell["sites"] - 1, cell
    assert site["corruption_id"] in core["per_class_survival"]["non_surviving_sites"]

    stages = funnel(
        manifest=manifest,
        extracted_claims=claims,
        resolutions=(),
        findings=(),
        equivalent=synth_survival_equivalent,
    )
    assert stages["per_corruption"][site["corruption_id"]] == STATE_LAUNDERED_AT_EXTRACTION
    # the scorecard's survival block is the funnel's, not a parallel recompute
    assert core["per_class_survival"] == stages["per_class_survival"]


def test_missing_extraction_lowers_survival_and_names_the_site() -> None:
    """If the corrupted rendering never becomes a claim at all, survival drops and
    the funnel calls it missed_at_extraction — the other extraction-loss state."""
    manifest, config = _smoke_manifest()
    site, bucket = _a_cross_doc_site(manifest)
    cls = site["corruption_class"]
    pred, entity = bucket.split(":", 1)
    claim_id = f"claim:{site['doc']}:{pred}:{entity}:0"

    claims = _lossless_claims(manifest)
    del claims[claim_id]  # the corrupted rendering produced no claim

    core = _score(manifest, config, claims)
    cell = core["per_class_survival"]["by_class"][cls]
    assert cell["survived"] == cell["sites"] - 1
    assert site["corruption_id"] in core["per_class_survival"]["non_surviving_sites"]

    stages = funnel(
        manifest=manifest,
        extracted_claims=claims,
        resolutions=(),
        findings=(),
        equivalent=synth_survival_equivalent,
    )
    assert stages["per_corruption"][site["corruption_id"]] == STATE_MISSED_AT_EXTRACTION


# --------------------------------------------------------------------------- #
# hallucinate: a claim with no trace row -> its finding is extractor-attributed #
# --------------------------------------------------------------------------- #
def test_hallucinated_finding_is_extractor_attributed() -> None:
    """A finding whose bucket holds a claim with NO trace row is charged to the
    extractor (it invented or mis-keyed the claim), never to the engine — the
    split the flat corrupted_unexpected_findings list could not make."""
    manifest, config = _smoke_manifest()
    claims = _lossless_claims(manifest)
    ghost_bucket = "body_weight:patient_ghost_9999"
    claims["claim:ghost.md:body_weight:patient_ghost_9999:0"] = "1234.0 kg"
    findings = (
        FindingView(finding_id="fp1", channel="definite", bucket_key=ghost_bucket),
    )
    core = _score(manifest, config, claims, findings=findings)
    fp = core["corrupted_unexpected_findings"]
    assert ghost_bucket in fp["extractor_attributed"]
    assert ghost_bucket not in fp["engine_attributed"]


def test_faithful_finding_is_engine_attributed() -> None:
    """A finding on a non-gold bucket whose every claim matches its trace row is
    engine noise, not an extractor defect — the complement of the hallucinate
    case, so the predicate is proven total in both directions."""
    manifest, config = _smoke_manifest()
    claims = _lossless_claims(manifest)
    bucket = _a_non_gold_trace_bucket(manifest)
    findings = (
        FindingView(finding_id="fp2", channel="definite", bucket_key=bucket),
    )
    core = _score(manifest, config, claims, findings=findings)
    fp = core["corrupted_unexpected_findings"]
    assert bucket in fp["engine_attributed"]
    assert bucket not in fp["extractor_attributed"]


def test_attribution_is_total_over_a_mixed_bucket() -> None:
    """One divergent claim in an otherwise-faithful bucket is enough: the predicate
    is ANY-claim-invented, so a single mis-keyed value taints the whole bucket to
    the extractor. Checked on the helper directly for a mixed bucket."""
    manifest, _config = _smoke_manifest()
    bucket = _a_non_gold_trace_bucket(manifest)
    pred, entity = bucket.split(":", 1)
    # one faithful claim (matches a trace surface) + one invented value
    faithful = None
    for row in manifest["trace"]:
        ent = manifest["patients"][row["fact_id"].split(":")[1]]["entity"]
        if synth_slug(row["predicate_label"]) == pred and ent == entity:
            faithful = (row["doc"], row["surface_value"])
            break
    assert faithful is not None
    claims = {
        f"claim:{faithful[0]}:{pred}:{entity}:0": faithful[1],
        f"claim:{faithful[0]}:{pred}:{entity}:1": "definitely-not-a-rendered-value",
    }
    split = _attribute_unexpected_findings(manifest, claims, [bucket])
    assert split == {"extractor_attributed": [bucket], "engine_attributed": []}


# --------------------------------------------------------------------------- #
# the report path (rescore -> survival + FP split + funnel), DB-free           #
# --------------------------------------------------------------------------- #
def test_report_from_synthesized_artifacts(tmp_path) -> None:
    """report_from_artifacts rescores a recorded artifact dir and renders the
    survival block, the FP split, and the funnel — no model, no live table. Here
    it runs on a synthesized dir (one laundered site + one hallucinated FP) so the
    report is non-vacuous in every section."""
    manifest, _config = _smoke_manifest()
    claims = _lossless_claims(manifest)
    site, bucket = _a_cross_doc_site(manifest)
    pred, entity = bucket.split(":", 1)
    claims[f"claim:{site['doc']}:{pred}:{entity}:0"] = site["pre_text"]  # laundered
    ghost_bucket = "body_weight:patient_ghost_9999"
    claims["claim:ghost.md:body_weight:patient_ghost_9999:0"] = "1234.0 kg"
    findings = (
        FindingView(finding_id="fp", channel="definite", bucket_key=ghost_bucket),
    )
    corrupted = PipelineState(claims=claims, candidates=frozenset(), findings=findings)
    clean = PipelineState(claims={}, candidates=frozenset(), findings=())

    write_manifest(manifest, tmp_path / MANIFEST_FILENAME)
    _write_engine_reads(
        tmp_path,
        clean_state=clean,
        corrupted_state=corrupted,
        routed=frozenset(),
        resolutions=(),
    )
    assert (tmp_path / ENGINE_READS_FILENAME).exists()

    report = report_from_artifacts(tmp_path)
    assert "injection survival" in report
    assert site["corruption_id"] in report, "the laundered site must be named"
    assert "extractor:" in report and ghost_bucket in report
    assert "funnel states" in report

    # the report is over the SAME rescore: the named site is the non-surviving one
    core = rescore_from_artifacts(tmp_path)
    assert site["corruption_id"] in core["per_class_survival"]["non_surviving_sites"]
    assert ghost_bucket in core["corrupted_unexpected_findings"]["extractor_attributed"]


@pytest.mark.skipif(
    not os.environ.get("SYNTH_PROSE_RESCORE"),
    reason="set SYNTH_PROSE_RESCORE=1 to rescore the live prose run's artifacts",
)
def test_prose_run_rescore_report() -> None:
    """Rescore the live prose run's recorded artifacts and print the survival +
    FP + funnel report. Read-only over .scratch/prose_run (never a model call or a
    live table read), so it works against any completed artifact dir and does not
    wait on or disturb a still-running capstone."""
    out_dir = _REPO_ROOT / ".scratch" / "prose_run"
    if not (out_dir / MANIFEST_FILENAME).exists() or not (
        out_dir / ENGINE_READS_FILENAME
    ).exists():
        pytest.skip(f"no completed prose-run artifacts at {out_dir}")
    report = report_from_artifacts(out_dir)
    print("\n" + report)
    assert "injection survival" in report
    assert "funnel states" in report
