"""The capstone composition flow: all four (C, Σ, ≻) axes in ONE run.

task.complete.capstone of synth-benchmark-complete. Every child landed and was
proven SEPARATELY; this is where they collide in a single generation. The flow
is three pinned generations over ONE sealed manifest:

    gen1  clean publish        (arm stamps + coverage policies)
    gen2  corrupted publish     (arm stamps + coverage policies)  -> SCORED
    gen3  corrections publish   (gen2's exact facts + correction nodes)

and one capstone report that carries, ALL READ FROM RUN OUTPUT (never config):

    (a) the stage-attributed funnel, exhaustive over the corruption set;
    (b) a basis histogram with all FIVE decisive precedence bases (gen2 rows);
    (c) a finding histogram with all FOUR Σ constraint kinds (gen2 findings);
    (d) at least one corruption whose C_eff membership changed via a correction
        (the claim_effective delta gen2 -> gen3);
    (e) prose documents published through the LLM lane (llm runs only);
    (f) per-class injection survival;
    (g) detection and repair rates in BOTH conditional and end-to-end form;
    (h) mis_repaired > 0 over sites whose REALIZED post-extraction vote preserved
        the planned corrupted majority, their count meeting the declared floor.

Nothing here re-implements scoring: build_synth_scorecard, funnel,
corrections_report, effective_claims_at and the artifact writers are reused
verbatim. What is new is the ORCHESTRATION of three generations and the gate that
fails loudly naming any axis the run did not carry.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

from okg.bench.synth.arms import ARM_PLAN_SCHEMA
from okg.bench.synth.config import (
    ADMITTABLE_SIGNALS,
    SynthConfig,
    load_preset,
)
from okg.bench.synth.corruption import CONSTRAINT_KINDS, Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import (
    build_manifest,
    scan_unsanctioned_surface,
    write_manifest,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import pack_bytes, render_corpus
from okg.bench.synth.resolution import (
    OUTCOME_CANONICAL,
    STATE_MIS_REPAIRED,
    STATE_REMOVED_FROM_SCOPE,
    funnel,
    poisoned_corruptions_of,
)
from okg.bench.synth.runner import (
    MANIFEST_FILENAME,
    RUN_BINDING_FILENAME,
    SCORECARD_FILENAME,
    LlmLane,
    RunnerError,
    _labels_for_world,
    _packaged_world_dir,
    _subset_world,
    _write_docs,
    _write_engine_reads,
    _write_llm_artifacts,
    apply_arm_stamps,
    apply_provenance_stamps,
    build_synth_scorecard,
    constraint_policy_facts,
    correction_facts,
    detection_state,
    effective_claims_at,
    extract_facts,
    finding_kinds,
    functional_policies,
    publish_fact_list,
    resolutions_at,
    routed_buckets,
    synth_survival_equivalent,
)
from okg.bench.synth.runner import (
    corrections_report as _corrections_report,
)
from okg.bench.synth.corrections import plan_corrections

CAPSTONE_FILENAME = "synth-capstone.json"
CAPSTONE_SCHEMA = "okg.synth.capstone.v1"


class CapstoneGateError(RunnerError):
    """The single capstone run did not carry one or more of the four axes.

    Raised AFTER every artifact (scorecard, engine reads, capstone report) is on
    disk, so a failed capstone stays fully inspectable, and it names EVERY missing
    axis at once rather than tripping on the first — the point of a capstone is to
    see which collisions survived, not to stop at the earliest.
    """


def _arm_stamped_claims(arm_plan: Any) -> frozenset[str]:
    if not arm_plan:
        return frozenset()
    return frozenset(
        claim_id
        for target in arm_plan.get("targets", ())
        for claim_id in target.get("stamps", {})
    )


def run_synth_capstone(
    *,
    dsn: str,
    deployment_dir: Path,
    out_dir: Path,
    preset: str = "capstone",
    config: SynthConfig | None = None,
    world_dir: Path | None = None,
) -> dict[str, Any]:
    """Compose the four axes in one run and gate on all of (a)-(h).

    Returns ``{"manifest", "scorecard", "capstone", "clean", "corrupted",
    "corrected", "corrections"}``. Raises :class:`CapstoneGateError` if any axis
    is missing — but only after the full artifact set is written.
    """
    config = config or load_preset(preset)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    world_dir = Path(world_dir) if world_dir else _packaged_world_dir(config)

    # -- generate -> corrupt -> render -> seal (one manifest, all generations) --
    provenance = json.loads((world_dir / "provenance.json").read_text(encoding="utf-8"))
    world = world_from_synthea_dir(
        world_dir,
        reference_date=provenance["reference_date"],
        provenance=provenance,
        max_facts_per_patient=config.scale.max_facts_per_patient,
    )
    world = _subset_world(world, config.scale.patients)

    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    manifest = build_manifest(config=config, world=world, plan=plan, rendered=rendered)

    leakage = scan_unsanctioned_surface(
        manifest,
        {
            "config": config.canonical_json_bytes().decode("utf-8"),
            "template_pack": pack_bytes().decode("utf-8"),
        },
    )
    if leakage:
        raise RunnerError(f"leakage on unsanctioned surface: {leakage}")

    clean_dir = out_dir / "corpus_clean"
    corrupted_dir = out_dir / "corpus_corrupted"
    _write_docs(clean_dir, rendered.clean_docs)
    _write_docs(corrupted_dir, rendered.corrupted_docs)
    write_manifest(manifest, out_dir / MANIFEST_FILENAME)

    lane: LlmLane | None = None
    if config.realism.extractor == "llm":
        from okg.bench.synth.llm_extract import LlmExtractor

        lane = LlmLane(extractor=LlmExtractor(labels=_labels_for_world(world)))

    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    arm_plan = manifest.get("precedence_arm", {}).get("plan")
    arm_admits = tuple(config.precedence_arm.admits)
    provenance_plan = manifest.get("provenance")

    def _build(corpus_dir: Path, phase: str) -> list[Any]:
        facts = extract_facts(corpus_dir, policies, lane=lane, phase=phase)
        facts = apply_provenance_stamps(facts, provenance_plan)
        facts = apply_arm_stamps(
            facts,
            arm_plan,
            arm_admits,
            provenance_admitted=bool((provenance_plan or {}).get("enabled")),
        )
        return list(facts) + list(extra_policies)

    ledger: list[dict[str, Any]] = []

    # -- gen1: clean --------------------------------------------------------
    started = time.monotonic()
    clean_facts = _build(clean_dir, "clean")
    out_clean, clean_ids = publish_fact_list(dsn, deployment_dir, clean_facts)
    ledger.append({"stage": "clean_publish", "latency_ms": (time.monotonic() - started) * 1000})
    clean_state = detection_state(dsn, out_clean["generation_id"])

    # -- gen2: corrupted (SCORED) ------------------------------------------
    started = time.monotonic()
    corrupted_facts = _build(corrupted_dir, "corrupted")
    out_corrupted, corrupted_ids = publish_fact_list(
        dsn, deployment_dir, corrupted_facts, clean_ids
    )
    ledger.append({"stage": "corrupted_publish", "latency_ms": (time.monotonic() - started) * 1000})
    corrupted_state = detection_state(dsn, out_corrupted["generation_id"])
    routed_snapshot = routed_buckets(dsn)
    resolutions_snapshot = resolutions_at(
        dsn, out_corrupted["generation_id"], corrupted_state.claims
    )
    finding_kinds_snapshot = finding_kinds(dsn, out_corrupted["generation_id"])
    core = build_synth_scorecard(
        manifest=manifest,
        clean_state=clean_state,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        config=config,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
    )
    effective_gen2 = effective_claims_at(dsn, out_corrupted["generation_id"])

    # The funnel ROWS (with the per-site realized vote) — the same pure funnel
    # build_synth_scorecard runs, re-called here only to read its per-row realized
    # votes for the majority-preservation gate (rows are per-run detail, kept out
    # of the golden-pinned core).
    gen2_funnel = funnel(
        manifest=manifest,
        extracted_claims=corrupted_state.claims,
        resolutions=resolutions_snapshot,
        findings=corrupted_state.findings,
        equivalent=synth_survival_equivalent,
    )
    if gen2_funnel["states"] != core["funnel"]["states"]:  # pragma: no cover
        raise RunnerError("funnel row rebuild disagrees with the scored funnel")

    # -- gen3: corrections (gen2's exact facts + correction nodes) ----------
    started = time.monotonic()
    corr_plan = plan_corrections(
        manifest,
        SynthPrf(config.seed),
        avoid_claims=_arm_stamped_claims(arm_plan),
        # Only target corruptions whose claim SURVIVED into gen2, so the retract /
        # override C_eff delta is observable on the lossy LLM lane (requirement d).
        present_claims=frozenset(corrupted_state.claims),
    )
    corr_facts = correction_facts(corr_plan)
    out_corrected, _ids = publish_fact_list(
        dsn, deployment_dir, list(corrupted_facts) + corr_facts, corrupted_ids
    )
    ledger.append({"stage": "corrections_publish", "latency_ms": (time.monotonic() - started) * 1000})
    effective_gen3 = effective_claims_at(dsn, out_corrected["generation_id"])
    corr_report = _corrections_report(corr_plan, effective_gen2, effective_gen3)

    # The corrected generation's funnel: the retracted / overridden corruptions
    # land in removed_from_scope, proven from the state tally (reused scorer).
    corrected_state = detection_state(dsn, out_corrected["generation_id"])
    corrected_core = build_synth_scorecard(
        manifest=manifest,
        clean_state=clean_state,
        corrupted_state=corrected_state,
        routed=routed_buckets(dsn),
        config=config,
        resolutions=resolutions_at(
            dsn, out_corrected["generation_id"], corrected_state.claims
        ),
        finding_kinds=finding_kinds(dsn, out_corrected["generation_id"]),
        removed_corruptions=corr_plan.removed_corruption_ids,
    )
    corr_report["funnel_states"] = corrected_core["funnel"]["states"]

    realized_surviving = core["funnel"]["detection_recall"]["conditional"]["denominator"]

    # -- the capstone report + gate -----------------------------------------
    report = _build_capstone_report(
        config=config,
        manifest=manifest,
        core=core,
        resolutions=resolutions_snapshot,
        gen2_funnel=gen2_funnel,
        corr_plan=corr_plan,
        corr_report=corr_report,
        lane=lane,
        generations={
            "clean": str(out_clean["generation_id"]),
            "corrupted": str(out_corrupted["generation_id"]),
            "corrected": str(out_corrected["generation_id"]),
        },
        realized_surviving=realized_surviving,
    )

    # Everything on disk BEFORE any gate raises (inspectable failed capstone).
    (out_dir / SCORECARD_FILENAME).write_text(
        json.dumps(core, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    (out_dir / CAPSTONE_FILENAME).write_text(
        json.dumps(report, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    (out_dir / RUN_BINDING_FILENAME).write_text(
        json.dumps(
            {
                "schema": "okg.synth.run-binding.v1",
                "deployment": "synthetic-corpus-smoke",
                "extractor": config.realism.extractor,
                "clean_generation_id": str(out_clean["generation_id"]),
                "corrupted_generation_id": str(out_corrupted["generation_id"]),
                "corrected_generation_id": str(out_corrected["generation_id"]),
                "surviving_conflict_floor": config.surviving_conflict_floor,
                "surviving_conflict_realized": realized_surviving,
                "surviving_conflict_floor_passed": (
                    realized_surviving >= config.surviving_conflict_floor
                ),
                "extraction_failed_documents": lane.all_failures() if lane else [],
                "extraction_cost": lane.extractor.cost.as_dict() if lane else None,
                "ledger": ledger,
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_engine_reads(
        out_dir,
        clean_state=clean_state,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
    )
    if lane is not None:
        _write_llm_artifacts(out_dir, lane)

    failures = [
        f"{axis}: {row['detail']}"
        for axis, row in sorted(report["axes"].items())
        if row["required"] and not row["passed"]
    ]
    if failures:
        raise CapstoneGateError(
            "capstone run did not carry every axis (read from RUN OUTPUT, not "
            f"config): {failures}. Artifacts are in {out_dir}."
        )

    return {
        "manifest": manifest,
        "scorecard": core,
        "capstone": report,
        "clean": out_clean,
        "corrupted": out_corrupted,
        "corrected": out_corrected,
        "corrections": corr_report,
        "lane": lane,
    }


def _build_capstone_report(
    *,
    config: SynthConfig,
    manifest: Any,
    core: Any,
    resolutions: Any,
    gen2_funnel: Any,
    corr_plan: Any,
    corr_report: Any,
    lane: LlmLane | None,
    generations: dict[str, str],
    realized_surviving: int,
) -> dict[str, Any]:
    """Assemble the axis-by-axis capstone report from RUN OUTPUT only."""
    is_llm = config.realism.extractor == "llm"

    # (b) basis histogram over the published gen2 canonical resolutions.
    basis_histogram = Counter(
        r.basis for r in resolutions if r.outcome == OUTCOME_CANONICAL
    )
    missing_bases = sorted(ADMITTABLE_SIGNALS - set(basis_histogram))

    # (c) finding-kind histogram over the published gen2 findings. Loudness is on
    # the END-TO-END denominator (a kind that never fired end-to-end), exactly
    # like the run gate — the conditional zero is data, not a gate.
    by_kind = core["per_kind_detection"]["by_kind"]
    required_kinds = tuple(config.require_constraint_kinds) or CONSTRAINT_KINDS
    missing_kinds = sorted(
        k
        for k in required_kinds
        if by_kind.get(k, {}).get("end_to_end", {}).get("detected", 0) < 1
    )

    # (d) C_eff membership change via a correction (retract removes a corruption's
    #     claim; assert adds an operator claim) — from the two claim_effective reads.
    retracted = list(corr_report["retracted_absent"])
    asserted = [row["claim_id"] for row in corr_report["asserted_present"]]
    removed_from_scope = corr_report["funnel_states"].get(STATE_REMOVED_FROM_SCOPE, 0)

    # (h) realized-majority-preserved mis-repair. The gold "corruptions" list holds
    # one row per corrupted RENDERING, so a majority-visibility corruption
    # (visibility_corrupted > 1) is exactly one whose corruption_id appears in more
    # than one corrupted-site row — read from the sealed gold, never the config.
    # POISONED buckets are excluded (synth-authority-poisoning): a wrong-authority
    # flip is the poison axis's evidence, not axis h's preserved-majority mis-repair,
    # so it must not leak into this count.
    poisoned_corruptions = poisoned_corruptions_of(manifest)
    rows_by_cid = {r["corruption_id"]: r for r in gen2_funnel["rows"]}
    corrupted_renderings = Counter(
        s["corruption_id"] for s in manifest["gold"]["corruptions"]
    )
    majority_ids = sorted(
        cid
        for cid, n in corrupted_renderings.items()
        if n > 1 and cid not in poisoned_corruptions
    )
    preserved: list[str] = []
    mis_repaired_preserved: list[str] = []
    for cid in majority_ids:
        row = rows_by_cid.get(cid)
        if row is None:  # pragma: no cover - defensive
            continue
        vote = row["realized_vote"]
        # A genuine realized majority-wrong conflict: corrupted still holds the
        # strict majority AND a clean counterpart survived to be wrong about
        # (clean == 0 is unanimous, not a conflict — no mis-repair is possible).
        if vote["corrupted"] > vote["clean"] and vote["clean"] >= 1:
            preserved.append(cid)
            if row["state"] == STATE_MIS_REPAIRED:
                mis_repaired_preserved.append(cid)

    funnel_block = core["funnel"]
    axes: dict[str, dict[str, Any]] = {}

    axes["a_funnel_exhaustive"] = {
        "required": True,
        "passed": sum(funnel_block["states"].values()) == funnel_block["total"],
        "states": funnel_block["states"],
        "total": funnel_block["total"],
        "detail": f"{sum(funnel_block['states'].values())} states == {funnel_block['total']} corruptions",
    }
    axes["b_basis_histogram"] = {
        "required": True,
        "passed": not missing_bases,
        "histogram": dict(sorted(basis_histogram.items())),
        "required_bases": sorted(ADMITTABLE_SIGNALS),
        "detail": (
            "all five decisive bases present"
            if not missing_bases
            else f"missing decisive bases: {missing_bases}"
        ),
    }
    axes["c_kind_histogram"] = {
        "required": True,
        "passed": not missing_kinds,
        "by_kind": by_kind,
        "required_kinds": sorted(required_kinds),
        "detail": (
            "all required Σ kinds fired"
            if not missing_kinds
            else f"Σ kind(s) never fired: {missing_kinds}"
        ),
    }
    axes["d_corrections_delta"] = {
        "required": True,
        "passed": bool(retracted) and removed_from_scope >= 1,
        "retracted_absent": retracted,
        "asserted_present": asserted,
        "removed_from_scope": removed_from_scope,
        "population_ok": corr_report["population_ok"],
        "detail": (
            f"C_eff membership changed: {len(retracted)} retracted absent, "
            f"{len(asserted)} asserted present, removed_from_scope={removed_from_scope}"
        ),
    }
    axes["e_prose_llm_lane"] = {
        "required": is_llm,
        "passed": (not is_llm)
        or (
            config.realism.render_mode == "prose"
            and lane is not None
            and any(lane.responses.values())
        ),
        "render_mode": config.realism.render_mode,
        "extractor": config.realism.extractor,
        "extraction_failures": (lane.all_failures() if lane else []),
        "detail": (
            "structured control lane (prose+LLM axis proven by the real run)"
            if not is_llm
            else "prose documents published through the LLM lane"
        ),
    }
    axes["f_per_class_survival"] = {
        "required": True,
        "passed": bool(core["per_class_survival"]["by_class"]),
        "by_class": core["per_class_survival"]["by_class"],
        "non_surviving_sites": core["per_class_survival"]["non_surviving_sites"],
        "detail": f"{len(core['per_class_survival']['by_class'])} classes reported",
    }
    rates_present = (
        "conditional" in funnel_block["detection_recall"]
        and "end_to_end" in funnel_block["detection_recall"]
        and "conditional" in funnel_block["repair_rate"]
        and "end_to_end" in funnel_block["repair_rate"]
    )
    axes["g_detection_repair_rates"] = {
        "required": True,
        "passed": rates_present
        and realized_surviving >= config.surviving_conflict_floor,
        "detection_recall": funnel_block["detection_recall"],
        "repair_rate": funnel_block["repair_rate"],
        "surviving_conflict_floor": config.surviving_conflict_floor,
        "surviving_conflict_realized": realized_surviving,
        "surviving_conflict_floor_passed": (
            realized_surviving >= config.surviving_conflict_floor
        ),
        "detail": (
            f"detection + repair conditional and end-to-end; surviving conflicts "
            f"{realized_surviving} vs floor {config.surviving_conflict_floor}"
        ),
    }
    axes["h_majority_mis_repair"] = {
        "required": True,
        "passed": (
            len(mis_repaired_preserved) >= 1
            and len(preserved) >= config.majority_wrong_floor
        ),
        "majority_sites": majority_ids,
        "realized_majority_preserved": preserved,
        "mis_repaired_preserved": mis_repaired_preserved,
        "majority_wrong_floor": config.majority_wrong_floor,
        "detail": (
            f"mis_repaired={len(mis_repaired_preserved)} over "
            f"{len(preserved)} realized-majority-preserved site(s) "
            f"(floor {config.majority_wrong_floor})"
        ),
    }

    return {
        "schema": CAPSTONE_SCHEMA,
        "preset": config.preset_name,
        "config_digest": manifest["config_digest"],
        "manifest_digest": manifest["manifest_digest"],
        "extractor": config.realism.extractor,
        "render_mode": config.realism.render_mode,
        "arm_plan_schema": ARM_PLAN_SCHEMA,
        "generations": generations,
        "axes": axes,
        "corrections": corr_report,
        "all_required_axes_passed": all(
            row["passed"] for row in axes.values() if row["required"]
        ),
    }
