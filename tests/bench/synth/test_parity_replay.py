"""PACT ``concord-exact-parity`` — E1 and E2 at scale on a generated history.

``req.parity.per-update`` (task.parity.replay). A seeded 22-update history
replays through COMPLETE-reference execution and through SCHEDULER-GOVERNED
DIFFERENTIAL execution, both consuming the SAME recorded fact set per update
(extraction ran once, in ``history_replay``; each executor deserializes
``recorded-facts/step_NN.json`` and publishes it, and the per-update digests are
compared so a verdict can never be an extraction artifact). After EVERY update
the differential accepted state must equal the reference's — verdict EQUAL, or
EQUAL-with-certified-differences where a certificate licenses a raw-extraction
difference that leaves the resolved DECISION identical — and BOTH executors must
also match gold's expected accepted-result diff for that step.

``req.parity.ledger`` (task.parity.ledger). The same history is charged in three
execution modes and the governed differential is shown to run strictly fewer
steps than dependency-only, itemized by skip cause, with skips-achieved over
gold's skips-achievable reported and shown able to read BELOW 1 when a
dependency-fact mutation invalidates the certificate a dominated step stood on.

The three modes run one after another against the same migrated database with
the runtime returned to the state ``clean_runtime`` yields in between, so each
starts from an identical empty graph — the comparison is of execution modes, not
of starting states. Each mode's per-update snapshots are extracted at the time of
that update and kept as plain data, so they survive the reset.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.corruption import Planner  # noqa: E402
from okg.bench.synth.goldseal import build_manifest  # noqa: E402
from okg.bench.synth.history import HistoryConfig, build_history  # noqa: E402
from okg.bench.synth.history_replay import replay_history  # noqa: E402
from okg.bench.synth.parity import (  # noqa: E402
    MODE_COMPLETE,
    MODE_DEPENDENCY,
    MODE_DIFFERENTIAL,
    MUTATION_DEPENDENCY_FACT,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    Mutation,
    grade_update,
    load_parity_history,
    render_ledger_table,
    render_verdict_table,
    replay_mode,
)
from okg.bench.synth.prf import SynthPrf  # noqa: E402
from okg.bench.synth.render import render_corpus  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    constraint_policy_facts,
    functional_policies,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency.ir_check import (  # noqa: E402
    load_ir_consistency_checks,
)
from okg.substrate.consistency.reuse_scheduler import (  # noqa: E402
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_CERTIFICATE_VALID,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_SKIP_JUSTIFIED,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
# The acceptance is stated over "a history of at least 20 updates".
_UPDATES = 22


# --------------------------------------------------------------------------- #
# Harness                                                                      #
# --------------------------------------------------------------------------- #
def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir(exist_ok=True)
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir(exist_ok=True)
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _reset_between_modes(dsn: str) -> None:
    """Return the runtime to the state ``clean_runtime`` yields.

    Same table families as the fixture body — which includes
    ``okg.reuse_certificates``, so one mode's certificates can never be read by
    the next. The ontology catalog survives, as it does in the fixture."""
    from tests.pytest.chronos_reset import reset_chronos_graph
    from tests.substrate.conftest import (
        _DERIVED_RUNTIME_TABLES,
        _RUNTIME_TABLES,
        _clean_runtime_subset,
    )

    _clean_runtime_subset(
        dsn,
        _RUNTIME_TABLES + _DERIVED_RUNTIME_TABLES,
        operation="reset the runtime between parity execution modes",
        reset_generations=True,
        clean_derived_schema=True,
    )
    reset_chronos_graph(dsn)


def _build_history(world, smoke_config, updates: int = _UPDATES):
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    history = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs,
        history_config=HistoryConfig(updates=updates),
    )
    return history, manifest


def _seal_recorded_history(dsn: str, tmp_path: Path, world, smoke_config, updates):
    """Produce the self-contained replay artifact directory the parity driver
    consumes: the sealed history, the per-update recorded fact sets, and the
    direct-path replay report. Extraction happens HERE and only here."""
    history, manifest = _build_history(world, smoke_config, updates)
    report = replay_history(
        dsn=dsn, deployment_dir=_DEP_DIR, history=history, manifest=manifest,
        out_dir=tmp_path / "replay",
    )
    assert report["expected_diff_problems"] == [], report["expected_diff_problems"][:3]
    extra, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    checks = tuple(load_ir_consistency_checks(_DEP_DIR))
    assert checks, "the smoke deployment must declare an IR check"
    return load_parity_history(tmp_path / "replay"), policies, checks, report


# =========================================================================== #
# req.parity.per-update — task.parity.replay                                   #
# =========================================================================== #
@pytest.mark.postgres
def test_exact_parity_holds_after_every_update_of_a_generated_history(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """After EVERY update the differential accepted state equals the reference's,
    and both agree with gold."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _UPDATES
    )
    assert len(parity.updates) >= 20, "the acceptance is stated over >= 20 updates"

    _reset_between_modes(dsn)
    reference = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-complete",
        parity=parity, mode=MODE_COMPLETE, policies=policies, checks=checks,
    )
    _reset_between_modes(dsn)
    differential = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-diff",
        parity=parity, mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
    )

    verdicts = [
        grade_update(
            gold_step=parity.gold_step(i),
            reference=reference.executions[i],
            differential=differential.executions[i],
        )
        for i in range(len(parity.updates))
    ]
    print("\n[parity] per-update three-way agreement "
          "(differential vs complete reference vs gold):")
    print(render_verdict_table(verdicts))

    # ---- ACCEPTANCE: three-way agreement at EVERY update -------------------- #
    for verdict in verdicts:
        assert verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED), (
            f"update {verdict.update}: differential != reference "
            f"({verdict.verdict}); withheld={verdict.withheld_claims}"
        )
        assert not verdict.gold_problems, (
            f"update {verdict.update}: the DIFFERENTIAL disagrees with gold: "
            f"{verdict.gold_problems[:3]}"
        )
        assert not verdict.reference_gold_problems, (
            f"update {verdict.update}: the REFERENCE disagrees with gold: "
            f"{verdict.reference_gold_problems[:3]}"
        )
        assert verdict.agrees, verdict

    # ---- ACCEPTANCE: both executors consumed byte-identical fact sets ------- #
    for index, update in enumerate(parity.updates):
        ref, diff = reference.executions[index], differential.executions[index]
        assert ref.consumed_digest == update.digest == diff.consumed_digest, (
            f"update {index}: the two executors did not consume the recorded "
            f"bytes (recorded={update.digest[:12]} "
            f"reference={ref.consumed_digest[:12]} "
            f"differential={diff.consumed_digest[:12]})"
        )
    print(f"[parity] {len(parity.updates)} updates, both executors on the "
          f"recorded bytes; first digest {parity.updates[0].digest[:16]}")

    # ---- the comparison is non-vacuous ------------------------------------- #
    last_ref = reference.executions[-1].snapshot
    assert len(last_ref.effective_claims) > 100, len(last_ref.effective_claims)
    assert len(last_ref.resolution_outcomes) > 0
    assert len(last_ref.selections) > 0
    # An update that changes nothing in the graph reuses the previous
    # generation (unchanged payloads are not rewritten). That is legitimate
    # only when the sealed gold expects no change for that update, and most
    # updates must still publish, or the parity comparison is vacuous.
    executions = reference.executions
    reused = [
        index for index in range(1, len(executions))
        if executions[index].generation_id == executions[index - 1].generation_id
    ]
    for index in reused:
        assert not parity.gold_step(index).expected_diff["changed"], (
            f"update {index} reused generation {executions[index].generation_id} "
            "but the sealed gold expected a change"
        )
    assert len(reused) <= _UPDATES // 4, reused
    assert len({e.generation_id for e in executions}) == _UPDATES - len(reused), (
        "the reference must publish a distinct generation per changing update"
    )
    print(
        f"[parity] reference state at the last pin: "
        f"{len(last_ref.effective_claims)} effective claims, "
        f"{len(last_ref.resolution_outcomes)} resolutions, "
        f"{len(last_ref.selections)} selections, "
        f"{len(last_ref.open_findings)} open findings"
    )

    # ---- ACCEPTANCE: all four ladder rungs, from the SKIP-CAUSE accounting --- #
    skip_causes = {
        cause
        for execution in differential.executions
        for cause in execution.ledger.skips_by_cause
    }
    run_causes = {
        cause
        for execution in differential.executions
        for cause in execution.ledger.run_causes
    }
    assert CAUSE_CERTIFICATE_VALID in skip_causes, skip_causes
    assert CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED in skip_causes, skip_causes
    assert CAUSE_SKIP_JUSTIFIED in skip_causes, skip_causes
    assert CAUSE_SKIP_DOMINANCE in skip_causes, (
        f"no genuine changed-inputs (bounded-dominance) skip: {skip_causes}"
    )
    assert CAUSE_EFFECTS_OVERLAP in run_causes, run_causes
    rung_updates = {
        cause: [
            e.index for e in differential.executions if cause in e.ledger.skips_by_cause
        ]
        for cause in sorted(skip_causes)
    }
    print("[parity] ladder rungs, read from the skip-cause accounting:")
    for cause, updates in sorted(rung_updates.items()):
        print(f"    {cause:<40} updates {updates}")
    print(f"    {'effects_overlap (RUN)':<40} updates "
          f"{[e.index for e in differential.executions if e.ledger.steps_run]}")

    # ---- ACCEPTANCE: the certified verdict, proven TWO-SIDED ---------------- #
    certified = [v for v in verdicts if v.verdict == VERDICT_CERTIFIED]
    assert certified, "no EQUAL-with-certified-differences verdict in the history"
    first = certified[0]
    ref_exec = reference.executions[first.update]
    diff_exec = differential.executions[first.update]
    ref_eff = set(ref_exec.snapshot.effective_claims)
    diff_eff = set(diff_exec.snapshot.effective_claims)
    # (a) the RAW states differ: the differential never extracted the loser.
    assert diff_eff < ref_eff, (
        "a certified verdict with no raw-extraction difference is a plain EQUAL "
        "wearing the wrong label"
    )
    never_extracted = ref_eff - diff_eff
    assert never_extracted == set(first.withheld_claims), (
        never_extracted, first.withheld_claims
    )
    # (b) the resolved DECISIONS do not differ.
    for bucket, cell in ref_exec.snapshot.resolution_outcomes.items():
        other = diff_exec.snapshot.resolution_outcomes[bucket]
        for field in ("outcome", "winner_claim_id", "basis"):
            assert cell[field] == other[field], (bucket, field, cell, other)
    print(
        f"\n[parity/certified] update {first.update}: raw states DIFFER "
        f"({len(never_extracted)} never-extracted claim(s): "
        f"{sorted(never_extracted)}), resolved decisions IDENTICAL across "
        f"{len(ref_exec.snapshot.resolution_outcomes)} buckets; covered buckets "
        f"{first.covered_buckets} certificates "
        f"{[d[:12] for d in first.certificate_digests]}"
    )

    # (c) NON-VACUITY: without the certificate the SAME divergence is a MISMATCH.
    from okg.substrate.consistency.equivalence_differ import (
        VERDICT_MISMATCH as DIFFER_MISMATCH,
        CertifiedDifferences,
        diff_resolved_states,
    )

    uncovered = diff_resolved_states(ref_exec.snapshot, diff_exec.snapshot)
    assert uncovered.verdict == DIFFER_MISMATCH, (
        "the certified verdict is decorative: the same divergence passes without "
        "a certificate, so the licence proves nothing"
    )
    wrong = diff_resolved_states(
        ref_exec.snapshot, diff_exec.snapshot,
        certified_differences=CertifiedDifferences(
            covered_buckets=frozenset({"a-bucket-no-certificate-covers"}),
        ),
    )
    assert wrong.verdict == DIFFER_MISMATCH, (
        "a certificate covering the WRONG bucket licensed the divergence"
    )
    print(
        f"[parity/certified] the same pair WITHOUT the licence -> "
        f"{uncovered.verdict}; with a wrong-bucket licence -> {wrong.verdict}"
    )

    # (d) a plain-EQUAL update really has nothing to certify.
    plain = [v for v in verdicts if v.verdict == VERDICT_EQUAL]
    assert plain, "every update certified something — the EQUAL arm is untested"
    for verdict in plain:
        assert not verdict.withheld_claims, (
            f"update {verdict.update} reported plain EQUAL while withholding "
            f"{verdict.withheld_claims}"
        )

    # ---- F3: a certificate written under update k's CANDIDATE label is found
    #      and revalidated at update k+1 ------------------------------------- #
    revalidating = [
        e for e in differential.executions
        if CAUSE_CERTIFICATE_VALID in e.ledger.skips_by_cause
    ]
    assert revalidating, "no certificate was ever revalidated across updates"
    witness = revalidating[0]
    prior = differential.executions[witness.index - 1]
    with psycopg.connect(dsn, autocommit=True) as conn:
        from okg.substrate.consistency.reuse_certificates import (
            ReuseCertificateStore,
        )

        stored = ReuseCertificateStore(
            deployment="synthetic-corpus-smoke"
        ).for_generation(conn, prior.candidate_generation_id)
    assert stored, (
        f"no certificate rows under the prior candidate label "
        f"{prior.candidate_generation_id!r} — the keying decision does not hold"
    )
    revalidated_steps = {
        step for step, cause in witness.skip_outcomes
        if cause == CAUSE_CERTIFICATE_VALID
    }
    assert revalidated_steps <= {c.step_id for c in stored}, (
        "a step revalidated a certificate that is not in the prior candidate's "
        "generation — the certificates came from somewhere else"
    )
    print(
        f"\n[parity/F3] certificates are keyed by the CANDIDATE label: "
        f"{len(stored)} rows written under {prior.candidate_generation_id!r} "
        f"(update {prior.index}) were read back and revalidated by "
        f"{len(revalidated_steps)} steps at update {witness.index}. "
        f"candidate -> published bindings recorded for all "
        f"{len(differential.bindings)} updates."
    )
    absorbed = [b for b in differential.bindings if b["published_generation_id"] is None]
    print(
        f"[parity/F3] {len(absorbed)} update(s) were ABSORBED (every document "
        f"step skipped, so the producer was never reached and no generation was "
        f"minted): {[b['update'] for b in absorbed]}"
    )

    # the direct-path replay that sealed the recorded facts published too, so the
    # report can name all three sets of pins.
    print(
        f"[parity] direct-path seal: {direct['updates']} updates, "
        f"{len(direct['skippable_steps'])} gold-skippable steps at "
        f"{direct['skippable_steps']}"
    )


# =========================================================================== #
# req.parity.ledger — task.parity.ledger                                       #
# =========================================================================== #
@pytest.mark.postgres
def test_work_ledger_charges_three_modes_on_one_history(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """Complete, dependency-only, and governed differential charged on the SAME
    history, with the differential shown to run strictly fewer steps."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _UPDATES
    )

    runs = {}
    for mode in (MODE_COMPLETE, MODE_DEPENDENCY, MODE_DIFFERENTIAL):
        _reset_between_modes(dsn)
        runs[mode] = replay_mode(
            dsn=dsn, deployment_dir=_DEP_DIR,
            corpus_dir=tmp_path / f"corpus-{mode}", parity=parity, mode=mode,
            policies=policies, checks=checks,
        )
    ordered = [runs[m] for m in (MODE_COMPLETE, MODE_DEPENDENCY, MODE_DIFFERENTIAL)]

    print("\n[ledger] three execution modes charged on ONE history:")
    print(render_ledger_table(ordered))
    print("\n[ledger] per-update rows (differential):")
    for row in runs[MODE_DIFFERENTIAL].ledger():
        print(f"    {json.dumps(row.to_json_dict(), sort_keys=True)}")

    complete = runs[MODE_COMPLETE].totals()
    dependency = runs[MODE_DEPENDENCY].totals()
    differential = runs[MODE_DIFFERENTIAL].totals()

    # ---- ACCEPTANCE: ledger rows for all three modes, from run output ------- #
    for totals in (complete, dependency, differential):
        assert totals["updates"] == _UPDATES
    assert complete["steps_run"] == complete["steps_reached"], (
        "complete execution must run everything it reached"
    )
    assert complete["steps_skipped"] == 0 and complete["certificates_written"] == 0
    assert dependency["steps_run"] == dependency["steps_reached"], (
        "dependency-only must run everything it reached — its only saving is the "
        "pruned worklist seed"
    )
    assert dependency["steps_skipped"] == 0 and dependency["certificates_written"] == 0

    # ---- ACCEPTANCE: differential runs STRICTLY FEWER steps ----------------- #
    assert differential["steps_run"] < dependency["steps_run"], (
        f"governed differential ran {differential['steps_run']} steps, "
        f"dependency-only ran {dependency['steps_run']} — no saving"
    )
    assert dependency["steps_run"] < complete["steps_run"]
    print(
        f"\n[ledger] steps RUN: complete={complete['steps_run']} > "
        f"dependency_only={dependency['steps_run']} > "
        f"differential={differential['steps_run']} "
        f"(differential saves {dependency['steps_run'] - differential['steps_run']} "
        f"steps over dependency pruning alone)"
    )
    print("[ledger] the differential's saving, itemized by skip cause:")
    for cause, count in sorted(differential["skips_by_cause"].items()):
        print(f"    {cause:<40} {count:>5}")
    assert set(differential["skips_by_cause"]) >= {
        CAUSE_CERTIFICATE_VALID, CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
        CAUSE_SKIP_JUSTIFIED, CAUSE_SKIP_DOMINANCE,
    }, differential["skips_by_cause"]
    assert differential["certificates_written"] == differential["steps_skipped"], (
        "every skip must be behind a certificate (§9.5)"
    )
    assert differential["certificates_invalidated"] > 0, (
        "no certificate was ever invalidated — the mechanism never bit"
    )

    # ---- ACCEPTANCE: skips achieved over gold's skips achievable ------------ #
    for totals in (complete, dependency, differential):
        print(
            f"[ledger] {totals['mode']:<16} skips achieved/achievable = "
            f"{totals['skips_achieved']}/{totals['skips_achievable']}; "
            f"steps NOT run/achievable = "
            f"{totals['unrun_achievable']}/{totals['skips_achievable']}"
        )
    assert complete["skips_achieved"] == 0, (
        "complete execution must read a visible ZERO against gold's denominator"
    )
    assert complete["unrun_achievable"] == 0
    assert differential["skips_achieved"] > 0
    assert differential["skips_achievable"] > 0
    assert differential["unrun_achievable"] > dependency["unrun_achievable"], (
        "the certificate mechanism must save work dependency pruning does not"
    )

    # ---- ACCEPTANCE: the ratio is shown able to read BELOW 1 ---------------- #
    # Take the first update the differential absorbed behind a dominance skip and
    # re-run the same prefix with ONE dependency-fact class mutated. Every
    # certificate the reached steps stood on invalidates, so the steps must RUN
    # and the achieved/achievable ratio for that update collapses.
    dominated = next(
        e for e in runs[MODE_DIFFERENTIAL].executions
        if CAUSE_SKIP_DOMINANCE in e.ledger.skips_by_cause
    )
    baseline = dominated.ledger
    _reset_between_modes(dsn)
    forced = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-forced",
        parity=parity, mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
        limit=dominated.index + 1,
        mutations={dominated.index: Mutation(kind=MUTATION_DEPENDENCY_FACT)},
    )
    mutated = forced.executions[-1].ledger
    print(
        f"\n[ledger/<1] update {dominated.index}: unmutated "
        f"skips {baseline.skips_achieved}/{baseline.skips_achievable} "
        f"(ratio {baseline.achieved_ratio:.3f}, "
        f"{baseline.skips_by_cause}); with ONE dependency-fact class "
        f"({MUTATION_DEPENDENCY_FACT}) invalidated: "
        f"{mutated.skips_achieved}/{mutated.skips_achievable} "
        f"(ratio {mutated.achieved_ratio:.3f}, run={mutated.steps_run}, "
        f"invalidated classes {mutated.invalidated_classes})"
    )
    assert baseline.achieved_ratio is not None and mutated.achieved_ratio is not None
    assert mutated.achieved_ratio < 1.0, (
        "the skips-achieved metric could not be made to read below 1"
    )
    assert mutated.achieved_ratio < baseline.achieved_ratio, (
        f"invalidating a certificate did not cost the ratio anything "
        f"({baseline.achieved_ratio} -> {mutated.achieved_ratio})"
    )
    assert mutated.steps_run > baseline.steps_run, (
        "the invalidation did not force any step to run"
    )
    # Every CERTIFICATE-BACKED skip is gone: the invalidated class made both the
    # revalidation rung and the degenerate carry-forward rung inapplicable.
    assert CAUSE_CERTIFICATE_VALID not in mutated.skips_by_cause, (
        f"a certificate revalidated after its dependency moved: "
        f"{mutated.skips_by_cause}"
    )
    assert CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED not in mutated.skips_by_cause, (
        f"the unchanged-inputs rung fired with a changed input: "
        f"{mutated.skips_by_cause}"
    )

    # FINDING, recorded rather than asserted away: a certificate invalidation is
    # NOT a universal force-this-step-to-run lever. The two rungs that CARRY a
    # prior justification (revalidation, degenerate carry-forward) both fall away,
    # but the ladder then re-compiles the effects and the bounded-dominance rung
    # RE-DERIVES its justification from the recorded incumbent decision and the
    # step's declared envelope — neither of which a constraint-set change touches
    # — so those steps skip again behind FRESH dominance certificates. That is
    # sound (the justification is proved, not carried) and it is why the ratio
    # lands above zero rather than at it.
    assert set(mutated.skips_by_cause) <= {CAUSE_SKIP_DOMINANCE}, (
        mutated.skips_by_cause
    )
    print(
        f"[ledger/<1] FINDING: the invalidation removed every CARRIED "
        f"justification (certificate_valid, certificate_degenerate_unchanged) "
        f"but {mutated.skips_by_cause.get(CAUSE_SKIP_DOMINANCE, 0)} step(s) "
        f"still skipped behind a FRESHLY DERIVED bounded-dominance certificate — "
        f"the dominance rung re-proves itself from the incumbent decision and the "
        f"declared envelope, which a dependency-fact change does not touch."
    )

    # ---- and the forced re-execution is still CORRECT ---------------------- #
    forced_verdict = grade_update(
        gold_step=parity.gold_step(dominated.index),
        reference=runs[MODE_COMPLETE].executions[dominated.index],
        differential=forced.executions[-1],
    )
    assert forced_verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED), (
        f"the forced re-execution broke parity: {forced_verdict.verdict}"
    )
    assert not forced_verdict.gold_problems, forced_verdict.gold_problems[:3]
    print(
        f"[ledger/<1] parity after the forced re-execution: "
        f"{forced_verdict.verdict} (gold ok) — safety costs work, never "
        f"correctness"
    )
