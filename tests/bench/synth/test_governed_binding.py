"""PACT ``concord-complete-binding`` — the benchmark meets the scheduler.

Two witnesses, one per requirement.

``req.bind.governed-path`` — a benchmark-generated corpus publishes through the
SCHEDULER-GOVERNED path: the reached-step worklist (seeded with the rendered
documents, extended by each RUN's observed dependents) processes to a fixed
point, the commit gate commits, and only then does the checkpoint writer publish.
The resulting generation is a first-class published generation the EXISTING
scorer reads at a pin. On a first publish the run/skip accounting shows ZERO
skips, and every run is an ``effects_overlap`` refusal — the change genuinely
reaches the step — never ``run_always``, which would make the zero read vacuous.

``req.bind.parity`` — the SAME rendered corpus published through the direct path
and through the governed path (complete mode, reuse disabled) yields the same
resolved state, compared ROW-WISE at the two pins over accepted claims, findings,
resolutions, and winners, and the same benchmark scorecard byte-for-byte after
generation-identifier normalization. Both non-vacuity obligations are witnessed:
the non-empty guard FIRES on an empty comparison, and a value mutated in a COPY
of one pin's state FAILS the comparison naming the row.

The two paths run against the same migrated database, one after the other, with
the runtime returned to the state ``clean_runtime`` yields in between — so both
start from an identical empty graph and the comparison is of the two publish
paths, not of two starting states. The direct path's rows are snapshotted (plain
data) BEFORE the reset, so they survive it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import load_preset  # noqa: E402
from okg.bench.synth.governed import (  # noqa: E402
    CLAIM_EFFECTIVE_STEP,
    CLAIM_PRODUCER_STEP,
    CLAIM_PROJECTION_STEP,
    FAMILY_ACCEPTED_CLAIMS,
    FAMILY_WINNERS,
    GENERATION_PLACEHOLDER,
    GovernedRunError,
    ParityError,
    ResolvedStateRows,
    assert_fact_parity,
    assert_non_empty,
    assert_scorecard_parity,
    assert_zero_skips,
    build_corpus_cone,
    compare_resolved_states,
    document_step_id,
    normalize_scorecard,
    resolved_state_rows,
    run_governed_benchmark,
)
from okg.bench.synth.runner import (  # noqa: E402
    MANIFEST_FILENAME,
    extract_facts,
    run_synth_benchmark,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency.reuse_scheduler import (  # noqa: E402
    ACTION_RUN,
    ACTION_SKIP,
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_EFFECTS_OVERLAP,
    TERMINAL_FIXED_POINT,
    ReusePassResult,
    StepOutcome,
)

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


def _direct_ingest_seam_facts(
    corpus_dir: Path, manifest: dict, config
) -> tuple[list, tuple[str, ...], list]:
    """The DIRECT path's fact list for one corpus, through its own ingest seam.

    Mirrors ``ingest_and_publish`` exactly (one-shot extraction, then the
    provenance and precedence-arm stamps, then the coverage Σ policies), so the
    governed path's per-document composition is compared against the real
    reference and not against a hand-rebuilt approximation."""
    from okg.bench.synth.runner import (
        apply_arm_stamps,
        apply_provenance_stamps,
        constraint_policy_facts,
        functional_policies,
    )

    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    provenance_plan = manifest.get("provenance")
    facts = extract_facts(corpus_dir, policies)
    facts = apply_provenance_stamps(facts, provenance_plan)
    facts = apply_arm_stamps(
        facts,
        manifest.get("precedence_arm", {}).get("plan"),
        tuple(config.precedence_arm.admits),
        provenance_admitted=bool((provenance_plan or {}).get("enabled")),
    )
    return list(facts) + list(extra_policies), policies, list(extra_policies)


def _reset_between_paths(dsn: str) -> None:
    """Return the runtime to exactly the state ``clean_runtime`` yields.

    Same table families, same generation reset, same Chronos truncation as the
    fixture body — so the second path starts from the identical empty graph the
    first did. The ontology catalog survives (as it does in the fixture), which
    is what lets one installed catalog serve both paths."""
    from tests.pytest.chronos_reset import reset_chronos_graph
    from tests.substrate.conftest import (
        _DERIVED_RUNTIME_TABLES,
        _RUNTIME_TABLES,
        _clean_runtime_subset,
    )

    _clean_runtime_subset(
        dsn,
        _RUNTIME_TABLES + _DERIVED_RUNTIME_TABLES,
        operation="reset the runtime between the direct and governed paths",
        reset_generations=True,
        clean_derived_schema=True,
    )
    reset_chronos_graph(dsn)


# =========================================================================== #
# req.bind.governed-path                                                       #
# =========================================================================== #
@pytest.mark.postgres
def test_benchmark_corpus_publishes_through_the_governed_path(
    clean_runtime: str, tmp_path: Path
) -> None:
    """A smoke-preset corpus reaches a published generation THROUGH the update
    protocol, with zero skips on the first publish."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    config = load_preset("smoke")

    # Render the corpus by running the DIRECT path once (it writes corpus_clean/,
    # corpus_corrupted/ and the sealed manifest); the governed run then consumes
    # those exact bytes.
    run_dir = tmp_path / "run"
    direct = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=run_dir
    )
    assert (run_dir / MANIFEST_FILENAME).is_file()
    _reset_between_paths(dsn)

    governed = run_governed_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, run_dir=run_dir, config=config
    )

    # ---- the worklist: seeded with documents, extended by observed dependents -
    for publish in (governed.clean, governed.corrupted):
        docs = tuple(
            step for step in publish.worklist_seed
            if step.startswith("claim_source:")
        )
        assert docs == publish.worklist_seed, (
            "the seed must be the document source steps ONLY — the producer and "
            "the two claim derivations are DISCOVERED as observed dependents"
        )
        assert len(docs) > 1, "a one-document corpus proves nothing about reach"
        reached = set(publish.reached)
        assert reached == set(docs) | {
            CLAIM_PRODUCER_STEP, CLAIM_EFFECTIVE_STEP, CLAIM_PROJECTION_STEP
        }, reached
        # the discovered steps ran AFTER the documents that reached them.
        order = list(publish.executed)
        assert order.index(CLAIM_PRODUCER_STEP) > max(
            order.index(d) for d in docs
        )
        assert order.index(CLAIM_EFFECTIVE_STEP) > order.index(CLAIM_PRODUCER_STEP)
        assert order.index(CLAIM_PROJECTION_STEP) > order.index(
            CLAIM_EFFECTIVE_STEP
        )

        # ---- ACCEPTANCE: zero skips on a first publish, from the accounting ---
        account = assert_zero_skips(
            publish, expected_reached=len(docs) + 3
        )
        print(
            f"\n[governed/{publish.phase}] terminal={account['terminal']} "
            f"reached={account['reached']} runs={account['runs']} "
            f"skips={account['skips']} run_causes={account['run_causes']} "
            f"certificates={account['certificates_committed']} "
            f"candidate={publish.candidate_generation_id} "
            f"-> generation={publish.generation_id} "
            f"(compat {publish.compat_generation_id})"
        )
        assert account["terminal"] == TERMINAL_FIXED_POINT
        assert account["skips"] == 0
        assert account["run_causes"] == [CAUSE_EFFECTS_OVERLAP]
        # zero skips ⇒ zero certificate rows: a skip is the ONLY thing that
        # writes one, so this is the same fact read from the store's side.
        assert account["certificates_committed"] == 0
        assert publish.publish["status"] == "published"

    # The governed clean publish really IS a first publish — the engine says so
    # (``full_reason == "first_publish"``), which is what makes "zero skips" the
    # right expectation rather than a lucky reading of a warm database.
    proj = governed.clean.publish["maintained_derivations"]["claim_projection"]
    assert proj["full_reason"] == "first_publish", proj

    # ---- ACCEPTANCE: the pinned generation is readable by the EXISTING scorer -
    core = governed.scorecard
    assert core["schema"] == "okg.synth.scorecard.v1"
    assert core["extraction"]["denominator"] > 0
    assert core["extraction"]["recovered"] > 0
    assert core["funnel"]["total"] > 0
    assert core["clean_gate_passed"], core["clean_run_fp"]
    detected = core["funnel"]["detection_recall"]["end_to_end"]["detected"]
    assert detected > 0, "a governed generation the scorer reads as empty is a gap"
    print(
        f"[governed] scorecard from the governed pins: extraction "
        f"{core['extraction']['recovered']}/{core['extraction']['denominator']} "
        f"detection e2e {detected}/"
        f"{core['funnel']['detection_recall']['end_to_end']['denominator']} "
        f"clean_fp={core['clean_run_fp']}"
    )

    # ---- the certificate store agrees the pass wrote nothing on a first pass --
    from okg.substrate.consistency.reuse_certificates import ReuseCertificateStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        stored = ReuseCertificateStore(
            deployment=governed.corrupted.deployment
        ).quotable_rows(conn)
    assert stored == [], (
        f"a first publish must record no certificate (zero skips): {stored}"
    )

    # the direct run this corpus came from also published — recorded so the
    # report can name both pins.
    print(
        f"[direct] generations clean={direct['clean']['generation_id']} "
        f"corrupted={direct['corrupted']['generation_id']}"
    )

    # ---- NON-VACUITY: the commit gate is UPSTREAM of the publish -------------
    # A pass that does not reach a fixed point must publish NOTHING. Reach one
    # step with no declared envelope (an unknown dependency, §9.6): the pass
    # DISCARDS, the checkpoint writer never runs, and no generation appears.
    from dataclasses import replace as _replace

    from okg.bench.synth.goldseal import load_manifest
    from okg.bench.synth.governed import build_corpus_cone, governed_publish
    from okg.bench.synth.runner import SMOKE_DEPLOYMENT
    from okg.substrate.consistency.reuse_scheduler import ReuseStep
    from okg.substrate.graph_store import ChronosGraphStore

    manifest = load_manifest(run_dir / MANIFEST_FILENAME)
    _clean_seam, policies, extra_policies = _direct_ingest_seam_facts(
        run_dir / "corpus_clean", manifest, config
    )
    cone = build_corpus_cone(
        run_dir / "corpus_clean", deployment=SMOKE_DEPLOYMENT,
        deployment_dir=_DEP_DIR, policies=policies, phase="discard-probe",
        checks=governed.checks, extra_policy_facts=extra_policies,
    )

    # ---- checks reach the analysis at their EXISTING kind --------------------
    # A check is not a step in the scheduler's vocabulary — it is a read surface
    # the possible-effects compiler maps envelopes onto, and a versioned fact the
    # certificate records. Both are witnessed here, from the deployment's REAL
    # IR check, so "checks map onto their existing kind" is not decorative.
    from okg.substrate.consistency.possible_effects import compile_possible_effects

    assert governed.checks, "the smoke deployment declares an IR check"
    check_id = governed.checks[0].check_id
    probe_step = cone.steps[cone.worklist_seed[0]]
    effects = compile_possible_effects(
        probe_step.envelope, checks=governed.checks,
        claim_predicates=probe_step.claim_predicates,
        producer_id=probe_step.resolved_producer_id(),
    )
    assert check_id in effects.claim_state_check_ids, effects.to_canonical_dict()
    recorded_constraints = cone.current_facts[
        cone.worklist_seed[0]
    ].constraint_set
    assert recorded_constraints.get(check_id) == governed.checks[
        0
    ].program.checksum(), recorded_constraints
    print(
        f"[governed/checks] {check_id} is a claim-state surface in the effects "
        f"summary and a recorded constraint-set fact"
    )
    blinded = dict(cone.steps)
    victim = cone.worklist_seed[0]
    blinded[victim] = ReuseStep(
        step_id=victim, envelope=None, producer_id="claim_source",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        before = len(
            store.list_generations(
                deployment_name=SMOKE_DEPLOYMENT, status="published"
            )
        )
    with pytest.raises(GovernedRunError) as discard_exc:
        governed_publish(
            dsn, run_dir / "corpus_clean", _DEP_DIR, policies,
            phase="discard-probe", checks=governed.checks,
            extra_policy_facts=extra_policies,
            cone=_replace(cone, steps=blinded),
        )
    assert "unknown_dependency" in str(discard_exc.value), discard_exc.value
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        after = len(
            store.list_generations(
                deployment_name=SMOKE_DEPLOYMENT, status="published"
            )
        )
    assert after == before, (
        "a discarded pass must publish NOTHING — the commit gate is upstream of "
        f"the publish ({before} -> {after} published generations)"
    )
    print(
        f"[governed/gate] discarded pass published nothing "
        f"({before} generations before and after)"
    )

    # ---- the REPLAY seam concord-exact-parity will drive --------------------
    # A history step records ONE fact set and every executor replays it verbatim.
    # Driven here over the recorded corrupted facts: the worklist shape is
    # unchanged (each document step owns its recorded slice), nothing is
    # re-extracted, and the published bytes are the recorded ones.
    recorded, _pol, _extra = _direct_ingest_seam_facts(
        run_dir / "corpus_corrupted", manifest, config
    )
    replayed = governed_publish(
        dsn, run_dir / "corpus_corrupted", _DEP_DIR, policies,
        governed.corrupted.claim_ids, phase="replay", checks=governed.checks,
        extra_policy_facts=extra_policies, recorded_facts=recorded,
    )
    assert assert_zero_skips(replayed)["skips"] == 0
    assert assert_fact_parity(list(replayed.facts), recorded) == len(recorded)
    replay_rows = resolved_state_rows(
        dsn, replayed.generation_id, label="governed/replay"
    )
    governed_rows = resolved_state_rows(
        dsn, governed.corrupted.generation_id, label="governed/corrupted"
    )
    replay_report = compare_resolved_states(governed_rows, replay_rows)
    print(
        f"[governed/replay] {len(recorded)} recorded facts published verbatim; "
        f"{replay_report['rows_compared']} rows identical to the "
        f"extraction-mode pin"
    )


# =========================================================================== #
# req.bind.parity                                                              #
# =========================================================================== #
@pytest.mark.postgres
def test_governed_complete_execution_reproduces_the_direct_path(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Row-wise parity at the two pins + a byte-identical scorecard, with both
    non-vacuity obligations witnessed."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    config = load_preset("smoke")
    run_dir = tmp_path / "run"

    # ---- the DIRECT path ----------------------------------------------------
    direct = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=run_dir
    )
    direct_core = direct["scorecard"]
    direct_clean_gen = str(direct["clean"]["generation_id"])
    direct_corrupted_gen = str(direct["corrupted"]["generation_id"])
    direct_clean = resolved_state_rows(
        dsn, direct_clean_gen, label="direct/clean"
    )
    direct_corrupted = resolved_state_rows(
        dsn, direct_corrupted_gen, label="direct/corrupted"
    )
    # The direct path's fact set through its OWN ingest seam, kept for the
    # recorded-facts contract concord-exact-parity will rest on.
    from okg.bench.synth.goldseal import load_manifest

    manifest = load_manifest(run_dir / MANIFEST_FILENAME)
    direct_facts, _policies, _extra = _direct_ingest_seam_facts(
        run_dir / "corpus_corrupted", manifest, config
    )

    _reset_between_paths(dsn)

    # ---- the GOVERNED path (complete mode, reuse disabled) ------------------
    governed = run_governed_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, run_dir=run_dir, config=config
    )
    governed_clean = resolved_state_rows(
        dsn, governed.clean.generation_id, label="governed/clean"
    )
    governed_corrupted = resolved_state_rows(
        dsn, governed.corrupted.generation_id, label="governed/corrupted"
    )

    # ---- ACCEPTANCE: row-wise parity, non-empty guard witnessed --------------
    # The CORRUPTED pin carries all four families and takes the full floor. The
    # CLEAN pin is thin BY CONSTRUCTION — a certified-clean corpus has no
    # contested bucket, so the projection emits no finding and no resolution row
    # (the clean-corpus gate is exactly that claim). Its floor is therefore
    # DECLARED against the family that does carry weight there, never silently
    # relaxed to zero everywhere.
    clean_floor = {
        "accepted_claims": 100, "findings": 0, "resolutions": 0, "winners": 0,
    }
    clean_report = compare_resolved_states(
        direct_clean, governed_clean, minimum=clean_floor
    )
    corrupted_report = compare_resolved_states(direct_corrupted, governed_corrupted)
    print(
        f"\n[parity/clean] compared {clean_report['rows_compared']} rows "
        f"{clean_report['left']['counts']} at pins "
        f"{clean_report['left']['generation_id']} vs "
        f"{clean_report['right']['generation_id']}"
    )
    print(
        f"[parity/corrupted] compared {corrupted_report['rows_compared']} rows "
        f"{corrupted_report['left']['counts']} at pins "
        f"{corrupted_report['left']['generation_id']} vs "
        f"{corrupted_report['right']['generation_id']}"
    )
    # non-trivial by construction: every family carries rows, and the corrupted
    # pin carries the findings a clean pin does not.
    counts = corrupted_report["left"]["counts"]
    assert counts["findings"] > 0 and counts["accepted_claims"] > 10, counts
    assert counts["resolutions"] > 0 and counts["winners"] > 0, counts
    assert direct_clean.row_counts()["findings"] == 0, (
        "the clean corpus must raise no finding — otherwise the corrupted "
        "comparison is not testing what it claims"
    )
    # the declared thin floor is REAL: the full floor refuses the clean pin.
    with pytest.raises(ParityError) as thin_exc:
        assert_non_empty(direct_clean)
    assert "findings" in str(thin_exc.value)

    # ---- ACCEPTANCE: the scorecard, byte-compared after normalization --------
    parity = assert_scorecard_parity(
        direct_core, governed.scorecard,
        generation_ids=[
            direct_clean_gen, direct_corrupted_gen, *governed.generation_ids(),
        ],
    )
    print(
        f"[parity/scorecard] {parity['bytes_compared']} bytes byte-identical "
        f"after normalizing {parity['generation_ids_normalized']} "
        f"(substitutions={parity['placeholder_substitutions']})"
    )
    assert parity["placeholder_substitutions"] == 0, (
        "the scorecard core must be generation-FREE — a core carrying a pin "
        "could compare equal for the wrong reason"
    )

    # ---- the recorded fact sets agree element for element -------------------
    n = assert_fact_parity(list(governed.corrupted.facts), direct_facts)
    print(f"[parity/facts] {n} facts identical element for element")

    # ---- NON-VACUITY 1: a constructed divergence FAILS, naming the row -------
    families = {
        name: {k: dict(v) for k, v in rows.items()}
        for name, rows in governed_corrupted.families.items()
    }
    victim = sorted(families[FAMILY_ACCEPTED_CLAIMS])[0]
    families[FAMILY_ACCEPTED_CLAIMS][victim]["value"] = "MUTATED-BY-THE-WITNESS"
    mutated = ResolvedStateRows(
        label="governed/corrupted(mutated)",
        dsn=governed_corrupted.dsn,
        generation_id=governed_corrupted.generation_id,
        families=families,
    )
    with pytest.raises(ParityError) as excinfo:
        compare_resolved_states(direct_corrupted, mutated)
    message = str(excinfo.value)
    assert FAMILY_ACCEPTED_CLAIMS in message and victim in message, message
    assert "MUTATED-BY-THE-WITNESS" in message
    print(f"[parity/non-vacuity] mutated row caught: {message[:160]}...")

    # a mutated WINNER is caught too (the winners family is compared, not
    # implied by the resolutions family).
    winner_families = {
        name: {k: dict(v) for k, v in rows.items()}
        for name, rows in governed_corrupted.families.items()
    }
    winner_key = sorted(winner_families[FAMILY_WINNERS])[0]
    winner_families[FAMILY_WINNERS][winner_key]["winner_value"] = "WRONG-WINNER"
    with pytest.raises(ParityError) as winner_exc:
        compare_resolved_states(
            direct_corrupted,
            ResolvedStateRows(
                label="governed/corrupted(winner-flipped)",
                dsn=governed_corrupted.dsn,
                generation_id=governed_corrupted.generation_id,
                families=winner_families,
            ),
        )
    assert FAMILY_WINNERS in str(winner_exc.value)
    assert winner_key in str(winner_exc.value)

    # ---- NON-VACUITY 2: the scorecard comparison catches a moved number ------
    moved = dict(governed.scorecard)
    moved["clean_run_fp"] = int(moved["clean_run_fp"]) + 1
    with pytest.raises(ParityError) as score_exc:
        assert_scorecard_parity(
            direct_core, moved, generation_ids=governed.generation_ids()
        )
    assert "clean_run_fp" in str(score_exc.value)


# =========================================================================== #
# The guards, witnessed to FIRE (no database needed).                          #
# =========================================================================== #
def test_non_empty_guard_fires_on_an_empty_comparison() -> None:
    """A silent pass over an empty comparison is itself a failure."""
    empty = ResolvedStateRows(
        label="empty", dsn="", generation_id="7",
        families={name: {} for name in (
            FAMILY_ACCEPTED_CLAIMS, "findings", "resolutions", FAMILY_WINNERS,
        )},
    )
    with pytest.raises(ParityError) as excinfo:
        assert_non_empty(empty)
    assert "trivial" in str(excinfo.value)
    # and the comparison itself refuses BEFORE comparing — two identical empty
    # states must NOT read as agreement.
    with pytest.raises(ParityError):
        compare_resolved_states(empty, empty)


def test_scorecard_normalizer_neutralizes_a_planted_generation_id() -> None:
    """The normalizer is shown to normalize, so its no-op on the real core is a
    measured fact rather than an untested assumption."""
    core = {"pin": "generation-4242", "extraction": {}, "funnel": {},
            "per_class": {}}
    text = normalize_scorecard(core, generation_ids=["generation-4242"])
    assert GENERATION_PLACEHOLDER in text and "4242" not in text
    # two cores differing ONLY in their pin compare equal after normalization…
    other = {**core, "pin": "generation-99"}
    assert assert_scorecard_parity(
        core, other, generation_ids=["generation-4242", "generation-99"]
    )["placeholder_substitutions"] == 1
    # …but a differing measurement still fails.
    with pytest.raises(ParityError):
        assert_scorecard_parity(
            core, {**core, "extraction": {"recovered": 1}},
            generation_ids=["generation-4242"],
        )


def _publish_with(outcomes: tuple[StepOutcome, ...]) -> object:
    from okg.bench.synth.governed import GovernedPublish

    return GovernedPublish(
        phase="clean", deployment="synthetic-corpus-smoke",
        candidate_generation_id="candidate", generation_id="1",
        compat_generation_id="1", publish={}, claim_ids=frozenset(),
        facts=(), executed=(), reached=tuple(o.step_id for o in outcomes),
        worklist_seed=(),
        reuse_summary={
            "result": ReusePassResult(
                deployment="synthetic-corpus-smoke", generation_id="candidate",
                terminal=TERMINAL_FIXED_POINT, outcomes=outcomes,
            ),
            "certificates_committed": sum(
                1 for o in outcomes if o.action == ACTION_SKIP
            ),
        },
    )


def test_zero_skip_witness_fires_on_a_skip_and_on_a_vacuous_run() -> None:
    """The first-publish accounting is falsifiable: a skip fails it, and so does
    a run whose cause is ``run_always`` (an unbounded envelope can never be
    skipped, so it would prove nothing about the ladder)."""
    good = _publish_with((
        StepOutcome(step_id=document_step_id("a.md"), action=ACTION_RUN,
                    cause=CAUSE_EFFECTS_OVERLAP),
        StepOutcome(step_id=CLAIM_PRODUCER_STEP, action=ACTION_RUN,
                    cause=CAUSE_EFFECTS_OVERLAP),
    ))
    assert assert_zero_skips(good, expected_reached=2)["skips"] == 0

    with_skip = _publish_with((
        StepOutcome(step_id=document_step_id("a.md"), action=ACTION_RUN,
                    cause=CAUSE_EFFECTS_OVERLAP),
        StepOutcome(step_id=CLAIM_PRODUCER_STEP, action=ACTION_SKIP,
                    cause=CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
                    certificate_digest="deadbeef"),
    ))
    with pytest.raises(GovernedRunError) as excinfo:
        assert_zero_skips(with_skip)
    assert "SKIPPED" in str(excinfo.value)

    vacuous = _publish_with((
        StepOutcome(step_id=CLAIM_PRODUCER_STEP, action=ACTION_RUN,
                    cause="run_always"),
    ))
    with pytest.raises(GovernedRunError) as vacuous_exc:
        assert_zero_skips(vacuous)
    assert "run_always" in str(vacuous_exc.value)


def test_cone_states_what_a_first_publish_changes(tmp_path: Path) -> None:
    """The zero-skip property comes from a TRUTHFUL change statement.

    The cone must state that a first publish plants every bucket the corpus
    writes, and each step's compiled effects must overlap that statement — that
    overlap, not the ladder, is what refuses the skip.

    Which statement is the right one for a first publish? ``nothing_changed()``
    is WRONG here and ``UNSTATED_CHANGE`` is wrong too: the cone was built from
    the corpus, so it KNOWS which buckets this publish plants. It is neither
    silent nor empty-handed, so it owes the ladder the buckets it named. The
    counterfactual arms below check the two ways of not doing that:

    * a FALSE ``nothing_changed()`` — the caller asserts emptiness — still
      approves a skip for every step. The ladder trusts a statement it cannot
      audit, so truthfulness here is load-bearing and this is why the cone
      states its buckets.
    * ``UNSTATED_CHANGE`` — the caller says nothing — REFUSES. Since PR #972 an
      omitted statement is a distinct type, not the permissive empty set, so
      silence is no longer a second unearned skip mechanism.
    * the ambiguous bare ``ChangedStateTerms()`` that used to spell both is now
      rejected at construction, so the conflation is inexpressible."""
    from okg.substrate.consistency.possible_effects import (
        REFUSE_BUCKET_OVERLAP,
        REFUSE_UNSTATED_CHANGE,
        UNSTATED_CHANGE,
        ChangedStateTerms,
        compile_possible_effects,
        skip_is_justifiable,
    )
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError, match="ambiguous"):
        ChangedStateTerms()

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "a.md").write_text(
        "# Patient Ann Lee\n\n## Vitals\nBody Weight: 82.0 kg\nHeight: 175.0 cm\n"
    )
    (corpus / "b.md").write_text(
        "# Patient Bob Ray\n\n## Vitals\nBody Weight: 90.0 kg\n"
    )
    cone = build_corpus_cone(
        corpus, deployment="synthetic-corpus-smoke", deployment_dir=_DEP_DIR,
        policies=("body_weight", "height"), phase="clean",
    )
    assert cone.worklist_seed == (
        document_step_id("a.md"), document_step_id("b.md")
    )
    assert cone.buckets, "a first publish with no changed bucket is inexpressible"

    for step_id, step in cone.steps.items():
        effects = compile_possible_effects(
            step.envelope, checks=(), claim_predicates=step.claim_predicates,
            producer_id=step.resolved_producer_id(),
        )
        stated = cone.changed_terms[step_id]
        judgment = skip_is_justifiable(effects, changed_state_terms=stated)
        assert not judgment.approved, (
            f"{step_id} would be SKIPPED on a first publish: {judgment}"
        )
        assert judgment.reason == REFUSE_BUCKET_OVERLAP, judgment
        # Counterfactual 1 — the caller LIES and asserts nothing changed. The
        # SAME step is approved for a skip, which is exactly why the binding
        # must state its buckets: the ladder audits the statement's reach, not
        # its truth.
        false_empty = skip_is_justifiable(
            effects, changed_state_terms=ChangedStateTerms.nothing_changed()
        )
        assert false_empty.approved, (
            "an explicit nothing_changed() is expected to approve a skip; if "
            "that ever stops being true this witness must be re-derived"
        )
        # Counterfactual 2 — the caller says NOTHING. Since #972 silence is a
        # distinct type and refuses, so omission is no longer a way to reach
        # counterfactual 1 by accident.
        silent = skip_is_justifiable(
            effects, changed_state_terms=UNSTATED_CHANGE
        )
        assert not silent.approved and silent.reason == REFUSE_UNSTATED_CHANGE, (
            silent
        )

    # the per-document envelope is scoped to its own document's keys (what makes
    # a per-document skip expressible for the consumer), not corpus-wide.
    a_keys = set(cone.steps[document_step_id("a.md")].envelope.claim_keys)
    b_keys = set(cone.steps[document_step_id("b.md")].envelope.claim_keys)
    assert a_keys and b_keys and a_keys != b_keys, (a_keys, b_keys)


def test_recorded_fact_replay_partitions_by_document(tmp_path: Path) -> None:
    """Under replay each document source step owns its RECORDED slice, and a
    recorded claim with nowhere to belong is refused rather than dropped."""
    from okg.bench.synth.governed import partition_recorded_facts

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "a.md").write_text("# Patient Ann Lee\n\nBody Weight: 82.0 kg\n")
    (corpus / "b.md").write_text("# Patient Bob Ray\n\nBody Weight: 90.0 kg\n")
    recorded = list(extract_facts(corpus, ("body_weight",)))
    by_doc, other = partition_recorded_facts(recorded, ("a.md", "b.md"))
    assert [f.node_id for f in by_doc["a.md"]] == [
        "claim:a.md:body_weight:patient_ann_lee:0"
    ]
    assert [f.node_id for f in by_doc["b.md"]] == [
        "claim:b.md:body_weight:patient_bob_ray:0"
    ]
    assert [f.subtype for f in other] == ["predicate_policy"]
    assert sum(len(v) for v in by_doc.values()) + len(other) == len(recorded)

    # a claim naming a document the cone does not carry is a LOUD refusal.
    with pytest.raises(GovernedRunError) as excinfo:
        partition_recorded_facts(recorded, ("a.md",))
    assert "b.md" in str(excinfo.value)

    # the cone built under replay derives its buckets from the RECORDED set and
    # pins that set on the whole-corpus steps' input dependencies.
    cone = build_corpus_cone(
        corpus, deployment="synthetic-corpus-smoke", deployment_dir=_DEP_DIR,
        policies=("body_weight",), phase="replay", recorded_facts=recorded,
    )
    assert "recorded_facts" in cone.current_facts[
        CLAIM_PRODUCER_STEP
    ].input_digests
    assert "recorded_facts" not in cone.current_facts[
        document_step_id("a.md")
    ].input_digests


def test_llm_lane_is_refused_rather_than_silently_bound() -> None:
    """The LLM lane cannot derive a per-document envelope from the extraction
    rule, so the binding REFUSES it instead of binding something unsound."""
    config = load_preset("prose_llm")
    with pytest.raises(GovernedRunError) as excinfo:
        run_governed_benchmark(
            dsn="postgres://unused/unused", deployment_dir=_DEP_DIR,
            run_dir=Path("/nonexistent"), config=config,
        )
    assert "OUT OF SCOPE" in str(excinfo.value)


def test_governed_module_records_its_step_role_map() -> None:
    """The vocabulary map is DECLARED, so a reader can see which benchmark
    artifact each scheduler step stands for."""
    from okg.bench.synth.governed import STEP_ROLES

    assert set(STEP_ROLES) == {
        "claim_source", CLAIM_PRODUCER_STEP, CLAIM_EFFECTIVE_STEP,
        CLAIM_PROJECTION_STEP,
    }
    assert json.dumps(STEP_ROLES)  # serializable for the report
