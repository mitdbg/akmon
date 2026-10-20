"""PACT ``concord-exact-parity``, ``req.parity.safety`` — one broken assumption
at a time, and the scheduler never reuses unsoundly.

Five mutations, each a witness that FIRES: the run/skip accounting shows the
uncertain step RAN, or the run FAILED. None of them touches the scheduler — each
moves an input the scheduler is entitled to trust, which is exactly what the
safety trial is about.

(a) a DEPENDENCY-FACT CLASS mutated under a valid certificate — the deployment's
    constraint set is re-authored. The certificate must invalidate on
    ``constraint_set`` and the step must run.
(b) a step's declared EFFECTS ENVELOPE violated by its actual output — a
    poisoning row for its producer. The pass must DISCARD into complete
    execution, never approve a justified skip.
(c) a recorded PRIOR RESULT deleted — reuse must be impossible, not silently
    assumed. This one also records what the SCHEDULER did (nothing: a prior
    result is not one of its inputs, so it still approves the skip) alongside
    what stopped the unsound publish.
(d) a CORRUPTED CERTIFICATE row — the store recomputes the digest on read and
    must refuse it.
(e) a VACUOUS CHANGE STATEMENT — a FALSE ``ChangedStateTerms.nothing_changed()``
    on a first publish. The report says what the scheduler DID (approved a skip
    for every reached step) and whether any PRODUCTION caller can reach that
    state by omission. Since PR #972 it cannot: an omitted statement is
    ``UNSTATED_CHANGE``, a distinct type the ladder refuses, and the bare
    ``ChangedStateTerms()`` that used to spell both readings raises at
    construction. What is left is an explicit false assertion, which is what
    this arm now mutates.

After each mutation's forced re-execution, parity with the complete reference is
re-established at that update: safety costs work, never correctness.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.bench.synth.governed import document_step_id  # noqa: E402
from okg.bench.synth.parity import (  # noqa: E402
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_DEPENDENCY_FACT,
    MUTATION_ENVELOPE_VIOLATION,
    MUTATION_PRIOR_RESULT_DELETED,
    MUTATION_VACUOUS_CHANGE,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    ModeState,
    Mutation,
    ParityReplayError,
    build_update_cone,
    grade_update,
    replay_mode,
    run_update,
)
from okg.substrate.consistency.reuse_scheduler import (  # noqa: E402
    ACTION_SKIP,
    CAUSE_CERTIFICATE_VALID,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_SKIP_JUSTIFIED,
    CAUSE_UNSTATED_CHANGE,
    SKIP_CAUSES,
    TERMINAL_FIXED_POINT,
    ReusePass,
    run_reuse_pass,
)
from tests.bench.synth.test_parity_replay import (  # noqa: E402
    _DEP_DIR,
    _install_composed_catalog,
    _reset_between_modes,
    _seal_recorded_history,
)

_DEPLOYMENT = "synthetic-corpus-smoke"
# Short by design: every mutation rebuilds the differential state from update 0,
# so the history is kept to the smallest prefix that reaches a steady state with
# real prior certificates (update 0 publishes, update 1 revalidates).
_UPDATES = 6


# --------------------------------------------------------------------------- #
# Harness                                                                      #
# --------------------------------------------------------------------------- #
def _build_to(
    dsn: str, *, parity, policies, checks, corpus_dir: Path, upto: int
) -> ModeState:
    """Replay updates 0..upto-1 in differential mode and return the live state.

    Resets the runtime first: every mutation starts from the empty graph the
    fixture yields, so update 0 really is a first publish rather than a second
    execution stacked on the reference's generations."""
    from okg.bench.synth.history_replay import materialize_base, materialize_step

    _reset_between_modes(dsn)
    materialize_base(parity.history, corpus_dir)
    state = ModeState()
    for update in parity.updates[:upto]:
        materialize_step(parity.history, update.index, corpus_dir)
        run_update(
            dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=corpus_dir,
            mode=MODE_DIFFERENTIAL, update=update,
            gold_step=parity.gold_step(update.index), policies=policies,
            checks=checks, state=state, deployment=_DEPLOYMENT,
        )
    return state


def _run_one(
    dsn: str, *, parity, policies, checks, corpus_dir: Path, index: int,
    state: ModeState, mode: str = MODE_DIFFERENTIAL, mutation=None,
):
    from okg.bench.synth.history_replay import materialize_step

    materialize_step(parity.history, index, corpus_dir)
    return run_update(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=corpus_dir, mode=mode,
        update=parity.updates[index], gold_step=parity.gold_step(index),
        policies=policies, checks=checks, state=state, deployment=_DEPLOYMENT,
        mutation=mutation,
    )


def _assert_parity_restored(*, parity, reference, execution, label: str) -> None:
    """After the forced re-execution, the state equals the reference's."""
    verdict = grade_update(
        gold_step=parity.gold_step(execution.index),
        reference=reference, differential=execution,
    )
    assert verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED), (
        f"{label}: parity did NOT survive the mutation — {verdict.verdict}"
    )
    assert not verdict.gold_problems, (verdict.gold_problems[:3])
    print(
        f"    parity re-established at update {execution.index}: "
        f"{verdict.verdict} (gold ok, "
        f"{len(execution.snapshot.effective_claims)} effective claims)"
    )


def _assert_mutation_applied(
    mutation: Mutation, *, required_detail: tuple[str, ...]
) -> None:
    """Prove the trial reached ``Mutation.apply`` before grading its result."""
    assert mutation.target_step, (
        f"{mutation.kind}: Mutation.apply did not populate target_step"
    )
    missing = [key for key in required_detail if key not in mutation.detail]
    assert not missing, (
        f"{mutation.kind}: Mutation.apply did not populate detail {missing!r}"
    )
    empty = [
        key
        for key in required_detail
        if mutation.detail[key] is None
        or (
            isinstance(mutation.detail[key], str)
            and not mutation.detail[key].strip()
        )
    ]
    assert not empty, (
        f"{mutation.kind}: Mutation.apply left detail {empty!r} empty"
    )


@pytest.fixture(scope="function")
def sealed(clean_runtime: str, tmp_path: Path, world, smoke_config):
    """One sealed history + the complete-reference execution over it."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _UPDATES
    )
    _reset_between_modes(dsn)
    reference = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-complete",
        parity=parity, mode=MODE_COMPLETE, policies=policies, checks=checks,
    )
    return dsn, tmp_path, parity, policies, checks, reference


# =========================================================================== #
# (a) a dependency-fact class mutated under a valid certificate               #
# =========================================================================== #
@pytest.mark.postgres
def test_mutating_a_dependency_fact_class_invalidates_and_forces_a_rerun(
    sealed,
) -> None:
    dsn, tmp_path, parity, policies, checks, reference = sealed
    index = 2
    corpus = tmp_path / "corpus-mut-a"

    # CONTROL: unmutated, the reached steps skip behind valid certificates.
    state = _build_to(
        dsn, parity=parity, policies=policies, checks=checks,
        corpus_dir=corpus, upto=index,
    )
    control = _run_one(
        dsn, parity=parity, policies=policies, checks=checks, corpus_dir=corpus,
        index=index, state=state,
    )
    print(
        f"\n[safety/a] CONTROL update {index}: run={control.ledger.steps_run} "
        f"skip={control.ledger.steps_skipped} "
        f"causes={control.ledger.skips_by_cause}"
    )
    assert CAUSE_CERTIFICATE_VALID in control.ledger.skips_by_cause, (
        "the control does not skip behind a valid certificate, so mutating one "
        "proves nothing"
    )
    assert control.ledger.steps_run == 0

    # MUTATION: re-author the deployment's constraint set.
    corpus = tmp_path / "corpus-mut-a2"
    state = _build_to(
        dsn, parity=parity, policies=policies, checks=checks,
        corpus_dir=corpus, upto=index,
    )
    mutation = Mutation(kind=MUTATION_DEPENDENCY_FACT)
    mutated = _run_one(
        dsn, parity=parity, policies=policies, checks=checks, corpus_dir=corpus,
        index=index, state=state, mutation=mutation,
    )
    _assert_mutation_applied(mutation, required_detail=("mutated_class",))
    assert mutation.detail["mutated_class"] == "constraint_set"
    print(
        f"[safety/a] MUTATED update {index} (class "
        f"{mutation.detail['mutated_class']!r} moved under the certificate for "
        f"{mutation.target_step!r}): run={mutated.ledger.steps_run} "
        f"skip={mutated.ledger.steps_skipped} "
        f"invalidated={mutated.ledger.invalidated_classes} "
        f"certificates_invalidated={mutated.ledger.certificates_invalidated}"
    )

    # ---- the witness FIRES: the certificate invalidated and the steps RAN --- #
    assert "constraint_set" in mutated.ledger.invalidated_classes, (
        f"the certificate did not invalidate on the mutated class: "
        f"{mutated.ledger.invalidated_classes}"
    )
    assert mutated.ledger.certificates_invalidated > 0
    assert mutated.ledger.steps_run > control.ledger.steps_run, (
        "the mutation cost no work — the step was reused under a certificate "
        "whose dependency had moved"
    )
    assert mutation.target_step in mutated.run_steps, (
        f"{mutation.target_step} did not re-run"
    )
    assert mutated.ledger.run_causes == {
        CAUSE_EFFECTS_OVERLAP: mutated.ledger.steps_run
    }
    # Every CARRIED justification is gone: neither the revalidation rung nor the
    # degenerate unchanged-inputs rung can stand on a moved dependency class.
    assert CAUSE_CERTIFICATE_VALID not in mutated.ledger.skips_by_cause, (
        mutated.ledger.skips_by_cause
    )
    assert set(mutated.ledger.skips_by_cause) <= {CAUSE_SKIP_DOMINANCE}, (
        f"a rung other than bounded dominance survived the invalidation: "
        f"{mutated.ledger.skips_by_cause}"
    )
    if CAUSE_SKIP_DOMINANCE in mutated.ledger.skips_by_cause:
        # Recorded, not asserted away: the dominance rung RE-DERIVES its
        # justification from the recorded incumbent decision and the step's
        # declared envelope, neither of which a constraint-set change touches, so
        # those steps skip again behind FRESH certificates. Sound — the
        # justification is proved, not carried — but it means a dependency-fact
        # invalidation is not a universal force-this-step-to-run lever.
        print(
            f"[safety/a] FINDING: "
            f"{mutated.ledger.skips_by_cause[CAUSE_SKIP_DOMINANCE]} step(s) "
            f"still skipped, behind a FRESHLY DERIVED bounded-dominance "
            f"certificate; every carried justification fell away"
        )

    # ---- and parity still holds -------------------------------------------- #
    _assert_parity_restored(
        parity=parity, reference=reference.executions[index], execution=mutated,
        label="safety/a",
    )


# =========================================================================== #
# (b) the declared effects envelope violated by the actual output             #
# =========================================================================== #
@pytest.mark.postgres
def test_an_envelope_violation_discards_the_pass_into_complete_execution(
    sealed,
) -> None:
    dsn, tmp_path, parity, policies, checks, reference = sealed
    index = 2
    corpus = tmp_path / "corpus-mut-b"
    state = _build_to(
        dsn, parity=parity, policies=policies, checks=checks,
        corpus_dir=corpus, upto=index,
    )
    mutation = Mutation(kind=MUTATION_ENVELOPE_VIOLATION)
    with pytest.raises(ParityReplayError) as excinfo:
        _run_one(
            dsn, parity=parity, policies=policies, checks=checks,
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
        )
    _assert_mutation_applied(mutation, required_detail=("poisoned_producer",))
    message = str(excinfo.value)
    print(
        f"\n[safety/b] producer {mutation.detail['poisoned_producer']!r} carries "
        f"an envelope-violation poisoning row -> the pass refused to commit:\n"
        f"    {message[:200]}"
    )
    # ---- the witness FIRES: DISCARD, not a justified skip ------------------- #
    assert "envelope_violation" in message, message
    assert "'discard'" in message or "discard" in message, message
    assert mutation.target_step in message or "claim_source" in message

    # ---- FALLBACK: complete execution, and parity holds --------------------- #
    fallback = _run_one(
        dsn, parity=parity, policies=policies, checks=checks, corpus_dir=corpus,
        index=index, state=state, mode=MODE_COMPLETE,
    )
    print(
        f"[safety/b] fallback to complete execution: "
        f"run={fallback.ledger.steps_run} skip={fallback.ledger.steps_skipped}"
    )
    assert fallback.ledger.steps_skipped == 0
    assert fallback.ledger.steps_run == fallback.ledger.steps_reached
    _assert_parity_restored(
        parity=parity, reference=reference.executions[index], execution=fallback,
        label="safety/b",
    )


# =========================================================================== #
# (c) a recorded prior result deleted                                         #
# =========================================================================== #
@pytest.mark.postgres
def test_a_deleted_prior_result_makes_reuse_impossible_not_assumed(
    sealed,
) -> None:
    dsn, tmp_path, parity, policies, checks, reference = sealed
    # An update where the producer really runs, so the assembly that would drop
    # the missing slice is actually reached.
    index = 4
    corpus = tmp_path / "corpus-mut-c"
    state = _build_to(
        dsn, parity=parity, policies=policies, checks=checks,
        corpus_dir=corpus, upto=index,
    )

    # ---- FIRST: what does the SCHEDULER do? -------------------------------- #
    # A prior RESULT is not one of the scheduler's inputs — it validates
    # certificates against dependency FACTS. So deleting the result changes
    # nothing it can see, and it still approves the skip. Measured, not assumed.
    from okg.bench.synth.history_replay import materialize_step
    from okg.bench.synth.parity import _load_prior_certificates

    materialize_step(parity.history, index, corpus)
    cone, _stated = build_update_cone(
        corpus_dir=corpus, deployment_dir=_DEP_DIR, policies=policies,
        checks=checks, update=parity.updates[index],
        facts=parity.updates[index].facts(), state=state,
        mode=MODE_DIFFERENTIAL, deployment=_DEPLOYMENT,
    )
    prior_certs = _load_prior_certificates(
        dsn, deployment=_DEPLOYMENT, candidate=state.prior_candidate
    )
    victim_doc = sorted(state.carried)[0]
    victim_step = document_step_id(victim_doc)
    before = _probe(
        dsn, cone=cone, checks=checks, prior_certificates=prior_certs,
        prior_facts=state.prior_facts,
    )
    stashed = state.carried.pop(victim_doc)          # the result really is gone
    after = _probe(
        dsn, cone=cone, checks=checks, prior_certificates=prior_certs,
        prior_facts=state.prior_facts,
    )
    state.carried[victim_doc] = stashed              # restore for the real run
    before_causes = {o.step_id: o.cause for o in before.outcomes}
    after_causes = {o.step_id: o.cause for o in after.outcomes}
    assert before_causes == after_causes, (
        "the scheduler's decision changed when a prior RESULT vanished; it has "
        "no such input, so this witness must be re-derived"
    )
    assert before_causes.get(victim_step) in SKIP_CAUSES, (
        f"the victim step did not skip in the control: "
        f"{before_causes.get(victim_step)}"
    )
    print(
        f"\n[safety/c] FINDING — the scheduler cannot see a missing prior "
        f"result: with {victim_step!r}'s stored result deleted it STILL decides "
        f"{after_causes[victim_step]!r} (a prior RESULT is not one of the ten "
        f"dependency classes a certificate records). Only the executor that "
        f"holds the result can refuse."
    )

    # ---- SECOND: the executor refuses, loudly ------------------------------ #
    mutation = Mutation(kind=MUTATION_PRIOR_RESULT_DELETED)
    with pytest.raises(ParityReplayError) as excinfo:
        _run_one(
            dsn, parity=parity, policies=policies, checks=checks,
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
        )
    _assert_mutation_applied(
        mutation, required_detail=("deleted_doc", "had_prior_result")
    )
    assert mutation.target_step == document_step_id(mutation.detail["deleted_doc"])
    assert mutation.detail["had_prior_result"] is True
    message = str(excinfo.value)
    print(
        f"[safety/c] deleted the stored prior result for "
        f"{mutation.detail['deleted_doc']!r} (had_prior_result="
        f"{mutation.detail['had_prior_result']}) -> the run FAILED:\n"
        f"    {message[:240]}"
    )
    assert "operator_failure" in message or "abort" in message, message
    assert mutation.target_step in message, message
    assert "prior result" in message and "GONE" in message, message
    assert "Reuse is impossible" in message, message

    # ---- FALLBACK: complete execution, and parity holds --------------------- #
    fallback = _run_one(
        dsn, parity=parity, policies=policies, checks=checks, corpus_dir=corpus,
        index=index, state=state, mode=MODE_COMPLETE,
    )
    assert fallback.ledger.steps_skipped == 0
    _assert_parity_restored(
        parity=parity, reference=reference.executions[index], execution=fallback,
        label="safety/c",
    )


def _probe(dsn: str, *, cone, checks, prior_certificates, prior_facts):
    """Run the scheduler pass for accounting ONLY and roll back.

    No step work, no publish, no committed certificate: the connection is rolled
    back, so the probe can be run twice from the same state and compared."""
    from okg.substrate.consistency.reuse_certificates import ReuseCertificateStore

    conn = psycopg.connect(dsn)
    try:
        return run_reuse_pass(
            conn, deployment=_DEPLOYMENT,
            generation_id=f"{cone.candidate_generation_id}:probe",
            worklist_seed=cone.worklist_seed, steps=cone.steps, checks=checks,
            prior_certificates=prior_certificates or None,
            current_facts=cone.current_facts, prior_facts=prior_facts or None,
            changed_terms=cone.changed_terms,
            incumbent_selection=cone.incumbent_selection,
            step_runner=lambda _step: (),
            cert_store=ReuseCertificateStore(deployment=_DEPLOYMENT),
        )
    finally:
        conn.rollback()
        conn.close()


# =========================================================================== #
# (d) a corrupted certificate row                                             #
# =========================================================================== #
@pytest.mark.postgres
def test_a_corrupted_certificate_row_is_refused_by_validation(sealed) -> None:
    dsn, tmp_path, parity, policies, checks, reference = sealed
    index = 2
    corpus = tmp_path / "corpus-mut-d"
    state = _build_to(
        dsn, parity=parity, policies=policies, checks=checks,
        corpus_dir=corpus, upto=index,
    )
    mutation = Mutation(kind=MUTATION_CORRUPT_CERTIFICATE)
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as excinfo:
        _run_one(
            dsn, parity=parity, policies=policies, checks=checks,
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
        )
    _assert_mutation_applied(mutation, required_detail=("corrupted_step",))
    assert mutation.target_step == mutation.detail["corrupted_step"]
    message = str(excinfo.value)
    print(
        f"\n[safety/d] tampered with the stored certificate for "
        f"{mutation.detail['corrupted_step']!r} under candidate "
        f"{state.prior_candidate!r} -> reading it back was REFUSED:\n"
        f"    {message[:220]}"
    )
    # ---- the witness FIRES: the digest check rejected the row --------------- #
    assert "stored reuse certificate digest" in message, message
    assert "recomputed digest" in message, message
    assert "corrupted after write" in message, message

    # ---- FALLBACK: complete execution, and parity holds --------------------- #
    fallback = _run_one(
        dsn, parity=parity, policies=policies, checks=checks, corpus_dir=corpus,
        index=index, state=state, mode=MODE_COMPLETE,
    )
    assert fallback.ledger.steps_skipped == 0
    _assert_parity_restored(
        parity=parity, reference=reference.executions[index], execution=fallback,
        label="safety/d",
    )


# =========================================================================== #
# (e) a vacuous change statement                                              #
# =========================================================================== #
@pytest.mark.postgres
def test_a_false_nothing_changed_statement_makes_the_ladder_approve_every_skip(
    sealed,
) -> None:
    """What the scheduler DID, measured — not what the binding avoided.

    The mutation is an explicit ``nothing_changed()`` asserted on a FIRST
    publish, where it is false. Before PR #972 it was spelled as a bare
    ``ChangedStateTerms()``, which also stood for a plain omission; that
    conflation is gone, so this arm mutates the assertion — the reading the
    ladder still trusts — and the omission reading is checked separately (and
    refused) by the production-caller test below."""
    dsn, tmp_path, parity, policies, checks, reference = sealed
    corpus = tmp_path / "corpus-mut-e"
    from okg.bench.synth.history_replay import materialize_base, materialize_step

    _reset_between_modes(dsn)
    materialize_base(parity.history, corpus)
    materialize_step(parity.history, 0, corpus)
    state = ModeState()
    cone, stated = build_update_cone(
        corpus_dir=corpus, deployment_dir=_DEP_DIR, policies=policies,
        checks=checks, update=parity.updates[0], facts=parity.updates[0].facts(),
        state=state, mode=MODE_DIFFERENTIAL, deployment=_DEPLOYMENT,
    )
    assert stated, "the truthful first-publish statement must be non-empty"

    from okg.substrate.consistency.possible_effects import (
        UNSTATED_CHANGE,
        ChangedStateTerms,
    )

    truthful = _probe(
        dsn, cone=cone, checks=checks, prior_certificates=None, prior_facts=None,
    )
    vacuous_cone = _replace_terms(cone, ChangedStateTerms.nothing_changed())
    vacuous = _probe(
        dsn, cone=vacuous_cone, checks=checks, prior_certificates=None,
        prior_facts=None,
    )
    # The third reading, which used to be indistinguishable from the second:
    # the caller says NOTHING. PR #972 made it a distinct type, so measure it
    # here rather than assuming the split holds end-to-end.
    silent_cone = _replace_terms(cone, UNSTATED_CHANGE)
    silent = _probe(
        dsn, cone=silent_cone, checks=checks, prior_certificates=None,
        prior_facts=None,
    )
    truthful_skips = [o for o in truthful.outcomes if o.action == ACTION_SKIP]
    vacuous_skips = [o for o in vacuous.outcomes if o.action == ACTION_SKIP]
    silent_skips = [o for o in silent.outcomes if o.action == ACTION_SKIP]

    print(
        f"\n[safety/e] FIRST PUBLISH, no prior certificate and no prior facts.\n"
        f"    TRUTHFUL statement ({len(stated)} buckets): "
        f"reached={len(truthful.outcomes)} run={len(truthful.runs)} "
        f"skip={len(truthful_skips)}\n"
        f"    FALSE nothing_changed() (0 buckets): "
        f"reached={len(vacuous.outcomes)} run={len(vacuous.runs)} "
        f"skip={len(vacuous_skips)} "
        f"causes={sorted({o.cause for o in vacuous_skips})}\n"
        f"    UNSTATED (no statement at all): "
        f"reached={len(silent.outcomes)} run={len(silent.runs)} "
        f"skip={len(silent_skips)} "
        f"causes={sorted({o.cause for o in silent.outcomes})}"
    )
    # ---- the witness FIRES: the scheduler APPROVED a skip for every step ---- #
    assert not truthful_skips, "the truthful statement must refuse every skip"
    assert len(vacuous_skips) == len(vacuous.outcomes) > 0, (
        "the vacuous statement did not approve every skip — this witness must be "
        "re-derived before it can be quoted"
    )
    assert {o.cause for o in vacuous_skips} == {CAUSE_SKIP_JUSTIFIED}
    assert vacuous.terminal == TERMINAL_FIXED_POINT
    # ---- and SILENCE does not: the scheduler refuses it on its own --------- #
    assert not silent_skips, (
        "an unstated change approved a skip — the #972 fail-closed split has "
        "regressed and silence is a skip mechanism again"
    )
    assert {o.cause for o in silent.outcomes} == {CAUSE_UNSTATED_CHANGE}
    print(
        f"    -> the same first publish: {len(vacuous_skips)} skips approved on "
        f"a FALSE nothing_changed(), 0 on silence "
        f"({CAUSE_UNSTATED_CHANGE!r}). Before #972 both were spelled "
        f"ChangedStateTerms() and both approved."
    )
    print(
        f"    -> the scheduler APPROVED {len(vacuous_skips)} skips on a first "
        f"publish, every one behind a freshly written {CAUSE_SKIP_JUSTIFIED!r} "
        f"certificate, keeping prior results that DO NOT EXIST."
    )

    # ---- and the executor refuses to publish that state -------------------- #
    with pytest.raises(ParityReplayError) as excinfo:
        _run_one(
            dsn, parity=parity, policies=policies, checks=checks,
            corpus_dir=corpus, index=0, state=ModeState(),
            mutation=Mutation(kind=MUTATION_VACUOUS_CHANGE),
        )
    assert "absorbed" in str(excinfo.value), str(excinfo.value)
    print(f"    executor guard: {str(excinfo.value)[:160]}")

    # ---- and parity is restored by a truthful complete execution ----------- #
    _reset_between_modes(dsn)
    fallback = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-mut-e2",
        parity=parity, mode=MODE_COMPLETE, policies=policies, checks=checks,
        limit=1,
    )
    _assert_parity_restored(
        parity=parity, reference=reference.executions[0],
        execution=fallback.executions[0], label="safety/e",
    )


def _replace_terms(cone, terms):
    import dataclasses

    return dataclasses.replace(
        cone, changed_terms={step: terms for step in cone.steps}
    )


# =========================================================================== #
# The production-caller question (req.parity.safety ACCEPTANCE)               #
# =========================================================================== #
def test_a_production_caller_can_no_longer_construct_an_empty_change_statement(
) -> None:
    """The inverse of the finding this test used to carry, and the reason it
    inverted.

    It originally answered "can any PRODUCTION caller construct an empty change
    statement?" with YES: ``run_reuse_pass_in_cycle``'s ``changed_terms`` was
    optional, ``ReusePass`` turned a missing entry into a bare
    ``ChangedStateTerms()``, that empty statement satisfied the ladder's
    disjointness test vacuously, and the publisher's post-cycle hook omitted the
    argument entirely — safe only incidentally, because it also passed
    ``prior_facts=facts`` so the degenerate rung fired first.

    PR #972 closed that hole by making the two readings different TYPES, so the
    original finding is no longer derivable and this test now pins the closure
    instead. Four facts, each read off the code rather than asserted from memory:

    1. the parameter is STILL optional — omission remains expressible at a
       callsite, which is why (2) has to be the thing that refuses;
    2. an omitted statement is now ``UNSTATED_CHANGE``, a distinct type, and the
       ladder REFUSES it with ``REFUSE_UNSTATED_CHANGE`` rather than approving;
    3. the ambiguous bare ``ChangedStateTerms()`` — the one value that used to
       spell both "nothing changed" and "I did not say" — is refused at
       CONSTRUCTION, so no caller can build it by any route;
    4. the publisher's post-cycle hook, still the only caller of
       ``run_reuse_pass_in_cycle`` in ``src/`` outside this benchmark, now states
       ``nothing_changed()`` per step. Its safety is a visible claim it is
       accountable for, not a silence that happened to be masked.

    What remains live, and is witnessed by the mutation arm above rather than
    here: a caller may still assert ``nothing_changed()`` FALSELY, and the ladder
    trusts it. That is a statement someone signed, not an omission."""
    from okg.substrate.consistency.possible_effects import (
        REFUSE_UNSTATED_CHANGE,
        UNSTATED_CHANGE,
        ChangedStateTerms,
        compile_possible_effects,
        skip_is_justifiable,
    )
    from okg.substrate.errors import SubstrateError
    from okg.substrate.producer_authority import normalize_write_envelope
    from okg.substrate.runtime.reuse_pass_workflow import (
        _run_fixture_cone_pass,
        run_reuse_pass_in_cycle,
    )

    # (1) the parameter is still optional -- omission is still expressible.
    parameter = inspect.signature(run_reuse_pass_in_cycle).parameters["changed_terms"]
    assert parameter.default is None, parameter

    # (2) an omitted statement is UNSTATED_CHANGE, and the ladder refuses it.
    reuse_pass = ReusePass(
        conn=None, deployment="probe", generation_id="g", worklist_seed=(),
        steps={},
    )
    assert reuse_pass._changed == {}
    assert reuse_pass._changed.get("any-step", UNSTATED_CHANGE) is UNSTATED_CHANGE
    envelope = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "probe:x"}],
         "claim_keys": ["k"]},
        source_name="probe",
    )
    effects = compile_possible_effects(
        envelope, checks=(), claim_predicates=("p",), producer_id="probe",
    )
    silent = skip_is_justifiable(effects, changed_state_terms=UNSTATED_CHANGE)
    assert not silent.approved, (
        "an omitted change statement approves a skip again — the #972 closure "
        "has regressed and the original finding is live"
    )
    assert silent.reason == REFUSE_UNSTATED_CHANGE, silent

    # (3) the ambiguous statement cannot be constructed at all.
    with pytest.raises(SubstrateError, match="ambiguous"):
        ChangedStateTerms()

    # (4) the production caller states its terms explicitly.
    source = inspect.getsource(_run_fixture_cone_pass)
    assert "run_reuse_pass_in_cycle(" in source
    assert "changed_terms=changed_terms" in source, (
        "the production caller stopped stating its change terms — the omission "
        "path is only refused, not stated, and this hook is back to relying on "
        "the degenerate rung"
    )
    assert "ChangedStateTerms.nothing_changed()" in source, source
    print(
        "\n[safety/e/production] NO, and by construction: "
        "okg/substrate/runtime/reuse_pass_workflow.py::_run_fixture_cone_pass — "
        "the publisher's post-cycle reuse hook and the only caller of "
        "run_reuse_pass_in_cycle in src/ outside this benchmark — now passes "
        "ChangedStateTerms.nothing_changed() per step. An omitted statement "
        f"reads as UNSTATED_CHANGE and refuses with {REFUSE_UNSTATED_CHANGE!r}, "
        "and the bare ChangedStateTerms() that used to spell both readings "
        "raises at construction. The remaining way to license an unearned skip "
        "is a caller asserting nothing_changed() untruthfully -- an explicit "
        "claim, witnessed by the mutation arm above."
    )
