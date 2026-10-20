"""PACT ``concord-disclosure-budget``, task.batch.support-index-theorem — the
two E1-completion metrics MEASURED on the live ``synthetic-corpus-smoke``
path.

§E1 names six metrics. ``e1_support_stress`` delivers four on the pure half
and deferred two by name to this task because both need something the pure
half does not have: a wall clock over real execution, and a live certificate
store to tamper with. Both are measured here.

* **runtime saved** — one ``MODE_COMPLETE`` and one ``MODE_DIFFERENTIAL``
  ``measure_replay`` over the SAME sealed history. The saving is
  complete-minus-differential in the adapter's declared units, with the
  setup-charged cycle-0 interval excluded from both sums and reported
  separately. Cost saved (executor steps) is deterministic and is asserted
  strictly positive: differential execution skips work complete execution
  does. Wall-clock duration is REPORTED, not asserted positive — a six-update
  history on a loaded laptop is not a performance claim, and turning a timing
  into a pass/fail gate here would make the suite flaky and the claim
  dishonest.
* **checker mutation detection** — the certificate-row corruption mutation
  and a dependency-fact mutation, both parity's own, applied to the same live
  replay. Each is proven to have actually mutated something before it is
  graded (parity refuses a no-op mutation; the classifiers refuse to grade
  one), and detection is classified from what the REAL execution produced: a
  named refusal reaching the caller, or an invalidated dependency class plus
  the victim step re-running.

Bounded on purpose, exactly like ``test_e1_support_stress.py``: a six-update
history, not a second marathon. The standing 22-update truthful parity
witness already exists.
"""
from __future__ import annotations

from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.bench.synth.e1_runtime_saved import (  # noqa: E402
    DETECTION_MODE_INVALIDATED_AND_RERAN,
    DETECTION_MODE_REFUSED,
    RUNTIME_SAVED_MEASURED,
    RUNTIME_SAVED_PARTIALLY_KNOWN,
    SHARED_BINDING_FIELDS,
    classify_certificate_corruption,
    classify_dependency_fact_mutation,
    measure_runtime_saved,
)
from okg.bench.synth.e1_support_stress import (  # noqa: E402
    build_manifest,
    run_report,
)
from okg.bench.synth.parity import (  # noqa: E402
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_DEPENDENCY_FACT,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    Mutation,
    grade_update,
    replay_mode,
)
from okg.substrate.consistency.batch_measurements import (  # noqa: E402
    COST_UNIT_EXECUTOR_STEPS,
    DURATION_UNIT_NANOSECONDS,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP,
    PARITY_RESOURCES,
    measure_replay,
    okg_repo_binding,
    resource_declaration_digest,
)
from okg.substrate.consistency.batch_schedule import Unknown  # noqa: E402
from okg.substrate.consistency.reuse_scheduler import (  # noqa: E402
    CAUSE_CERTIFICATE_VALID,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from tests.bench.synth.test_parity_replay import (  # noqa: E402
    _DEP_DIR,
    _install_composed_catalog,
    _reset_between_modes,
    _seal_recorded_history,
)

#: Short on purpose — see the module docstring.
_E1_UPDATES = 6
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _binding(parity, limit):
    return okg_repo_binding(
        repo_root=_REPO_ROOT,
        corpus_manifest_digest=parity.updates[limit - 1].digest,
        resource_config_version=1,
        resource_config_digest=resource_declaration_digest(
            PARITY_RESOURCES,
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
        ),
    )


#: The executor-step saving on the sealed smoke history, PINNED.
#:
#: Deterministic and re-measured twice on 2026-08-29 (176 both times) while the
#: wall-clock figure beside it moved 2.77s -> 1.36s -> 1.69s across the same
#: runs — which is why duration is reported and NOT gated, and why only this
#: column is pinnable. concord-audit-corrections, req.audit.evidence-currency.
_PINNED_COST_SAVED = 176


@pytest.mark.postgres
def test_runtime_saved_is_measured_from_two_modes_of_one_sealed_history(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    limit = min(_E1_UPDATES, len(parity.updates))
    binding = _binding(parity, limit)

    _reset_between_modes(dsn)
    complete = measure_replay(
        replay_mode, binding=binding, trace_set_id="e1-runtime-saved/complete",
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-complete", parity=parity,
        mode=MODE_COMPLETE, policies=policies, checks=checks, limit=limit,
    )
    _reset_between_modes(dsn)
    differential = measure_replay(
        replay_mode, binding=binding,
        trace_set_id="e1-runtime-saved/differential",
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-differential", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks, limit=limit,
    )

    # The two arms must be the same experiment: the same recorded facts at
    # every update. Otherwise the difference is not a saving.
    for index in range(limit):
        verdict = grade_update(
            gold_step=parity.gold_step(index),
            reference=complete.run.executions[index],
            differential=differential.run.executions[index],
        )
        assert verdict.same_facts, (
            f"update {index}: the two arms consumed different facts, so "
            f"their runtime difference is not a saving on one history"
        )
        assert verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED), (
            f"update {index}: {verdict.verdict}"
        )
        assert not verdict.gold_problems, verdict.gold_problems[:3]

    saved = measure_runtime_saved(
        complete, differential,
        history_binding=parity.updates[limit - 1].digest,
    )

    print(
        f"\n[e1/runtime-saved] status={saved.status} "
        f"duration_saved={saved.duration_saved} {saved.duration_unit} "
        f"cost_saved={saved.cost_saved} {saved.cost_unit}\n"
        f"    complete:     compared={saved.complete.compared_cycles} "
        f"duration={saved.complete.duration_known_subtotal} "
        f"cost={saved.complete.cost_known_subtotal} "
        f"setup={saved.complete.setup_duration}\n"
        f"    differential: compared={saved.differential.compared_cycles} "
        f"duration={saved.differential.duration_known_subtotal} "
        f"cost={saved.differential.cost_known_subtotal} "
        f"setup={saved.differential.setup_duration}\n"
        f"    unknown components: "
        f"{list(saved.complete.duration_unknown_components)}"
        f"{list(saved.differential.duration_unknown_components)}"
    )

    assert saved.status in (
        RUNTIME_SAVED_MEASURED, RUNTIME_SAVED_PARTIALLY_KNOWN
    )
    assert saved.duration_unit == DURATION_UNIT_NANOSECONDS
    assert saved.cost_unit == COST_UNIT_EXECUTOR_STEPS
    assert saved.complete.compared_cycles == limit - 1
    assert saved.differential.compared_cycles == limit - 1

    # --- the cycle-0 setup interval is EXCLUDED, and disclosed -------------- #
    for arm, measured in (("complete", complete),
                          ("differential", differential)):
        by_id = sorted(measured.cycle_traces.traces, key=lambda t: t.action_id)
        assert by_id[0].realized.measurement_source == \
            MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP, arm
        assert {t.realized.measurement_source for t in by_id[1:]} == \
            {MEASUREMENT_SOURCE_CYCLE_INTERVAL}, arm
    assert not isinstance(saved.complete.setup_duration, Unknown)
    assert not isinstance(saved.differential.setup_duration, Unknown)
    # the setup interval is real work and is NOT inside the compared sum.
    assert saved.complete.duration_known_subtotal == sum(
        t.realized.duration
        for t in complete.cycle_traces.traces
        if t.realized.measurement_source == MEASUREMENT_SOURCE_CYCLE_INTERVAL
    )

    # --- cost saved is deterministic, so it IS asserted --------------------- #
    assert not isinstance(saved.cost_saved, Unknown), (
        "executor-step cost is charged by the ledger and is never unknown"
    )
    assert saved.cost_saved == _PINNED_COST_SAVED, (
        f"the executor-step saving on this sealed history is "
        f"{saved.cost_saved}, not the pinned {_PINNED_COST_SAVED} "
        f"({saved.differential.cost_known_subtotal} differential vs "
        f"{saved.complete.cost_known_subtotal} complete). This figure is "
        f"QUOTED in concord-disclosure-budget's evidence, so it must not move "
        f"without the quote moving in the same diff. Until 2026-08-29 this "
        f"assertion read `> 0`, which let the quoted 176 drift to any positive "
        f"value with nothing failing (audit finding)."
    )

    # --- duration is REPORTED, never zero-filled ---------------------------- #
    if saved.status == RUNTIME_SAVED_MEASURED:
        assert isinstance(saved.duration_saved, int)
        assert saved.complete.duration_known_subtotal > 0
        assert saved.differential.duration_known_subtotal > 0
    else:
        assert isinstance(saved.duration_saved, Unknown)
        assert saved.duration_saved.reason
        named = (
            list(saved.complete.duration_unknown_components)
            + list(saved.differential.duration_unknown_components)
        )
        assert named, (
            "the metric reported partially_known without naming a single "
            "unknown component"
        )

    payload = saved.to_canonical_dict()
    assert payload["history_binding"] == parity.updates[limit - 1].digest
    assert payload["schema"] == "okg.synth.e1-runtime-saved.v1"

    # --- the number is falsifiable after the fact --------------------------- #
    # A reader of the stored payload can check that the two arms really were
    # one experiment, instead of taking the difference on trust. All four
    # §12 bindings are emitted and match what the live run actually bound.
    assert set(payload["shared_bindings"]) == set(SHARED_BINDING_FIELDS)
    assert payload["shared_bindings"] == {
        "corpus_manifest_digest": binding.corpus_manifest_digest,
        "granularity": complete.cycle_traces.granularity,
        "okg_commit": binding.okg_commit,
        "resource_config_digest": binding.resource_config_digest,
    }
    assert payload["history_binding"] == (
        payload["shared_bindings"]["corpus_manifest_digest"]
    )


@pytest.mark.postgres
def test_the_certificate_corruption_mutation_is_caught_by_a_named_refusal(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """A stored certificate row is tampered with after write. Reading it back
    must be REFUSED by the store's recomputed-digest check, and the E1
    checker-mutation-detection row records that refusal by name."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    mutation_at = 2
    limit = mutation_at + 1

    # CONTROL: the unmutated differential run must have written a certificate
    # the mutation can corrupt. Corrupting nothing proves nothing.
    _reset_between_modes(dsn)
    control = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-corrupt-control", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks, limit=limit,
    )
    assert CAUSE_CERTIFICATE_VALID in (
        control.executions[mutation_at].ledger.skips_by_cause
    ), (
        f"the control did not reuse behind a stored certificate at update "
        f"{mutation_at}, so there is no row for the corruption mutation to "
        f"tamper with: "
        f"{dict(control.executions[mutation_at].ledger.skips_by_cause)}"
    )

    _reset_between_modes(dsn)
    mutation = Mutation(kind=MUTATION_CORRUPT_CERTIFICATE)
    raised: BaseException | None = None
    try:
        replay_mode(
            dsn=dsn, deployment_dir=_DEP_DIR,
            corpus_dir=tmp_path / "corpus-corrupt", parity=parity,
            mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
            mutations={mutation_at: mutation}, limit=limit,
        )
    except SubstrateError as exc:
        raised = exc

    row = classify_certificate_corruption(
        mutation=mutation, update_index=mutation_at, raised=raised,
    )
    print(
        f"\n[e1/checker-mutation] {row.mutation_kind} at update "
        f"{row.update_index} on {row.target_step!r}: "
        f"detected={row.detected} mode={row.detection_mode}\n"
        f"    refusal: {row.refusal}"
    )
    assert row.detected, (
        f"the tampered certificate row was NOT caught "
        f"(evidence: {dict(row.mutation_evidence)})"
    )
    assert row.detection_mode == DETECTION_MODE_REFUSED
    assert row.mutation_kind == MUTATION_CORRUPT_CERTIFICATE
    assert row.target_step == mutation.detail["corrupted_step"]


@pytest.mark.postgres
def test_the_dependency_fact_mutation_is_caught_by_invalidation_and_rerun(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """A dependency class moves UNDER a valid certificate. The certificate
    must invalidate on that class and the victim step must actually re-run —
    a reuse that survived a moved dependency would be the unsafe skip E1
    exists to catch."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    mutation_at = 2
    limit = mutation_at + 1

    _reset_between_modes(dsn)
    control = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-dep-control", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks, limit=limit,
    )
    _reset_between_modes(dsn)
    mutation = Mutation(kind=MUTATION_DEPENDENCY_FACT)
    mutated = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-dep", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
        mutations={mutation_at: mutation}, limit=limit,
    )

    row = classify_dependency_fact_mutation(
        mutation=mutation, update_index=mutation_at,
        control=control.executions[mutation_at],
        mutated=mutated.executions[mutation_at],
    )
    print(
        f"\n[e1/checker-mutation] {row.mutation_kind} at update "
        f"{row.update_index} on {row.target_step!r}: "
        f"detected={row.detected} mode={row.detection_mode}\n"
        f"    refusal: {row.refusal}\n"
        f"    evidence: {dict(row.mutation_evidence)}"
    )
    assert row.detected, (
        f"the moved dependency class was NOT caught "
        f"(evidence: {dict(row.mutation_evidence)})"
    )
    assert row.detection_mode == DETECTION_MODE_INVALIDATED_AND_RERAN
    assert row.mutation_kind == MUTATION_DEPENDENCY_FACT
    # the mutation cost real work: the step it broke re-ran.
    assert (mutated.executions[mutation_at].ledger.steps_run
            > control.executions[mutation_at].ledger.steps_run)


@pytest.mark.postgres
def test_the_measured_metrics_attach_to_the_e1_report_and_it_still_passes(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """E1 completion, end to end: the pure-half report with BOTH live-path
    metrics attached. The verdict is graded on the attached rows too, so a
    checker mutation that escaped detection would refute the report rather
    than sit beside a passing one."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    limit = min(_E1_UPDATES, len(parity.updates))
    binding = _binding(parity, limit)

    _reset_between_modes(dsn)
    complete = measure_replay(
        replay_mode, binding=binding, trace_set_id="e1-report/complete",
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-rep-complete", parity=parity,
        mode=MODE_COMPLETE, policies=policies, checks=checks, limit=limit,
    )
    _reset_between_modes(dsn)
    differential = measure_replay(
        replay_mode, binding=binding, trace_set_id="e1-report/differential",
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-rep-differential", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks, limit=limit,
    )
    saved = measure_runtime_saved(
        complete, differential,
        history_binding=parity.updates[limit - 1].digest,
    )

    mutation_at = 2
    mutation_limit = mutation_at + 1
    _reset_between_modes(dsn)
    control = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-rep-ctl", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
        limit=mutation_limit,
    )
    _reset_between_modes(dsn)
    dep_mutation = Mutation(kind=MUTATION_DEPENDENCY_FACT)
    dep_mutated = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-rep-dep", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
        mutations={mutation_at: dep_mutation}, limit=mutation_limit,
    )
    dep_row = classify_dependency_fact_mutation(
        mutation=dep_mutation, update_index=mutation_at,
        control=control.executions[mutation_at],
        mutated=dep_mutated.executions[mutation_at],
    )

    _reset_between_modes(dsn)
    corrupt_mutation = Mutation(kind=MUTATION_CORRUPT_CERTIFICATE)
    raised: BaseException | None = None
    try:
        replay_mode(
            dsn=dsn, deployment_dir=_DEP_DIR,
            corpus_dir=tmp_path / "corpus-rep-corrupt", parity=parity,
            mode=MODE_DIFFERENTIAL, policies=policies, checks=checks,
            mutations={mutation_at: corrupt_mutation}, limit=mutation_limit,
        )
    except SubstrateError as exc:
        raised = exc
    corrupt_row = classify_certificate_corruption(
        mutation=corrupt_mutation, update_index=mutation_at, raised=raised,
    )

    report = run_report(build_manifest(seeds=(11,), updates_per_seed=12))
    assert report.verdict == "pass"
    attached = report.with_smoke_metrics(
        runtime_saved=saved, checker_mutations=(dep_row, corrupt_row),
    )

    import json

    payload = json.loads(attached.canonical_json())
    print(
        f"\n[e1/report] verdict={payload['verdict']} "
        f"runtime_saved={payload['runtime_saved']['duration_saved']} "
        f"cost_saved={payload['runtime_saved']['cost_saved']} "
        f"mutations={[(r['mutation_kind'], r['detected']) for r in payload['checker_mutation_detection']['rows']]}"
    )
    assert payload["verdict"] == "pass"
    assert payload["checker_mutation_detection"]["status"] == "measured"
    assert len(payload["checker_mutation_detection"]["rows"]) == 2
    assert all(
        r["detected"] for r in payload["checker_mutation_detection"]["rows"]
    )
    assert payload["runtime_saved"]["status"] in (
        RUNTIME_SAVED_MEASURED, RUNTIME_SAVED_PARTIALLY_KNOWN
    )
    assert not attached.undetected_mutations()
