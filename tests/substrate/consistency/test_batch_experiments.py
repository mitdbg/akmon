"""PACT ``concord-disclosure-budget``, task.batch.candidate-planner —
the E2 scheduler comparison and E3's synthetic mechanics.

Covers the task's verification clause: the candidate roster (no CP-SAT or
ILP — a NAMED deferral), the preregistered manifest and its pinned
digest, the hand-built instance corpus, checked feasibility and value
through the independent checker only, oracle gaps inside the size guard
and a named refusal outside it, deterministic replay, clone/split
non-credit, and every falsification clause reported whether it fired or
not.

Design references: ``designs/batch-scheduling-research.md`` §8, §9, §9.1,
§11 (preamble, E2, E3), §12. Pure tests: nothing here touches a database.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.bench.synth import batch_experiments as bx
from okg.bench.synth.batch_experiments import (
    CLAUSES,
    DEFERRED,
    DETERMINISTIC_CANDIDATES,
    PARTIAL_E2_DEFERRALS,
    PREREGISTERED_MANIFEST_DIGEST,
    RANDOM_CANDIDATE,
    REPLICATE_NONE,
    STATUS_CHECKED_FEASIBLE,
    STATUS_PLANNER_REFUSED,
    STATUS_REFUSED,
    ZERO_TOLERANCE,
    BatchExperimentError,
    build_manifest,
    clone_obligation,
    hand_built_specs,
    plan_seeded_random,
    run_report,
    split_obligation,
    swap_tier_boundary,
    synthetic_raw,
)
from okg.substrate.consistency.batch_schedule import (
    load_batch_instance,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    BaselineRefused,
    plan_fifo,
)
from okg.substrate.consistency.batch_schedule_checker import (
    OPTIMALITY_RECHECK_MAX_ACTIONS,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import DEFAULT_MAX_ACTIONS


# --------------------------------------------------------------------------- #
# One full run, shared. The grid is exhaustive per instance, so running it     #
# once and asserting over the report is the honest shape — every assertion     #
# below reads the SAME evidence a reader of the report would.                  #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def report() -> bx.E2Report:
    return run_report(build_manifest())


@pytest.fixture(scope="module")
def small_manifest() -> bx.E2Manifest:
    """A deliberately tiny manifest for the repeat-run witnesses."""
    return build_manifest(
        hand_built=("shared-coverage", "unknown-revelation-defer"),
        synthetic_seeds=(101,),
        replicate_seeds=(7,),
    )


# --------------------------------------------------------------------------- #
# Preregistration (§11 preamble).                                              #
# --------------------------------------------------------------------------- #
def test_the_manifest_digest_is_pinned() -> None:
    """An outcome-driven edit cannot pass silently.

    Changing the corpus, the seeds, the clauses, the comparator, or the
    threshold changes this digest, so it must be changed in the same
    reviewable diff. That is the whole mechanism §11's preamble asks for."""
    assert build_manifest().digest() == PREREGISTERED_MANIFEST_DIGEST


def test_the_manifest_declares_every_preregistered_element() -> None:
    manifest = build_manifest()
    assert manifest.zero_tolerance, "zero-tolerance rules are preregistered"
    assert set(manifest.zero_tolerance) == set(ZERO_TOLERANCE)
    assert set(manifest.falsification_clauses) == set(CLAUSES)
    for field in (manifest.primary_metric, manifest.comparator,
                  manifest.comparator_threshold, manifest.aggregation,
                  manifest.uncertainty, manifest.threshold):
        assert field, "every preregistered field carries text, not a blank"
    assert manifest.replicate_seeds, "the seed/replicate set is preregistered"
    assert "fifo" in manifest.candidates
    assert RANDOM_CANDIDATE in manifest.candidates


def test_the_manifest_is_built_from_configuration_alone() -> None:
    """A manifest that could read a result could be shaped by one."""
    import inspect

    signature = inspect.signature(build_manifest)
    for name, parameter in signature.parameters.items():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, name
    assert "report" not in signature.parameters
    assert "result" not in signature.parameters
    assert "outcome" not in signature.parameters


def test_the_manifest_refuses_an_empty_corpus() -> None:
    with pytest.raises(BatchExperimentError):
        build_manifest(hand_built=())
    with pytest.raises(BatchExperimentError):
        build_manifest(replicate_seeds=())


# --------------------------------------------------------------------------- #
# The deferrals (§9 solver decision, E2 history windows, E3 elicitation).      #
# --------------------------------------------------------------------------- #
def test_every_deferral_is_a_named_manifest_row(
    report: bx.E2Report,
) -> None:
    """A deferral is a row. An absence that is not written down is a gap."""
    required = {
        "cpsat_ilp_candidates",
        "history_window_batches",
        "e3_stakeholder_elicitation",
    }
    assert required <= set(DEFERRED)
    assert required <= set(build_manifest().deferred)
    assert required <= set(report.deferred)
    assert required <= set(report.to_canonical_dict()["deferred"])
    for name in required:
        assert len(DEFERRED[name]) > 80, (
            f"the {name} deferral states WHY and WHAT INSTEAD, not a stub")


def test_no_solver_candidate_is_silently_absent() -> None:
    """CP-SAT/ILP are deferred by name, and the roster says what ran."""
    manifest = build_manifest()
    assert "none" in manifest.solver_families
    assert "DEFERRED" in manifest.deferred["cpsat_ilp_candidates"]
    assert "CP-SAT" in bx.__doc__ or "CP-SAT" in manifest.solver_families
    for name in manifest.candidates:
        assert "cpsat" not in name and "ilp" not in name


def test_no_elicited_cardinal_utility_can_reach_the_scheduler() -> None:
    """E3: cardinal values score robustness; they never drive selection.

    There are none to use — the elicitation half is deferred — so the
    assertion is structural: the objective has no numeric stage where one
    could enter."""
    import inspect

    bx.assert_no_cardinal_utility_inputs()

    # Structural, not lexical: a planner that cannot be HANDED a utility
    # vector cannot be steered by one. Every deterministic arm takes the
    # instance and nothing else; the random arm takes the instance and a
    # seed.
    for name, planner in DETERMINISTIC_CANDIDATES:
        parameters = list(inspect.signature(planner).parameters)
        assert parameters == ["instance"], f"{name}: {parameters}"
    assert list(inspect.signature(plan_seeded_random).parameters) == [
        "instance", "seed"]

    # And the elicitation half is deferred by name rather than simulated.
    assert "AUTHORIZED HUMAN DOMAIN OWNER" in DEFERRED[
        "e3_stakeholder_elicitation"]
    assert "never fabricated" in DEFERRED["e3_stakeholder_elicitation"]


# --------------------------------------------------------------------------- #
# The instance corpus.                                                         #
# --------------------------------------------------------------------------- #
def test_every_hand_built_instance_loads_fail_closed() -> None:
    """It loads through the real loader, and its demands name every pool."""
    for spec in hand_built_specs():
        instance = spec.load()
        assert 4 <= len(instance.actions) <= 12
        resource_ids = {r.id for r in instance.resources}
        for action in instance.actions:
            assert set(action.demand) == resource_ids, (
                f"{spec.name}/{action.id} must name every resource; an "
                "omitted key is never a zero")
        assert any(o.priority == "must" for o in instance.obligations), (
            f"{spec.name} has no mandatory unit, so its hard feasibility "
            "condition would be vacuous")


def test_the_corpus_covers_every_named_edge() -> None:
    names = {s.name for s in hand_built_specs()}
    assert {
        "shared-coverage",
        "prerequisite-closure",
        "conflicts-and-group",
        "cumulative-resources",
        "capacity-breakpoint",
        "must-critical-chain",
        "over-oracle-guard",
    } <= names
    specs = {s.name: s for s in hand_built_specs()}
    # A group constraint, not only pairwise conflicts.
    assert specs["conflicts-and-group"].load().conflict_groups
    # A real capacity breakpoint, not a constant pool.
    breakpoints = [
        len(r.segments) for r in specs["capacity-breakpoint"].load().resources]
    assert max(breakpoints) >= 2
    # A must-critical prerequisite chain at least three deep.
    chain = specs["must-critical-chain"].load()
    gate = next(a for a in chain.actions if "o_must" in a.covers)
    depth, current = 1, gate
    while current.requires:
        current = chain.action_by_id(current.requires[0])
        depth += 1
    assert depth >= 3


def test_every_hand_built_instance_is_checker_valid_for_some_candidate(
    report: bx.E2Report,
) -> None:
    for spec in hand_built_specs():
        rows = [o for o in report.outcomes if o.instance == spec.name]
        assert rows, spec.name
        assert any(o.status == STATUS_CHECKED_FEASIBLE for o in rows), (
            f"{spec.name} has no candidate the checker accepts")


def test_the_synthetic_generator_is_prf_keyed_and_replays() -> None:
    first = json.dumps(synthetic_raw(101), sort_keys=True)
    second = json.dumps(synthetic_raw(101), sort_keys=True)
    assert first == second
    assert first != json.dumps(synthetic_raw(102), sort_keys=True)
    for seed in (101, 102, 103, 104):
        instance = load_batch_instance(synthetic_raw(seed))
        assert len(instance.actions) in bx.SYNTHETIC_ACTION_SIZES
        assert len(instance.actions) <= DEFAULT_MAX_ACTIONS


# --------------------------------------------------------------------------- #
# The candidates.                                                              #
# --------------------------------------------------------------------------- #
def test_each_deterministic_candidate_is_deterministic() -> None:
    for spec in hand_built_specs():
        instance = spec.load()
        for name, planner in DETERMINISTIC_CANDIDATES:
            try:
                first = planner(instance)
            except BaselineRefused as refusal:
                # A NAMED infeasibility is an answer; it must be the same
                # answer every time.
                with pytest.raises(BaselineRefused) as second:
                    planner(instance)
                assert (second.value.data.get("code")
                        == refusal.data.get("code")), f"{spec.name}/{name}"
                continue
            assert first.digest == planner(instance).digest, (
                f"{spec.name}/{name} did not replay byte-equal")


def test_each_candidate_plan_is_checked_or_refused_by_name(
    report: bx.E2Report,
) -> None:
    """No third state. Either the checker accepted it, or something is named."""
    for outcome in report.outcomes:
        assert outcome.status in (
            STATUS_CHECKED_FEASIBLE, STATUS_REFUSED, STATUS_PLANNER_REFUSED)
        if outcome.status != STATUS_CHECKED_FEASIBLE:
            assert outcome.causes, (
                f"{outcome.instance}/{outcome.candidate} failed without a "
                "named cause")


def test_the_seeded_random_arm_replays_byte_equal() -> None:
    instance = hand_built_specs()[0].load()
    first = plan_seeded_random(instance, seed="7")
    assert first.digest == plan_seeded_random(instance, seed="7").digest
    other = plan_seeded_random(instance, seed="19")
    assert other.digest != first.digest, (
        "a diagnostic whose draw does not move with its seed is not a "
        "diagnostic")


def test_the_random_arm_uses_no_module_level_randomness() -> None:
    text = Path(bx.__file__ or "").read_text(encoding="utf-8")
    assert "import random" not in text
    assert "random.random" not in text
    assert "time.time" not in text


# --------------------------------------------------------------------------- #
# The checker is the only authority.                                           #
# --------------------------------------------------------------------------- #
def test_the_reported_value_is_the_checkers_recomputation(
    report: bx.E2Report,
) -> None:
    """Re-check one accepted plan per instance and compare tuples.

    The harness must never report a value the checker did not compute."""
    seen: set[str] = set()
    for outcome in report.outcomes:
        if (outcome.status != STATUS_CHECKED_FEASIBLE
                or outcome.instance in seen
                or outcome.candidate == RANDOM_CANDIDATE):
            continue
        seen.add(outcome.instance)
        spec = next(
            (s for s in hand_built_specs() if s.name == outcome.instance), None)
        if spec is None:
            continue
        instance = spec.load()
        plan = bx._plan_candidate(
            instance, outcome.candidate, outcome.replicate)
        certificate = bx._certify(
            instance, plan, outcome.candidate, None)
        result = check_schedule(instance, plan, certificate)
        assert result.accepted
        assert result.recomputed_objective == outcome.objective
    assert seen


def test_a_candidate_is_never_called_optimal_without_an_oracle_artifact(
    report: bx.E2Report,
) -> None:
    admitted = {r.instance for r in report.oracle_rows if r.admitted}
    for outcome in report.outcomes:
        if outcome.optimality != bx.OPTIMALITY_CERTIFIED:
            continue
        assert outcome.instance in admitted, (
            f"{outcome.instance}/{outcome.candidate} claims optimality on an "
            "instance the oracle never admitted")
        assert outcome.status == STATUS_CHECKED_FEASIBLE
        assert outcome.oracle_gap is not None
        assert outcome.oracle_gap["first_worse_stage"] == 0
        assert not outcome.not_established, (
            "optimality is not available over an objective with an "
            "unenforceable dimension")


def test_an_unenforceable_bound_blocks_optimality_but_not_checking(
    report: bx.E2Report,
) -> None:
    """§7: an unknown bound cannot support a hard-budget guarantee."""
    rows = [o for o in report.outcomes
            if o.instance == "unenforceable-bound"
            and o.status == STATUS_CHECKED_FEASIBLE]
    assert rows
    assert any(o.not_established for o in rows), (
        "the UNKNOWN reservation/cap must surface as not_established, not "
        "as a silent zero")
    for outcome in rows:
        if outcome.not_established:
            assert outcome.optimality != bx.OPTIMALITY_CERTIFIED


# --------------------------------------------------------------------------- #
# The oracle boundary (§9.1).                                                  #
# --------------------------------------------------------------------------- #
def test_gap_rows_exist_only_where_the_oracle_admitted_the_instance(
    report: bx.E2Report,
) -> None:
    admitted = {r.instance for r in report.oracle_rows if r.admitted}
    for outcome in report.outcomes:
        if outcome.oracle_gap is None:
            continue
        assert outcome.instance in admitted
        assert outcome.status == STATUS_CHECKED_FEASIBLE


def test_the_straddling_instance_yields_a_named_refusal(
    report: bx.E2Report,
) -> None:
    """The over-guard instance documents the refusal instead of hiding it."""
    spec = next(s for s in hand_built_specs() if s.straddles_oracle_guard)
    assert len(spec.load().actions) > DEFAULT_MAX_ACTIONS
    assert len(spec.load().actions) > OPTIMALITY_RECHECK_MAX_ACTIONS
    row = next(r for r in report.oracle_rows if r.instance == spec.name)
    assert not row.admitted
    assert row.refusal_code == "instance_too_large"
    assert row.best_objective is None
    assert row.candidates_examined is None, (
        "an unexamined search space is None, never 0")
    rows = [o for o in report.outcomes if o.instance == spec.name]
    assert rows
    for outcome in rows:
        assert outcome.oracle_gap is None
        assert outcome.optimality != bx.OPTIMALITY_CERTIFIED
        if outcome.status == STATUS_CHECKED_FEASIBLE:
            assert "refused" in outcome.optimality_detail


def test_an_infeasible_instance_is_reported_not_repaired(
    report: bx.E2Report,
) -> None:
    """No publishable plan is an ANSWER (§9.1), not a reason to relax."""
    refusals = [r for r in report.oracle_rows
                if not r.admitted and r.refusal_code == "no_feasible_candidate"]
    for row in refusals:
        rows = [o for o in report.outcomes if o.instance == row.instance]
        assert rows
        assert all(o.status != STATUS_CHECKED_FEASIBLE for o in rows), (
            "the oracle found no feasible candidate, so no planner may have "
            "one accepted either")


# --------------------------------------------------------------------------- #
# E3's synthetic mechanics.                                                    #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mutate", [clone_obligation, split_obligation])
def test_cloning_or_splitting_an_obligation_earns_no_credit(
    mutate: Any,
) -> None:
    """Every candidate, every instance: the WHOLE decision must not move.

    Stages 1-2 are invariant under both mutators for every possible
    candidate, because coverage is counted in units and the mutators are
    unit-preserving by construction. So the witness compares stages 1-4
    plus the selected set: cost and makespan are the stages where a
    relabelling-driven improvement can actually land."""
    checked = 0
    for spec in hand_built_specs():
        instance = spec.load()
        target = next(
            (o.id for o in instance.obligations
             if o.priority in ("should", "may")), None)
        if target is None:
            continue
        variant = load_batch_instance(mutate(spec.raw, target))
        for name, planner in DETERMINISTIC_CANDIDATES:
            try:
                base_plan = planner(instance)
                variant_plan = planner(variant)
            except BaselineRefused:
                continue
            base = check_schedule(
                instance, base_plan, bx._certify(instance, base_plan, name, None)
            ).recomputed_objective
            moved = check_schedule(
                variant, variant_plan,
                bx._certify(variant, variant_plan, name, None)
            ).recomputed_objective
            if base is None or moved is None:
                continue
            checked += 1
            assert (base.should_units, base.may_units, base.total_cost,
                    base.makespan) == (
                moved.should_units, moved.may_units, moved.total_cost,
                moved.makespan), (
                f"{spec.name}/{name}: relabelling an obligation moved the "
                "checked objective")
            assert (sorted(s.action_id for s in base_plan.selected)
                    == sorted(s.action_id for s in variant_plan.selected)), (
                f"{spec.name}/{name}: relabelling an obligation moved the "
                "selected set")
    assert checked, "the clone/split witness exercised nothing"


# --------------------------------------------------------------------------- #
# EWD-BXP-1: the clone/split falsifier must check the clause it names.         #
# --------------------------------------------------------------------------- #
def _plan_obligation_count_greedy(instance: Any) -> Any:
    """The reviewer's GAMED candidate, ported verbatim in rule.

    "Among actions with equal new-unit gain, prefer the one that closes
    the most obligations." Counting obligations rather than units is the
    exact mistake §6 forbids, and it is invisible to a falsifier that
    only compares coverage units: the gaming shows up in COST."""
    chosen: list[str] = []
    covered: set[str] = set()
    for priority in ("must", "should", "may"):
        while True:
            ranked: list[tuple[Any, ...]] = []
            for action in instance.actions:
                if action.id in chosen:
                    continue
                gain = bx._closure_units(
                    instance, action.id, priority) - covered
                if not gain:
                    continue
                closes = 0
                for member in bx._dependency_order(instance, [action.id]):
                    for oid in bx._action_of(instance, member).covers:
                        obligation = instance.obligation_by_id(oid)
                        if (obligation is not None
                                and obligation.priority == priority):
                            closes += 1
                ranked.append((-len(gain), -closes, action.id))
            if not ranked:
                break
            best_id = min(ranked)[-1]
            for member in bx._dependency_order(instance, [best_id]):
                if member not in chosen:
                    chosen.append(member)
                for oid in bx._action_of(instance, member).covers:
                    obligation = instance.obligation_by_id(oid)
                    if obligation is not None:
                        covered.add(obligation.unit.key)
    return bx._assemble(instance, bx._dependency_order(instance, chosen))


def _clone_gaming_spec() -> bx.InstanceSpec:
    """Two rival actions on the SAME unit, filed as separate obligations.

    Cloning the cheap rival's obligation gives the counting rule a reason
    to switch actions without changing one coverage unit."""
    return bx.InstanceSpec(
        name="clone-gaming",
        source="hand_built",
        exercises="an adversarial candidate that counts obligations, not units",
        raw=bx._instance(
            "e2-clone-gaming",
            obligations=[
                bx._obligation("o_gate", "must", "gate",
                               obligation_class="mandatory"),
                # The clone target: `_run_robustness` takes the first
                # should/may obligation, and this one belongs to the
                # cheap rival.
                bx._obligation("o_cheap", "should", "s"),
                bx._obligation("o_exp", "should", "s"),
            ],
            actions=[
                bx._action("a_gate", covers=["o_gate"], demand={"cpu": 1},
                           duration=1, execution_cost=1),
                # Ids matter: the closure-based rule breaks ties on the
                # action id, so `a_gate` must sort ahead of both rivals
                # and the expensive rival ahead of the cheap one — that
                # is what makes the BASE plan take the expensive action.
                bx._action("b_exp", action_class="sweep", covers=["o_exp"],
                           requires=["a_gate"], demand={"cpu": 1},
                           duration=1, execution_cost=9),
                bx._action("z_cheap", action_class="check",
                           covers=["o_cheap"], requires=["a_gate"],
                           demand={"cpu": 1}, duration=1, execution_cost=1),
            ],
            resources=[bx._pool("cpu", [{"start": 0, "capacity": 2}])],
        ),
    )


def test_the_clone_falsifier_catches_a_candidate_that_counts_obligations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The memo's repro: an eight-unit cost win bought purely by cloning.

    The weak form of this invariant — stages 1 and 2 only — cannot see
    it, because both mutators preserve coverage units by construction.
    The shipped form compares stages 1-4 and the selected set, so the
    zero-tolerance clause FIRES and the run is refuted."""
    spec = _clone_gaming_spec()
    base_instance = spec.load()
    clone_instance = load_batch_instance(
        clone_obligation(spec.raw, "o_cheap"))

    base_plan = _plan_obligation_count_greedy(base_instance)
    clone_plan = _plan_obligation_count_greedy(clone_instance)
    base = check_schedule(
        base_instance, base_plan,
        bx._certify(base_instance, base_plan, "gamed", None),
    ).recomputed_objective
    moved = check_schedule(
        clone_instance, clone_plan,
        bx._certify(clone_instance, clone_plan, "gamed", None),
    ).recomputed_objective
    assert base is not None and moved is not None

    # The premise: cloning bought a real objective improvement...
    assert moved.total_cost < base.total_cost
    assert sorted(s.action_id for s in base_plan.selected) != sorted(
        s.action_id for s in clone_plan.selected)
    # ...that the weak, units-only comparison cannot observe.
    assert (base.should_units, base.may_units) == (
        moved.should_units, moved.may_units), (
        "the repro is only interesting while the weak form stays quiet")

    monkeypatch.setattr(bx, "hand_built_specs", lambda: (spec,))
    monkeypatch.setattr(bx, "DETERMINISTIC_CANDIDATES", (
        ("fifo", plan_fifo),
        ("obligation_count_greedy", _plan_obligation_count_greedy),
    ))
    gamed = run_report(build_manifest(
        hand_built=("clone-gaming",), synthetic_seeds=(), replicate_seeds=(7,)))

    clause = next(row for row in gamed.falsifications
                  if row.clause == "clone_or_split_changes_coverage_value")
    assert clause.fired, "the strengthened falsifier must catch the gaming"
    assert clause.evaluated > 0
    assert gamed.verdict == (
        "refuted:clone_or_split_changes_coverage_value")
    broken = [row for row in gamed.robustness
              if row.kind in ("clone", "split") and not row.invariant_held]
    assert broken
    assert all(row.candidate == "obligation_count_greedy" for row in broken), (
        "fifo does not count obligations and must not be implicated")


def test_the_clone_falsifier_stays_quiet_on_every_shipped_candidate(
    report: bx.E2Report,
) -> None:
    """Strengthening it cost no shipped result: 0 violations on the corpus."""
    rows = [row for row in report.robustness
            if row.kind in ("clone", "split")]
    assert rows
    assert all(row.invariant_held for row in rows)
    # The rows must SAY what was compared, so a reader can tell the
    # strong form from the weak one without reading the source.
    for row in rows:
        assert "cost,makespan" in row.detail
        assert "selected" in row.detail
    clause = next(row for row in report.falsifications
                  if row.clause == "clone_or_split_changes_coverage_value")
    assert not clause.fired
    assert clause.evaluated == len(rows)
    assert "cost" in CLAUSES["clone_or_split_changes_coverage_value"]
    assert "SELECTED SET" in CLAUSES[
        "clone_or_split_changes_coverage_value"]


# --------------------------------------------------------------------------- #
# EWD-BXP-2: the seed-leak probe must have a channel to observe.               #
# --------------------------------------------------------------------------- #
def test_the_seed_probe_catches_a_deterministic_arm_that_reads_its_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The memo's spy repro: the seed must actually ARRIVE.

    Before this, `_plan_candidate` accepted the replicate and then called
    `planner(instance)`, so the probe issued three byte-identical calls
    and could only ever detect global nondeterminism. The entry point now
    offers the seed to any arm that declares one, and an arm that reads
    it is caught."""
    seen: list[str] = []

    def plan_seed_spy(instance: Any, *, seed: str = REPLICATE_NONE) -> Any:
        seen.append(seed)
        wanted = [a.id for a in instance.actions]
        if seed.isdigit() and int(seed) > 10:
            wanted = [a.id for a in instance.actions if a.covers]
        return bx._assemble(
            instance, bx._dependency_order(instance, wanted))

    monkeypatch.setattr(bx, "DETERMINISTIC_CANDIDATES", (
        ("fifo", plan_fifo),
        ("seed_spy", plan_seed_spy),
    ))
    monkeypatch.setattr(
        bx, "hand_built_specs",
        lambda: tuple(s for s in hand_built_specs()
                      if s.name == "shared-coverage"))
    leaky = run_report(build_manifest(
        hand_built=("shared-coverage",), synthetic_seeds=(),
        replicate_seeds=(7, 19, 31)))

    assert {"7", "19", "31"} <= set(seen), (
        "the replicate seed never reached the arm, so the probe would be "
        f"measuring nothing; saw {sorted(set(seen))}")
    clause = next(row for row in leaky.falsifications
                  if row.clause == "deterministic_candidate_unstable_across_"
                                   "seeds")
    assert clause.fired
    assert leaky.verdict == (
        "refuted:deterministic_candidate_unstable_across_seeds")
    caught = [row for row in leaky.robustness
              if row.kind == "seed_stability" and not row.invariant_held]
    assert [row.candidate for row in caught] == ["seed_spy"]


def test_the_shipped_deterministic_arms_survive_the_seed_taking_entry_point(
    report: bx.E2Report,
) -> None:
    """Same probe, shipped roster: byte-equal digests across every seed."""
    manifest = build_manifest()
    checked = 0
    for spec in hand_built_specs():
        instance = spec.load()
        for name, _ in DETERMINISTIC_CANDIDATES:
            digests: set[str] = set()
            for seed in manifest.replicate_seeds:
                try:
                    digests.add(
                        bx._plan_candidate(instance, name, str(seed)).digest)
                except BaselineRefused as refusal:
                    digests.add(f"refused:{refusal.data.get('code')}")
            assert len(digests) == 1, f"{spec.name}/{name}: {digests}"
            checked += 1
    assert checked
    clause = next(row for row in report.falsifications
                  if row.clause == "deterministic_candidate_unstable_across_"
                                   "seeds")
    assert not clause.fired and clause.evaluated > 0


def test_the_entry_point_offers_the_seed_only_where_there_is_a_channel(
) -> None:
    """No shipped deterministic arm declares a seed, and none is invented."""
    import inspect

    for name, planner in DETERMINISTIC_CANDIDATES:
        assert not bx._takes_seed(planner), name
        assert list(inspect.signature(planner).parameters) == ["instance"]

    def takes_one(instance: Any, *, seed: str = "-") -> Any:
        return plan_fifo(instance)

    assert bx._takes_seed(takes_one)


# --------------------------------------------------------------------------- #
# EWD-BXP-3: makespan must decide something, or say that it does not.          #
# --------------------------------------------------------------------------- #
def test_makespan_is_a_deciding_stage_somewhere_in_the_corpus(
    report: bx.E2Report,
) -> None:
    """§8 stage 4 decides real comparisons, and the histogram shows it.

    Before the two `makespan-decides-*` instances, stage 4 decided ZERO
    comparisons in the whole report: every non-trivial comparison was
    settled by cost, and the one baseline built for makespan never won.
    The histogram is a reported field so a stage that stops deciding
    anything becomes visible instead of having to be reverse-engineered."""
    stages = dict(report.comparator.deciding_stages)
    assert stages.get("stage_4_makespan", 0) >= 2, stages

    for name in ("makespan-decides-parallel", "makespan-decides-breakpoint"):
        winner = report.comparator.per_instance_winner[name]
        assert winner == "tier_earliest_finish", f"{name}: {winner}"
        base = next(o for o in report.outcomes
                    if o.instance == name and o.candidate == "fifo")
        rival = next(o for o in report.outcomes
                     if o.instance == name
                     and o.candidate == "tier_earliest_finish")
        assert base.objective is not None and rival.objective is not None
        # Stages 1-3 tie, so the comparison is settled on makespan alone.
        assert (base.objective.should_units, base.objective.may_units,
                base.objective.total_cost) == (
            rival.objective.should_units, rival.objective.may_units,
            rival.objective.total_cost)
        assert rival.objective.makespan < base.objective.makespan
        assert bx._deciding_stage(
            base.objective, rival.objective) == "stage_4_makespan"

    row = next(r for r in report.comparator.per_candidate
               if r.candidate == "tier_earliest_finish")
    assert row.better >= 2, (
        "the baseline built for makespan must be able to win on it")


def test_the_makespan_instances_stay_inside_the_oracles_size_guard() -> None:
    for name in ("makespan-decides-parallel", "makespan-decides-breakpoint"):
        spec = next(s for s in hand_built_specs() if s.name == name)
        instance = spec.load()
        assert len(instance.actions) <= DEFAULT_MAX_ACTIONS
        assert not spec.straddles_oracle_guard
        # Every action discharges a distinct unit, which is what makes a
        # deferral cost coverage and forces stages 1-3 to tie.
        units = [o.unit.key for o in instance.obligations]
        assert len(units) == len(set(units)) == len(instance.actions)


# --------------------------------------------------------------------------- #
# EWD-BXP-4: one rule counted twice must be visible as one rule.               #
# --------------------------------------------------------------------------- #
def test_every_candidate_carries_its_own_comparator_row(
    report: bx.E2Report,
) -> None:
    row = report.comparator
    named = [r.candidate for r in row.per_candidate]
    assert named == [name for name, _ in DETERMINISTIC_CANDIDATES
                     if name != "fifo"]
    instances = {o.instance for o in report.outcomes}
    for candidate in row.per_candidate:
        total = (candidate.better + candidate.tied + candidate.worse
                 + candidate.not_comparable)
        assert total == len(instances), candidate.candidate
        comparable = candidate.better + candidate.tied + candidate.worse
        assert candidate.beats_comparator == (
            f"{candidate.better}/{comparable}")
        # Recompute this candidate's own record from the outcomes.
        better = 0
        for instance in sorted(instances):
            base = next((o for o in report.outcomes
                         if o.instance == instance and o.candidate == "fifo"),
                        None)
            rival = next((o for o in report.outcomes
                          if o.instance == instance
                          and o.candidate == candidate.candidate), None)
            if (base is None or rival is None or base.objective is None
                    or rival.objective is None
                    or base.status != STATUS_CHECKED_FEASIBLE
                    or rival.status != STATUS_CHECKED_FEASIBLE):
                continue
            if objective_sort_key(rival.objective) < objective_sort_key(
                    base.objective):
                better += 1
        assert better == candidate.better, candidate.candidate

    # Two of the four rivals never beat the baseline at all — a fact the
    # pool aggregate on its own cannot express.
    assert [r.candidate for r in row.per_candidate if r.better == 0] == [
        "dependency_tier_greedy"]


def test_duplicate_candidates_are_disclosed_as_one_rule(
    report: bx.E2Report,
) -> None:
    """The memo's per-candidate breakdown: two winners, one function.

    `marginal_coverage_greedy` and `coverage_per_cost_greedy` produce the
    same checked outcome on every instance of this corpus. Counting them
    as two independent winners would be counting one rule twice, so the
    equivalence is a named row and the headline is derived from it."""
    groups = report.comparator.equivalent_candidates
    assert ("coverage_per_cost_greedy", "marginal_coverage_greedy") in groups

    # The disclosure is true: identical status, selection and value on
    # every instance.
    for instance in sorted({o.instance for o in report.outcomes}):
        rows = {o.candidate: o for o in report.outcomes
                if o.instance == instance}
        first = rows["marginal_coverage_greedy"]
        second = rows["coverage_per_cost_greedy"]
        assert (first.status, first.selected, first.objective) == (
            second.status, second.selected, second.objective), instance

    headline = report.comparator.headline
    assert "2 distinct rule(s)" in headline
    assert "coverage_per_cost_greedy == marginal_coverage_greedy" in headline
    assert "carried by 3 candidate(s)" in headline
    # The clause detail carries it too, so the digested report cannot
    # read as two independent winners.
    clause = next(row for row in report.falsifications
                  if row.clause == "simple_baseline_comparator_not_beaten")
    assert headline in clause.detail
    # And the new surfaces travel in the digested content.
    document = json.loads(report.canonical_json())["comparator"]
    assert document["per_candidate"]
    assert document["equivalent_candidates"]
    assert document["deciding_stages"]
    assert document["headline"] == headline


def test_the_preregistered_comparator_threshold_is_unchanged(
    report: bx.E2Report,
) -> None:
    """The per-candidate surfaces sit BESIDE the registered rule, not on it.

    The threshold text stays existential over the pool and the achieved
    value stays the instance fraction; nothing here retro-fits the
    registered comparison to the finding."""
    from fractions import Fraction

    row = report.comparator
    manifest = build_manifest()
    assert row.threshold == manifest.comparator_threshold == "1/2"
    assert "at least 1/2 of the instances" in manifest.threshold
    achieved = Fraction(
        row.instances_candidate_better, row.instances_compared)
    assert row.achieved == f"{achieved.numerator}/{achieved.denominator}"
    assert row.met is (achieved >= Fraction(1, 2))


# --------------------------------------------------------------------------- #
# EWD-BXP-5: a partial E2 must not read as E2.                                 #
# --------------------------------------------------------------------------- #
def test_a_partial_e2_run_never_reports_a_bare_pass(
    report: bx.E2Report,
) -> None:
    """The verdict carries its own partiality, derived from the rows."""
    assert report.verdict == (
        "pass_partial_e2:cpsat_ilp_candidates,history_window_batches,"
        "overhead_adjusted_comparator")
    assert report.verdict != "pass"
    assert not report.fired()
    for name in PARTIAL_E2_DEFERRALS:
        assert name in report.deferred
        assert name in report.verdict
    # It is derived: drop a deferral and the verdict follows.
    assert bx.E2Report(
        manifest_digest=report.manifest_digest,
        instances=report.instances,
        oracle_rows=report.oracle_rows,
        outcomes=report.outcomes,
        comparator=report.comparator,
        robustness=report.robustness,
        falsifications=report.falsifications,
        deferred={},
        timings=report.timings,
    ).verdict == "pass"
    assert json.loads(report.canonical_json())["verdict"] == report.verdict


def test_the_comparator_clause_text_keeps_the_designs_overhead_words(
) -> None:
    """The digested clause must not silently drop words from the design.

    The design's E2 falsifier says "after planning overhead". The run
    does not charge overhead, so the words stay in the clause with an
    explicit deferral marker rather than being deleted."""
    text = CLAUSES["simple_baseline_comparator_not_beaten"]
    assert "AFTER PLANNING OVERHEAD" in text
    assert "[DEFERRED: overhead_adjusted_comparator" in text
    for phrase in ("declared seeds", "aggregation", "uncertainty method",
                   "threshold"):
        assert phrase in text
    assert text == build_manifest().falsification_clauses[
        "simple_baseline_comparator_not_beaten"]


def test_the_corpus_action_range_shortfall_is_a_named_deferral() -> None:
    """The design asks for 6-12 actions; the oracle's bound is 6.

    Those two sentences are jointly unsatisfiable, enumerability was
    kept, and the shortfall is a row rather than a silent cap."""
    row = DEFERRED["corpus_action_range_6_to_12"]
    assert "6-12" in row
    assert "DEFERRED" in row
    assert "ewd-review-batch-experiments.md" in row
    assert row == build_manifest().deferred["corpus_action_range_6_to_12"]
    sizes = [len(s.load().actions) for s in hand_built_specs()]
    assert max(s for s in sizes if s <= DEFAULT_MAX_ACTIONS) == (
        DEFAULT_MAX_ACTIONS)
    over = [s for s in sizes if s > DEFAULT_MAX_ACTIONS]
    assert len(over) == 1, (
        "exactly one instance sits over the guard, and its refusal is a row")


def test_the_tier_boundary_perturbation_never_touches_must() -> None:
    for spec in hand_built_specs():
        swapped = swap_tier_boundary(spec.raw)
        if swapped is None:
            continue
        before = sorted(o["id"] for o in spec.raw["obligations"]
                        if o["priority"] == "must")
        after = sorted(o["id"] for o in swapped["obligations"]
                       if o["priority"] == "must")
        assert before == after, spec.name


def test_the_robustness_rows_report_every_mechanic(
    report: bx.E2Report,
) -> None:
    kinds = {row.kind for row in report.robustness}
    assert {"clone", "split", "tier_swap", "seed_stability"} <= kinds
    for row in report.robustness:
        assert row.detail, "a robustness row without a detail says nothing"


def test_a_deterministic_arm_does_not_move_with_a_replicate_seed(
    report: bx.E2Report,
) -> None:
    deterministic = {name for name, _ in DETERMINISTIC_CANDIDATES}
    rows = [r for r in report.robustness
            if r.kind == "seed_stability" and r.candidate in deterministic]
    assert rows
    assert all(r.invariant_held for r in rows)
    for outcome in report.outcomes:
        if outcome.candidate in deterministic:
            assert outcome.replicate == REPLICATE_NONE


# --------------------------------------------------------------------------- #
# Falsification reporting and the derived verdict.                             #
# --------------------------------------------------------------------------- #
def test_every_falsification_clause_is_reported(
    report: bx.E2Report,
) -> None:
    """Fired or not, each clause appears with an evaluation count."""
    reported = {row.clause for row in report.falsifications}
    assert reported == set(CLAUSES)
    for row in report.falsifications:
        assert row.detail
        assert row.zero_tolerance == (row.clause in ZERO_TOLERANCE)
        if row.clause != "simple_baseline_comparator_not_beaten":
            assert row.evaluated > 0, (
                f"clause {row.clause} fired no evaluations, so a 'clear' "
                "result would be vacuous")


def test_no_zero_tolerance_clause_fired(report: bx.E2Report) -> None:
    fired = [row.clause for row in report.falsifications
             if row.fired and row.zero_tolerance]
    assert not fired, fired


def test_the_mutation_clause_is_not_vacuous(report: bx.E2Report) -> None:
    """The mutations must actually reach the checker and be refused.

    A clause evaluated over zero applicable mutants would be a 'clear'
    that means nothing. Every one of the five named mutations must bite
    somewhere in the corpus and be refused every time it does — a
    mutation with nowhere to apply on one plan (nothing deferred, no
    disclosure location) is not evidence about that plan."""
    applied_somewhere: set[str] = set()
    for spec in hand_built_specs():
        instance = spec.load()
        for name, planner in DETERMINISTIC_CANDIDATES:
            try:
                plan = planner(instance)
            except BaselineRefused:
                continue
            certificate = bx._certify(instance, plan, name, None)
            if not check_schedule(instance, plan, certificate).accepted:
                continue
            for mutation, mutate in bx.PLAN_MUTATIONS:
                mutant = mutate(instance, plan)
                if mutant is None:
                    continue
                applied_somewhere.add(mutation)
                result = check_schedule(
                    instance, mutant,
                    bx._certify(instance, mutant, name, None))
                assert not result.accepted, (
                    f"{spec.name}/{name}: the checker accepted mutation "
                    f"{mutation}")
                assert result.causes(), mutation
    assert applied_somewhere == {name for name, _ in bx.PLAN_MUTATIONS}, (
        "a mutation that never applies anywhere proves nothing")
    assert not any(o.mutation_survivals for o in report.outcomes)
    clause = next(row for row in report.falsifications
                  if row.clause == "checker_accepts_mutated_plan")
    assert clause.evaluated == sum(
        o.mutations_applied for o in report.outcomes), (
        "the reported evaluation count is the number of mutations that "
        "actually applied, not the number that could have")


def test_the_verdict_is_derived_not_authored(report: bx.E2Report) -> None:
    verdict = report.verdict
    assert verdict == "pass" or verdict.startswith(
        ("pass_partial_e2:", "refuted:", "performance_claim_rejected:",
         "invalid:"))
    fired = report.fired()
    if verdict.startswith(("pass", "pass_partial_e2:")):
        assert not fired
    else:
        assert fired or verdict.startswith("invalid:")
    # The verdict is recomputed from the rows, not stored.
    assert "verdict" not in {f for f in vars(report)}


def test_the_comparator_is_computed_from_checked_tuples(
    report: bx.E2Report,
) -> None:
    row = report.comparator
    assert row.instances_compared > 0
    total = (row.instances_candidate_better + row.instances_tied
             + row.instances_comparator_better)
    assert total == row.instances_compared
    for instance, winner in row.per_instance_winner.items():
        assert winner
    # Every recorded winner really does sort ahead of FIFO under the §8
    # ordering — and CO-WINNERS are recorded, so each named rule is
    # checked rather than only the alphabetically first one.
    checked = 0
    for instance, winner in row.per_instance_winner.items():
        if winner.startswith(("tie:", "not_compared:")) or winner == "fifo":
            continue
        base = next(o for o in report.outcomes
                    if o.instance == instance and o.candidate == "fifo")
        for name in winner.split("+"):
            rival = next(o for o in report.outcomes
                         if o.instance == instance and o.candidate == name)
            assert base.objective is not None and rival.objective is not None
            assert (objective_sort_key(rival.objective)
                    < objective_sort_key(base.objective))
            checked += 1
    assert checked, "no instance had a strict winner to spot-check"


# --------------------------------------------------------------------------- #
# Replay, digests, and the timing boundary (§12).                              #
# --------------------------------------------------------------------------- #
def test_the_report_replays_byte_equal(
    small_manifest: bx.E2Manifest,
) -> None:
    first = run_report(small_manifest)
    second = run_report(small_manifest)
    assert first.canonical_json() == second.canonical_json()
    assert first.digest() == second.digest()


def test_the_report_round_trips_through_canonical_json(
    report: bx.E2Report,
) -> None:
    document = json.loads(report.canonical_json())
    assert document["manifest_digest"] == build_manifest().digest()
    assert document["verdict"] == report.verdict
    assert len(document["candidates"]) == len(report.outcomes)
    assert len(document["oracle"]) == len(report.oracle_rows)
    assert document["deferred"]
    assert json.dumps(document, sort_keys=True, separators=(",", ":")) == (
        report.canonical_json())


def test_wall_times_are_measured_but_never_digested(
    small_manifest: bx.E2Manifest,
) -> None:
    """Mutate the clock; the digested comparison must not move.

    A preregistered comparison whose digest tracked machine load would
    not be a replay, so the timings live beside the report, not in it."""
    def fast() -> int:
        fast.tick += 5  # type: ignore[attr-defined]
        return fast.tick  # type: ignore[attr-defined]
    fast.tick = 0  # type: ignore[attr-defined]

    def slow() -> int:
        slow.tick += 1_000_000  # type: ignore[attr-defined]
        return slow.tick  # type: ignore[attr-defined]
    slow.tick = 0  # type: ignore[attr-defined]

    quick = run_report(small_manifest, clock=fast)
    sluggish = run_report(small_manifest, clock=slow)

    assert quick.digest() == sluggish.digest()
    assert quick.canonical_json() == sluggish.canonical_json()
    assert "timings" not in quick.to_canonical_dict()
    assert quick.timings and sluggish.timings
    assert [t.planning_ns for t in quick.timings] != [
        t.planning_ns for t in sluggish.timings], (
        "the injected clock must actually reach the measurement")


def test_the_timing_rows_cover_every_candidate_row(
    report: bx.E2Report,
) -> None:
    keyed = {(t.instance, t.candidate, t.replicate) for t in report.timings}
    assert keyed == {
        (o.instance, o.candidate, o.replicate) for o in report.outcomes}
    for timing in report.timings:
        assert timing.planning_ns >= 0 and timing.checking_ns >= 0


# --------------------------------------------------------------------------- #
# Oracle optimality, PINNED (audit 2026-08-29, concord-audit-corrections).      #
# --------------------------------------------------------------------------- #
#: How often the winning rule sits at the exhaustive optimum, and how often ANY
#: candidate does, over the instances where the oracle returned a verdict.
#:
#: These are pinned because an evidence summary quoted "oracle optimum matched
#: 10/10 where computable" long after two makespan-decisive instances were added
#: that the winning rule misses. Nothing re-derived the figure, so it went stale
#: in the record while the report said something else. A pin makes the corpus
#: growing and the quoted figure disagreeing the SAME reviewable diff.
_WINNER_AT_OPTIMUM = (9, 11)          # coverage_per_cost_greedy: 9 of 11
_ANY_CANDIDATE_AT_OPTIMUM = (11, 13)  # some candidate optimal: 11 of 13


def _optimality_by_instance(report: bx.E2Report) -> dict[str, set[str]]:
    """instance -> the set of optimality verdicts its candidate rows carry."""
    out: dict[str, set[str]] = {}
    for row in report.to_canonical_dict()["candidates"]:
        out.setdefault(row["instance"], set()).add(row["optimality"])
    return out


def test_the_oracle_optimality_figure_is_pinned_to_what_the_rows_say(
    report: bx.E2Report,
) -> None:
    """The quoted optimality figure is DERIVED from the report's own rows.

    Both readings are pinned, because the stale record was ambiguous between
    them and neither reading gives ten of ten:

    * the winning rule at the optimum, over instances with an oracle verdict;
    * any candidate at the optimum, over the same instances.

    An instance whose every row reads ``unavailable`` has no oracle verdict and
    is excluded from both denominators rather than counted as a miss - the
    oracle declines an instance above its action bound, which is a property of
    the oracle and not of the candidate.
    """
    rows = report.to_canonical_dict()["candidates"]
    winner = [r for r in rows if r["candidate"] == "coverage_per_cost_greedy"]
    assert winner, "the winning rule has no rows; the pin below is meaningless"
    decided = [r for r in winner if r["optimality"] != "unavailable"]
    optimal = [r for r in decided if r["optimality"] == "certified_optimal"]
    assert (len(optimal), len(decided)) == _WINNER_AT_OPTIMUM, (
        f"the winning rule is at the oracle optimum on {len(optimal)} of "
        f"{len(decided)} instance(s) with an oracle verdict, not "
        f"{_WINNER_AT_OPTIMUM[0]} of {_WINNER_AT_OPTIMUM[1]}. If the corpus "
        f"changed, update the pin AND every quoted figure in the same diff - "
        f"that is what this pin exists to force."
    )

    by_instance = _optimality_by_instance(report)
    with_verdict = {i: v for i, v in by_instance.items() if v != {"unavailable"}}
    any_optimal = [i for i, v in with_verdict.items() if "certified_optimal" in v]
    assert (len(any_optimal), len(with_verdict)) == _ANY_CANDIDATE_AT_OPTIMUM, (
        f"some candidate is at the oracle optimum on {len(any_optimal)} of "
        f"{len(with_verdict)} instance(s) with an oracle verdict, not "
        f"{_ANY_CANDIDATE_AT_OPTIMUM[0]} of {_ANY_CANDIDATE_AT_OPTIMUM[1]}"
    )

    # NON-VACUITY: the pin would be worthless if nothing could miss. The two
    # misses are the makespan-decisive instances, which is WHY they were added.
    missed = sorted(
        r["instance"] for r in decided if r["optimality"] != "certified_optimal"
    )
    assert missed == ["makespan-decides-breakpoint", "makespan-decides-parallel"], (
        f"the winning rule's misses moved: {missed}"
    )
