"""PACT ``concord-disclosure-budget``, req.batch.checker-implementation —
the tiny exhaustive oracle and the optimality boundary.

Design reference: ``designs/batch-scheduling-research.md`` §9.1. The
rule under test is that the words "optimal" or "certified optimal" are
available only through a checked artifact — and that the oracle refuses,
loudly, rather than trimming its search and calling the survivor best.

Fixtures are shared with ``test_batch_schedule_checker.py``, which owns
the instance builders.
"""
from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    OPTIMALITY_METHOD_CHECKED_BOUND,
    OPTIMALITY_METHOD_EXHAUSTIVE,
    TERMINATION_STOPPED,
    OptimalityArtifact,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    BASELINES,
    plan_fifo,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM,
    CAUSE_ARTIFACT_DIGEST_MISMATCH,
    CAUSE_ARTIFACT_METHOD_UNKNOWN,
    CAUSE_ARTIFACT_NOT_BOUND,
    CAUSE_ARTIFACT_PRODUCER_UNREVIEWED,
    CAUSE_ARTIFACT_SEARCH_MISMATCH,
    CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE,
    CAUSE_ARTIFACT_UNESTABLISHED_BOUND,
    CAUSE_ARTIFACT_VALUE_MISMATCH,
    OPTIMALITY_RECHECK_MAX_ACTIONS,
    REVIEWED_OPTIMALITY_PRODUCERS,
    VERDICT_CHECKED_FEASIBLE,
    ScheduleCheckResult,
    check_optimality_artifact,
    check_schedule,
    derive_may_change,
)
from okg.substrate.consistency.batch_schedule_oracle import (
    ORACLE_VERSION,
    REFUSE_INSTANCE_TOO_LARGE,
    REFUSE_NO_FEASIBLE_CANDIDATE,
    REFUSE_NODE_BUDGET_EXCEEDED,
    REFUSE_UNKNOWN_QUANTITY,
    OracleRefused,
    build_certificate,
    enumerate_optimum,
    producer_ref,
)
from tests.substrate.consistency.test_batch_schedule_checker import (
    action_dict,
    build,
    certify,
    check,
    make_plan,
)


def add_redundant_expensive_action(raw: dict[str, Any]) -> None:
    """A fourth action that re-covers an already-covered unit at high cost.

    It separates the baselines: FIFO takes it because FIFO selects
    everything, the coverage-driven greedy leaves it out because it adds
    no DISTINCT unit (§6)."""
    raw["actions"].append(action_dict(
        "a4", action_class="sweep", covers=["o_may"], duration=1,
        execution_cost=50))


# --------------------------------------------------------------------------- #
# 1. The oracle produces a checked, certified-optimal result.                  #
# --------------------------------------------------------------------------- #
def test_oracle_result_passes_the_independent_checker() -> None:
    instance = build()
    result = enumerate_optimum(instance)
    assert result.check.verdict == VERDICT_CHECKED_FEASIBLE, \
        result.check.findings
    recheck = check_schedule(instance, result.plan,
                             build_certificate(instance, result.plan))
    assert recheck.verdict == VERDICT_CHECKED_FEASIBLE
    assert recheck.recomputed_objective == result.best_objective


def test_oracle_artifact_certifies_optimality() -> None:
    instance = build()
    result = enumerate_optimum(instance)
    verdict = check_optimality_artifact(
        instance, result.check, result.artifact,
        certificate=result.certificate)
    assert verdict.certified_optimal is True
    assert verdict.method == OPTIMALITY_METHOD_EXHAUSTIVE
    assert verdict.findings == ()
    assert verdict.not_established == ()


def test_oracle_is_deterministic() -> None:
    assert enumerate_optimum(build()).plan.digest == \
        enumerate_optimum(build()).plan.digest


def test_oracle_covers_the_mandatory_unit_or_it_would_not_be_feasible() -> None:
    result = enumerate_optimum(build())
    assert "a1" in {s.action_id for s in result.plan.selected}
    assert result.feasible_candidates >= 1
    assert result.candidates_examined >= result.feasible_candidates


# --------------------------------------------------------------------------- #
# 2. A checked result cannot express optimality on its own (§9.1).             #
# --------------------------------------------------------------------------- #
def test_a_checked_result_carries_no_optimality_claim() -> None:
    """The verdict vocabulary and the result type have no place to put
    one. Saying "optimal" requires the artifact-taking function."""
    instance = build()
    result = check(instance, enumerate_optimum(instance).plan)
    assert result.verdict == VERDICT_CHECKED_FEASIBLE
    fields = set(ScheduleCheckResult.__dataclass_fields__)
    assert not any("optimal" in name for name in fields)
    assert not any("optimal" in v for v in (result.verdict,))


@pytest.mark.parametrize("mutate,expected", [
    pytest.param(
        lambda artifact: replace(artifact, best_objective=replace(
            artifact.best_objective, total_cost=artifact
            .best_objective.total_cost - 1)),
        CAUSE_ARTIFACT_VALUE_MISMATCH, id="value"),
    pytest.param(
        lambda artifact: replace(artifact, instance_digest="0" * 64),
        CAUSE_ARTIFACT_DIGEST_MISMATCH, id="instance_digest"),
    pytest.param(
        lambda artifact: replace(artifact, unit_mapping_digest="0" * 64),
        CAUSE_ARTIFACT_DIGEST_MISMATCH, id="unit_mapping_digest"),
    pytest.param(
        lambda artifact: replace(artifact, method="solver_said_optimal"),
        CAUSE_ARTIFACT_METHOD_UNKNOWN, id="method"),
])
def test_a_wrong_optimality_artifact_is_rejected(mutate, expected) -> None:
    instance = build()
    result = enumerate_optimum(instance)
    tampered = mutate(result.artifact)
    assert tampered != result.artifact, \
        "the mutation changed nothing; this test would pass vacuously"
    # Re-bind the certificate to the TAMPERED artifact, so the refusal
    # under test is the named one and not merely the §12 binding.
    certificate = replace(
        result.certificate, optimality_artifact_digest=tampered.digest)
    verdict = check_optimality_artifact(instance, result.check, tampered,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert expected in verdict.causes()


def test_an_artifact_cannot_upgrade_a_refused_candidate() -> None:
    instance = build()
    result = enumerate_optimum(instance)
    dishonest_plan = replace(result.plan, claimed_objective=replace(
        result.plan.claimed_objective, total_cost=0))
    refused = check(instance, dishonest_plan)
    assert refused.verdict != VERDICT_CHECKED_FEASIBLE
    verdict = check_optimality_artifact(instance, refused, result.artifact,
                                        certificate=result.certificate)
    assert verdict.certified_optimal is False


# --------------------------------------------------------------------------- #
# 3. The oracle weakly dominates every baseline.                               #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mutate", [
    pytest.param(None, id="base"),
    pytest.param(add_redundant_expensive_action, id="redundant_action"),
])
def test_oracle_weakly_dominates_every_baseline(mutate) -> None:
    instance = build(mutate)
    oracle = enumerate_optimum(instance)
    best_key = objective_sort_key(oracle.best_objective)
    compared = 0
    for name, planner in BASELINES:
        plan = planner(instance)
        result = check(instance, plan)
        if result.verdict != VERDICT_CHECKED_FEASIBLE:
            continue  # an infeasible proposal has no tuple to compare
        assert result.recomputed_objective is not None
        compared += 1
        assert best_key <= objective_sort_key(result.recomputed_objective), (
            f"{name} beat the exhaustive oracle, which is impossible unless "
            "the enumeration is incomplete or the comparison is wrong")
    assert compared >= 1, "no baseline produced a comparable tuple"


def test_a_matching_baseline_equals_the_oracle_exactly() -> None:
    """On the base instance every action earns distinct credit and the
    chain fixes the makespan, so at least one baseline should land on
    the oracle's tuple — not merely near it."""
    instance = build()
    oracle = enumerate_optimum(instance)
    matches = []
    for name, planner in BASELINES:
        result = check(instance, planner(instance))
        if (result.verdict == VERDICT_CHECKED_FEASIBLE
                and result.recomputed_objective == oracle.best_objective):
            matches.append(name)
    assert matches, "expected at least one baseline to match the oracle"


def test_the_redundant_action_separates_the_baselines() -> None:
    """Guards the dominance test above from being vacuous: if every
    baseline produced the same tuple, dominance would prove nothing."""
    instance = build(add_redundant_expensive_action)
    tuples = set()
    for _, planner in BASELINES:
        result = check(instance, planner(instance))
        if result.recomputed_objective is not None:
            tuples.add(objective_sort_key(result.recomputed_objective))
    assert len(tuples) >= 2


# --------------------------------------------------------------------------- #
# 4. The size guard REFUSES; it never caps silently.                           #
# --------------------------------------------------------------------------- #
def test_an_oversized_instance_is_refused() -> None:
    def many_actions(raw: dict[str, Any]) -> None:
        for index in range(4, 12):
            raw["actions"].append(action_dict(
                f"a{index}", action_class="sweep", duration=1,
                execution_cost=1))
    instance = build(many_actions)
    assert len(instance.actions) > 6
    with pytest.raises(OracleRefused) as excinfo:
        enumerate_optimum(instance)
    assert excinfo.value.data["code"] == REFUSE_INSTANCE_TOO_LARGE


def test_an_exhausted_node_budget_is_refused_not_truncated() -> None:
    instance = build()
    with pytest.raises(OracleRefused) as excinfo:
        enumerate_optimum(instance, node_budget=3)
    assert excinfo.value.data["code"] == REFUSE_NODE_BUDGET_EXCEEDED


@pytest.mark.parametrize("field,value", [
    ("duration", {"unknown": "never measured"}),
    ("execution_cost", {"unknown": "no estimator trace"}),
])
def test_an_unknown_quantity_is_refused_not_treated_as_zero(
    field: str, value: dict[str, str],
) -> None:
    instance = build(lambda raw: raw["actions"][0].__setitem__(field, value))
    with pytest.raises(OracleRefused) as excinfo:
        enumerate_optimum(instance)
    assert excinfo.value.data["code"] == REFUSE_UNKNOWN_QUANTITY
    assert excinfo.value.data["quantity"] == field


def test_an_instance_with_no_publishable_plan_is_refused() -> None:
    """Zero capacity means the mandatory action can never run. "No
    feasible plan" is an answer, not a reason to relax a check."""
    def starve(raw: dict[str, Any]) -> None:
        raw["resources"][0]["segments"] = [{"start": 0, "capacity": 0}]
    with pytest.raises(OracleRefused) as excinfo:
        enumerate_optimum(build(starve))
    assert excinfo.value.data["code"] == REFUSE_NO_FEASIBLE_CANDIDATE


# --------------------------------------------------------------------------- #
# 5. Independence.                                                             #
# --------------------------------------------------------------------------- #
def test_oracle_imports_no_baseline_or_planner() -> None:
    """§9.1: the oracle derives its search and comparisons independently
    of candidate-planner encodings."""
    module = __import__(
        "okg.substrate.consistency.batch_schedule_oracle",
        fromlist=["__file__"])
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    assert not [n for n in names if "baseline" in n or "planner" in n]
    assert names <= {
        "__future__", "dataclasses", "typing",
        "okg.substrate.consistency.batch_schedule",
        "okg.substrate.consistency.batch_schedule_checker",
        "okg.substrate.errors",
    }, names


# --------------------------------------------------------------------------- #
# 6. Adversarial-review regressions (EWD-BSC-1, -3, -4, -7).                    #
# --------------------------------------------------------------------------- #
def test_a_hand_written_artifact_cannot_certify_a_suboptimal_plan() -> None:
    """EWD-BSC-1, the memo's repro verbatim. `check_optimality_artifact`
    used to compare the artifact's best value against the CANDIDATE'S own
    recomputed value, so a forger set one to the other and a plan
    covering zero of two available units was certified optimal."""
    instance = build()
    bad = make_plan(instance, [("a1", 0)])  # covers the must, defers the rest
    certificate = certify(instance, bad)
    result = check_schedule(instance, bad, certificate)
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings
    assert result.recomputed_objective is not None
    assert result.recomputed_objective.should_units == 0
    assert result.recomputed_objective.may_units == 0

    forged = OptimalityArtifact(
        method=OPTIMALITY_METHOD_EXHAUSTIVE,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        best_objective=result.recomputed_objective,  # its own value
        candidates_examined=1,
        search_space_digest="0" * 64,
        producer_version="my-own-solver/hand-written",
        max_actions=6,
        node_budget=200_000,
    )
    verdict = check_optimality_artifact(instance, result, forged,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM in verdict.causes()
    assert CAUSE_ARTIFACT_PRODUCER_UNREVIEWED in verdict.causes()
    assert CAUSE_ARTIFACT_NOT_BOUND in verdict.causes()


def test_the_recomputation_alone_refuses_an_otherwise_honest_forgery() -> None:
    """The hard case: every hygiene field is the oracle's own honest
    record and the certificate binds the artifact properly, so the ONLY
    thing left that can refuse is the checker re-deriving the optimum.

    Neuter that re-derivation and this test fails."""
    instance = build()
    oracle = enumerate_optimum(instance)
    bad = make_plan(instance, [("a1", 0)])
    honest_value = check_schedule(
        instance, bad, certify(instance, bad, producer=producer_ref())
    ).recomputed_objective
    assert honest_value is not None
    assert honest_value != oracle.best_objective

    repointed = replace(oracle.artifact, best_objective=honest_value)
    certificate = certify(instance, bad, producer=producer_ref(),
                          optimality_artifact_digest=repointed.digest)
    result = check_schedule(instance, bad, certificate)
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings

    verdict = check_optimality_artifact(instance, result, repointed,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert set(verdict.causes()) == {
        CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM,
        CAUSE_ARTIFACT_VALUE_MISMATCH,
    }, verdict.findings


def test_an_inflated_candidate_count_is_caught_by_the_recomputation() -> None:
    """`candidates_examined` and `search_space_digest` used to be carried
    and never read. The checker now recomputes both."""
    instance = build()
    oracle = enumerate_optimum(instance)
    inflated = replace(
        oracle.artifact,
        candidates_examined=oracle.artifact.candidates_examined + 1)
    certificate = replace(oracle.certificate,
                          optimality_artifact_digest=inflated.digest)
    verdict = check_optimality_artifact(instance, oracle.check, inflated,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_SEARCH_MISMATCH in verdict.causes()


def test_optimality_beyond_the_checkers_own_bound_is_refused() -> None:
    """§9.1's fallback, enforced: past the size the checker will redo,
    the claim is refused rather than granted. There is no checked proof
    format for production-size instances yet."""
    def many_actions(raw: dict[str, Any]) -> None:
        for index in range(4, 12):
            raw["actions"].append(action_dict(
                f"a{index}", action_class="sweep", duration=1,
                execution_cost=1))
    instance = build(many_actions)
    assert len(instance.actions) > OPTIMALITY_RECHECK_MAX_ACTIONS
    plan = plan_fifo(instance)
    certificate = build_certificate(instance, plan)
    result = check_schedule(instance, plan, certificate)
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings
    assert result.recomputed_objective is not None

    artifact = OptimalityArtifact(
        method=OPTIMALITY_METHOD_EXHAUSTIVE,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        best_objective=result.recomputed_objective,
        candidates_examined=1,
        search_space_digest="0" * 64,
        producer_version=ORACLE_VERSION,
        max_actions=len(instance.actions),
        node_budget=200_000,
    )
    bound = replace(certificate,
                    optimality_artifact_digest=artifact.digest)
    verdict = check_optimality_artifact(
        instance, check_schedule(instance, plan, bound), artifact,
        certificate=bound)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE in verdict.causes()


def test_a_checked_bound_has_no_checked_proof_format_yet() -> None:
    """§9.1 admits a solver-independent bound only when it is CHECKED.
    Nothing in this slice can check one, so the claim is refused."""
    instance = build()
    oracle = enumerate_optimum(instance)
    claimed = replace(oracle.artifact,
                      method=OPTIMALITY_METHOD_CHECKED_BOUND)
    certificate = replace(oracle.certificate,
                          optimality_artifact_digest=claimed.digest)
    verdict = check_optimality_artifact(instance, oracle.check, claimed,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE in verdict.causes()


def test_the_reviewed_producer_list_still_names_this_oracle() -> None:
    """A drift guard: the checker cannot import the oracle, so the
    allow-list is a constant. This is what keeps it honest."""
    assert ORACLE_VERSION in REVIEWED_OPTIMALITY_PRODUCERS


@pytest.mark.parametrize("rebind", [
    pytest.param(lambda cert, artifact: replace(
        cert, optimality_artifact_digest=ARTIFACT_UNBOUND), id="unbound"),
    pytest.param(lambda cert, artifact: replace(
        cert, optimality_artifact_digest="0" * 64), id="other_artifact"),
])
def test_a_certificate_that_does_not_bind_the_artifact_refuses(
    rebind,
) -> None:
    """EWD-BSC-7, second half. Nothing tied a certificate to an artifact,
    so any artifact could be paired with any certificate over the same
    instance."""
    instance = build()
    oracle = enumerate_optimum(instance)
    certificate = rebind(oracle.certificate, oracle.artifact)
    verdict = check_optimality_artifact(instance, oracle.check,
                                        oracle.artifact,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_NOT_BOUND in verdict.causes()


def test_a_stopped_producer_cannot_support_an_exhaustive_claim() -> None:
    instance = build()
    oracle = enumerate_optimum(instance)
    certificate = replace(
        oracle.certificate,
        producer=producer_ref(termination_status=TERMINATION_STOPPED))
    certificate = replace(
        certificate, optimality_artifact_digest=oracle.artifact.digest)
    verdict = check_optimality_artifact(instance, oracle.check,
                                        oracle.artifact,
                                        certificate=certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE in verdict.causes()


def test_an_unestablished_bound_blocks_certified_optimality() -> None:
    """EWD-BSC-4. `not_established` used to stop at the checked result:
    the strongest claim in the system had no field to carry the caveat
    and no rule that consulted it."""
    instance = build(lambda raw: raw["resources"][0].__setitem__(
        "reservation", {"unknown": "pool has no enforceable budget"}))
    oracle = enumerate_optimum(instance)
    assert "reservation:cpu" in oracle.check.not_established
    assert oracle.check.verdict == VERDICT_CHECKED_FEASIBLE
    verdict = check_optimality_artifact(instance, oracle.check,
                                        oracle.artifact,
                                        certificate=oracle.certificate)
    assert verdict.certified_optimal is False
    assert CAUSE_ARTIFACT_UNESTABLISHED_BOUND in verdict.causes()
    assert verdict.not_established == ("reservation:cpu",)


def test_oracle_disclosure_is_its_own_and_strictly_wider() -> None:
    """EWD-BSC-3. The oracle used to set every plan's `may_change` to the
    CHECKER's `derive_may_change` answer, so the checker's containment
    check compared a value against itself: neutering that derivation
    failed five tests, none of them in this module.

    Neuter `_own_may_change` and this test fails — the best candidate is
    no longer the one that defers a4."""
    instance = build(add_redundant_expensive_action)
    oracle = enumerate_optimum(instance)
    assert oracle.plan.deferred == ("a4",)
    derived = set(derive_may_change(instance, oracle.plan.deferred).locations)
    claimed = set(oracle.plan.may_change)
    assert claimed >= derived, "the oracle's claim must contain the closure"
    assert claimed > derived, \
        "borrowed disclosure would make containment a self-comparison"
    assert oracle.check.verdict == VERDICT_CHECKED_FEASIBLE


def test_the_oracle_does_not_import_the_checkers_disclosure_rule() -> None:
    """Asserted structurally, because the previous version's docstring
    presented the borrowing as a virtue."""
    source = Path(__import__(
        "okg.substrate.consistency.batch_schedule_oracle",
        fromlist=["__file__"]).__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.update(alias.name for alias in node.names)
    assert "derive_may_change" not in imported


def test_certificate_binding_is_reproducible() -> None:
    instance = build()
    plan = enumerate_optimum(instance).plan
    assert build_certificate(instance, plan) == certify(
        instance, plan, producer=producer_ref())
