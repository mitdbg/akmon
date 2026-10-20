"""PACT ``concord-disclosure-budget``, task.batch.support-index-theorem — the
refinement witnesses for "the stress-tested support result feeds the
certificate-only skip path", plus the pure half of the two E1-completion
metrics.

What this file proves, and what it deliberately does not
--------------------------------------------------------
It does NOT re-prove the Lean noninterference theorem (T1) or the
certificate-currency lemma. Those are owned, stated, and machine-checked by
``concord-formal-currency-t1`` (``formal/consistency/lean/Currency.lean``,
bound to the engine by ``test_formal_refinement.py``). This task CONSUMES that
result, and what remains to be shown empirically is the link the formal side
assumes but cannot establish: that the object E1 stress-tests — a recorded
support index — reaches the skip decision only through certificate validation,
and reaches it in the shape the formal `ReadSet` and the engine's
``CertificateDependencies`` both model.

Three claims, three sections:

1. **The certificate is the ONLY skip mechanism in the harness.** Asserted
   twice over, because a runtime tally alone would miss a second mechanism on
   a path this run happened not to take: (a) over every outcome the E1 engine
   drive actually produced — every SKIP carries a non-empty certificate digest
   and a cause in ``SKIP_CAUSES``; and (b) by AST inspection of the harness
   sources — ``ACTION_SKIP`` is only ever COMPARED against, never assigned as
   an outcome, and ``run_reuse_pass`` is the only decision function called.
2. **The recorded support is the shape the certificate stores and the Lean
   ``ReadSet`` models.** The harness's recorded read set becomes
   ``CertificateDependencies.input_digests`` key-for-key, and its closed term
   vocabulary partitions into exactly the two halves ``ReadSet`` declares:
   value-addressed facts and membership/adjacency collections.
3. **A support-index term set becomes a skip input only through a certificate
   gate.** There are TWO such gates, not one, and the tests say so rather than
   asserting the tidier claim: ``validate_certificate`` (a valid prior
   certificate) and ``dependency_classes_differ`` (the degenerate
   unchanged-inputs carry-forward, which is the old manifest-hash
   carry-forward re-expressed so that it mints a certificate instead of
   bypassing one). Both are keyed on the same ten-class dependency snapshot
   the support index populates, and both record through the same
   ``record_skip``. Witnessed two ways: every skip the shipped arm grades
   carries one of those two causes, and NEUTERING both drives those CAUSES to
   zero while removing no skip at all: every step-update still skips, under
   the effect-bound ``skip_justified`` rung, which never reads the dependency
   snapshot and is reachable only after both gates fall through. That
   migration is asserted precisely — same total, zero gate causes, zero
   unsafe, and every residual skip still carrying a certificate — rather than
   asserted away, because it is also the rung the E1 harness review
   (EWD-E1-2) recorded as unexercised.

The two E1-completion metrics are also exercised here on their pure surface —
the UNKNOWN discipline, the setup-cycle exclusion, the unit and mode refusals,
and the mutation-detection classifiers' refusal to grade a mutation that did
not mutate. The measured numbers themselves come from the live replay in
``tests/bench/synth/test_e1_runtime_saved.py``; nothing here fabricates one.
"""
from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path

import pytest

from okg.bench.synth import e1_runtime_saved, e1_support_stress
from okg.bench.synth.e1_runtime_saved import (
    CORRUPTION_REFUSAL_MARKERS,
    DETECTION_MODE_INVALIDATED_AND_RERAN,
    DETECTION_MODE_NOT_CAUGHT,
    DETECTION_MODE_REFUSED,
    RUNTIME_SAVED_MEASURED,
    RUNTIME_SAVED_PARTIALLY_KNOWN,
    SHARED_BINDING_FIELDS,
    E1MetricError,
    MutationDetection,
    classify_certificate_corruption,
    classify_dependency_fact_mutation,
    measure_runtime_saved,
)
from okg.bench.synth.e1_support_stress import (
    DECL_COMPLETE,
    E1StressError,
    UpdateGrade,
    build_checks,
    build_manifest,
    build_world,
    declared_support,
    run_arm,
    run_report,
)
from okg.bench.synth.parity import (
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_DEPENDENCY_FACT,
)
from okg.substrate.consistency import reuse_scheduler
from okg.substrate.consistency.batch_measurements import (
    COST_UNIT_EXECUTOR_STEPS,
    DURATION_UNIT_NANOSECONDS,
    GRANULARITY_EXECUTOR_STEP,
    GRANULARITY_PUBLISH_CYCLE,
    MEASUREMENT_SCHEMA_VERSION,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP,
    PARITY_RESOURCES,
    PARITY_RESOURCE_CERTIFICATE_WRITES,
    PARITY_RESOURCE_EXECUTOR_STEPS,
    PARITY_RESOURCE_PUBLISH_COMMITS,
    ActionTrace,
    ActionTraceSet,
    PlanEstimate,
    RealizedMeasurement,
    ReproducibilityBinding,
    TREE_CLEAN,
)
from okg.substrate.consistency.batch_schedule import OUTCOME_SUCCESS, Unknown
from okg.substrate.consistency.reuse_certificates import (
    CertificateDependencies,
    validate_certificate,
)
from okg.substrate.consistency.reuse_scheduler import (
    ACTION_RUN,
    ACTION_SKIP,
    SKIP_CAUSES,
    StepOutcome,
)

_SEED = 11
_UPDATES = 12
_LEAN_SOURCE = (
    Path(__file__).resolve().parents[3]
    / "formal" / "consistency" / "lean" / "Currency.lean"
)


@pytest.fixture(scope="module")
def graded() -> tuple[tuple[UpdateGrade, ...], object]:
    """One declared-complete arm over the seeded world, with every update's
    step grades collected — the outcomes the engine actually produced."""
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    collected: list[UpdateGrade] = []
    result = run_arm(
        world, checks, arm="theorem:complete", index_kind=DECL_COMPLETE,
        collect=collected,
    )
    return tuple(collected), result


# --------------------------------------------------------------------------- #
# 1. The certificate is the ONLY skip mechanism.                               #
# --------------------------------------------------------------------------- #
def test_every_skip_the_harness_produced_carries_a_certificate_digest(
    graded,
) -> None:
    """Over the recorded outcomes, not over the type invariant alone: every
    SKIP names a certificate digest and a cause the scheduler declares as a
    skip cause; every RUN names neither."""
    updates, result = graded
    steps = [step for update in updates for step in update.steps]
    assert steps, "the arm graded nothing, so this witness would be vacuous"

    skips = [s for s in steps if s.action == ACTION_SKIP]
    assert len(skips) == result.skips_proposed
    assert skips, (
        "the complete arm proposed no skip at all, so 'every skip carries a "
        "certificate' would hold vacuously"
    )
    for step in skips:
        assert step.certificate_digest, (
            f"{step.check_id} skipped with no certificate digest — a second "
            f"skip mechanism exists (cause {step.cause!r})"
        )
        assert step.cause in SKIP_CAUSES, step.cause
    for step in steps:
        if step.action != ACTION_SKIP:
            assert step.action == ACTION_RUN
            assert step.certificate_digest == "", (
                f"{step.check_id} RAN but carried a certificate digest"
            )
            assert step.cause not in SKIP_CAUSES, step.cause


def test_a_skip_outcome_without_a_certificate_is_structurally_impossible(
) -> None:
    """The runtime tally above could only ever cover the paths this run took.
    The scheduler's own type refuses the shape outright — quoted here so the
    'only skip mechanism' claim does not rest on a sample."""
    with pytest.raises(Exception) as excinfo:
        StepOutcome(
            step_id="s", action=ACTION_SKIP,
            cause=sorted(SKIP_CAUSES)[0], certificate_digest=None,
        )
    assert "certificate is the only skip mechanism" in str(excinfo.value)


def _skip_action_nodes(source: str) -> tuple[list[ast.AST], list[ast.AST]]:
    """(compared, assigned) occurrences of the ``ACTION_SKIP`` name."""
    tree = ast.parse(source)
    compared: list[ast.AST] = []
    assigned: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            names = [node.left, *node.comparators]
            if any(isinstance(n, ast.Name) and n.id == "ACTION_SKIP"
                   for n in names):
                compared.append(node)
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            for sub in ast.walk(node.value) if node.value else ():
                if isinstance(sub, ast.Name) and sub.id == "ACTION_SKIP":
                    assigned.append(node)
        if isinstance(node, ast.keyword) and node.arg == "action":
            for sub in ast.walk(node.value):
                if isinstance(sub, ast.Name) and sub.id == "ACTION_SKIP":
                    assigned.append(node)
    return compared, assigned


#: Both E1 modules, watched by the guard below. ``e1_support_stress`` drives
#: the engine and must COMPARE against ``ACTION_SKIP``; ``e1_runtime_saved``
#: derives metrics from what the engine already decided and must not mention
#: the name at all. Either way, neither may ASSIGN it.
_E1_MODULES = (e1_support_stress, e1_runtime_saved)


def _assert_never_authors_a_skip(module_name: str, source: str) -> None:
    _compared, assigned = _skip_action_nodes(source)
    assert not assigned, (
        f"{module_name} ASSIGNS ACTION_SKIP "
        f"(lines {[getattr(n, 'lineno', -1) for n in assigned]}) — the "
        "harness may only read the engine's decision, never author one"
    )


def test_the_harness_never_authors_a_skip_outcome_itself() -> None:
    """AST inspection of both E1 modules: ``ACTION_SKIP`` appears only in
    comparisons against what the engine decided — never assigned into an
    outcome, an action field, or a fabricated result. A harness that could
    mint its own skip would make every soundness count meaningless."""
    for module in _E1_MODULES:
        _assert_never_authors_a_skip(module.__name__, inspect.getsource(module))

    # ...and the guard is watching a name that is actually there. The two
    # modules earn that differently, so the non-vacuity claim is stated per
    # module rather than assumed uniform.
    compared, _assigned = _skip_action_nodes(inspect.getsource(
        e1_support_stress
    ))
    assert compared, (
        "e1_support_stress does not compare against ACTION_SKIP at all — "
        "this guard is watching the wrong name"
    )
    assert "ACTION_SKIP" not in inspect.getsource(e1_runtime_saved), (
        "e1_runtime_saved now names ACTION_SKIP; it derives metrics from "
        "decisions the engine already made and has no business handling the "
        "outcome vocabulary — restate this guard deliberately if that changed"
    )


@pytest.mark.parametrize("module_name", [m.__name__ for m in _E1_MODULES])
def test_a_forged_skip_in_either_e1_module_trips_the_guard(
    module_name: str,
) -> None:
    """The guard's own sensitivity, on a MUTATED COPY of each module's source.

    It used to inspect a one-element tuple while its docstring said "both E1
    modules", so a forged ``ACTION_SKIP`` in ``e1_runtime_saved`` left the
    suite green. Both are now watched, and this proves it for each."""
    module = next(m for m in _E1_MODULES if m.__name__ == module_name)
    clean = inspect.getsource(module)
    _assert_never_authors_a_skip(module_name, clean)

    forged = clean + "\n\n_FORGED_SKIP = ACTION_SKIP\n"
    with pytest.raises(AssertionError, match="ASSIGNS ACTION_SKIP"):
        _assert_never_authors_a_skip(module_name, forged)


def test_run_reuse_pass_is_the_only_decision_function_the_harness_calls(
) -> None:
    """The harness imports the real pass and calls it; no other scheduler
    entry point, and no local re-implementation of the ladder, appears."""
    source = inspect.getsource(e1_support_stress)
    tree = ast.parse(source)
    called = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "run_reuse_pass" in called
    scheduler_entry_points = {
        name for name in dir(reuse_scheduler)
        if name.startswith("run_") or name.startswith("decide")
    }
    leaked = (called & scheduler_entry_points) - {"run_reuse_pass"}
    assert not leaked, (
        f"the harness calls scheduler entry points other than run_reuse_pass: "
        f"{sorted(leaked)}"
    )


# --------------------------------------------------------------------------- #
# 2. Recorded support == CertificateDependencies shape == Lean ReadSet shape.  #
# --------------------------------------------------------------------------- #
def test_recorded_support_becomes_certificate_input_digests_key_for_key(
) -> None:
    """The support index is not summarized, bucketed, or widened on its way
    into a certificate: the recorded term set IS the ``input_digests`` key
    set. If it were lossy, the E1 omission arms would be stressing a
    different object than the one the skip stands on."""
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    seen_nonempty = False
    for state in world.states:
        for check in checks:
            support = declared_support(
                check, state, index_kind=DECL_COMPLETE,
                omission=e1_support_stress.OmissionPlan(),
            )
            facts = e1_support_stress._step_facts(
                check, state, support, "env-fp",
            )
            assert isinstance(facts, CertificateDependencies)
            assert set(facts.input_digests) == set(support), (
                f"{check.check_id}: the recorded read set and the "
                f"certificate's input_digests are not the same key set"
            )
            seen_nonempty = seen_nonempty or bool(support)
    assert seen_nonempty, "every declared support was empty — vacuous"


def _lean_structure_fields(source: str, name: str) -> tuple[str, ...]:
    """The field lines of the ``structure <name>`` block, and nothing else.

    Bounded deliberately: a wildcard span from the ``structure`` header to the
    fields it expects can slide PAST the declaration it claims to pin and bind
    a later one that happens to have the same field names — ``Currency.lean``
    declares ``ChangeSet`` with the same two ``facts``/``colls`` fields, so an
    unanchored match cannot tell a renamed ``ReadSet`` from an intact one.
    The block ends at the first blank line or first unindented line after the
    header, which is where the next declaration begins."""
    lines = source.splitlines()
    starts = [
        i for i, line in enumerate(lines)
        if re.match(rf"^structure {re.escape(name)}\b", line)
    ]
    assert len(starts) == 1, (
        f"expected exactly one `structure {name}` declaration in the Lean "
        f"source, found {len(starts)} at lines "
        f"{[i + 1 for i in starts]}"
    )
    header = lines[starts[0]]
    assert header.rstrip().endswith("where"), header
    fields: list[str] = []
    for line in lines[starts[0] + 1:]:
        if not line.strip() or not line.startswith((" ", "\t")):
            break
        fields.append(line.strip())
    return tuple(fields)


def _assert_lean_readset_shape(source: str) -> None:
    """The one executable tie to the consumed Lean T1/currency result."""
    assert _lean_structure_fields(source, "ReadSet") == (
        "facts : Finset Fact",
        "colls : Finset CollId",
    ), "the Lean ReadSet no longer matches the documented shape"


def test_the_term_vocabulary_partitions_into_the_lean_readset_halves() -> None:
    """The Lean ``ReadSet`` is ``{facts, colls}``. The E1 term vocabulary is
    closed, and every term is exactly one of those two: a value-addressed
    fact (``entity:``, ``alias:``, ``policy:``) or a collection whose
    MEMBERSHIP is the read (``members:``, ``rel:``). The collection half is
    what makes absence- and closure-style reads certifiable at all, so the
    partition is the binding, not a naming convention."""
    source = _LEAN_SOURCE.read_text()
    _assert_lean_readset_shape(source)

    fact_kinds = {"entity", "alias", "policy"}
    coll_kinds = {"members", "rel"}
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    kinds_seen: set[str] = set()
    for state in world.states:
        for check in checks:
            for term in check.gold_support(state):
                kind = term.split(":", 1)[0]
                assert kind in fact_kinds | coll_kinds, (
                    f"term {term!r} is in neither ReadSet half"
                )
                kinds_seen.add(kind)
    assert kinds_seen & fact_kinds, "no fact-half term was exercised"
    assert kinds_seen & coll_kinds, "no collection-half term was exercised"


#: The unanchored pattern this guard used to carry. Kept only as the negative
#: control below: it is what a wildcard span looks like when it slides.
_UNANCHORED_READSET_PATTERN = (
    r"structure ReadSet.*?where\s+facts : Finset Fact\s+colls : Finset CollId"
)


def _rename_readset_field(source: str, old: str, new: str) -> str:
    """Rename ONE field of the ``ReadSet`` block in a scratch copy of the
    source. The real file is never touched."""
    lines = source.splitlines(keepends=True)
    start = next(
        i for i, line in enumerate(lines)
        if re.match(r"^structure ReadSet\b", line)
    )
    for offset in range(start + 1, len(lines)):
        line = lines[offset]
        if not line.strip() or not line.startswith((" ", "\t")):
            break
        if line.strip().startswith(f"{old} :"):
            lines[offset] = line.replace(f"{old} :", f"{new} :", 1)
            return "".join(lines)
    raise AssertionError(f"ReadSet has no field {old!r} to rename")


def test_a_renamed_lean_readset_field_fails_the_shape_assertion() -> None:
    """The negative control for the tie above, on a MUTATED COPY of the Lean
    source — the real file is untouched.

    Renaming ``ReadSet.colls`` must break the assertion. It did not use to:
    the guard matched ``structure ReadSet`` and then wildcarded across the
    whole file to any ``facts``/``colls`` pair, and ``Currency.lean`` declares
    ``ChangeSet`` with exactly that pair. The old pattern therefore still
    matched a renamed ``ReadSet``, silently re-binding to ``ChangeSet`` — the
    single executable tie to the consumed T1 result could not detect a change
    to the very structure it claimed to pin."""
    source = _LEAN_SOURCE.read_text()
    mutated = _rename_readset_field(source, "colls", "collections")
    assert mutated != source

    with pytest.raises(AssertionError, match="no longer matches"):
        _assert_lean_readset_shape(mutated)

    # the defect this replaces, demonstrated on the same mutated copy: the
    # unanchored pattern happily matches, binding ChangeSet's body.
    assert re.search(_UNANCHORED_READSET_PATTERN, mutated, re.DOTALL), (
        "the negative control no longer demonstrates the wildcard slide; "
        "check that Currency.lean still declares a later structure with the "
        "same field pair"
    )
    # and the anchored extraction still reads ReadSet, not its neighbour.
    assert _lean_structure_fields(mutated, "ReadSet") == (
        "facts : Finset Fact",
        "collections : Finset CollId",
    )
    assert _lean_structure_fields(source, "ChangeSet") == (
        "facts : Finset Fact",
        "colls : Finset CollId",
    ), (
        "ChangeSet no longer shares ReadSet's field shape, so the wildcard "
        "hazard this test pins would need restating"
    )


def test_a_support_term_is_addressable_and_total() -> None:
    """Every term the index can name has a canonical value in every state —
    absence included. ``ReadSet`` membership is decidable in the formal
    model; a term that could be neither present nor absent would break the
    correspondence."""
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    terms = {
        t for state in world.states for check in checks
        for t in check.gold_support(state)
    }
    for state in world.states:
        for term in sorted(terms):
            value = e1_support_stress.term_value(state, term)
            assert isinstance(value, str)
    with pytest.raises(E1StressError):
        e1_support_stress.term_value(world.states[0], "not_a_term_kind:x")


# --------------------------------------------------------------------------- #
# 3. A support index becomes a skip input ONLY via validate_certificate.       #
# --------------------------------------------------------------------------- #
class _Invalid:
    valid = False
    invalidated_classes = ("input_digests",)
    detail: dict = {}


def _neutered_arm(
    monkeypatch, *, validation: bool, degenerate: bool,
    collect: list[UpdateGrade] | None = None,
):
    """Rerun the same arm with one or both certificate gates forced shut."""
    if validation:
        monkeypatch.setattr(
            reuse_scheduler, "validate_certificate",
            lambda *_a, **_k: _Invalid(),
        )
    if degenerate:
        monkeypatch.setattr(
            reuse_scheduler, "dependency_classes_differ",
            lambda *_a, **_k: True,
        )
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    return run_arm(
        world, checks, arm="theorem:neutered", index_kind=DECL_COMPLETE,
        collect=collect,
    )


#: The two rungs keyed on the ten-class dependency snapshot the support index
#: populates. Both mint their certificate through the SAME ``record_skip``.
_SUPPORT_INDEX_GATES = frozenset({
    reuse_scheduler.CAUSE_CERTIFICATE_VALID,
    reuse_scheduler.CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
})


def test_every_skip_e1_grades_comes_from_a_support_index_certificate_gate(
    graded,
) -> None:
    """The claim, measured on the configuration E1 actually grades.

    A recorded support index reaches a skip decision through exactly TWO
    gates in ``run_reuse_pass``, both keyed on the SAME ten-class dependency
    snapshot the index populates:

    * ``validate_certificate`` — a fully valid prior certificate (rung a);
    * ``dependency_classes_differ`` — the degenerate unchanged-inputs
      carry-forward (rung a2), which is the re-expression of the old
      manifest-hash carry-forward and no longer bypasses the certificate.

    Every skip in the shipped arm carries one of those two causes, so every
    skip E1's soundness counts grade is a support-index skip. This is the
    premise the next test then makes sensitive."""
    _updates, baseline = graded
    assert baseline.skips_proposed > 0
    assert set(baseline.skip_cause_tally) <= _SUPPORT_INDEX_GATES, (
        f"a skip reached E1's grading through a rung that does not consult "
        f"the dependency snapshot: {dict(baseline.skip_cause_tally)}"
    )
    assert set(baseline.skip_cause_tally) == _SUPPORT_INDEX_GATES, (
        "only one of the two support-index gates fired, so shutting them "
        "one at a time would not be a two-sided witness"
    )


def test_shutting_both_gates_migrates_every_support_index_skip_cause(
    monkeypatch: pytest.MonkeyPatch, graded,
) -> None:
    """The sensitivity witness, named for what the run actually shows.

    Shutting both support-index gates drives THEIR CAUSES to zero: no third
    route turns a dependency snapshot into a skip. It does not remove a single
    skip. Every one of the baseline's step-updates still skips, under the
    effect-bound ``skip_justified`` rung — which reads the change STATEMENT
    and the compiled effects envelope, never the dependency snapshot, and is
    reachable only AFTER both certificate gates fall through (which is why the
    shipped arm never shows it, see the test above). So what is witnessed here
    is cause MIGRATION, asserted precisely: same total, zero gate causes,
    zero unsafe. This is also the rung the E1 harness review (EWD-E1-2)
    recorded as unexercised, recorded rather than asserted away.

    Every one of those residual skips still carries a certificate — the
    "certificate is the only skip mechanism" property is not what the gates
    provide, and it survives their removal. That is asserted below over the
    neutered arm's own step outcomes, not left in prose."""
    _updates, baseline = graded
    assert baseline.skips_proposed > 0, "nothing to neuter"

    collected: list[UpdateGrade] = []
    both = _neutered_arm(
        monkeypatch, validation=True, degenerate=True, collect=collected,
    )
    surviving_gate_skips = {
        cause: count for cause, count in both.skip_cause_tally.items()
        if cause in _SUPPORT_INDEX_GATES
    }
    assert not surviving_gate_skips, (
        f"a support-index gate still approved skips with both gates "
        f"neutered: {surviving_gate_skips}"
    )
    residual = set(both.skip_cause_tally)
    assert residual <= SKIP_CAUSES, residual
    assert residual <= {reuse_scheduler.CAUSE_SKIP_JUSTIFIED,
                        reuse_scheduler.CAUSE_SKIP_DOMINANCE}, (
        f"an unexpected rung took over: {sorted(residual)}"
    )
    assert both.unsafe_skips == 0, (
        "the effect-bound rung approved an unsafe skip once the certificate "
        "gates were removed"
    )

    # MIGRATION, stated exactly: the skips did not go away, their cause moved.
    assert both.skips_proposed == baseline.skips_proposed, (
        f"the neutered arm proposed a different number of skips "
        f"({both.skips_proposed}) than the baseline "
        f"({baseline.skips_proposed}); this test claims cause migration, so "
        f"a changed total means the claim needs restating"
    )
    assert sum(both.skip_cause_tally.values()) == baseline.skips_proposed

    # EWD-SIT-5: the certificate property, executed rather than asserted in
    # prose. Not one residual skip lacks a certificate digest.
    neutered_skips = [
        step for update in collected for step in update.steps
        if step.action == ACTION_SKIP
    ]
    assert len(neutered_skips) == both.skips_proposed
    assert neutered_skips, "no residual skip to check — the witness is vacuous"
    uncertified = [
        (s.check_id, s.cause) for s in neutered_skips
        if not s.certificate_digest
    ]
    assert not uncertified, (
        f"{len(uncertified)} of {len(neutered_skips)} residual skips carry no "
        f"certificate digest, so the effect-bound rung is a SECOND skip "
        f"mechanism that does not mint one: {uncertified[:3]}"
    )


def test_each_certificate_gate_is_individually_load_bearing(
    monkeypatch: pytest.MonkeyPatch, graded,
) -> None:
    """Neither gate is dead code standing in for the other: shutting one
    removes exactly its own cause and leaves the other's reachable. Without
    this, the joint zero above could be produced by one gate alone."""
    _updates, baseline = graded
    assert baseline.skip_cause_tally.get(
        reuse_scheduler.CAUSE_CERTIFICATE_VALID, 0
    ) or baseline.skip_cause_tally.get(
        reuse_scheduler.CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED, 0
    ), "the baseline exercised neither certificate rung"

    no_validation = _neutered_arm(
        monkeypatch, validation=True, degenerate=False,
    )
    assert no_validation.skip_cause_tally.get(
        reuse_scheduler.CAUSE_CERTIFICATE_VALID, 0
    ) == 0
    monkeypatch.undo()

    no_degenerate = _neutered_arm(
        monkeypatch, validation=False, degenerate=True,
    )
    assert no_degenerate.skip_cause_tally.get(
        reuse_scheduler.CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED, 0
    ) == 0
    # every surviving skip in each arm still carries a certificate cause.
    for arm in (no_validation, no_degenerate):
        assert set(arm.skip_cause_tally) <= SKIP_CAUSES, arm.skip_cause_tally


def test_the_neutering_probe_is_not_self_fulfilling() -> None:
    """The probe above must fail for the RIGHT reason: the real
    ``validate_certificate`` does report valid for an unchanged snapshot, so
    the zero it produces is caused by the neutering and not by a snapshot
    that never validated."""
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    check = checks[0]
    state = world.states[0]
    support = declared_support(
        check, state, index_kind=DECL_COMPLETE,
        omission=e1_support_stress.OmissionPlan(),
    )
    facts = e1_support_stress._step_facts(check, state, support, "env-fp")
    from okg.substrate.consistency.reuse_certificates import ReuseCertificate

    certificate = ReuseCertificate(
        deployment="e1-pure", generation_id="g0", step_id=check.check_id,
        comparison={
            "kind": "degenerate_unchanged_inputs",
            "incumbent_outcome": "carry_forward",
            "incumbent_basis": "unchanged_inputs",
            "unchanged_inputs": {"statement": "support-index refinement",
                                 "dependency_classes_matched": []},
        },
        dependencies=facts,
    )
    assert validate_certificate(facts, certificate).valid


def test_dropping_one_support_term_changes_the_certificate_identity() -> None:
    """The support index is load-bearing on the certificate, not decorative:
    removing one term changes the dependency snapshot's canonical form. This
    is the mechanism that makes an OMITTED term a real hazard rather than a
    bookkeeping difference."""
    world = build_world(_SEED, updates=_UPDATES)
    checks = build_checks(world.states[0])
    check = next(c for c in checks if len(c.gold_support(world.states[0])) > 1)
    state = world.states[0]
    full = check.gold_support(state)
    dropped = frozenset(sorted(full)[1:])
    a = e1_support_stress._step_facts(check, state, full, "env-fp")
    b = e1_support_stress._step_facts(check, state, dropped, "env-fp")
    assert a.canonical_json() != b.canonical_json()


# --------------------------------------------------------------------------- #
# Runtime saved — the pure surface of the metric.                              #
# --------------------------------------------------------------------------- #
#: The corpus digest the fake arms bind. ``history_binding`` must BE this
#: value — the metric refuses a free-text label the traces do not corroborate.
_CORPUS = "a" * 64


def _binding(
    *, corpus: str = _CORPUS, commit: str = "0" * 40,
    resource_config_digest: str = "b" * 64,
) -> ReproducibilityBinding:
    return ReproducibilityBinding(
        okg_commit=commit,
        okg_tree_status=TREE_CLEAN,
        corpus_manifest_digest=corpus,
        resource_config_version=1,
        resource_config_digest=resource_config_digest,
        machine_runtime_label="test-runtime",
    )


def _cycle(
    mode: str, index: int, *, duration, cost, setup: bool = False,
    duration_unit: str = DURATION_UNIT_NANOSECONDS,
    binding: ReproducibilityBinding | None = None,
    action_class: str = GRANULARITY_PUBLISH_CYCLE,
) -> ActionTrace:
    return ActionTrace(
        action_id=f"{mode}/update-{index:04d}",
        action_class=action_class,
        cost_unit=COST_UNIT_EXECUTOR_STEPS,
        duration_unit=duration_unit,
        estimate=PlanEstimate.unestimated("no estimator exists yet"),
        reservation=Unknown("the replay path enforces no cap"),
        realized=RealizedMeasurement(
            duration=duration, cost=cost,
            measurement_source=(
                MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP if setup
                else MEASUREMENT_SOURCE_CYCLE_INTERVAL
            ),
        ),
        demand={
            PARITY_RESOURCE_CERTIFICATE_WRITES: 1,
            PARITY_RESOURCE_EXECUTOR_STEPS: 1,
            PARITY_RESOURCE_PUBLISH_COMMITS: 1,
        },
        outcome=OUTCOME_SUCCESS,
        binding=binding if binding is not None else _binding(),
        provenance={"mode": mode, "update": str(index)},
    )


class _FakeMeasuredReplay:
    def __init__(self, traces, *, mode, duration_unit=DURATION_UNIT_NANOSECONDS,
                 cost_unit=COST_UNIT_EXECUTOR_STEPS,
                 granularity=GRANULARITY_PUBLISH_CYCLE):
        self.cycle_traces = ActionTraceSet(
            schema_version=MEASUREMENT_SCHEMA_VERSION,
            trace_set_id=f"fake/{mode}/{granularity}",
            epoch=0,
            granularity=granularity,
            cost_unit=cost_unit,
            duration_unit=duration_unit,
            declared_resources=tuple(PARITY_RESOURCES),
            traces=tuple(traces),
        )


def _arm(mode: str, durations, costs, *, setup_duration=500, setup_cost=9,
         binding: ReproducibilityBinding | None = None,
         granularity: str = GRANULARITY_PUBLISH_CYCLE):
    traces = [
        _cycle(mode, 0, duration=setup_duration, cost=setup_cost, setup=True,
               binding=binding, action_class=granularity)
    ]
    for index, (duration, cost) in enumerate(zip(durations, costs), start=1):
        traces.append(_cycle(mode, index, duration=duration, cost=cost,
                             binding=binding, action_class=granularity))
    return _FakeMeasuredReplay(traces, mode=mode, granularity=granularity)


def test_runtime_saved_subtracts_and_excludes_the_setup_cycle() -> None:
    complete = _arm(MODE_COMPLETE, [100, 200, 300], [10, 10, 10],
                    setup_duration=9_000_000, setup_cost=999)
    differential = _arm(MODE_DIFFERENTIAL, [50, 100, 150], [4, 4, 4],
                        setup_duration=8_000_000, setup_cost=888)
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    assert saved.status == RUNTIME_SAVED_MEASURED
    # 600 - 300; the huge setup intervals are NOT in the difference.
    assert saved.duration_saved == 300
    assert saved.cost_saved == 18
    assert saved.complete.setup_duration == 9_000_000
    assert saved.differential.setup_duration == 8_000_000
    assert saved.complete.compared_cycles == 3
    payload = json.loads(saved.canonical_json())
    assert payload["duration_unit"] == DURATION_UNIT_NANOSECONDS
    assert payload["cost_unit"] == COST_UNIT_EXECUTOR_STEPS
    assert payload["history_binding"] == _CORPUS
    assert "setup" in payload["setup_disclosure"]
    # the exclusion's real limit is stated at the metric, not only in a report.
    assert "LABEL-driven" in payload["setup_disclosure"]
    assert "relocat" in payload["setup_disclosure"]
    # every shared binding is EMITTED, so the number stays falsifiable.
    assert payload["shared_bindings"] == {
        "corpus_manifest_digest": _CORPUS,
        "granularity": GRANULARITY_PUBLISH_CYCLE,
        "okg_commit": "0" * 40,
        "resource_config_digest": "b" * 64,
    }
    assert set(payload["shared_bindings"]) == set(SHARED_BINDING_FIELDS)


def test_an_unknown_component_makes_the_saving_unknown_not_zero() -> None:
    """The UNKNOWN discipline, at the one place it matters most: a metric
    that quietly treated an unmeasured cycle as 0 ns would REPORT A SAVING
    THE RUN DID NOT MEASURE."""
    complete = _arm(
        MODE_COMPLETE, [100, Unknown("the clock did not resolve"), 300],
        [10, 10, 10],
    )
    differential = _arm(MODE_DIFFERENTIAL, [50, 100, 150], [4, 4, 4])
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    assert saved.status == RUNTIME_SAVED_PARTIALLY_KNOWN
    assert isinstance(saved.duration_saved, Unknown)
    assert "the clock did not resolve" in saved.duration_saved.reason
    # the KNOWN part is disclosed beside it, never summed into the metric.
    assert saved.complete.duration_known_subtotal == 400
    assert len(saved.complete.duration_unknown_components) == 1
    # cost was fully measured, so it stays a number.
    assert saved.cost_saved == 18
    payload = json.loads(saved.canonical_json())
    assert "unknown" in payload["duration_saved"]
    assert payload["duration_saved"]["unknown"]
    assert payload["cost_saved"] == 18


def test_a_negative_saving_is_reported_not_clamped() -> None:
    complete = _arm(MODE_COMPLETE, [100, 100], [4, 4])
    differential = _arm(MODE_DIFFERENTIAL, [300, 300], [4, 4])
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    assert saved.status == RUNTIME_SAVED_MEASURED
    assert saved.duration_saved == -400
    assert saved.cost_saved == 0


def test_runtime_saved_refuses_unlike_units() -> None:
    complete = _arm(MODE_COMPLETE, [100], [4])
    differential = _FakeMeasuredReplay(
        [_cycle(MODE_DIFFERENTIAL, 0, duration=10, cost=1, setup=True,
                duration_unit="microseconds"),
         _cycle(MODE_DIFFERENTIAL, 1, duration=10, cost=1,
                duration_unit="microseconds")],
        mode=MODE_DIFFERENTIAL, duration_unit="microseconds",
    )
    with pytest.raises(E1MetricError, match="unlike duration units"):
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )


def test_runtime_saved_refuses_a_mode_mixup() -> None:
    complete = _arm(MODE_COMPLETE, [100], [4])
    also_complete = _arm(MODE_COMPLETE, [50], [2])
    with pytest.raises(E1MetricError, match="expected execution mode"):
        measure_runtime_saved(
            complete, also_complete, history_binding=_CORPUS,
        )


def test_runtime_saved_refuses_unequal_cycle_counts() -> None:
    complete = _arm(MODE_COMPLETE, [100, 100, 100], [4, 4, 4])
    differential = _arm(MODE_DIFFERENTIAL, [50, 50], [2, 2])
    with pytest.raises(E1MetricError, match="different number of update"):
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )


def test_runtime_saved_refuses_an_unbound_history() -> None:
    complete = _arm(MODE_COMPLETE, [100], [4])
    differential = _arm(MODE_DIFFERENTIAL, [50], [2])
    with pytest.raises(E1MetricError, match="bind the sealed history"):
        measure_runtime_saved(complete, differential, history_binding="")


# --------------------------------------------------------------------------- #
# Cross-arm binding: the two arms must be ONE experiment (EWD-SIT-1).          #
# --------------------------------------------------------------------------- #
# Matching units and matching cycle counts do NOT make two replays the same
# experiment. Each scenario below produces a perfectly well-formed pair of arms
# whose difference means nothing; each must be REFUSED, not measured.
def test_runtime_saved_refuses_arms_bound_to_different_corpora() -> None:
    """The headline hole: complete over corpus A minus differential over
    corpus B is not a saving on any history."""
    complete = _arm(MODE_COMPLETE, [100, 200, 300], [10, 10, 10])
    differential = _arm(
        MODE_DIFFERENTIAL, [50, 100, 150], [4, 4, 4],
        binding=_binding(corpus="f" * 64),
    )
    with pytest.raises(E1MetricError, match="not the same experiment") as exc:
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )
    assert "corpus_manifest_digest" in str(exc.value)


def test_runtime_saved_refuses_arms_bound_to_different_commits() -> None:
    """Two code states are two engines. Their difference is a code change, not
    a scheduling saving."""
    complete = _arm(MODE_COMPLETE, [100, 200], [10, 10])
    differential = _arm(
        MODE_DIFFERENTIAL, [50, 100], [4, 4],
        binding=_binding(commit="1" * 40),
    )
    with pytest.raises(E1MetricError, match="not the same experiment") as exc:
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )
    assert "okg_commit" in str(exc.value)


def test_runtime_saved_refuses_arms_under_different_resource_configs() -> None:
    """§12 binds the resource configuration precisely so that a number
    measured under one cannot be quoted against another."""
    complete = _arm(MODE_COMPLETE, [100, 200], [10, 10])
    differential = _arm(
        MODE_DIFFERENTIAL, [50, 100], [4, 4],
        binding=_binding(resource_config_digest="c" * 64),
    )
    with pytest.raises(E1MetricError, match="not the same experiment") as exc:
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )
    assert "resource_config_digest" in str(exc.value)


def test_runtime_saved_refuses_arms_at_different_granularities() -> None:
    """A publish-cycle total minus an executor-step total is not a quantity.
    ``measure_runtime_saved`` reads ``cycle_traces`` only, so the adapter's own
    granularity refusal is never reached from this path — the check has to be
    here."""
    complete = _arm(MODE_COMPLETE, [100, 200], [10, 10])
    differential = _arm(
        MODE_DIFFERENTIAL, [50, 100], [4, 4],
        granularity=GRANULARITY_EXECUTOR_STEP,
    )
    with pytest.raises(E1MetricError, match="not the same experiment") as exc:
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )
    assert "granularity" in str(exc.value)


def test_runtime_saved_refuses_a_history_binding_the_traces_do_not_carry(
) -> None:
    """A free-text label is not a binding. ``history_binding`` must BE the
    corpus digest both arms bound, or a report can name a history it never
    replayed."""
    complete = _arm(MODE_COMPLETE, [100, 200], [10, 10])
    differential = _arm(MODE_DIFFERENTIAL, [50, 100], [4, 4])
    with pytest.raises(E1MetricError, match="must BE the corpus manifest"):
        measure_runtime_saved(
            complete, differential, history_binding="sealed:A-COMPLETE-LIE",
        )


def test_runtime_saved_refuses_one_arm_that_spans_two_corpora() -> None:
    """The per-arm half of the binding, kept separately load-bearing: an arm
    whose own traces disagree is not one replay of one history, and that is
    true even when the two arms happen to agree on everything else. Deleting
    the per-arm uniqueness refusal breaks THIS test by name."""
    mixed = _FakeMeasuredReplay(
        [
            _cycle(MODE_COMPLETE, 0, duration=500, cost=9, setup=True),
            _cycle(MODE_COMPLETE, 1, duration=100, cost=10),
            _cycle(MODE_COMPLETE, 2, duration=200, cost=10,
                   binding=_binding(corpus="e" * 64)),
        ],
        mode=MODE_COMPLETE,
    )
    differential = _arm(MODE_DIFFERENTIAL, [50, 100], [4, 4])
    with pytest.raises(
        E1MetricError, match="more than one corpus_manifest_digest"
    ):
        measure_runtime_saved(
            mixed, differential, history_binding=_CORPUS,
        )


def test_runtime_saved_refuses_an_empty_arm() -> None:
    complete = _FakeMeasuredReplay([], mode=MODE_COMPLETE)
    differential = _arm(MODE_DIFFERENTIAL, [50], [2])
    with pytest.raises(E1MetricError, match="empty trace set"):
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )


def test_runtime_saved_refuses_a_setup_only_arm() -> None:
    """Every cycle setup-charged means nothing comparable is left. Silently
    comparing setup intervals would attribute corpus materialization to the
    scheduler."""
    complete = _FakeMeasuredReplay(
        [_cycle(MODE_COMPLETE, 0, duration=100, cost=4, setup=True)],
        mode=MODE_COMPLETE,
    )
    differential = _arm(MODE_DIFFERENTIAL, [50], [2])
    with pytest.raises(E1MetricError, match="setup-charged"):
        measure_runtime_saved(
            complete, differential, history_binding=_CORPUS,
        )


# --------------------------------------------------------------------------- #
# Checker mutation detection — the classifiers' pure surface.                  #
# --------------------------------------------------------------------------- #
class _FakeMutation:
    def __init__(self, kind, target_step="step:doc-a", detail=None):
        self.kind = kind
        self.target_step = target_step
        self.detail = detail if detail is not None else {}


class _FakeLedger:
    def __init__(self, *, skips_by_cause, steps_run, invalidated_classes=(),
                 certificates_invalidated=0):
        self.skips_by_cause = dict(skips_by_cause)
        self.steps_run = steps_run
        self.invalidated_classes = tuple(invalidated_classes)
        self.certificates_invalidated = certificates_invalidated


class _FakeExecution:
    def __init__(self, ledger, run_steps=()):
        self.ledger = ledger
        self.run_steps = tuple(run_steps)


_REAL_REFUSAL = (
    "the stored reuse certificate digest 'abc' does not match the "
    "recomputed digest 'def' — the row was corrupted after write"
)


def test_certificate_corruption_is_detected_by_the_named_refusal() -> None:
    mutation = _FakeMutation(
        MUTATION_CORRUPT_CERTIFICATE,
        detail={"corrupted_step": "step:doc-a"},
    )
    row = classify_certificate_corruption(
        mutation=mutation, update_index=2,
        raised=RuntimeError(_REAL_REFUSAL),
    )
    assert row.detected
    assert row.detection_mode == DETECTION_MODE_REFUSED
    assert "corrupted after write" in row.refusal
    assert all(m in _REAL_REFUSAL for m in CORRUPTION_REFUSAL_MARKERS)
    assert row.to_canonical_dict()["mutation_kind"] == (
        MUTATION_CORRUPT_CERTIFICATE
    )


def test_an_unrelated_failure_is_not_banked_as_a_detection() -> None:
    """A connection drop is not the certificate store catching tampering.
    Counting it would make the metric report a detection the engine never
    performed."""
    mutation = _FakeMutation(
        MUTATION_CORRUPT_CERTIFICATE,
        detail={"corrupted_step": "step:doc-a"},
    )
    row = classify_certificate_corruption(
        mutation=mutation, update_index=2,
        raised=RuntimeError("connection to server was lost"),
    )
    assert not row.detected
    assert row.detection_mode == DETECTION_MODE_NOT_CAUGHT
    assert "raised_but_unattributable" in row.mutation_evidence


def test_a_corruption_nobody_noticed_is_reported_not_dropped() -> None:
    mutation = _FakeMutation(
        MUTATION_CORRUPT_CERTIFICATE,
        detail={"corrupted_step": "step:doc-a"},
    )
    row = classify_certificate_corruption(
        mutation=mutation, update_index=2, raised=None,
    )
    assert not row.detected
    assert row.detection_mode == DETECTION_MODE_NOT_CAUGHT
    assert row.refusal == ""


def test_a_mutation_that_never_applied_is_refused_not_graded() -> None:
    """Parity refuses no-op mutations; the classifiers keep the property. A
    mutation that changed nothing can produce neither a detection nor a
    miss."""
    with pytest.raises(E1MetricError, match="never named a target step"):
        classify_certificate_corruption(
            mutation=_FakeMutation(MUTATION_CORRUPT_CERTIFICATE,
                                   target_step=""),
            update_index=1, raised=RuntimeError(_REAL_REFUSAL),
        )
    with pytest.raises(E1MetricError, match="mutates nothing proves nothing"):
        classify_certificate_corruption(
            mutation=_FakeMutation(MUTATION_CORRUPT_CERTIFICATE, detail={}),
            update_index=1, raised=RuntimeError(_REAL_REFUSAL),
        )


def test_dependency_fact_mutation_is_detected_by_invalidation_and_rerun(
) -> None:
    mutation = _FakeMutation(
        MUTATION_DEPENDENCY_FACT, detail={"mutated_class": "constraint_set"},
    )
    control = _FakeExecution(_FakeLedger(
        skips_by_cause={"certificate_valid": 3}, steps_run=0,
    ))
    mutated = _FakeExecution(
        _FakeLedger(
            skips_by_cause={}, steps_run=3,
            invalidated_classes=("constraint_set",),
            certificates_invalidated=3,
        ),
        run_steps=("step:doc-a", "step:doc-b"),
    )
    row = classify_dependency_fact_mutation(
        mutation=mutation, update_index=2, control=control, mutated=mutated,
    )
    assert row.detected
    assert row.detection_mode == DETECTION_MODE_INVALIDATED_AND_RERAN
    assert "constraint_set" in row.refusal


def test_a_dependency_mutation_under_no_valid_certificate_is_refused() -> None:
    """Mutating a step that was going to run anyway proves nothing. Grading it
    as a detection would inflate the metric with a decision the mutation did
    not cause."""
    mutation = _FakeMutation(
        MUTATION_DEPENDENCY_FACT, detail={"mutated_class": "constraint_set"},
    )
    control = _FakeExecution(_FakeLedger(skips_by_cause={}, steps_run=3))
    mutated = _FakeExecution(
        _FakeLedger(skips_by_cause={}, steps_run=3,
                    invalidated_classes=("constraint_set",),
                    certificates_invalidated=1),
        run_steps=("step:doc-a",),
    )
    with pytest.raises(E1MetricError, match="valid certificate"):
        classify_dependency_fact_mutation(
            mutation=mutation, update_index=2, control=control,
            mutated=mutated,
        )


def test_an_invalidation_without_a_rerun_is_not_a_detection() -> None:
    mutation = _FakeMutation(
        MUTATION_DEPENDENCY_FACT, detail={"mutated_class": "constraint_set"},
    )
    control = _FakeExecution(_FakeLedger(
        skips_by_cause={"certificate_valid": 2}, steps_run=0,
    ))
    mutated = _FakeExecution(
        _FakeLedger(skips_by_cause={"skip_dominance": 2}, steps_run=0,
                    invalidated_classes=("constraint_set",),
                    certificates_invalidated=2),
        run_steps=(),
    )
    row = classify_dependency_fact_mutation(
        mutation=mutation, update_index=2, control=control, mutated=mutated,
    )
    assert not row.detected
    assert row.mutation_evidence["victim_reran"] == "False"


def test_a_detection_must_name_how_it_was_detected() -> None:
    with pytest.raises(E1MetricError, match="detected and detection_mode"):
        MutationDetection(
            mutation_kind=MUTATION_DEPENDENCY_FACT, update_index=1,
            target_step="s", detected=True,
            detection_mode=DETECTION_MODE_NOT_CAUGHT, refusal="x",
            mutation_evidence={},
        )
    with pytest.raises(E1MetricError, match="name the refusal"):
        MutationDetection(
            mutation_kind=MUTATION_DEPENDENCY_FACT, update_index=1,
            target_step="s", detected=True,
            detection_mode=DETECTION_MODE_REFUSED, refusal="",
            mutation_evidence={},
        )


# --------------------------------------------------------------------------- #
# Report integration.                                                          #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def pure_report():
    return run_report(build_manifest(seeds=(11,), updates_per_seed=12))


def test_attached_smoke_metrics_reach_the_report_canonically(
    pure_report,
) -> None:
    complete = _arm(MODE_COMPLETE, [100, 200], [10, 10])
    differential = _arm(MODE_DIFFERENTIAL, [50, 100], [4, 4])
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    detection = classify_certificate_corruption(
        mutation=_FakeMutation(MUTATION_CORRUPT_CERTIFICATE,
                               detail={"corrupted_step": "step:doc-a"}),
        update_index=2, raised=RuntimeError(_REAL_REFUSAL),
    )
    attached = pure_report.with_smoke_metrics(
        runtime_saved=saved, checker_mutations=(detection,),
    )
    payload = json.loads(attached.canonical_json())
    assert payload["runtime_saved"]["duration_saved"] == 150
    assert payload["checker_mutation_detection"]["status"] == "measured"
    assert payload["checker_mutation_detection"]["rows"][0]["detected"] is True
    assert payload["verdict"] == "pass"
    # the pure report itself is untouched — attaching returns a new report.
    assert pure_report.runtime_saved is None


def test_an_undetected_checker_mutation_refutes_the_report(
    pure_report,
) -> None:
    complete = _arm(MODE_COMPLETE, [100], [10])
    differential = _arm(MODE_DIFFERENTIAL, [50], [4])
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    missed = classify_certificate_corruption(
        mutation=_FakeMutation(MUTATION_CORRUPT_CERTIFICATE,
                               detail={"corrupted_step": "step:doc-a"}),
        update_index=2, raised=None,
    )
    attached = pure_report.with_smoke_metrics(
        runtime_saved=saved, checker_mutations=(missed,),
    )
    assert attached.verdict == (
        f"refuted:checker_mutation_not_detected:{MUTATION_CORRUPT_CERTIFICATE}"
    )


def test_attaching_an_empty_mutation_set_is_refused(pure_report) -> None:
    """An empty detection set would turn "not run" into "nothing escaped"."""
    complete = _arm(MODE_COMPLETE, [100], [10])
    differential = _arm(MODE_DIFFERENTIAL, [50], [4])
    saved = measure_runtime_saved(
        complete, differential, history_binding=_CORPUS,
    )
    with pytest.raises(E1StressError, match="at least one checker-mutation"):
        pure_report.with_smoke_metrics(
            runtime_saved=saved, checker_mutations=(),
        )
