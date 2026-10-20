"""PACT ``concord-system-stress`` — req.stress.derived, task.stress.derived.

Derived content exists at two levels and the system treats them differently.

A derived FACT declares the function and inputs it was computed from — a BMI
knows it comes from a height and a weight, by name. CHECKING uses that: a
derived-value conflict is judged adjudicable only when the input facts are
present. REPAIR ignores it entirely and settles the conflict by counting how
many documents repeat each number.

That is the sharpest illustration of the repair defect in the whole program.
When a BMI disagrees with the height and weight it was computed from, the
answer is available by recomputation — and the engine instead counts copies.

A derived VIEW (a generation-role view that renders and attaches, or a
graph-region corpus that makes derived content a source in its own right) is
expressible in the specification and reachable from no runtime. And
document-to-document derivation — a summary generated from a record, echoing
its claims — is ABSENT from the corpus generator entirely. It is reported as
absent here rather than simulated by a label, because a stipulated copy label
read by the engine would grade it on an oracle handing it the answer to the
exact question a real deployment must infer.
"""
from __future__ import annotations

import json
import pathlib



#: The derived-value rows from the scaled taxonomy run (442 graded errors over
#: 8 settings x 5 seeds). Only this subset is committed: the full 442-row
#: classification is 8,000 lines that nothing would read, and the findings it
#: supports are summarised in the run report. An earlier version of this file
#: read an absolute path under the author's scratch directory, so it skipped on
#: every other machine including CI — a test that runs nowhere asserts nothing.
_SCALED = (
    pathlib.Path(__file__).resolve().parent
    / "fixtures/scaled_run_derived_value_rows.json"
)


# --------------------------------------------------------------------------- #
# The world MODELS derived facts.                                             #
# --------------------------------------------------------------------------- #
def test_a_derived_fact_names_the_function_and_inputs_it_came_from(world) -> None:
    derived = [f for f in world.facts if f.derived_from is not None]
    assert derived, "the world declares no derived facts — nothing to test"
    for fact in derived:
        function, inputs = fact.derived_from
        assert function, fact.fact_id
        assert inputs, f"{fact.fact_id} declares a function with no inputs"
        known = {f.fact_id for f in world.facts}
        for dep in inputs:
            assert dep in known, f"{fact.fact_id} derives from unknown {dep}"


def test_checking_consults_the_derivation_but_repair_does_not() -> None:
    """The asymmetry, stated against the code that implements each side.

    Detection reads the derivation: ``_fd_input_buckets`` resolves a derived
    value's input buckets and ``_terminal_state`` only calls a conflict
    adjudicable when they are present. The precedence relation that settles a
    conflict takes ``ClaimSignals`` — supersession, effective date, source
    authority, corroboration count, recency, maintenance — and no member of
    that structure carries a derivation. Repair therefore CANNOT consult it.
    """
    import inspect

    from okg.bench.synth import resolution
    from okg.substrate.consistency.claim_model.precedence import ClaimSignals

    detection = inspect.getsource(resolution)
    assert "_fd_input_buckets" in detection
    assert "fd_resolvable" in detection

    signal_fields = set(ClaimSignals.__dataclass_fields__)
    for leak in ("derived_from", "derivation", "inputs", "recomputed"):
        assert leak not in signal_fields, (
            f"repair now carries {leak!r}: the derivation may have become "
            "available to the ranking, and this test's claim is stale"
        )
    assert "independent_support" in signal_fields, (
        "the corroboration COUNT is what repair ranks on instead"
    )


# --------------------------------------------------------------------------- #
# Measured: repair settles derived-value conflicts by counting.               #
# --------------------------------------------------------------------------- #
def test_every_derived_value_conflict_was_settled_by_counting() -> None:
    """ACCEPTANCE. Over the scaled run's derived-value errors, every resolution
    was reached by corroboration count or abandoned on a tie — never once by
    recomputing the value from the inputs the fact names."""
    rows = json.load(_SCALED.open())["derived_value_rows"]
    derived = rows
    assert derived, "no derived-value errors in the scaled run"

    by_mechanism: dict[str, int] = {}
    for row in derived:
        by_mechanism[row["failure_category"]] = (
            by_mechanism.get(row["failure_category"], 0) + 1
        )
    print(f"\n\nDERIVED-VALUE ERRORS ({len(derived)} graded)")
    for mechanism, count in sorted(by_mechanism.items(), key=lambda kv: -kv[1]):
        print(f"  {mechanism:<32}{count:>4}")

    # every settled one was settled by COUNT, and none by recomputation
    settled = [r for r in derived if r["state"] in ("repaired", "mis_repaired")]
    assert settled, "no derived-value conflict was settled at all"
    bases = {r.get("resolution_basis") or "" for r in settled}
    assert bases <= {"dependency_adjusted_support", ""}, (
        f"a derived-value conflict settled on something else: {bases}"
    )
    assert "repaired_by_majority" in by_mechanism, by_mechanism


# --------------------------------------------------------------------------- #
# Document-to-document derivation is ABSENT, and that is the report.          #
# --------------------------------------------------------------------------- #
def test_no_document_declares_that_it_derives_from_another() -> None:
    """The gap, pinned. The corpus renders every fact independently from one
    world — a star, not a chain — so two documents agreeing are two independent
    renderings BY CONSTRUCTION. There is no way to say "these two are the same
    document twice", which is exactly what separates corroboration from an echo.

    This is the same gap the engine names as its own reason for mis-repairing:
    the production precedence module records that copies cannot be discounted
    because no dependency relation exists to adjust by.
    """
    from okg.bench.synth.render import RenderResult, TraceRow

    trace_fields = set(TraceRow.__dataclass_fields__)
    for name in ("derived_from_doc", "source_document", "provenance_parent"):
        assert name not in trace_fields, (
            f"TraceRow now carries {name!r} — document derivation may exist and "
            "this test's claim is stale"
        )
    assert "doc" in trace_fields and "fact_id" in trace_fields, (
        "a rendering links a FACT to a DOCUMENT and never a document to another"
    )
    assert "corrupted_docs" in set(RenderResult.__dataclass_fields__)
