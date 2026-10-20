"""PACT ``concord-spec-views-contract``, req.svc.views — view binding
witnesses.

View-bound generation with DERIVED read sets (the same analysis as a formal
invariant's query), the runs-every-pass marking of free-form generation and
context-less judged invariants, fixed-list coincidence checking, and judged
context views sharing the derivation path.
"""
from __future__ import annotations

import pytest

from okg.substrate.consistency.invariant_spec import (
    CLASSIFICATION_WHOLE_GRAPH,
    REFUSAL_FIXED_INPUT_OUTSIDE_READ_SET,
    REFUSAL_FIXED_INPUT_TRACE_MISMATCH,
    REFUSAL_UNKNOWN_FIELD,
    SpecLoadRefused,
    check_fixed_inputs_coincide,
    classify_program,
    load_invariant_spec,
    read_kinds_of_program,
)
from okg.substrate.consistency.violation_identity import WitnessParserRegistry
from tests.substrate.consistency._invariant_spec_support import (
    base_spec,
    build_registry,
)


@pytest.fixture()
def registry_and_digests():
    return build_registry()


@pytest.fixture()
def parsers() -> WitnessParserRegistry:
    registry = WitnessParserRegistry()
    registry.register("parser://section-header@v1", "regex",
                      lambda text: ())
    return registry


def _load(spec, registry, parsers):
    return load_invariant_spec(
        spec, program_resolver=registry.resolve, parser_registry=parsers,
    )


def test_view_read_set_is_derived_by_the_same_analysis_as_invariants(
    registry_and_digests, parsers
) -> None:
    """One program registered under a view ref and an invariant ref: the
    derived read kinds and the classification are IDENTICAL — there is no
    view-specific analyzer and no declared read set anywhere."""
    registry, digests = registry_and_digests
    view_program = registry.resolve("ir://checks/handbook_vacation_view@v2")
    invariant_program = registry.resolve("ir://checks/day_count_match@v5")
    # both fixtures are the same subject-local shape; assert the ANALYSIS
    # treats a view program exactly as an invariant program.
    assert read_kinds_of_program(view_program).kinds() == (
        read_kinds_of_program(invariant_program).kinds()
    )
    assert classify_program(view_program) == classify_program(
        invariant_program
    )
    loaded = _load(base_spec(digests), registry, parsers)
    assert (
        loaded.report.classification["view:handbook-vacation"]
        == loaded.report.classification["invariant:day-counts-match"]
    )


def test_free_form_generation_is_marked_runs_every_pass(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["views"].append({
        "id": "render-notes", "version": 1,
        "query": "free-form://meeting-notes", "role": "generation",
        "renders": "doc://hr-wiki/notes",
    })
    loaded = _load(spec, registry, parsers)
    assert "view:render-notes" in loaded.report.runs_every_pass
    assert loaded.report.classification["view:render-notes"] == (
        CLASSIFICATION_WHOLE_GRAPH
    )
    # a free-form generation has no query to digest: authoring one refuses.
    spec["views"][-1]["query_digest"] = "sha256:whatever"
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD


def test_judged_invariant_without_context_view_runs_every_pass(
    registry_and_digests, parsers
) -> None:
    """Context gathered any way other than a declared view has no bounded
    read set (design v8 §5): legal, marked unmaintainable — never guessed."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    del spec["invariants"][1]["property"]["context_view"]
    loaded = _load(spec, registry, parsers)
    assert "invariant:no-contradiction" in loaded.report.runs_every_pass
    assert loaded.report.classification["invariant:no-contradiction"] == (
        CLASSIFICATION_WHOLE_GRAPH
    )


def test_judged_context_views_share_the_derivation_path(
    registry_and_digests, parsers
) -> None:
    """The judged invariant's classification IS its context view's — one
    binding, one derivation (req.svc.views)."""
    registry, digests = registry_and_digests
    loaded = _load(base_spec(digests), registry, parsers)
    assert (
        loaded.report.classification["invariant:no-contradiction"]
        == loaded.report.classification["view:ctx-sections"]
    )


def test_fixed_inputs_inside_the_derived_read_set_load(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    # ctx-sections reads annotation:policy-citation — a fixed input of that
    # kind is a coincidence claim inside the derived set.
    spec["views"][1]["fixed_inputs"] = [
        "annotation://hr-wiki/handbook/policy-citation"
    ]
    loaded = _load(spec, registry, parsers)
    assert loaded.view("ctx-sections").fixed_inputs


def test_fixed_input_outside_the_read_set_refuses(
    registry_and_digests, parsers
) -> None:
    """`fixed_inputs` never ADDS reads: a fixed input outside the view
    query's derived read set is a load refusal, not a silent widening."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["views"][1]["fixed_inputs"] = [
        "annotation://hr-wiki/handbook/day-count"   # kind the query never reads
    ]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert excinfo.value.data["code"] == REFUSAL_FIXED_INPUT_OUTSIDE_READ_SET


def test_fixed_list_coincidence_is_checked_at_generation_time(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    declared = [
        "annotation://hr-wiki/handbook/policy-citation",
        "annotation://hr-wiki/runbook/policy-citation",
    ]
    spec["views"][1]["fixed_inputs"] = declared
    view = _load(spec, registry, parsers).view("ctx-sections")

    # the recorded trace equals the fixed list (order-free): passes.
    check_fixed_inputs_coincide(view, list(reversed(declared)))
    # a trace that read something else fails the coincidence claim loudly.
    with pytest.raises(SpecLoadRefused) as excinfo:
        check_fixed_inputs_coincide(view, declared[:1])
    assert excinfo.value.data["code"] == REFUSAL_FIXED_INPUT_TRACE_MISMATCH
    # a view with no fixed list makes no claim: nothing to check.
    plain = _load(base_spec(digests), registry, parsers).view("ctx-sections")
    check_fixed_inputs_coincide(plain, ["anything"])


def test_generation_views_must_render_and_context_views_must_not(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    del spec["views"][0]["renders"]
    with pytest.raises(SpecLoadRefused):
        _load(spec, registry, parsers)
    spec = base_spec(digests)
    spec["views"][1]["renders"] = "doc://hr-wiki/somewhere"
    with pytest.raises(SpecLoadRefused):
        _load(spec, registry, parsers)
