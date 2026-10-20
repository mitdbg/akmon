"""Admission end to end: declared in the manifest, enforced in the cycle.

Closes the remaining child-6 tasks with the assertions their clauses ask for:
exhaustion holds the watermark rather than skipping, the rate term throttles
independently of width, the overshoot bound holds at the cap boundary, and a
deployment can declare all of it through the enricher config the manifest
already passes through.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.enrichers.extraction import base as base_mod
from okg.substrate.llm.fanout import ModelCallJob


class _Llm:
    model = "test/model"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def complete(self, system, user, **kw):
        self.calls.append(user)
        return f"content:{user}"


class _Ex(base_mod.LLMExtractor):
    name = "admission-probe"
    emits_typed_subtype = "probe"

    def extract_candidates(self, conn, generation_id):
        return []

    def emit_typed_node(self, conn, candidate, *, generation_id, run_id, observed_at):
        return 0


def _jobs(n):
    return [
        ModelCallJob(
            system="s", user=f"u{i}", typed_input={"i": i}, item_ref=f"item-{i}"
        )
        for i in range(n)
    ]


def test_a_deployment_declares_spend_through_the_enricher_config() -> None:
    """The manifest surface: the enricher loader passes `config` straight
    through as constructor kwargs, so these are declarable today without new
    plumbing."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=1.0, spend_estimate_usd=0.1,
        model_calls_per_second=25.0,
    )
    spend = ex._model_call_spend()

    assert spend.enabled
    assert spend.cap_usd == 1.0
    assert spend.scope == "enricher:admission-probe"
    assert ex._model_call_rate_limiter() is not None


def test_an_undeclared_lane_is_unbounded_and_reports_it() -> None:
    ex = _Ex(
        confidence_floor=0.5,
        llm=_Llm(),
        input_quarantine_max_attempts=3,
    )
    assert not ex._model_call_spend().enabled
    assert ex._model_call_rate_limiter() is None


def test_exhaustion_stops_calls_and_holds_the_watermark() -> None:
    """The P0 clause: a refused item must be recorded as an unprocessed input so
    the publisher holds the watermark, NOT skipped. A cap of $0.25 at $0.10 a
    call admits two of five; the other three must all be retryable."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=0.25, spend_estimate_usd=0.1,
    )

    outcomes = ex._complete_many(object(), _jobs(5), generation_id=1)

    assert sum(1 for o in outcomes if o.ok) == 2
    refused = [o for o in outcomes if o.budget_refused]
    assert len(refused) == 3
    assert all(o.holds_watermark for o in refused)

    # Only the admitted calls reached the provider.
    assert len(ex.llm.calls) == 2

    # And every refused item is recorded so the run is incomplete.
    refs = {ref for ref, _ in ex._input_failures}
    assert len(refs) == 3, ex._input_failures


def test_exhaustion_does_not_crash_the_cycle() -> None:
    """It must not propagate as an unhandled exception either — the clause
    forbids both the silent skip and the aborted publish."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=0.05, spend_estimate_usd=0.1,
    )
    outcomes = ex._complete_many(object(), _jobs(3), generation_id=1)

    assert len(outcomes) == 3
    assert all(o.budget_refused for o in outcomes)
    assert ex.llm.calls == []


def test_no_silent_model_downgrade_on_exhaustion() -> None:
    """`BudgetExhaustedError` exists to let a caller downgrade to a cheaper
    model. The substrate must NOT: model_id is an OutcomeKey field, so switching
    mid-run turns recorded outcomes into replay misses."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=0.05, spend_estimate_usd=0.1,
    )
    before = ex.llm.model
    ex._complete_many(object(), _jobs(2), generation_id=1)
    assert ex.llm.model == before


def test_the_rate_term_throttles_independently_of_width() -> None:
    """Rate is not a function of width. At width 4 with a 1-per-20ms limit, four
    calls cannot all fire at once."""
    import time

    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        model_calls_per_second=50.0,
    )
    started = time.monotonic()
    outcomes = ex._complete_many(object(), _jobs(4), generation_id=1)
    elapsed = time.monotonic() - started

    assert all(o.ok for o in outcomes)
    assert elapsed >= 0.04, (
        f"4 calls at 50/s finished in {elapsed:.3f}s — the rate term did not pace"
    )


def test_overshoot_at_the_cap_boundary_stays_within_the_declared_bound() -> None:
    """The clause requires driving concurrent consumption at the boundary and
    asserting the measured overshoot is within the stated bound."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=0.5, spend_estimate_usd=0.1,
    )
    spend = ex._model_call_spend()
    bound = spend.overshoot_bound_usd(width=4)

    outcomes = ex._complete_many(object(), _jobs(20), generation_id=1)
    admitted = sum(1 for o in outcomes if o.ok)

    # Exactly the cap's worth of calls, never more.
    assert admitted == 5, f"admitted {admitted} calls against a 5-call cap"
    assert bound == pytest.approx(0.4)


def test_refused_items_are_distinguishable_from_provider_failures() -> None:
    """An operator seeing 'the enricher is behind' needs to know whether the
    provider broke or the budget ran out — different repairs."""
    ex = _Ex(
        confidence_floor=0.5, llm=_Llm(),
        input_quarantine_max_attempts=3,
        spend_deployment="dep", spend_cap_usd=0.1, spend_estimate_usd=0.1,
    )
    outcomes = ex._complete_many(object(), _jobs(3), generation_id=1)

    assert outcomes[0].ok
    assert all(o.budget_refused for o in outcomes[1:])
    assert all(o.provider_error is None for o in outcomes[1:])
