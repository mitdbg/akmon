"""Spend admission: a cap that actually stops calls, over the shared ledger.

The substrate had no economic bound on model calls. Width bounds occupancy and
nothing else, so the fan-out multiplied burn rate with nothing to stop a
runaway. These assert the properties whose absence is invisible until a bill
arrives.
"""
from __future__ import annotations

import pytest

from okg.substrate.llm.spend import ModelCallSpend


def _spend(cap=1.0, estimate=0.1, deployment="test") -> ModelCallSpend:
    return ModelCallSpend(
        deployment=deployment, scope="enricher:probe",
        cap_usd=cap, estimated_usd_per_call=estimate,
    )


def test_reservations_accumulate_and_the_cap_refuses() -> None:
    spend = _spend(cap=0.25, estimate=0.1)

    assert spend.reserve(None).admitted
    assert spend.reserve(None).admitted
    refused = spend.reserve(None)

    assert not refused.admitted
    assert "spend cap exhausted" in refused.reason
    assert "enricher:probe" in refused.reason


def test_the_refusal_names_the_numbers_an_operator_needs() -> None:
    spend = _spend(cap=0.15, estimate=0.1)
    spend.reserve(None)
    refused = spend.reserve(None)

    assert "$0.1000 of $0.1500" in refused.reason
    assert "estimates $0.1000" in refused.reason


def test_an_undeclared_cap_leaves_the_lane_unbounded_and_says_so() -> None:
    """A missing declaration must NOT become a default cap. An invented cap
    would silently bound a lane nobody budgeted; an invented estimate would be
    wrong by an unknown factor. Absent means unbounded, reported."""
    spend = ModelCallSpend(
        deployment="test", scope="s", cap_usd=None, estimated_usd_per_call=None,
    )

    assert not spend.enabled
    decision = spend.reserve(None)
    assert decision.admitted
    assert "disabled" in decision.reason
    assert spend.overshoot_bound_usd(8) == float("inf")


def test_each_reservation_gets_a_distinct_consumption_id() -> None:
    """The ledger row is keyed on (scope, consumption_id); colliding ids would
    make a second real charge a silent no-op via ON CONFLICT."""
    spend = _spend(cap=10.0, estimate=0.1)
    ids = {spend.reserve(None).consumption_id for _ in range(20)}
    assert len(ids) == 20


def test_overshoot_bound_is_a_number_not_a_hope() -> None:
    """The requirement forbids leaving the bound unstated. With
    reserve-before-call the exposure is per-call estimate error across at most
    `width` in-flight calls."""
    spend = _spend(cap=100.0, estimate=0.25)
    assert spend.overshoot_bound_usd(1) == pytest.approx(0.25)
    assert spend.overshoot_bound_usd(8) == pytest.approx(2.0)


def test_the_reservation_critical_section_is_locked() -> None:
    """The check-and-increment must be INSIDE `self._lock`.

    Asserted structurally because the load test below cannot distinguish: I
    removed the lock and ran it, and it still passed — the read-check-write
    window is short enough that CPython's bytecode interleaving rarely splits
    it. So the concurrency test is a smoke test, and THIS is the assertion that
    would notice the lock going away.
    """
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(ModelCallSpend.reserve)))
    withs = [n for n in ast.walk(tree) if isinstance(n, ast.With)]
    assert withs, "reserve() has no `with` block; the critical section is unguarded"

    guarded = "\n".join(ast.unparse(w) for w in withs)
    assert "_lock" in guarded, "reserve()'s critical section is not under _lock"
    assert "_reserved_usd +=" in guarded, (
        "the increment moved OUTSIDE the locked region; concurrent reservations "
        "can now lose an update and over-admit"
    )


def test_concurrent_reservations_respect_the_cap_under_load() -> None:
    """A smoke test, deliberately labelled as one.

    It exercises many threads colliding on the cap and asserts the total is
    exact. It does NOT prove the lock is load-bearing: removing the lock and
    re-running this still passes, because the critical section is short. The
    structural test above is what guards the lock itself.
    """
    import threading

    for _ in range(25):
        spend = _spend(cap=0.5, estimate=0.1)  # exactly 5 admissions
        barrier = threading.Barrier(24)
        admitted: list[bool] = []
        lock = threading.Lock()

        def _try() -> None:
            barrier.wait()
            decision = spend.reserve(None)
            with lock:
                admitted.append(decision.admitted)

        threads = [threading.Thread(target=_try) for _ in range(24)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(admitted) == 5, (
            f"cap of $0.50 at $0.10/call admitted {sum(admitted)} calls; "
            f"reservations lost an update and over-admitted"
        )
        assert spend.reserved_usd == pytest.approx(0.5)


def test_settlement_never_raises_to_the_caller() -> None:
    """The provider call is already paid for. Turning a bookkeeping failure
    into a failed enrichment would lose data to protect an accounting row."""
    spend = _spend()
    decision = spend.reserve(None)
    # conn=None makes the real cost_budget path fail; it must be swallowed.
    spend.settle(None, decision, 0.1)
    assert spend.settled_usd == pytest.approx(0.1)


def test_settlement_is_skipped_for_a_refused_call() -> None:
    spend = _spend(cap=0.05, estimate=0.1)
    refused = spend.reserve(None)
    assert not refused.admitted

    spend.settle(None, refused, 0.1)
    assert spend.settled_usd == 0.0


def test_extractor_refuses_a_cap_without_an_estimate() -> None:
    """A cap with no estimate cannot reserve; an estimate with no cap bounds
    nothing. Declaring one without the other is a configuration error, not a
    thing to paper over."""
    from okg.substrate.library.enrichers.extraction import base as base_mod

    class _Ex(base_mod.LLMExtractor):
        name = "cfg-probe"

        def extract_candidates(self, conn, generation_id):
            return []

        def emit_typed_node(
            self, conn, candidate, *, generation_id, run_id, observed_at
        ):
            return 0

    with pytest.raises(ValueError, match="declared together"):
        _Ex(confidence_floor=0.5, spend_cap_usd=1.0)

    with pytest.raises(ValueError, match="declared together"):
        _Ex(confidence_floor=0.5, spend_estimate_usd=0.1)


@pytest.mark.parametrize("bad", [0, -1.0])
def test_extractor_refuses_non_positive_spend_config(bad) -> None:
    from okg.substrate.library.enrichers.extraction import base as base_mod

    class _Ex(base_mod.LLMExtractor):
        name = "cfg-probe-2"

        def extract_candidates(self, conn, generation_id):
            return []

        def emit_typed_node(
            self, conn, candidate, *, generation_id, run_id, observed_at
        ):
            return 0

    with pytest.raises(ValueError):
        _Ex(confidence_floor=0.5, spend_cap_usd=bad, spend_estimate_usd=0.1)
    with pytest.raises(ValueError):
        _Ex(confidence_floor=0.5, spend_cap_usd=1.0, spend_estimate_usd=bad)
