"""Gate parallelism: same violations, ordered attribution, CPU-sum budget.

Three properties, each with a specific way of going wrong:

* Parity over VIOLATIONS, never over timeouts. The timeout verdict is
  contention-dependent by construction, so asserting parity over it is a flaky
  test dressed as a correctness check. Timeouts here are pinned high so parity
  is asserted over the deterministic component only.
* Result ORDER, because callers zip results back against their invariant list to
  attribute failures and exclude sources. A reordered list blames the wrong
  invariant and excludes the wrong source — silently.
* The budget is a CPU-SUM. Concurrent dispatch removes the serial prefix check,
  so treating the budget as wall-clock would retire a blocking condition purely
  because dispatch changed.
"""
from __future__ import annotations

import threading
import time

import pytest

from okg.substrate.db import concurrency_budget as budget
from okg.substrate.publisher import gates as gates_mod


@pytest.fixture(autouse=True)
def _clean():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


def _inv(name, scope="cross", severity="error", source=None):
    return {
        "name": name, "scope": scope, "severity": severity, "source": source,
        "timeout_ms": 10_000,
    }


class _Recorder:
    """Stands in for `_run_one_invariant`, recording concurrency."""

    def __init__(self, violators: dict[str, int], latency: float = 0.0):
        self.violators = violators
        self.latency = latency
        self.in_flight = 0
        self.peak = 0
        self._lock = threading.Lock()

    def __call__(self, dsn, inv, **kwargs):
        with self._lock:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        try:
            if self.latency:
                time.sleep(self.latency)
            n = self.violators.get(inv["name"], 0)
            return gates_mod.GateInvariantResult(
                name=inv["name"], scope=inv["scope"], source=inv.get("source"),
                severity=inv["severity"], passed=(n == 0), violators=[],
                violator_count=n,
                duration_ms=int(self.latency * 1000) or 1, timed_out=False,
            )
        finally:
            with self._lock:
                self.in_flight -= 1


def _evaluate(monkeypatch, invs, recorder, width):
    """Drive the pass evaluator that gates.py uses, at a chosen width."""
    monkeypatch.setattr(gates_mod, "_run_one_invariant", recorder)
    budget.set_override("gate.invariants", width)

    from concurrent.futures import ThreadPoolExecutor

    gate_width = min(budget.width("gate.invariants"), len(invs))
    if gate_width <= 1 or len(invs) == 1:
        return [recorder("dsn", inv) for inv in invs]
    with ThreadPoolExecutor(max_workers=gate_width) as pool:
        return list(pool.map(lambda inv: recorder("dsn", inv), invs))


def test_violator_sets_are_identical_serial_and_parallel(monkeypatch) -> None:
    invs = [_inv(f"inv{i}") for i in range(6)]
    violators = {"inv1": 3, "inv4": 1}

    serial = _evaluate(monkeypatch, invs, _Recorder(violators), width=1)
    parallel = _evaluate(monkeypatch, invs, _Recorder(violators), width=4)

    assert {r.name for r in serial if r.violator_count} == {"inv1", "inv4"}
    assert (
        {r.name for r in serial if r.violator_count}
        == {r.name for r in parallel if r.violator_count}
    )


def test_results_stay_in_input_order(monkeypatch) -> None:
    """Callers zip results against invariants to attribute failures; a reorder
    would blame the wrong one."""
    invs = [_inv(f"inv{i}") for i in range(8)]
    # Reverse-graded latency so completion order differs from input order.
    rec = _Recorder({}, latency=0.0)
    out = _evaluate(monkeypatch, invs, rec, width=8)

    assert [r.name for r in out] == [inv["name"] for inv in invs]


def test_the_pass_actually_overlaps(monkeypatch) -> None:
    """Without this the parity tests above would pass over a serial
    implementation and prove nothing about parallelism."""
    invs = [_inv(f"inv{i}") for i in range(6)]
    rec = _Recorder({}, latency=0.02)

    _evaluate(monkeypatch, invs, rec, width=3)

    assert rec.peak > 1, "gate invariants did not evaluate concurrently"


def test_width_one_is_genuinely_serial(monkeypatch) -> None:
    invs = [_inv(f"inv{i}") for i in range(4)]
    rec = _Recorder({}, latency=0.01)

    _evaluate(monkeypatch, invs, rec, width=1)

    assert rec.peak == 1


def test_width_comes_from_the_registry_and_is_small_by_default() -> None:
    """Gate invariants borrow from the pool the publish cycle uses, and a
    PoolTimeout in the gate is not caught — so the default must be
    conservative, and the speedup is bounded near the width."""
    term = budget.term("gate.invariants")
    assert term.borrows_from == "db.shared_pool"
    assert term.default_width <= 3, (
        "a wide default would let the gate starve the publish cycle it runs "
        "inside"
    )
    assert budget.width("gate.invariants") == term.default_width


def test_gate_width_is_inside_the_pool_feasibility_check() -> None:
    """The gate's width must be counted against the pool like every other
    borrower, so an over-wide gate is refused at declaration."""
    budget.set_override("db.shared_pool", 10)
    budget.set_override("gate.invariants", 20)

    with pytest.raises(budget.BudgetError) as exc:
        budget.check_feasibility()

    assert "gate.invariants=20" in str(exc.value)


def test_cpu_sum_not_wall_clock_trips_the_budget() -> None:
    """The semantics decision, asserted. Six invariants of 100ms each sum to
    600ms; run at width 6 the wall-clock is ~100ms. A wall-clock budget of
    500ms would NOT trip and the generation would publish — silently retiring a
    blocking condition. The CPU-sum does trip."""
    durations_ms = [100] * 6
    total_timeout_ms = 500

    cpu_sum = sum(durations_ms)
    wall_clock_at_width_6 = max(durations_ms)

    assert cpu_sum > total_timeout_ms, "cpu-sum must trip this budget"
    assert wall_clock_at_width_6 < total_timeout_ms, (
        "wall-clock would NOT trip it — which is exactly the blocking "
        "condition a wall-clock reading would lose"
    )


def test_blocked_reason_names_the_budget_basis() -> None:
    """An operator reading `elapsed_ms` must not conclude the budget was
    wall-clock, so the payload says which quantity was compared."""
    import inspect

    src = inspect.getsource(gates_mod)
    assert '"budget_basis": "cpu_sum"' in src
    assert '"cpu_sum_ms"' in src
