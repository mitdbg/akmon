"""`Publisher.run_gates` must refuse consistency gates rather than mis-evaluate them.

`run_gates` has no production caller: the live publish cycle evaluates
consistency gates in `_cycle_chronos_native`, which knows the pre_view/post_view
pass split (#1426) and prefers a gate's `evaluate_incremental` when it exposes
one. A second implementation of `_evaluate_consistency_gates` — older, and
missing both of those — used to sit in this class and was shadowed by the live
one on 2026-08-27, so `run_gates` raised `TypeError` on every call with gates
configured. The repair deleted the divergent body rather than reviving it,
because reviving it would have turned a loud crash into a quiet code path that
evaluates gates by the wrong rules.

This pins the refusal. It runs without a database: `run_gates` checks
`consistency_gates` before it touches the DSN, which is the property that makes
the refusal cheap and testable.
"""

from __future__ import annotations

import pytest

from okg.substrate.publisher.publisher import Publisher


class _StubPublisher:
    """Only the attributes `run_gates` reads before it refuses."""

    def __init__(self, consistency_gates: list[object]) -> None:
        self.consistency_gates = consistency_gates
        self.dsn = "postgresql://unreachable.invalid/never-opened"


def test_run_gates_refuses_when_consistency_gates_are_configured() -> None:
    stub = _StubPublisher(consistency_gates=[object()])
    with pytest.raises(NotImplementedError) as excinfo:
        Publisher.run_gates(stub, object(), object())
    assert "_cycle_chronos_native" in str(excinfo.value)


def test_run_gates_refusal_precedes_any_database_work() -> None:
    """The refusal must not depend on a reachable DSN or a real generation."""
    stub = _StubPublisher(consistency_gates=[object()])
    # `object()` stands in for a ReservedGeneration and a PhaseAResult. If the
    # refusal ever moves below the cache-key or gate-evaluation calls, those
    # would raise AttributeError or a connection error instead.
    with pytest.raises(NotImplementedError):
        Publisher.run_gates(stub, object(), object())


def test_the_shadowed_legacy_gate_evaluator_is_gone() -> None:
    """The divergent body must not come back under any name."""
    assert not hasattr(Publisher, "_evaluate_legacy_consistency_gates")
    evaluator = Publisher._evaluate_consistency_gates
    parameters = evaluator.__code__.co_varnames[: evaluator.__code__.co_argcount]
    # The surviving evaluator is the live one: it takes a connection, not a
    # ReservedGeneration plus a PhaseBResult.
    assert parameters == ("self", "conn"), parameters
