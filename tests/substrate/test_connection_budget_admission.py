"""Boot admission: an over-declared process is refused, by name.

`req.unify-connection-budget.boot-admission` requires the per-process tier to
fail closed with the overspending components named — an operator should be able
to act on the message without reading the registry. The arithmetic is tested
against an injected `ServerCapacity` so the failure path is exercised without
needing a server tuned to the wrong size.

The host-wide tier is deliberately NOT a sum here: see
`observed_headroom`, which measures what is free rather than predicting what
other processes will declare.
"""
from __future__ import annotations

import pytest

from okg.substrate.db import concurrency_budget as budget
from okg.substrate.db.capacity import (
    ServerCapacity,
    admit_process_demand,
    observed_headroom,
    server_capacity,
)


@pytest.fixture(autouse=True)
def _clean_overrides():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


def _capacity(max_connections=100, reserved=3, active=10) -> ServerCapacity:
    return ServerCapacity(
        max_connections=max_connections,
        superuser_reserved=reserved,
        active_client_backends=active,
    )


def test_usable_and_available_arithmetic() -> None:
    """`available` discounts the probe's own connection: it is live in
    pg_stat_activity while the probe runs and closed immediately after, so
    counting it would refuse a declaration that exactly fits."""
    cap = _capacity(max_connections=100, reserved=3, active=10)
    assert cap.usable == 97
    assert cap.available == 88


def test_available_never_goes_negative() -> None:
    cap = _capacity(max_connections=5, reserved=3, active=90)
    assert cap.available == 0


def test_a_fitting_deployment_is_admitted_and_reports_its_breakdown() -> None:
    demand = admit_process_demand("postgres://unused", capacity=_capacity())
    assert demand["_total"] > 0
    assert "db.shared_pool" in demand


def test_an_over_declared_process_is_refused_with_the_components_named() -> None:
    """The refusal must be actionable: the total, the per-term breakdown, and
    the server-side arithmetic all appear in the message."""
    cap = _capacity(max_connections=20, reserved=3, active=15)

    with pytest.raises(budget.BudgetError) as exc:
        admit_process_demand("postgres://unused", capacity=cap)

    message = str(exc.value)
    assert "db.shared_pool=" in message
    assert "runtime.dbos_pool=" in message
    assert "max_connections=20" in message
    assert "already-active client backends" in message
    assert "Short by" in message


def test_refusal_does_not_clamp_the_declared_widths() -> None:
    """Fail closed, not quietly smaller. A clamped width would boot a process
    that reports success while running narrower than declared."""
    cap = _capacity(max_connections=20, reserved=3, active=15)
    before = budget.width("db.shared_pool")

    with pytest.raises(budget.BudgetError):
        admit_process_demand("postgres://unused", capacity=cap)

    assert budget.width("db.shared_pool") == before


def test_a_narrower_declaration_fits_where_the_default_did_not() -> None:
    """The refusal is about the DECLARATION, not the server: narrowing the
    widths makes the same server acceptable."""
    cap = _capacity(max_connections=20, reserved=3, active=8)
    with pytest.raises(budget.BudgetError):
        admit_process_demand("postgres://unused", capacity=cap)

    budget.set_override("db.shared_pool", 4)
    budget.set_override("runtime.dbos_pool", 2)
    demand = admit_process_demand("postgres://unused", capacity=cap)
    assert demand["_total"] <= cap.available


def test_borrowers_do_not_inflate_the_admitted_demand() -> None:
    """A borrower occupies connections its pool already owns. If the admission
    check counted them again it would refuse deployments that fit — the
    fail-closed direction makes double-counting an outage, not a rounding
    error."""
    cap = _capacity(max_connections=100, reserved=3, active=0)
    baseline = admit_process_demand("postgres://unused", capacity=cap)["_total"]

    budget.set_override("ingest.source_layer", 2)
    assert admit_process_demand(
        "postgres://unused", capacity=cap
    )["_total"] == baseline


def test_mcp_is_excluded_from_the_process_sum_by_default() -> None:
    cap = _capacity()
    without = admit_process_demand("postgres://unused", capacity=cap)
    with_mcp = admit_process_demand(
        "postgres://unused", capacity=cap, include_mcp=True
    )
    assert with_mcp["_total"] > without["_total"]


def test_observed_headroom_is_negative_when_the_process_cannot_fit() -> None:
    # usable = 20 - 3 = 17; active 16 minus the probe's own connection = 15;
    # available = 2.
    cap = _capacity(max_connections=20, reserved=3, active=16)
    assert cap.available == 2
    assert observed_headroom(cap, demand=10) == -8
    assert observed_headroom(cap, demand=2) == 0


def test_capacity_probe_reads_the_three_server_numbers(monkeypatch) -> None:
    """The probe must read max_connections, superuser_reserved_connections and
    the live backend count — not assume any of them."""
    executed: list[str] = []

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False

        def execute(self, sql):
            executed.append(sql)

        def fetchone(self):
            if "max_connections" in executed[-1]:
                return (100,)
            if "superuser_reserved" in executed[-1]:
                return (3,)
            return (7,)

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def cursor(self): return _Cur()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: _Conn())

    cap = server_capacity("postgres://unused")
    assert (cap.max_connections, cap.superuser_reserved) == (100, 3)
    assert cap.active_client_backends == 7
    assert any("pg_stat_activity" in s for s in executed)
