"""The concurrency registry: widths resolve, and bad widths refuse.

`req.coherent-parallelism.manifest-expressed` forbids a silent fallback on a
bad declaration, and `req.unify-connection-budget.pool-sizing-honesty` forbids
handing a caller a pool of a size it did not ask for. Both are assertions about
what happens on the FAILURE path, so most of this file is about refusals.
"""
from __future__ import annotations

import pytest

from okg.substrate.db import concurrency_budget as budget


@pytest.fixture(autouse=True)
def _clean_overrides():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


def test_every_term_has_a_positive_default_and_a_description() -> None:
    terms = budget.declared_terms()
    assert terms, "registry must not be empty"
    for t in terms:
        assert t.default_width >= 1, t.name
        assert t.description.strip(), f"{t.name} has no description"


def test_width_resolves_default_then_override() -> None:
    assert budget.width("ingest.source_layer") == 6
    budget.set_override("ingest.source_layer", 3)
    assert budget.width("ingest.source_layer") == 3


def test_unknown_term_names_the_declared_set() -> None:
    with pytest.raises(budget.BudgetError) as exc:
        budget.width("ingest.does_not_exist")
    assert "ingest.source_layer" in str(exc.value)


@pytest.mark.parametrize("bad", [0, -1, 10_000])
def test_out_of_range_width_refuses_rather_than_clamping(bad: int) -> None:
    """A width of 0 must NOT become 1. Clamping is the shape a
    default-value-as-fallback takes for concurrency knobs."""
    with pytest.raises(budget.BudgetError):
        budget.set_override("ingest.source_layer", bad)
    assert budget.width("ingest.source_layer") == 6


@pytest.mark.parametrize("bad", ["4", 4.0, True, None])
def test_non_int_width_refuses(bad: object) -> None:
    with pytest.raises(budget.BudgetError):
        budget.set_override("ingest.source_layer", bad)  # type: ignore[arg-type]


def test_malformed_env_override_refuses_instead_of_defaulting(monkeypatch) -> None:
    """An unparseable env value must raise, not silently use the default —
    otherwise a typo'd knob looks like it took effect."""
    monkeypatch.setenv("CHRONOS_INTERVAL_GC_WORKERS", "lots")
    with pytest.raises(budget.BudgetError) as exc:
        budget.width("chronos.interval_gc")
    assert "CHRONOS_INTERVAL_GC_WORKERS" in str(exc.value)


def test_env_override_applies_when_well_formed(monkeypatch) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_GC_WORKERS", "3")
    assert budget.width("chronos.interval_gc") == 3


def test_connection_demand_sums_declared_terms() -> None:
    demand = budget.connection_demand()
    expected = sum(v for k, v in demand.items() if k != "_total")
    assert demand["_total"] == expected
    assert demand["_total"] > 0


def test_connection_demand_excludes_mcp_by_default() -> None:
    """The MCP pool belongs to separate processes, so a worker's boot-time sum
    cannot know how many exist. Counting one would be wrong under agent
    concurrency — the honest default is to leave it out of the sum."""
    without = budget.connection_demand()
    with_mcp = budget.connection_demand(include_mcp=True)

    assert "mcp.read_pool" not in without
    assert with_mcp["mcp.read_pool"] == 16
    assert with_mcp["_total"] > without["_total"]


def test_borrower_width_does_not_change_connection_demand() -> None:
    """A borrower occupies connections its pool already owns, so widening it
    must NOT inflate the total.

    Summing a borrower on top of its pool double-counts. That matters because
    the boot admission check is fail-closed: an inflated total refuses
    deployments that actually fit.
    """
    before = budget.connection_demand()["_total"]
    budget.set_override("gate.invariants", 1)
    assert budget.connection_demand()["_total"] == before
    assert "gate.invariants" not in budget.connection_demand()


def test_source_child_terms_are_not_this_process_demand() -> None:
    """A source child's pool lives in the child. The parent admits each child
    against the live server count instead (ingest/source_jobs.py), so neither
    the child pool nor the child count may inflate the parent's boot demand."""
    before = budget.connection_demand()["_total"]
    budget.set_override("ingest.source_process", 5)
    budget.set_override("ingest.source_layer", 2)
    demand = budget.connection_demand()
    assert demand["_total"] == before
    assert "ingest.source_process" not in demand
    assert "ingest.source_layer" not in demand
    assert budget.width("ingest.source_process") == 5


def test_independent_pool_width_does_change_connection_demand() -> None:
    before = budget.connection_demand()["_total"]
    budget.set_override("runtime.dbos_pool", 3)
    assert budget.connection_demand()["_total"] == before - 3


def test_feasible_defaults_pass_the_feasibility_check() -> None:
    budget.check_feasibility()


def test_borrowers_exceeding_their_pool_are_refused() -> None:
    """Each width can be individually legal while their SUM cannot fit. Without
    this check the failure appears as a PoolTimeout mid-publish, attributed to
    whichever lane happened to ask last."""
    budget.set_override("db.shared_pool", 8)
    budget.set_override("gate.invariants", 6)
    budget.set_override("llm.telemetry", 4)

    with pytest.raises(budget.BudgetError) as exc:
        budget.check_feasibility()

    message = str(exc.value)
    assert "db.shared_pool" in message
    assert "gate.invariants=6" in message
    assert "llm.telemetry=4" in message


def test_feasibility_accounts_for_the_publish_reservation() -> None:
    """Borrowers may not consume the whole pool: the publish cycle draws from
    it too, and a PoolTimeout there costs a full cycle."""
    budget.set_override("db.shared_pool", 8)
    budget.set_override("gate.invariants", 6)
    budget.set_override("llm.telemetry", 1)

    with pytest.raises(budget.BudgetError):
        budget.check_feasibility()
