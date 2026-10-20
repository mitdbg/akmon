"""Sanity tests for the standardized sweep definition (bench-okg-internals
task.standardized-sweep-proof). The end-to-end run + reproducibility are proven
by a real double-invocation of `orchestrator.py sweep`; these guard the sweep
config against manifest drift."""
from __future__ import annotations


import okg.bench.orchestrator as orch


def test_sweep_cells_are_tinydb_lab_cells():
    cells = orch._sweep_cells()
    ids = {e.id for e in cells}
    # the config-axes lab: enricher sweep, chronos sweep, branching cell
    for expect in ("tdb-enr-en0", "tdb-enr-enall", "tdb-chr-50", "tdb-chr-95",
                   orch.SWEEP_BRANCHING_CELL):
        assert expect in ids, f"{expect} missing from sweep cells {ids}"
    assert all(e.id.startswith(orch.SWEEP_PREFIX) for e in cells)
    assert all(e.repo.name == "tinydb" for e in cells)


def test_comparison_groups_resolve_to_multiple_cells():
    cells = orch._sweep_cells()
    for group in orch.SWEEP_COMPARISON_GROUPS:
        members = [e for e in cells if e.id.startswith(f"{group}-")]
        assert len(members) >= 2, f"{group} needs >=2 cells to compare"


def test_sweep_cells_use_reproducible_first_n_strategy():
    # first_n is root-anchored (NOT HEAD-relative), so the commit list is pinned.
    for e in orch._sweep_cells():
        assert e.replay.strategy == "first_n", (
            f"{e.id} uses {e.replay.strategy}; sweep cells must use the "
            "root-anchored first_n strategy for reproducibility")
