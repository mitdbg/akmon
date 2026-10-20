"""Unit tests for the read-side percentile helper (bench-okg-internals
task.read-side-perf). The live p50/p95 measurement + the attributable-delta
proof come from a real tinydb run; this locks the percentile math."""
from __future__ import annotations


import okg.bench.orchestrator as orch


def test_percentile_basic():
    vals = [10.0, 20.0, 30.0, 40.0]
    assert orch._percentile(vals, 0.0) == 10.0
    assert orch._percentile(vals, 1.0) == 40.0
    # linear interpolation at p50 of 4 points: between 20 and 30
    assert orch._percentile(vals, 0.5) == 25.0


def test_percentile_p95_interpolates():
    vals = list(range(1, 101))  # 1..100
    # p95 of 1..100 -> 95.05 with (n-1)*p interpolation
    assert abs(orch._percentile([float(v) for v in vals], 0.95) - 95.05) < 1e-9


def test_percentile_edge_cases():
    assert orch._percentile([], 0.5) is None
    assert orch._percentile([7.0], 0.95) == 7.0
