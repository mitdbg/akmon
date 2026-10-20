"""PACT friction benchmark.

A replayable, quantitative instrument for PACT itself: six metric families
computed from one frozen fixture corpus, with no live DSN and no graph. The
harness MEASURES today's behavior — it deliberately does not fix it, so the
baseline it freezes is an honest pre-fix reading.

Entry: ``uv run pytest tests/pact/bench -q``, or
``uv run python -m okg.substrate.pact.bench`` to print the report.
"""
from __future__ import annotations

from okg.substrate.pact.bench.isolation import (
    IsolationRefusal,
    denied_environment,
    require_clean_environment,
    scrub_environment,
)
from okg.substrate.pact.bench.ratchet import compare, load_baseline
from okg.substrate.pact.bench.report import SCHEMA, emit, human_table
from okg.substrate.pact.bench.runner import BenchResult, run_benchmark

BASELINE_PATH = "tests/pact/bench/baseline/metrics.json"

__all__ = [
    "BASELINE_PATH",
    "BenchResult",
    "IsolationRefusal",
    "SCHEMA",
    "compare",
    "denied_environment",
    "emit",
    "human_table",
    "load_baseline",
    "require_clean_environment",
    "run_benchmark",
    "scrub_environment",
]
