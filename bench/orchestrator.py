#!/usr/bin/env python3
"""Thin launcher: the perf-lab harness ships as okg.bench.orchestrator
(PACT ship-okg-bench); this shim keeps the repo-root invocation
`python bench/orchestrator.py ...` working (CI's nightly bench-correctness
job runs its `run` and `gate` commands; PACT nightly-drop-bench-internals
deleted the bench-internals job).
Lab state stays in this directory (OKG_BENCH_ROOT overrides)."""

import os
import sys
from pathlib import Path

# Preserve the historical __file__-anchored behavior: this launcher's own
# directory is the bench root, whatever the cwd.
os.environ.setdefault("OKG_BENCH_ROOT", str(Path(__file__).resolve().parent))

from okg.bench.orchestrator import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
