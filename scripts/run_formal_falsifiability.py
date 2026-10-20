#!/usr/bin/env python3
"""Falsifiability harness for the formal suite's named counterexamples.

Every invariant and liveness property the differential-core models claim
(PACT ``formalize-differential-core``) must be *falsifiable by name*: a
committed counterexample config exists beside each spec, and running it
must produce exactly its named violation. A counterexample that stops
firing means the model (or the suite plumbing) rotted — this harness turns
that into a hard failure instead of silent staleness.

Two mechanisms, both exercised:

1. ``run_formal_suite.py --mode counterexamples`` — every registered
   ``DIRECT_COUNTEREXAMPLES`` row (all models, not only the new ones) must
   refute its named invariant.
2. The in-process self-proving checkers, whose counterexamples are
   temporal-property refutations or exhibit-style traces the suite's
   named-invariant witness path cannot classify: each is invoked so its
   own counterexample assertions run.

Exit 0 iff every named falsification fired. Runs are TLC-backed; keep
``OKG_TLC_WORKERS`` at or below 4 locally (standing rule).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Checkers whose counterexamples are proven in-process (temporal
# refutations / exhibit traces) rather than via DIRECT_COUNTEREXAMPLES.
SELF_PROVING = (
    ("scripts/check_formal_enricher_backlog_drain.py", ()),
    ("scripts/check_formal_watermark_retry.py", ()),
    ("scripts/check_formal_watermark_quarantine.py", ()),
    ("scripts/check_formal_qualification_gate.py", ()),
    ("scripts/check_formal_reuse_pass.py", ("--counterexamples",)),
    ("scripts/check_formal_dbos_commit.py", ("--counterexamples",)),
)


def _run(argv: list[str]) -> int:
    print(f"falsifiability: $ {' '.join(argv)}", flush=True)
    return subprocess.call(argv, cwd=REPO)


def main() -> int:
    env_workers = os.environ.get("OKG_TLC_WORKERS")
    if env_workers is None:
        os.environ["OKG_TLC_WORKERS"] = "4"
    failures: list[str] = []

    rc = _run([sys.executable, "scripts/run_formal_suite.py",
               "--mode", "counterexamples"])
    if rc != 0:
        failures.append("run_formal_suite --mode counterexamples")

    for script, extra in SELF_PROVING:
        rc = _run([sys.executable, script, *extra])
        if rc != 0:
            failures.append(script)

    if failures:
        print("falsifiability: FAILED — a named counterexample did not fire "
              f"(or a positive model broke): {failures}", flush=True)
        return 1
    print("falsifiability: every named counterexample refuted its "
          "invariant/property", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
