"""Non-triviality witness for the readiness liveness theorem (wave 5).

EventuallyReadyOrParked is a temporal property, so the suite's
named-invariant witness machinery does not apply; this test IS the witness:
it runs real TLC on the no-fairness configuration and asserts the property
is REFUTED there — a lazy worker may stutter forever — proving the fairness
conjunction in the fair Spec is load-bearing rather than decorative.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.formal_tlc import tlc_jvm_args, tlc_workers
from tests.helpers.tlc import TLA_JAR as JAR
from tests.helpers.tlc import TLC_AVAILABLE, assert_tlc_ran, tlc_skip_reason

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "formal" / "source_sync"


@pytest.mark.skipif(not TLC_AVAILABLE, reason=tlc_skip_reason())
def test_liveness_refuted_without_fairness() -> None:
    assert JAR is not None  # narrowed by the TLC_AVAILABLE gate above
    # No -workers, as before: TLC's single-worker default is already
    # machine-friendly; only the heap needed bounding.
    proc = subprocess.run(
        [
            "java", *tlc_jvm_args(tlc_workers()),
            "-cp", str(JAR), "tlc2.TLC", "-deadlock",
            "-config", "ReadinessConvergenceLivenessNoFairness.cfg",
            "ReadinessConvergenceLiveness.tla",
        ],
        cwd=str(MODEL_DIR),
        text=True,
        capture_output=True,
        timeout=180,
    )
    # A jar that never launches produces neither the banner nor the
    # refutation; separate that from a spec that failed to refute.
    assert_tlc_ran(proc)
    assert "Temporal properties were violated" in proc.stdout, (
        "the no-fairness spec must REFUTE EventuallyReadyOrParked — if it "
        "passes, the liveness theorem is satisfiable without any fairness "
        "assumption and proves nothing about the worker:\n"
        + proc.stdout[-800:]
    )
