#!/usr/bin/env python3
"""Run the DbosCommitResume publish commit/resume TLA+ model with TLC.

Positive run (the default, and what the formal suite's smoke tier invokes):
drives the exhaustive small-scope ``DbosCommitResume.cfg`` -- two successive
publish cycles, per-cycle delta inputs ranged, a crash allowed at every step
boundary with resume honoring the durable DBOS step markers -- and asserts the
three crash-atomicity invariants (no partial generation observable, no
double-commit, no certificate without its checkpoint), emitting a strict
``formal-report.yaml``. ``--counterexamples`` additionally drives each named
counterexample config and asserts it refutes exactly its named invariant --
the self-contained local falsifiability proof (``scripts/run_formal_suite.py``
also runs these three configs via its DIRECT_COUNTEREXAMPLES registry). State
counts and wall time are printed for every config.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "dbos-commit"
DEFAULT_MODEL = MODEL_DIR / "DbosCommitResume.tla"
DEFAULT_CONFIG = MODEL_DIR / "DbosCommitResume.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_dbos_commit.py "
    "--out .scratch/formal/dbos-commit"
)

# Measured non-vacuity floor: the positive config exhausts to 2944 distinct
# states (OKG_TLC_WORKERS=4, pinned tla2tools). 2800 leaves a small headroom
# while staying far above the DEFAULT_MIN_DISTINCT_STATES=2 vacuity floor, so a
# constraint-collapsed state space fails closed with failure_class
# vacuous_state_space. This is a MEASURED vacuity floor, NOT the CI timeout
# budget -- that budget placeholder is set from measurement in
# task.formal.lane-integration.
MIN_DISTINCT_STATES = 2800

# Real crash/commit-protocol tests that drive the same decision surfaces the
# model abstracts (bound on each result row; the folder README's maps expand
# the per-invariant bindings). The crash-mid-update witness is the single-trace
# empirical anchor this model generalizes over every interleaving in scope.
REFINEMENT_TESTS = (
    "tests/substrate/consistency/test_reuse_scheduler.py::"
    "test_crash_mid_update_resumes_or_discards",
    "tests/substrate/consistency/test_reuse_scheduler.py::"
    "test_operator_failure_aborts_leaving_prior_state_intact",
    "tests/substrate/consistency/test_reuse_scheduler.py::"
    "test_discarded_pass_writes_zero_certificates",
    "tests/substrate/consistency/test_reuse_scheduler.py::"
    "test_commit_refused_before_fixed_point",
    "tests/substrate/consistency/test_reuse_scheduler.py::"
    "test_reuse_pass_registers_as_a_dbos_step",
)

PROPERTIES = [
    TLCProperty(
        "DbosCommitResumeSoundness",
        "TLC verified the publish commit/resume protocol under the DBOS "
        "workflow: across every crash/resume interleaving of two publish "
        "cycles, the visible generation pointer only ever reaches a "
        "fully-committed generation, no generation is committed twice with "
        "different content (effects are transactional and "
        "input-deterministic), and no durable certificate references a "
        "generation whose checkpoint never committed -- because the commit "
        "set lands in one transaction and a step's durability marker becomes "
        "durable no earlier than its effects.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "NoPartialGenerationObservable",
            "NoDoubleCommit",
            "NoCertWithoutCheckpoint",
            "DbosCommitResumeSoundness",
        ),
    ),
]

# Each named counterexample enables exactly one hazard toggle and MUST refute
# exactly one invariant by name (TLC halts at the first violation).
# run_formal_suite.py also runs these via DIRECT_COUNTEREXAMPLES;
# --counterexamples here is the self-contained local proof.
COUNTEREXAMPLES: tuple[tuple[str, str], ...] = (
    ("DbosCommitResumeMarkerFirstCounterexample.cfg",
     "NoPartialGenerationObservable"),
    ("DbosCommitResumeSplitCommitCounterexample.cfg",
     "NoCertWithoutCheckpoint"),
    ("DbosCommitResumeNonIdempotentCounterexample.cfg",
     "NoDoubleCommit"),
)


def _run_positive(args: argparse.Namespace) -> int:
    start = time.monotonic()
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=args.out,
            properties=select_properties(PROPERTIES, args.property_ids),
            expected_properties=PROPERTIES,
            checker_command=args.checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=MIN_DISTINCT_STATES,
        )
    except TLCSetupError as exc:
        print(f"formal-dbos-commit: {exc}", file=sys.stderr)
        return exc.exit_code
    wall = time.monotonic() - start
    if result.run_failed:
        print(
            "formal-dbos-commit: TLC failed "
            f"(distinct_states={result.distinct_states}, {wall:.1f}s); see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-dbos-commit: TLC passed "
        f"(distinct_states={result.distinct_states}, floor={MIN_DISTINCT_STATES}, "
        f"{wall:.1f}s); report at {result.report_path}"
    )
    return 0


def _run_counterexamples(args: argparse.Namespace) -> int:
    ok = True
    for config_name, invariant in COUNTEREXAMPLES:
        prop = TLCProperty(
            invariant,
            f"{invariant} is genuinely violable when its guard is disabled.",
            expected_invariants=(invariant,),
        )
        out_dir = args.out / "counterexamples" / Path(config_name).stem
        start = time.monotonic()
        try:
            result = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=args.model,
                config=MODEL_DIR / config_name,
                out_dir=out_dir,
                properties=[prop],
                expected_properties=[prop],
                checker_command=args.checker_command,
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
                write_report=False,
            )
        except TLCSetupError as exc:
            print(f"formal-dbos-commit: {exc}", file=sys.stderr)
            return exc.exit_code
        wall = time.monotonic() - start
        if result.run_failed:
            ok = False
            print(
                f"  [FAIL] {config_name}: expected {invariant} violated but got "
                f"{list(result.violated_invariants) or 'no violation'} "
                f"({wall:.1f}s); see {result.stdout_path}",
                file=sys.stderr,
            )
        else:
            print(
                f"  [OK]   {config_name}: {invariant} violated as expected "
                f"({wall:.1f}s)"
            )
    if not ok:
        print("formal-dbos-commit: counterexample proof FAILED", file=sys.stderr)
        return 1
    print(
        "formal-dbos-commit: all three counterexamples refuted their named "
        "invariant"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="directory for TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/consistency",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="TLC config path; relative paths resolve under formal/consistency",
    )
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
        help="PACT formal binding checker_command represented by this report",
    )
    parser.add_argument(
        "--refinement-test",
        action="append",
        default=[],
        dest="extra_refinement_tests",
        help="additional refinement test ref included on each result row",
    )
    parser.add_argument(
        "--property-id",
        action="append",
        default=[],
        dest="property_ids",
        help="property_id row to emit; repeat to emit multiple bound rows",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="maximum seconds to allow TLC to run (generous local default; the "
        "gating CI smoke budget is the suite's smoke_timeout, a placeholder set "
        "from measurement in task.formal.lane-integration)",
    )
    parser.add_argument(
        "--counterexamples",
        action="store_true",
        help="also drive each named counterexample config and assert it refutes "
        "its named invariant (self-contained falsifiability proof)",
    )
    args = parser.parse_args(argv)

    rc = _run_positive(args)
    if rc == 0 and args.counterexamples:
        rc = _run_counterexamples(args)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
