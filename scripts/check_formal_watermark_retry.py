#!/usr/bin/env python3
"""Run the WatermarkRetry liveness TLA+ model with TLC.

Two configs, one invocation, in the house falsifiability idiom (the
qualification-gate checker's shape -- counterexamples are proven in the same
run, no separate flag):

* positive (guarded)      -- GuardConsultsRetryState = TRUE (the P6 engine:
                             the generation-level no-op-reuse fast path
                             refuses to skip the enrichers stage while any
                             enricher's last_status != 'success'). TLC checks
                             the temporal PROPERTY EveryItemEventuallyProcessed
                             under weak fairness on publish cycles; passes.
* stranded counterexample -- GuardConsultsRetryState = FALSE (the pre-P6 dev
                             defect: the fast path skips on source-quiescence
                             alone). The SAME fair spec MUST refute the same
                             property with the stranded-watermark lasso (item
                             fails -> source goes permanently quiescent ->
                             no-op-reuse skips the enrichers stage forever).
                             The produced violation IS the pass condition.

The counterexample is a TEMPORAL-property refutation ("Temporal properties
were violated"), which the suite's DIRECT_COUNTEREXAMPLES registry cannot
express (named_invariant_violated keys on ``Invariant X is violated``), so
this script checks it in-process by driving TLC directly -- the same idiom as
tests/formal/test_readiness_liveness_witness.py.

Exits 0 iff the positive config proves clean (liveness checked, distinct
states at or above the measured floor) AND the counterexample config produces
the temporal violation. Prints distinct-state count and wall time per config.
Both configs run under the shared TLC resource profile (``tlc_workers`` /
``tlc_jvm_args``); see the "Machine resources a run takes" table in
formal/README.md.
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    build_formal_report_rows,
    find_tla2tools_jar,
    run_tlc_formal_report,
    tlc_jvm_args,
    tlc_workers,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "watermark-retry"
DEFAULT_MODEL = MODEL_DIR / "WatermarkRetry.tla"
POSITIVE_CONFIG = MODEL_DIR / "WatermarkRetry.cfg"
COUNTEREXAMPLE_CONFIG = MODEL_DIR / "WatermarkRetryStrandedCounterexample.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_watermark_retry.py "
    "--out .scratch/formal/watermark-retry"
)

# Measured non-vacuity floor: the positive config exhausts to 19 distinct
# states (42 generated, depth 5) at OKG_TLC_WORKERS=4 on the pinned
# tla2tools; the liveness pass covers the complete state space. 16 leaves a
# small headroom while staying far above the DEFAULT_MIN_DISTINCT_STATES=2
# vacuity floor, so a constraint-collapsed state space fails closed with
# failure_class vacuous_state_space. This is a MEASURED vacuity floor, NOT
# the CI timeout budget -- that budget placeholder is set from measurement in
# task.formal.lane-integration.
MIN_DISTINCT_STATES = 16

# The record-and-hold contract the model abstracts: a failed per-item LLM
# call marks the run incomplete, the publisher holds the watermark, and
# last_status != 'success' forces the re-run + full re-read.
WATERMARK_HOLD_TESTS = (
    "tests/substrate/test_enricher_input_failure_holds_watermark.py::"
    "test_incomplete_result_holds_watermark_but_admits_good_outputs",
    "tests/substrate/test_enricher_input_failure_holds_watermark.py::"
    "test_incomplete_status_forces_rerun_success_status_skips",
    "tests/substrate/test_enricher_input_failure_holds_watermark.py::"
    "test_complete_result_advances_watermark",
    "tests/substrate/extraction/test_extractor_llm_failure_watermark.py::"
    "test_llm_failure_marks_run_incomplete_but_keeps_good_outputs",
)
# The generation-level guard the positive config models (P6 of
# specify-publish-commit-protocol): a held enricher retry invalidates
# no-op reuse so the enrichers stage cannot be skipped past it.
REUSE_GUARD_TESTS = (
    "tests/substrate/runtime/test_publish_commit_protocol.py::"
    "test_p6_held_enricher_retry_invalidates_reuse",
    "tests/substrate/runtime/test_publish_commit_protocol.py::"
    "test_p6_never_run_enricher_is_pending",
)

POSITIVE_PROPERTIES = [
    TLCProperty(
        "WatermarkRetryLiveness",
        "TLC verified the eventual-retry liveness property under weak "
        "fairness on publish cycles: with the generation-level no-op-reuse "
        "guard consulting enricher last_status (the P6 engine) and LLM "
        "failure transient, every item whose call failed and whose watermark "
        "was held is eventually retried and processed "
        "(EveryItemEventuallyProcessed), and a held status always stands for "
        "concrete pending inputs.",
        refinement_tests=(*WATERMARK_HOLD_TESTS, *REUSE_GUARD_TESTS),
        expected_invariants=(
            "TypeOK",
            "HeldWatermarkImpliesPendingInput",
        ),
        expected_temporal_properties=("EveryItemEventuallyProcessed",),
    ),
]
COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "WatermarkRetryStrandedCounterexample",
        "TLC produced the stranded-watermark lasso: with the guard removed "
        "(GuardConsultsRetryState = FALSE, the pre-P6 dev defect) an item's "
        "LLM call fails and holds the watermark, the source goes permanently "
        "quiescent, and the no-op-reuse fast path skips the enrichers stage "
        "on every later cycle -- the item is never retried, refuting "
        "EveryItemEventuallyProcessed under the SAME fair spec. The guard, "
        "not the fairness, is the load-bearing difference.",
        refinement_tests=(*REUSE_GUARD_TESTS, *WATERMARK_HOLD_TESTS),
        expected_temporal_properties=("EveryItemEventuallyProcessed",),
    ),
]

_TEMPORAL_VIOLATION = "Temporal properties were violated"
_HARD_ERROR_MARKERS = (
    "Parsing or semantic analysis failed",
    "TLC threw an unexpected exception",
    "Semantic errors:",
)


def _distinct_states(stdout: str) -> int | None:
    matches = re.findall(r"(\d+)\s+distinct states found", stdout)
    return int(matches[-1]) if matches else None


def _report_line(label: str, ok: bool, states: int | None, wall: float) -> str:
    verdict = (
        ("proved clean" if label == "positive" else "counterexample produced")
        if ok
        else "FAILED"
    )
    states_text = (
        f"{states} distinct states" if states is not None
        else "distinct states: unparsed"
    )
    return (
        f"formal-watermark-retry: [{label:14s}] {verdict:24s} "
        f"{states_text:24s} {wall:5.1f}s"
    )


def _run_positive(args: argparse.Namespace, out_dir: Path) -> tuple[list[dict], int]:
    start = time.monotonic()
    result = run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=args.model,
        config=POSITIVE_CONFIG,
        out_dir=out_dir / "positive",
        properties=POSITIVE_PROPERTIES,
        expected_properties=POSITIVE_PROPERTIES,
        checker_command=args.checker_command,
        timeout_seconds=args.timeout_seconds,
        min_distinct_states=MIN_DISTINCT_STATES,
    )
    wall = time.monotonic() - start
    print(
        _report_line("positive", not result.run_failed, result.distinct_states, wall),
        flush=True,
    )
    if result.run_failed:
        print(
            "formal-watermark-retry: [positive] check failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return [], result.exit_code or 1
    return list(result.rows), 0


def _run_stranded_counterexample(
    args: argparse.Namespace, out_dir: Path
) -> tuple[list[dict], int]:
    """Drive TLC directly and require the TEMPORAL violation by phrase.

    ``run_tlc_formal_report(expect_counterexample=True)`` keys on named
    INVARIANT violations and would classify a temporal violation as an
    unexpected error, so the liveness refutation is asserted here the same
    way tests/formal/test_readiness_liveness_witness.py asserts its
    no-fairness refutation.
    """
    jar = find_tla2tools_jar(REPO_ROOT)
    if jar is None:
        raise TLCSetupError(
            "TLC is required but no tla2tools.jar was found. Set "
            "TLA2TOOLS_JAR or run scripts/bootstrap_formal_admission.py."
        )
    java = shutil.which("java")
    if java is None:
        raise TLCSetupError("Java is required to run TLC but was not found on PATH.")
    ce_dir = (out_dir / "stranded").resolve()
    ce_dir.mkdir(parents=True, exist_ok=True)
    metadir = ce_dir / "tlc-meta"
    metadir.mkdir(parents=True, exist_ok=True)
    stdout_path = ce_dir / "tlc.stdout.log"
    stderr_path = ce_dir / "tlc.stderr.log"
    workers = tlc_workers()
    model_path = (
        args.model if args.model.is_absolute() else (MODEL_DIR / args.model).resolve()
    )
    cmd = (
        java,
        *tlc_jvm_args(workers),
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-config",
        str(COUNTEREXAMPLE_CONFIG),
        "-deadlock",
        "-workers",
        workers,
        "-metadir",
        str(metadir),
        str(model_path),
    )
    start = time.monotonic()
    timed_out = False
    try:
        proc = subprocess.run(
            list(cmd),
            cwd=str(MODEL_DIR),
            text=True,
            capture_output=True,
            check=False,
            timeout=args.timeout_seconds,
        )
        stdout, stderr = proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = (
            (exc.stderr if isinstance(exc.stderr, str) else "")
            + f"\nTLC timed out after {args.timeout_seconds} seconds.\n"
        )
    wall = time.monotonic() - start
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")

    states = _distinct_states(stdout)
    hard_errors = [
        marker for marker in _HARD_ERROR_MARKERS if marker in stdout or marker in stderr
    ]
    invariant_violated = "Invariant" in stdout and "is violated" in stdout
    ok = (
        not timed_out
        and _TEMPORAL_VIOLATION in stdout
        and not hard_errors
        # The witness must be the temporal refutation, not a stray safety
        # violation standing in for it.
        and not invariant_violated
    )
    print(_report_line("stranded", ok, states, wall), flush=True)
    if not ok:
        reason = (
            "TLC timed out"
            if timed_out
            else (
                f"hard TLC error(s): {'; '.join(hard_errors)}"
                if hard_errors
                else (
                    "an INVARIANT violation appeared instead of the temporal one"
                    if invariant_violated
                    else f"expected '{_TEMPORAL_VIOLATION}' did not appear"
                )
            )
        )
        print(
            f"formal-watermark-retry: [stranded] {reason}; see {stdout_path}",
            file=sys.stderr,
        )
        return [], 1
    rows = build_formal_report_rows(
        properties=COUNTEREXAMPLE_PROPERTIES,
        status="pass",
        checker_command=args.checker_command,
        command=cmd,
        artifact=stdout_path,
        model_path=model_path,
        config_path=COUNTEREXAMPLE_CONFIG,
        summary_override=(
            "Temporal-property counter-example produced as expected: "
            "EveryItemEventuallyProcessed refuted by the stranded-watermark "
            "lasso (guard off, source permanently quiescent, enrichers stage "
            "skipped forever)."
        ),
        exit_code=0,
        distinct_states=states,
    )
    return rows, 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="base directory for per-config TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/consistency",
    )
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
        help="PACT formal binding checker_command represented by this report",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="maximum seconds per TLC run (generous local default; the gating "
        "CI smoke budget is the suite's smoke_timeout, a placeholder set from "
        "measurement in task.formal.lane-integration)",
    )
    args = parser.parse_args(argv)

    out_dir = args.out.resolve()
    try:
        positive_rows, rc = _run_positive(args, out_dir)
        if rc != 0:
            return rc
        ce_rows, rc = _run_stranded_counterexample(args, out_dir)
        if rc != 0:
            return rc
    except TLCSetupError as exc:
        print(f"formal-watermark-retry: {exc}", file=sys.stderr)
        return exc.exit_code

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump(
            {"results": [*positive_rows, *ce_rows]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print(f"formal-watermark-retry: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
