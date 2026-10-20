#!/usr/bin/env python3
"""Run the QualificationGate blocking-policy TLA+ model with TLC.

Four configs, one invocation, in the house falsifiability idiom:

* smoke (never)          -- sanity invariants hold; passes.
* standing (any_open)    -- the standing qualification invariant
                            NoPublishWhileMandatoryOpen holds exhaustively; passes.
* one-shot exhibit       -- under new_findings the SAME invariant is VIOLATED by
                            the one-shot-mandatory ratchet; the produced
                            counterexample IS the pass condition.
* standing counterexample-- a gate that consults the newly-opened count instead
                            of the open count degrades any_open into the ratchet
                            and violates the standing invariant (falsifiability).

Exits 0 iff the two positive configs prove clean AND the two counterexample
configs each produce their named violation. Prints distinct-state count and
wall time per config. OKG_TLC_WORKERS caps TLC workers (set =4 locally).
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCReportResult,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "qualification-gate"
DEFAULT_MODEL = MODEL_DIR / "QualificationGate.tla"
SMOKE_CONFIG = MODEL_DIR / "QualificationGateSmoke.cfg"
STANDING_CONFIG = MODEL_DIR / "QualificationGateStanding.cfg"
EXHIBIT_CONFIG = MODEL_DIR / "QualificationGateOneShotExhibit.cfg"
COUNTEREXAMPLE_CONFIG = MODEL_DIR / "QualificationGateStandingCounterexample.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_qualification_gate.py "
    "--out .scratch/formal/qualification-gate"
)

# Measured non-vacuity floors for the positive configs (159 distinct states
# each on the delivered scope; a constraint-collapsed model falls below).
STANDING_MIN_DISTINCT_STATES = 140
SMOKE_MIN_DISTINCT_STATES = 140

# Refinement tests: the pytest witnesses that drive the SAME finding-blocking
# decision through the real gate (evaluate_consistency_publish_gate) and the
# section-9.10 mandatory facet (finding_is_mandatory).
GATE_POLICY_TESTS = (
    "tests/substrate/test_consistency_gates.py::"
    "test_block_on_any_open_reproduces_fail_and_block_byte_for_byte",
    "tests/substrate/test_consistency_gates.py::"
    "test_block_on_never_does_not_block_open_findings",
    "tests/substrate/consistency/test_qualification_mapping.py::"
    "test_finding_is_mandatory_truth_table",
    "tests/substrate/consistency/test_qualification_mapping.py::"
    "test_open_finding_on_blocking_check_is_mandatory_and_blocks",
    "tests/substrate/consistency/test_qualification_mapping.py::"
    "test_resolving_the_mandatory_finding_unblocks_publish",
)
RATCHET_TESTS = (
    "tests/substrate/test_consistency_gates.py::"
    "test_block_on_new_findings_passes_pre_existing_backlog",
    "tests/substrate/test_consistency_gates.py::"
    "test_backlog_has_zero_newly_opened_and_resolution_is_counted",
    "tests/substrate/consistency/test_qualification_mapping.py::"
    "test_backlog_under_new_findings_is_not_mandatory_and_does_not_block",
)

SMOKE_PROPERTIES = [
    TLCProperty(
        "QualificationGateSmokeSanity",
        "TLC verified the never policy never blocks on findings, a force "
        "override always blocks, and an advisory override always publishes, "
        "over the finding lifecycle x deployment override scope.",
        refinement_tests=GATE_POLICY_TESTS,
        expected_invariants=(
            "TypeOK",
            "NeverPolicyNeverBlocksOnFindings",
            "OverrideForceBlocksWhenDecided",
            "OverrideAdvisoryPublishesWhenDecided",
        ),
    ),
]
STANDING_PROPERTIES = [
    TLCProperty(
        "QualificationGateStandingQualification",
        "TLC verified that under block_on=any_open no generation publishes "
        "while a mandatory (floored-open) finding stands -- Concord's standing "
        "qualification predicate -- exhaustively over the finding lifecycle x "
        "deployment override x mixed-severity scope.",
        refinement_tests=GATE_POLICY_TESTS,
        expected_invariants=(
            "TypeOK",
            "NoPublishWhileMandatoryOpen",
            "OverrideForceBlocksWhenDecided",
            "OverrideAdvisoryPublishesWhenDecided",
        ),
    ),
]
EXHIBIT_PROPERTIES = [
    TLCProperty(
        "QualificationGateOneShotMandatoryRatchet",
        "TLC produced the one-shot-mandatory ratchet counterexample: under "
        "block_on=new_findings a mandatory finding opens (blocks, newly "
        "observed) then, on the next attempt with no re-observed event "
        "(bounded-event-growth), publishes over the same open finding -- "
        "refuting NoPublishWhileMandatoryOpen. Only any_open implements the "
        "standing qualification.",
        refinement_tests=RATCHET_TESTS,
        expected_invariants=("NoPublishWhileMandatoryOpen",),
    ),
]
COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "QualificationGateStandingWrongCountCounterexample",
        "TLC produced the expected counterexample: an any_open gate that "
        "consults the newly-opened count instead of the open count degrades "
        "into the one-shot ratchet and publishes over a still-open mandatory "
        "finding, refuting NoPublishWhileMandatoryOpen by name -- proving the "
        "open-count consultation is load-bearing.",
        refinement_tests=(
            *RATCHET_TESTS,
            "tests/substrate/test_consistency_gates.py::"
            "test_block_on_any_open_reproduces_fail_and_block_byte_for_byte",
        ),
        expected_invariants=("NoPublishWhileMandatoryOpen",),
    ),
]


def _run(
    *,
    config: Path,
    out_sub: Path,
    properties,
    checker_command: str,
    timeout_seconds: int,
    expect_counterexample: bool,
    min_distinct_states: int | None,
) -> TLCReportResult:
    return run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=DEFAULT_MODEL,
        config=config,
        out_dir=out_sub,
        properties=properties,
        expected_properties=properties,
        checker_command=checker_command,
        expect_counterexample=expect_counterexample,
        timeout_seconds=timeout_seconds,
        min_distinct_states=min_distinct_states,
    )


def _report_line(label: str, result: TLCReportResult, elapsed_s: float) -> str:
    states = (
        f"{result.distinct_states} distinct states"
        if result.distinct_states is not None
        else "distinct states: unparsed"
    )
    if result.run_failed:
        verdict = "FAILED"
    elif label in ("exhibit", "counterexample"):
        verdict = "counterexample produced"
    else:
        verdict = "proved clean"
    return (
        f"formal-qualification-gate: [{label:14s}] {verdict:24s} "
        f"{states:24s} {elapsed_s:5.1f}s"
    )


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
        help="maximum seconds per TLC run",
    )
    args = parser.parse_args(argv)

    out_dir = args.out.resolve()
    plan = (
        ("smoke", SMOKE_CONFIG, SMOKE_PROPERTIES, False, SMOKE_MIN_DISTINCT_STATES),
        ("standing", STANDING_CONFIG, STANDING_PROPERTIES, False,
         STANDING_MIN_DISTINCT_STATES),
        ("exhibit", EXHIBIT_CONFIG, EXHIBIT_PROPERTIES, True, None),
        ("counterexample", COUNTEREXAMPLE_CONFIG, COUNTEREXAMPLE_PROPERTIES, True,
         None),
    )

    results: list[tuple[str, TLCReportResult]] = []
    try:
        for label, config, properties, expect_ce, floor in plan:
            start = time.monotonic()
            result = _run(
                config=config,
                out_sub=out_dir / label,
                properties=properties,
                checker_command=args.checker_command,
                timeout_seconds=args.timeout_seconds,
                expect_counterexample=expect_ce,
                min_distinct_states=floor,
            )
            elapsed = time.monotonic() - start
            print(_report_line(label, result, elapsed), flush=True)
            results.append((label, result))
    except TLCSetupError as exc:
        print(f"formal-qualification-gate: {exc}", file=sys.stderr)
        return exc.exit_code

    failed = [(label, res) for label, res in results if res.run_failed]
    if failed:
        for label, res in failed:
            print(
                f"formal-qualification-gate: [{label}] check failed; see "
                f"{res.stdout_path} and {res.stderr_path}",
                file=sys.stderr,
            )
        return next(res.exit_code for _, res in failed if res.exit_code) or 1

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump(
            {"results": [row for _, res in results for row in res.rows]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print(f"formal-qualification-gate: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
