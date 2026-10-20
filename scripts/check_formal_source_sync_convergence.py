#!/usr/bin/env python3
"""Run the source-sync readiness convergence TLA+ model with TLC."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCReportResult,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "source_sync"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "source-sync-convergence"
DEFAULT_CHECKER_COMMAND = (
    "java -cp .tools/tla2tools.jar tlc2.TLC -config "
    "formal/source_sync/ReadinessConvergence.cfg -workers auto "
    "formal/source_sync/ReadinessConvergence.tla"
)
DEFAULT_REPORT_PROPERTY_IDS = ("FrontierAdvanceResumeIsSafe",)
COUNTEREXAMPLE_MODEL = Path("ReadinessConvergenceCounterexample.tla")
NONVACUITY_CHECKS = (
    (
        "ReadyWitness",
        Path("ReadinessConvergenceReadyCounterexample.cfg"),
        TLCProperty(
            "source-sync.ready-witness-reachable",
            "TLC produced the expected counterexample proving ReadyWitness is reachable.",
            refinement_tests=(
                "tests/substrate/source_sync/test_frontier_resume.py::"
                "test_frontier_ready_after_watermark_publish_and_mcp_visibility",
            ),
            expected_invariants=("ReadyWitnessNeverOccurs",),
        ),
    ),
    (
        "StuckWitness",
        Path("ReadinessConvergenceStuckCounterexample.cfg"),
        TLCProperty(
            "source-sync.stuck-witness-reachable",
            "TLC produced the expected counterexample proving StuckWitness is reachable.",
            refinement_tests=(
                "tests/substrate/test_source_sync_convergence_tracking.py::"
                "test_stuck_recovery_is_reported_as_blocked",
            ),
            expected_invariants=("StuckWitnessNeverOccurs",),
        ),
    ),
    (
        "McpBlockedWitness",
        Path("ReadinessConvergenceMcpBlockedCounterexample.cfg"),
        TLCProperty(
            "source-sync.mcp-blocked-witness-reachable",
            "TLC produced the expected counterexample proving published-without-MCP is blocked and reachable.",
            refinement_tests=(
                "tests/substrate/source_sync/test_frontier_resume.py::"
                "test_published_generation_without_mcp_visibility_is_not_ready",
            ),
            expected_invariants=("McpBlockedWitnessNeverOccurs",),
        ),
    ),
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "NoFalseReadyFromIncompleteWork": [
        "tests/substrate/test_source_sync_convergence_state.py::" +
        "test_scope_incomplete_does_not_satisfy_completed_freshness",
    ],
    "EventualReadyRequiresCompletedFreshnessAndPublishedGeneration": [
        "tests/substrate/test_source_sync_convergence_state.py::" +
        "test_ready_requires_watermark_and_published_generation",
    ],
    "FrontierAdvanceResumeIsSafe": [
        "tests/substrate/source_sync/test_frontier_resume.py::" +
        "test_frontier_ready_after_watermark_publish_and_mcp_visibility",
    ],
    "FreshWatermarkRequiresPublishAndMcpVisibility": [
        "tests/substrate/test_source_sync_convergence_state.py::" +
        "test_ready_requires_watermark_and_published_generation",
        "tests/substrate/source_sync/test_frontier_resume.py::" +
        "test_fresh_watermark_without_publish_is_not_ready",
        "tests/substrate/source_sync/test_frontier_resume.py::" +
        "test_published_generation_without_mcp_visibility_is_not_ready",
    ],
    "NoHiddenStuckRecoveryWork": [
        "tests/substrate/test_source_sync_convergence_tracking.py::" +
        "test_stuck_recovery_is_reported_as_blocked",
    ],
    "SourceSyncRecoveryBounded": [
        "tests/substrate/test_source_sync_convergence_tracking.py::" +
        "test_stuck_recovery_is_reported_as_blocked",
        "tests/substrate/test_source_sync_liveness.py::" +
        "test_dbos_queue_status_treats_started_pending_as_running",
        "tests/substrate/test_source_sync_liveness.py::" +
        "test_dbos_queue_status_does_not_flag_priority_waiting_as_replaceable",
    ],
}
PROPERTIES = [
    TLCProperty(
        "NoFalseReadyFromIncompleteWork",
        "TLC verified incomplete work cannot imply readiness.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["NoFalseReadyFromIncompleteWork"]
        ),
    ),
    TLCProperty(
        "EventualReadyRequiresCompletedFreshnessAndPublishedGeneration",
        "TLC verified ready requires completed freshness and graph visibility.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY[
                "EventualReadyRequiresCompletedFreshnessAndPublishedGeneration"
            ]
        ),
    ),
    TLCProperty(
        "FrontierAdvanceResumeIsSafe",
        "TLC verified frontier advancement and resume is safe.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["FrontierAdvanceResumeIsSafe"]
        ),
    ),
    TLCProperty(
        "FreshWatermarkRequiresPublishAndMcpVisibility",
        "TLC verified fresh completed-scope watermarks require publish and MCP visibility before readiness.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY[
                "FreshWatermarkRequiresPublishAndMcpVisibility"
            ]
        ),
        expected_invariants=(
            "FreshWatermarkRequiresPublishAndMcpVisibility",
            "McpVisibilityRequiresPublishedGeneration",
        ),
    ),
    TLCProperty(
        "NoHiddenStuckRecoveryWork",
        "TLC verified stale completed-scope recovery is surfaced as stuck.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["NoHiddenStuckRecoveryWork"]
        ),
    ),
    TLCProperty(
        "SourceSyncRecoveryBounded",
        "TLC verified stale source-sync recovery is stuck with a concrete recovery action, not observe-only.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["SourceSyncRecoveryBounded"]
        ),
    ),
]


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
        default=MODEL_DIR / "ReadinessConvergence.tla",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "ReadinessConvergence.cfg",
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
        default=300,
        help="maximum seconds to allow TLC to run",
    )
    args = parser.parse_args(argv)

    try:
        nonvacuity_results = _run_nonvacuity_counterexamples(
            args.out,
            args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(
            f"formal-source-sync-convergence: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    failed_nonvacuity = [
        result for result in nonvacuity_results
        if result.run_failed
    ]
    if failed_nonvacuity:
        print(
            "formal-source-sync-convergence: non-vacuity counterexample failed; see "
            f"{failed_nonvacuity[0].stdout_path} and "
            f"{failed_nonvacuity[0].stderr_path}",
            file=sys.stderr,
        )
        return failed_nonvacuity[0].exit_code or 1
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=args.out,
            properties=select_properties(
                PROPERTIES,
                args.property_ids or DEFAULT_REPORT_PROPERTY_IDS,
            ),
            expected_properties=PROPERTIES,
            checker_command=args.checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(
            f"formal-source-sync-convergence: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-source-sync-convergence: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    _append_nonvacuity_summary(result.stdout_path, nonvacuity_results)
    print(
        "formal-source-sync-convergence: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


def _run_nonvacuity_counterexamples(
    out_dir: Path,
    timeout_seconds: int,
) -> tuple[TLCReportResult, ...]:
    results = []
    for witness_name, config, prop in NONVACUITY_CHECKS:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=COUNTEREXAMPLE_MODEL,
            config=config,
            out_dir=out_dir / f"nonvacuity-{witness_name}",
            properties=[prop],
            expected_properties=[prop],
            checker_command=_counterexample_checker_command(config),
            expect_counterexample=True,
            timeout_seconds=timeout_seconds,
        )
        results.append(result)
    return tuple(results)


def _counterexample_checker_command(config: Path) -> str:
    return (
        "java -cp .tools/tla2tools.jar tlc2.TLC -config "
        f"formal/source_sync/{config.name} -workers auto "
        "formal/source_sync/ReadinessConvergenceCounterexample.tla"
    )


def _append_nonvacuity_summary(
    stdout_path: Path,
    results: tuple[TLCReportResult, ...],
) -> None:
    lines = [
        "",
        "Non-vacuity witnesses:",
        *[
            "- "
            + result.rows[0]["property_id"]
            + ": TLC violated "
            + ", ".join(result.violated_invariants)
            + f"; artifact={result.stdout_path}"
            for result in results
        ],
    ]
    with stdout_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
