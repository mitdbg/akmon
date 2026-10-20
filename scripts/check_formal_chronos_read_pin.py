#!/usr/bin/env python3
"""Run the ChronosReadPin TLA+ model with TLC."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "chronos"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "chronos-read-pin"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_chronos_read_pin.py "
    "--out .scratch/formal/chronos-read-pin"
)
REFINEMENT_TESTS = (
    "tests/substrate/test_chronos_graph_store.py::"
    "test_mcp_session_pin_stays_on_original_chronos_generation_after_later_publish",
    "tests/substrate/mcp/test_v_source_runs_readability.py::"
    "test_operational_run_sql_refuses_string_chronos_pin_without_silent_zero_fallback",
    "tests/substrate/mcp/test_v_source_runs_readability.py::"
    "test_mcp_query_default_session_preserves_compat_pin_for_operational_view",
    "tests/substrate/mcp/test_run_sql_dispatcher.py::"
    "test_run_sql_graph_join_uses_one_checkout_for_nodes_and_edges",
)
PROPERTIES = [
    TLCProperty(
        "ChronosReadPinSafety",
        "TLC verified Chronos checkpoint publication, frozen session pins, "
        "pinned graph reads, checkpoint-to-session compatibility pin "
        "preservation, and non-graph SQL pin rejection without silent defaults.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "PinnedOnlyPublishedOkgGeneration",
            "SessionPinFrozenAcrossPublish",
            "LatestAdvanceDoesNotMutateOpenSession",
            "GraphReadUsesPinnedGeneration",
            "NoMixedGenerationObservation",
            "SessionPreservesCheckpointCompat",
            "OperationalSqlNeverDefaultsGeneration",
            "OperationalSqlRequiresSessionCompatOrRejects",
            "OperationalSqlUsesOnlyPreservedCompat",
            "OperationalSqlRejectsMissingSessionCompat",
            "NoUnpinnedRead",
            "ChronosReadPinSafety",
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
        default=MODEL_DIR / "ChronosReadPin.tla",
        help="TLA+ module path; relative paths resolve under formal/chronos",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "ChronosReadPin.cfg",
        help="TLC config path; relative paths resolve under formal/chronos",
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
        help="maximum seconds to allow TLC to run",
    )
    args = parser.parse_args(argv)

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
        )
    except TLCSetupError as exc:
        print(
            f"formal-chronos-read-pin: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-chronos-read-pin: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-chronos-read-pin: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
