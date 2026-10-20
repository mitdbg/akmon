#!/usr/bin/env python3
"""Run the PACT v4 decision-ledger merge-boundary TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "pact"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "pact-v4-decision-ledger"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_pact_v4_decision_ledger.py "
    "--out .scratch/formal/pact-v4-decision-ledger"
)
REFINEMENT_TESTS = (
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_without_any_decision",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_passes_with_matching_approval",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_pre_merge_gate_accepts_legacy_archived_approval_without_v4_lifecycle_fields",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_when_intent_changed_after_approval",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_matching_approval_after_reopen",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_matching_approval_when_manifest_is_active",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_when_ledger_unreachable",
)
DEFAULT_REPORT_PROPERTY_IDS = ("PactV4DecisionLedgerContract",)
PROPERTIES = [
    TLCProperty(
        "LedgerRowsWellFormed",
        "TLC verified bounded decision-ledger rows stay well formed.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "RuntimeDecisionMatchesStatus",
        "TLC verified merge acceptance matches the runtime status.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "BlockersReflectDecisionRows",
        "TLC verified refusal blockers reflect decision-ledger row state.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMergeWithoutReachableLedger",
        "TLC verified an unreachable decision ledger cannot authorize merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMergeWithoutCurrentApproval",
        "TLC verified missing current-digest approval cannot authorize merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMergeWithStaleDigest",
        "TLC verified stale approval digests cannot authorize merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMergeAfterReopen",
        "TLC verified a later reopen invalidates the previous approval for merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMergeWithActiveFileState",
        "TLC verified active stale file states cannot authorize merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "OnlyLatestTerminalApprovalCanMerge",
        "TLC verified merge requires the latest terminal review approval.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "ArchivedCurrentApprovalCanMerge",
        "TLC verified the positive archived-current-approval witness can merge.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "PactV4DecisionLedgerContract",
        "TLC verified the combined PACT v4 decision-ledger merge contract.",
        refinement_tests=REFINEMENT_TESTS,
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
        default=MODEL_DIR / "PactV4DecisionLedger.tla",
        help="TLA+ module path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "PactV4DecisionLedger.cfg",
        help="TLC config path; relative paths resolve under formal/pact",
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
        print(f"formal-pact-v4-decision-ledger: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-pact-v4-decision-ledger: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-pact-v4-decision-ledger: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
