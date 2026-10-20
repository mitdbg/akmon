#!/usr/bin/env python3
"""Run the PACT workflow-authority TLA+ model with TLC."""
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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "workflow-authority"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_workflow_authority.py "
    "--out .scratch/formal/workflow-authority"
)
REFINEMENT_TESTS = (
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_refuses_stale_or_unpinned_surfaces",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_requires_git_evidence_for_code_claims",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_requires_explicit_freshness_surfaces",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_git_evidence_must_be_live_checked",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_does_not_invent_unclaimed_graph_authority",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_replaces_default_graph_projection",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_requires_allowed_foreign_checkout",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_refuses_stale_surfaces",
)
DEFAULT_REPORT_PROPERTY_IDS = ("WorkflowAuthorityContract",)
PROPERTIES = [
    TLCProperty(
        "RuntimeDecisionMatchesBlockers",
        "TLC verified runtime acceptance is derived from the explicit blocker set.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "BlockersReflectStaleSurfaces",
        "TLC verified runtime blockers reflect stale authority surfaces.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "LiveChecksReflectStaleSurfaces",
        "TLC verified required live checks reflect stale authority surfaces.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "FreshAuthorityCanAuthorize",
        "TLC verified a fully current authority packet can authorize progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoStaleFileAuthority",
        "TLC verified stale file-canonical state cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoStaleGraphAuthority",
        "TLC verified stale or unavailable graph projection cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoStaleLedgerAuthority",
        "TLC verified stale decision-ledger state cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoUnapprovedLedgerAuthority",
        "TLC verified a read ledger with no approval binding the current intent "
        "digest cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoUnarchivedApprovalAuthority",
        "TLC verified a current approval cannot authorize workflow progress "
        "while the change is unarchived.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoMismatchedGenerationAuthority",
        "TLC verified generation-pin mismatch cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoStaleEvidenceAuthority",
        "TLC verified stale evidence artifacts cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoCoordinationDisagreementAuthority",
        "TLC verified PACT/coordination disagreement cannot authorize workflow progress.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "CodeClaimsRequireGitEvidence",
        "TLC verified code claims require live git evidence.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "ReviewMergeTaskDoneRequireCurrentAuthority",
        "TLC verified review, merge, and task completion require current authority.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "WorkflowAuthorityContract",
        "TLC verified the combined workflow authority staleness contract.",
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
        default=MODEL_DIR / "WorkflowAuthority.tla",
        help="TLA+ module path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "WorkflowAuthority.cfg",
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
        print(f"formal-workflow-authority: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-workflow-authority: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-workflow-authority: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
