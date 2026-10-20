#!/usr/bin/env python3
"""Model-check issue #801 admission, trigger, and reaper safety."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "agent_operator"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "agent-operator"
DEFAULT_COMMAND = (
    "uv run --extra dev python "
    "scripts/check_formal_agent_operator_ergonomics.py "
    "--out .scratch/formal/agent-operator"
)
REFINEMENT = (
    "tests/substrate/runtime/test_admission_workflows.py",
    "tests/substrate/test_source_sync_poll_bounded.py",
    "tests/substrate/runtime/test_worker_schedule_modes.py",
)
APPROVAL_SAFETY = TLCProperty(
    "AgentOperatorApprovalSafety",
    (
        "TLC verified that every accepted admission belongs to the abstract "
        "eligible class; exact rule cardinality and payload completeness are "
        "refinement-test obligations."
    ),
    refinement_tests=(REFINEMENT[0],),
    expected_invariants=("TypeOK", "ApprovalIsBounded"),
)
TRIGGER_SAFETY = TLCProperty(
    "AgentOperatorTriggerSafety",
    (
        "TLC verified active-watch suppression, exact applied-token baseline "
        "advancement, and due-backstop authority."
    ),
    refinement_tests=(REFINEMENT[1],),
    expected_invariants=(
        "TypeOK",
        "ActiveWatchSuppressesOrdinaryPoll",
        "BackstopRetainsAuthority",
        "BaselineAdvanceRequiresAppliedDelegation",
    ),
)
REAPER_SAFETY = TLCProperty(
    "AgentOperatorReaperSafety",
    "TLC verified that automatic cleanup never reaps started work.",
    refinement_tests=(REFINEMENT[2],),
    expected_invariants=("TypeOK", "StartedWorkflowIsNotReaped"),
)
RUNTIME_SAFETY = TLCProperty(
    "AgentOperatorRuntimeSafety",
    "TLC verified bounded approval, cross-trigger suppression, and started-work preservation.",
    refinement_tests=REFINEMENT,
    expected_invariants=(
        "TypeOK",
        "ApprovalIsBounded",
        "ActiveWatchSuppressesOrdinaryPoll",
        "BackstopRetainsAuthority",
        "BaselineAdvanceRequiresAppliedDelegation",
        "StartedWorkflowIsNotReaped",
        "AgentOperatorRuntimeSafety",
    ),
)
UNSAFE_APPROVAL = TLCProperty(
    "AgentOperatorUnsafeApprovalWitness",
    "Expected counterexample when an ineligible admission can be accepted.",
    refinement_tests=(REFINEMENT[0],),
    expected_invariants=("ApprovalIsBounded",),
)
DUPLICATE_POLL = TLCProperty(
    "AgentOperatorDuplicatePollWitness",
    "Expected counterexample when active watch work can be double-fired by poll.",
    refinement_tests=(REFINEMENT[1],),
    expected_invariants=("ActiveWatchSuppressesOrdinaryPoll",),
)
BACKSTOP_SUPPRESSION = TLCProperty(
    "AgentOperatorBackstopSuppressionWitness",
    "Expected counterexample when event dedupe can suppress a due reconcile.",
    refinement_tests=(REFINEMENT[1],),
    expected_invariants=("BackstopRetainsAuthority",),
)
STARTED_REAP = TLCProperty(
    "AgentOperatorStartedReapWitness",
    "Expected counterexample when automatic cleanup can reap started work.",
    refinement_tests=(REFINEMENT[2],),
    expected_invariants=("StartedWorkflowIsNotReaped",),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        safety = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "AgentOperatorSafety.tla",
            config=MODEL_DIR / "AgentOperatorSafety.cfg",
            out_dir=args.out / "safety",
            properties=[
                APPROVAL_SAFETY,
                TRIGGER_SAFETY,
                REAPER_SAFETY,
                RUNTIME_SAFETY,
            ],
            expected_properties=[
                APPROVAL_SAFETY,
                TRIGGER_SAFETY,
                REAPER_SAFETY,
                RUNTIME_SAFETY,
            ],
            checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=12,
        )
        witnesses = []
        for dirname, config_name, prop in (
            ("unsafe-approval", "AgentOperatorUnsafeApproval.cfg", UNSAFE_APPROVAL),
            ("duplicate-poll", "AgentOperatorDuplicatePoll.cfg", DUPLICATE_POLL),
            (
                "backstop-suppression",
                "AgentOperatorBackstopSuppression.cfg",
                BACKSTOP_SUPPRESSION,
            ),
            ("started-reap", "AgentOperatorStartedReap.cfg", STARTED_REAP),
        ):
            witness = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=MODEL_DIR / "AgentOperatorSafety.tla",
                config=MODEL_DIR / config_name,
                out_dir=args.out / dirname,
                properties=[prop],
                expected_properties=[prop],
                checker_command=DEFAULT_COMMAND,
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
            )
            witnesses.append(witness)
    except TLCSetupError as exc:
        print(f"formal-agent-operator: {exc}", file=sys.stderr)
        return exc.exit_code
    if safety.run_failed or any(witness.run_failed for witness in witnesses):
        print(
            f"formal-agent-operator: failed; see {args.out}",
            file=sys.stderr,
        )
        return safety.exit_code or next(
            (item.exit_code for item in witnesses if item.run_failed), 1
        ) or 1
    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump(
            {"results": [*safety.rows, *(row for item in witnesses for row in item.rows)]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print(f"formal-agent-operator: TLC passed; report at {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
