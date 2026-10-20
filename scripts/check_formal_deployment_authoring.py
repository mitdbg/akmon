#!/usr/bin/env python3
"""Run the deployment-authoring TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "deployment_authoring"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-authoring"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_deployment_authoring.py "
    "--out .scratch/formal/deployment-authoring"
)

REFINEMENT_TESTS_BY_PROPERTY = {
    "Issue252ScaffoldSafety": (
        "tests/substrate/cli/test_deployment_new.py",
    ),
    "Issue253WiringPlanDescriptorOnly": (
        "tests/substrate/test_deployment_wiring_plan.py",
    ),
    "Issue254LintStaticDiagnostics": (
        "tests/substrate/cli/test_deployment_lint.py",
    ),
    "Issue255EnvDoctorAuthoritySafety": (
        "tests/substrate/cli/test_deployment_env_doctor.py",
    ),
    "Issue256ProfileExplainSafety": (
        "tests/substrate/cli/test_deployment_profile_explain.py",
    ),
    "DeploymentAuthoringApproachContract": (
        "tests/substrate/cli/test_deployment_new.py",
        "tests/substrate/test_deployment_wiring_plan.py",
        "tests/substrate/cli/test_deployment_lint.py",
        "tests/substrate/cli/test_deployment_env_doctor.py",
        "tests/substrate/cli/test_deployment_profile_explain.py",
    ),
}

PROPERTIES = [
    TLCProperty(
        "Issue252ScaffoldSafety",
        "TLC verified deployment scaffold generation writes committed skeleton config only, rejects unsafe names, and does not expose secrets.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY["Issue252ScaffoldSafety"],
        expected_invariants=("Issue252ScaffoldSafety",),
    ),
    TLCProperty(
        "Issue253WiringPlanDescriptorOnly",
        "TLC verified deployment plan inspection remains descriptor-only and does not mutate files or execute custom code.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY[
            "Issue253WiringPlanDescriptorOnly"
        ],
        expected_invariants=("Issue253WiringPlanDescriptorOnly",),
    ),
    TLCProperty(
        "Issue254LintStaticDiagnostics",
        "TLC verified deployment lint is static by default, DB-free, diagnostic-field complete, and non-mutating.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY["Issue254LintStaticDiagnostics"],
        expected_invariants=("Issue254LintStaticDiagnostics",),
    ),
    TLCProperty(
        "Issue255EnvDoctorAuthoritySafety",
        "TLC verified env doctor redacts by default, keeps the existing deployment env command supported, reports the authority winner, and classifies DSN target conflicts by read/write/runtime risk.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY[
            "Issue255EnvDoctorAuthoritySafety"
        ],
        expected_invariants=("Issue255EnvDoctorAuthoritySafety",),
    ),
    TLCProperty(
        "Issue256ProfileExplainSafety",
        "TLC verified profile explain is no-write, no-secret, and does not execute custom code.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY["Issue256ProfileExplainSafety"],
        expected_invariants=("Issue256ProfileExplainSafety",),
    ),
    TLCProperty(
        "DeploymentAuthoringApproachContract",
        "TLC verified the cross-issue deployment-authoring approach contract: additive inspection, static lint boundary, secret redaction, scaffold safety, and ready explain compatibility.",
        refinement_tests=REFINEMENT_TESTS_BY_PROPERTY[
            "DeploymentAuthoringApproachContract"
        ],
        expected_invariants=("DeploymentAuthoringApproachContract",),
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
        default=MODEL_DIR / "DeploymentAuthoring.tla",
        help="TLA+ module path; relative paths resolve under formal/deployment_authoring",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "DeploymentAuthoring.cfg",
        help="TLC config path; relative paths resolve under formal/deployment_authoring",
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
            properties=select_properties(PROPERTIES, args.property_ids),
            expected_properties=PROPERTIES,
            checker_command=args.checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-deployment-authoring: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-deployment-authoring: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-deployment-authoring: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
