#!/usr/bin/env python3
"""Run the PACT formal-evidence TLA+ model with TLC."""
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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "pact-formal-evidence"
DEFAULT_MODEL = MODEL_DIR / "PactFormalEvidence.tla"
DEFAULT_CONFIG = MODEL_DIR / "PactFormalEvidence.cfg"
DEFAULT_REPORT_PROPERTY_IDS = ("PactFormalEvidenceFresh",)
REFINEMENT_TESTS = (
    "tests/pact/test_pact_formal_evidence_formal.py::"
    "test_pact_formal_evidence_formal_binding_covers_contract",
    "tests/pact/test_v3_formal_flow.py::"
    "test_formal_binding_requires_checker_refinement_and_freshness",
    "tests/pact/test_v3_formal_flow.py::"
    "test_stale_formal_evidence_no_longer_satisfies_requirements_tasks_or_gates",
    "tests/pact/test_v3_formal_flow.py::"
    "test_formal_report_rejects_unknown_and_omitted_bound_properties",
    "tests/pact/test_formal_binding.py::"
    "test_formal_binding_validates_artifacts_when_repo_root_is_available",
    "tests/pact/test_evidence_ingest.py::"
    "test_formal_report_attaches_property_evidence",
)
PROPERTIES = [
    TLCProperty(
        "TypeOK",
        "TLC verified the PACT formal-evidence state shape.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "VerificationRequiresImplementationOrBypass",
        "TLC verified verification requires implementation approval or bypass.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "VerificationEnteredFresh",
        (
            "TLC verified no verification advance ever happened while the "
            "formal evidence was stale (entry freshness, history form)."
        ),
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "NoSilentStaleAfterVerification",
        (
            "TLC verified a corpus sweep that runs while a verified "
            "change's formal evidence is stale must flag it (evidence can "
            "go stale after verification; detection is the contract)."
        ),
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "VerificationRequiresSemanticAcceptance",
        "TLC verified verification requires semantic acceptance evidence.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "VerificationRequiresCompletedTask",
        "TLC verified verification requires completed task evidence.",
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        "PactFormalEvidenceFresh",
        (
            "TLC verified the combined PACT formal-evidence freshness "
            "contract."
        ),
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
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="TLC config path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--checker-command",
        default=None,
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

    checker_command = args.checker_command or _checker_command(args)

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
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-pact-formal-evidence: {exc}", file=sys.stderr)
        return exc.exit_code

    if result.run_failed:
        print(
            "formal-pact-formal-evidence: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1

    print(
        "formal-pact-formal-evidence: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


def _checker_command(args: argparse.Namespace) -> str:
    parts = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "scripts/check_formal_pact_formal_evidence.py",
    ]
    if _resolve_model_arg(args.model) != DEFAULT_MODEL.resolve():
        parts.extend(["--model", str(args.model)])
    if _resolve_model_arg(args.config) != DEFAULT_CONFIG.resolve():
        parts.extend(["--config", str(args.config)])
    parts.extend(["--out", str(args.out)])
    for property_id in args.property_ids:
        parts.extend(["--property-id", property_id])
    for refinement_test in args.extra_refinement_tests:
        parts.extend(["--refinement-test", refinement_test])
    if args.timeout_seconds != 300:
        parts.extend(["--timeout-seconds", str(args.timeout_seconds)])
    return " ".join(parts)


def _resolve_model_arg(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (MODEL_DIR / path).resolve()


if __name__ == "__main__":
    raise SystemExit(main())
