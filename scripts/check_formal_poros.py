#!/usr/bin/env python3
"""Run the Poros TLA+ model with TLC and emit strict PACT formal evidence."""
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
MODEL_DIR = REPO_ROOT / "formal" / "poros"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "poros"
DEFAULT_MODEL = MODEL_DIR / "PorosExchange.tla"
DEFAULT_CONFIG = MODEL_DIR / "PorosExchange.cfg"
DEFAULT_COUNTEREXAMPLE_MODEL = MODEL_DIR / "PorosExchangeCounterexample.tla"
DEFAULT_COUNTEREXAMPLE_CONFIG = MODEL_DIR / "PorosExchangeCounterexample.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_poros.py "
    "--out .scratch/formal/poros"
)
DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_poros.py "
    "--model PorosExchangeCounterexample.tla "
    "--config PorosExchangeCounterexample.cfg "
    "--out .scratch/formal/poros-counterexample "
    "--expect-counterexample"
)
DEFAULT_REPORT_PROPERTY_IDS = ("formal.poros.exchange-safety",)
DEFAULT_COUNTEREXAMPLE_PROPERTY_IDS = (
    "formal.poros.receipt-publication-nonvacuity",
)
# Measured floor for the positive PorosExchange.cfg run (1,942,272 distinct
# states, 2026-07-02): fail closed if the reachable state space collapses,
# which would silently vacate the safety invariants.
POSITIVE_MIN_DISTINCT_STATES = 1_900_000

POROS_POSITIVE_INVARIANTS = (
    "ConstantSetsOK",
    "TypeOK",
    "NoUnverifiedAdmission",
    "NoReplayDoubleEffect",
    "ReceiptIsNotPublication",
    "NomosDecisionRequired",
    "QuarantineNotLive",
    "GenerationPinPreserved",
    "LargeObjectHashBound",
    "PromotionRequiresReceiverPolicy",
    "NoUnauthorizedPromotion",
    "PrivatePayloadNotPublic",
    "ProofCompleteness",
    "NoSenderPolicyAuthority",
)
POSITIVE_REFINEMENT_TESTS = (
    "scripts/check_poros_refinement_fixture.py",
    "scripts/check_poros_refinement_from_db.py",
    "tests/substrate/exchange/test_poros_http.py::"
    "test_http_receiver_service_exported_outbox_message_refines_published_flow",
    "tests/substrate/exchange/test_poros_http.py::"
    "test_http_sender_hinted_denial_refines_receiver_unauthorized",
    "tests/substrate/exchange/test_poros_inbox_and_authority.py",
    "tests/substrate/exchange/test_poros_admission.py",
    "tests/substrate/exchange/test_poros_graph_shape.py",
)
COUNTEREXAMPLE_REFINEMENT_TESTS = (
    "tests/substrate/test_formal_poros.py",
    "tests/substrate/exchange/test_poros_inbox_and_authority.py",
    "tests/substrate/exchange/test_poros_admission.py",
)
PROPERTIES = [
    TLCProperty(
        "formal.poros.exchange-safety",
        (
            "TLC verified the Poros exchange safety contract: verified receiver "
            "admission, replay protection, receipt/publication separation, "
            "receiver-side Nomos authority, quarantine isolation, generation pins, "
            "object-hash binding, private payload exclusion, and proof completeness."
        ),
        refinement_tests=POSITIVE_REFINEMENT_TESTS,
        expected_invariants=POROS_POSITIVE_INVARIANTS,
    ),
]
COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "formal.poros.receipt-publication-nonvacuity",
        (
            "TLC produced the expected counterexample proving that treating a "
            "receipt as publication is reachable in the broken Poros model."
        ),
        refinement_tests=COUNTEREXAMPLE_REFINEMENT_TESTS,
        expected_invariants=("BrokenReceiptImpliesPublication",),
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
        help="TLA+ module path; relative paths resolve under formal/poros",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="TLC config path; relative paths resolve under formal/poros",
    )
    parser.add_argument(
        "--expect-counterexample",
        action="store_true",
        help=(
            "return success only when TLC reports the expected Poros "
            "counterexample invariant"
        ),
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

    properties = COUNTEREXAMPLE_PROPERTIES if args.expect_counterexample else PROPERTIES
    default_property_ids = (
        DEFAULT_COUNTEREXAMPLE_PROPERTY_IDS
        if args.expect_counterexample
        else DEFAULT_REPORT_PROPERTY_IDS
    )
    checker_command = args.checker_command or _checker_command(args)

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=args.out,
            properties=select_properties(
                properties,
                args.property_ids or default_property_ids,
            ),
            expected_properties=properties,
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            expect_counterexample=args.expect_counterexample,
            timeout_seconds=args.timeout_seconds,
            include_checker_output=args.expect_counterexample,
            min_distinct_states=(
                None
                if args.expect_counterexample
                else POSITIVE_MIN_DISTINCT_STATES
            ),
        )
    except TLCSetupError as exc:
        print(f"formal-poros: {exc}", file=sys.stderr)
        return exc.exit_code

    if args.expect_counterexample:
        _write_counterexample_artifact(args.out, result)
        if not result.run_failed:
            print(
                "formal-poros: expected counterexample captured at "
                f"{args.out.resolve() / 'counterexample.txt'}; report at "
                f"{result.report_path}",
            )
            return 0

    if result.run_failed:
        print(
            "formal-poros: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1

    print(f"formal-poros: TLC passed; report at {result.report_path}")
    return 0


def _checker_command(args: argparse.Namespace) -> str:
    if _is_default_positive_run(args):
        return DEFAULT_CHECKER_COMMAND
    if _is_default_counterexample_run(args):
        return DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND
    parts = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "scripts/check_formal_poros.py",
    ]
    if _resolve_model_arg(args.model) != DEFAULT_MODEL.resolve():
        parts.extend(["--model", str(args.model)])
    if _resolve_model_arg(args.config) != DEFAULT_CONFIG.resolve():
        parts.extend(["--config", str(args.config)])
    parts.extend(["--out", str(args.out)])
    if args.expect_counterexample:
        parts.append("--expect-counterexample")
    for property_id in args.property_ids:
        parts.extend(["--property-id", property_id])
    for refinement_test in args.extra_refinement_tests:
        parts.extend(["--refinement-test", refinement_test])
    if args.timeout_seconds != 300:
        parts.extend(["--timeout-seconds", str(args.timeout_seconds)])
    return " ".join(parts)


def _is_default_positive_run(args: argparse.Namespace) -> bool:
    return (
        not args.expect_counterexample
        and _resolve_model_arg(args.model) == DEFAULT_MODEL.resolve()
        and _resolve_model_arg(args.config) == DEFAULT_CONFIG.resolve()
        and args.out.resolve() == DEFAULT_OUT.resolve()
        and not args.property_ids
        and not args.extra_refinement_tests
        and args.timeout_seconds == 300
    )


def _is_default_counterexample_run(args: argparse.Namespace) -> bool:
    return (
        args.expect_counterexample
        and _resolve_model_arg(args.model) == DEFAULT_COUNTEREXAMPLE_MODEL.resolve()
        and _resolve_model_arg(args.config)
        == DEFAULT_COUNTEREXAMPLE_CONFIG.resolve()
        and args.out.resolve()
        == (REPO_ROOT / ".scratch" / "formal" / "poros-counterexample").resolve()
        and not args.property_ids
        and not args.extra_refinement_tests
        and args.timeout_seconds == 300
    )


def _resolve_model_arg(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (MODEL_DIR / path).resolve()


def _write_counterexample_artifact(
    out_dir: Path,
    result: TLCReportResult,
) -> None:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    row = result.rows[0] if result.rows else {}
    expected = ", ".join(COUNTEREXAMPLE_PROPERTIES[0].expected_invariants)
    observed = ", ".join(result.violated_invariants) or "<none>"
    lines = [
        "TLC Poros counterexample result.",
        "",
        f"property_id: {row.get('property_id', '<none>')}",
        f"expected: {expected}",
        f"observed: {observed}",
        f"status: {'fail' if result.run_failed else 'pass'}",
        f"command: {' '.join(result.command)}",
        f"stdout: {result.stdout_path}",
        f"stderr: {result.stderr_path}",
    ]
    if result.unexpected_errors:
        lines.append("unexpected_errors:")
        lines.extend(f"- {error}" for error in result.unexpected_errors)
    (out_dir / "counterexample.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
