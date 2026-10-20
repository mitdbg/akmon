#!/usr/bin/env python3
"""Run the admission TLA+ model with TLC."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCExecutionProfile,
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "admission"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "admission"
SMOKE_OUT = REPO_ROOT / ".scratch" / "formal" / "admission-smoke"
DEFAULT_MODEL = MODEL_DIR / "Admission.tla"
DEFAULT_CONFIG = MODEL_DIR / "Admission.cfg"
SMOKE_CONFIG = MODEL_DIR / "AdmissionSmoke.cfg"

REFINEMENT_TESTS = (
    "tests/substrate/test_ingest_service.py::"
    "test_zero_output_source_sync_uses_bounded_admission_path",
    "tests/substrate/test_ingest_service.py::"
    "test_zero_output_source_sync_accepts_non_fast_track_modes",
)

PROPERTIES = [
    TLCProperty(
        "ZeroOutputAdmissionCannotPublishWithoutRefs",
        (
            "TLC verified that accepted zero-output admissions are modeled and "
            "cannot publish without admission refs."
        ),
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "NoDuplicateIdempotency",
            "FastTrackHasChecks",
            "RefsBeforePublish",
            "ZeroOutputAcceptedHasNoRefs",
            "ZeroOutputAdmissionCannotPublishWithoutRefs",
            "PublishedHaveGeneration",
            "RejectedNeverPublished",
            "RejectedNeverLaterPublished",
            "ReviewPublishRequiresDecision",
            "EventsReferenceKnownEnvelope",
            "WorkflowAttemptsAreNamed",
            "VisibleDerivedImpliesPublishedEnvelope",
            "RejectedDerivedNeverVisible",
            "ExtractorCannotBypassProjectability",
            "WorkflowEventNotAuthority",
        ),
    ),
]

COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "AdmissionNonVacuityPublishesWithRefs",
        "TLC produced the expected counterexample showing publish feedback is reachable.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("BrokenNoPublish",),
    ),
]

PROFILES = {
    "smoke": TLCExecutionProfile(
        name="smoke",
        description=(
            "CI/operator freshness profile over singleton universes; keeps the "
            "positive admission safety invariant set but bounds TLC for short "
            "runs."
        ),
        config=SMOKE_CONFIG,
        out_dir=SMOKE_OUT,
        timeout_seconds=30,
        property_ids=("ZeroOutputAdmissionCannotPublishWithoutRefs",),
        full_strength=False,
        ci_smoke=True,
        min_distinct_states=632,
    ),
    "full": TLCExecutionProfile(
        name="full",
        description=(
            "Release/PACT evidence profile over the full configured admission "
            "state space."
        ),
        config=DEFAULT_CONFIG,
        out_dir=DEFAULT_OUT,
        timeout_seconds=900,
        property_ids=tuple(prop.property_id for prop in PROPERTIES),
        full_strength=True,
        ci_smoke=False,
        min_distinct_states=22317952,
    ),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="full",
        help="execution profile to apply when config/out/timeout are not supplied",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="list available execution profiles and exit",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable output for --list-profiles",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="directory for TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/admission",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="TLC config path; relative paths resolve under formal/admission",
    )
    parser.add_argument(
        "--expect-counterexample",
        action="store_true",
        help=(
            "return success only when TLC reports the expected invariant "
            "violation"
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
        default=None,
        help="maximum seconds to allow TLC to run",
    )
    args = parser.parse_args(argv)

    if args.list_profiles:
        _emit_profiles(json_output=args.json)
        return 0

    profile = PROFILES[args.profile]
    out_dir = args.out or profile.out_dir
    config = args.config or profile.config
    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else profile.timeout_seconds
    )
    properties = (
        COUNTEREXAMPLE_PROPERTIES
        if args.expect_counterexample
        else PROPERTIES
    )
    property_ids = args.property_ids or (
        [] if args.expect_counterexample else list(profile.property_ids)
    )
    selected_properties = select_properties(properties, property_ids)
    expected_properties = (
        selected_properties
        if args.expect_counterexample
        else select_properties(
            properties,
            list(profile.expected_property_ids or profile.property_ids),
        )
    )
    checker_command = args.checker_command or _checker_command(
        args,
        out_dir=out_dir,
        config=config,
        timeout_seconds=timeout_seconds,
        property_ids=property_ids,
    )

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=config,
            out_dir=out_dir,
            properties=selected_properties,
            expected_properties=expected_properties,
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            expect_counterexample=args.expect_counterexample,
            timeout_seconds=timeout_seconds,
            execution_profile=args.profile,
            profile_timeout_seconds=timeout_seconds,
            min_distinct_states=profile.min_distinct_states,
        )
    except TLCSetupError as exc:
        print(f"formal-admission: {exc}", file=sys.stderr)
        return exc.exit_code

    if args.expect_counterexample and not result.run_failed:
        counterexample_path = out_dir.resolve() / "counterexample.txt"
        counterexample_path.write_text(
            "TLC produced the expected admission counterexample.\n\n"
            f"Command: {' '.join(result.command)}\n\n"
            "STDOUT:\n"
            f"{result.stdout_path.read_text(encoding='utf-8')}\n\n"
            "STDERR:\n"
            f"{result.stderr_path.read_text(encoding='utf-8')}\n",
            encoding="utf-8",
        )
        print(
            "formal-admission: expected counterexample captured at "
            f"{counterexample_path}; report at {result.report_path}",
        )
        return 0

    if result.run_failed:
        print(
            "formal-admission: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1

    print(f"formal-admission: TLC passed; report at {result.report_path}")
    return 0


def _checker_command(
    args: argparse.Namespace,
    *,
    out_dir: Path,
    config: Path,
    timeout_seconds: int,
    property_ids: list[str],
) -> str:
    parts = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "scripts/check_formal_admission.py",
    ]
    parts.extend(["--profile", args.profile])
    if _resolve_model_arg(args.model) != DEFAULT_MODEL.resolve():
        parts.extend(["--model", str(args.model)])
    parts.extend(["--config", _command_path(config)])
    parts.extend(["--out", _command_path(out_dir)])
    if args.expect_counterexample:
        parts.append("--expect-counterexample")
    for property_id in property_ids:
        parts.extend(["--property-id", property_id])
    for refinement_test in args.extra_refinement_tests:
        parts.extend(["--refinement-test", refinement_test])
    parts.extend(["--timeout-seconds", str(timeout_seconds)])
    return " ".join(parts)


def _emit_profiles(*, json_output: bool) -> None:
    payload = {
        "default_profile": "full",
        "profiles": [
            PROFILES[name].as_dict(repo_root=REPO_ROOT)
            for name in sorted(PROFILES)
        ],
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for profile in payload["profiles"]:
        print(
            f"{profile['name']}: config={profile['config']} "
            f"out={profile['out']} timeout={profile['timeout_seconds']}s"
        )


def _resolve_model_arg(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (MODEL_DIR / path).resolve()


def _command_path(path) -> str:
    """checker_command is a durable, repo-portable string: relativize so a
    recorded command survives other checkouts. Paths under the model dir
    relativize to it (the checker resolves --config against MODEL_DIR);
    other repo paths (e.g. --out) relativize to the repo root."""
    from pathlib import Path
    resolved = Path(path).resolve()
    for base in (MODEL_DIR, REPO_ROOT):
        try:
            return str(resolved.relative_to(base.resolve()))
        except ValueError:
            continue
    return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
