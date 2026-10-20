#!/usr/bin/env python3
"""Run the Nomos TLA+ model with TLC and emit strict PACT formal evidence."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCExecutionProfile,
    TLCProperty,
    TLCReportResult,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "nomos"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "nomos-control-plane"
SMOKE_OUT = REPO_ROOT / ".scratch" / "formal" / "nomos-smoke"
DEFAULT_MODEL = MODEL_DIR / "Nomos.tla"
DEFAULT_CONFIG = MODEL_DIR / "Nomos.cfg"
SMOKE_CONFIG = MODEL_DIR / "NomosSourceLineage.cfg"
FINE_GRAINED_ACCESS_CONTROL_CONFIG = MODEL_DIR / "NomosFineGrainedAccess.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_nomos.py "
    "--profile full --config formal/nomos/Nomos.cfg "
    "--out .scratch/formal/nomos-control-plane "
    "--property-id NomosControlPlaneContract --timeout-seconds 900"
)
DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_nomos.py --model NomosCounterexample.tla "
    "--config NomosCounterexample.cfg --expect-counterexample "
    "--out .scratch/formal/nomos-control-plane-counterexample"
)
DEFAULT_REPORT_PROPERTY_IDS = ("NomosControlPlaneContract",)
SOURCE_LINEAGE_PACT_PROPERTY_ID = "formal.nomos-source-lineage.visibility"
FINE_GRAINED_ACCESS_CONTROL_PACT_PROPERTY_ID = (
    "formal.nomos-fgac.fine-grained-lineage"
)
SOURCE_LINEAGE_TLC_INVARIANT = "NomosSourceLineageVisibility"
FINE_GRAINED_ACCESS_CONTROL_TLC_INVARIANT = "NomosFineGrainedVisibility"
SOURCE_LINEAGE_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_nomos.py --profile smoke --config "
    "formal/nomos/NomosSourceLineage.cfg --out .scratch/formal/nomos-smoke "
    f"--property-id {SOURCE_LINEAGE_PACT_PROPERTY_ID} --timeout-seconds 30"
)
FINE_GRAINED_ACCESS_CONTROL_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_nomos.py --profile smoke --config "
    "formal/nomos/NomosFineGrainedAccess.cfg "
    "--out .scratch/nomos/fine-grained-access-control-formal "
    f"--property-id {FINE_GRAINED_ACCESS_CONTROL_PACT_PROPERTY_ID} "
    "--timeout-seconds 30"
)
PINS_CONFIG = MODEL_DIR / "NomosPinRotation.cfg"
PINS_OUT = REPO_ROOT / ".scratch" / "formal" / "nomos-pins"
PINS_PACT_PROPERTY_ID = "formal.nomos-publish-pins.integrity"
PINS_TLC_INVARIANT = "NomosPublishPinContract"
IAM_CONFIG = MODEL_DIR / "NomosIamClaims.cfg"
IAM_OUT = REPO_ROOT / ".scratch" / "formal" / "nomos-iam"
IAM_PACT_PROPERTY_ID = "formal.nomos-iam-claims.isolation"
IAM_TLC_INVARIANT = "NomosIamClaimIsolationContract"
REFINEMENT_TESTS = (
    "tests/substrate/test_formal_nomos.py",
    "tests/substrate/nomos/test_nomos_core.py",
    "tests/substrate/nomos/test_nomos_institutional_iam.py",
    "tests/substrate/nomos/test_nomos_identity_store.py",
    "tests/substrate/nomos/test_nomos_institutional_iam_postgres.py",
)
SOURCE_LINEAGE_REFINEMENT_TESTS = (
    "tests/substrate/test_formal_nomos.py",
    "tests/substrate/nomos/test_nomos_institutional_iam.py",
    "tests/substrate/nomos/test_nomos_institutional_iam_postgres.py",
    "tests/substrate/nomos/test_nomos_source_lineage_derived_closure.py",
    "tests/substrate/nomos/test_nomos_source_lineage_read_filter.py",
)
FINE_GRAINED_ACCESS_CONTROL_REFINEMENT_TESTS = (
    *SOURCE_LINEAGE_REFINEMENT_TESTS,
    "tests/substrate/nomos/test_nomos_fine_grained_access_control.py",
)
NONVACUITY_REFINEMENT_TESTS = ("tests/substrate/test_formal_nomos.py",)
PINS_REFINEMENT_TESTS = (
    "tests/substrate/nomos/test_nomos_active_store.py",
    "tests/substrate/nomos/test_nomos_runtime_enforcement.py",
)
IAM_REFINEMENT_TESTS = (
    "tests/substrate/nomos/test_nomos_institutional_iam.py",
    "tests/substrate/nomos/test_nomos_institutional_iam_postgres.py",
    "tests/substrate/nomos/test_nomos_identity_store.py",
)
PROPERTIES = [
    TLCProperty(
        "NomosControlPlaneContract",
        (
            "TLC verified the combined Nomos control-plane safety contract: "
            "default deny, scoped authority, privacy-safe public output, "
            "deployment/source posture, active pins, obligations, and traces."
        ),
        refinement_tests=REFINEMENT_TESTS,
    ),
    TLCProperty(
        SOURCE_LINEAGE_TLC_INVARIANT,
        (
            "TLC verified that source-lineage graph reads are visible only "
            "when every contributing source is authorized; partial, missing, "
            "malformed, unknown, or stale-policy lineage fails closed without "
            "redaction or declassification widening."
        ),
        refinement_tests=SOURCE_LINEAGE_REFINEMENT_TESTS,
    ),
    TLCProperty(
        SOURCE_LINEAGE_PACT_PROPERTY_ID,
        (
            "TLC verified the PACT-bound Nomos source-lineage visibility "
            "contract: every contributing source must be authorized and stale "
            "source-policy fingerprints cannot make graph facts visible."
        ),
        refinement_tests=SOURCE_LINEAGE_REFINEMENT_TESTS,
        expected_invariants=(SOURCE_LINEAGE_TLC_INVARIANT,),
    ),
    TLCProperty(
        FINE_GRAINED_ACCESS_CONTROL_PACT_PROPERTY_ID,
        (
            "TLC verified the PACT-bound Nomos fine-grained visibility "
            "contract: source-lineage reads and fine-grained source-record, "
            "graph-object, as-of horizon, and unaddressable-lineage reads fail "
            "closed unless every policy-addressable contributor is authorized."
        ),
        refinement_tests=FINE_GRAINED_ACCESS_CONTROL_REFINEMENT_TESTS,
        expected_invariants=(FINE_GRAINED_ACCESS_CONTROL_TLC_INVARIANT,),
    ),
    TLCProperty(
        PINS_PACT_PROPERTY_ID,
        (
            "TLC verified the publish-pin contract over the focused "
            "pin-rotation universe (bundle universe >= 2 with RotateBundle): "
            "a published generation's policy pin is exactly the bundle "
            "active at publish time, frozen against later rotation."
        ),
        refinement_tests=PINS_REFINEMENT_TESTS,
        expected_invariants=(PINS_TLC_INVARIANT,),
    ),
    TLCProperty(
        IAM_PACT_PROPERTY_ID,
        (
            "TLC verified the institutional IAM claim-isolation contract over "
            "the focused claim universe: each of the seven claim checks blocks "
            "its own otherwise-grantable request, and an allowed external "
            "request cites a matching provider, binding, and grant."
        ),
        refinement_tests=IAM_REFINEMENT_TESTS,
        expected_invariants=(IAM_TLC_INVARIANT,),
    ),
]
NONVACUITY_CHECKS = (
    (
        "missing-policy",
        Path("NomosMissingPolicyCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuityMissingPolicy",
            "TLC produced the expected missing-policy counterexample.",
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=("BrokenMissingPolicyCanAllow",),
        ),
    ),
    (
        "weak-deployment-posture",
        Path("NomosWeakDeploymentPostureCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuityWeakDeploymentPosture",
            "TLC produced the expected weak-deployment-posture counterexample.",
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=("BrokenWeakDeploymentPostureCanAllow",),
        ),
    ),
    (
        "unscoped-identity-claim",
        Path("NomosUnscopedIdentityCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuityUnscopedIdentityClaim",
            "TLC produced the expected unscoped-identity-claim counterexample.",
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=("BrokenUnscopedIdentityClaimCanGrant",),
        ),
    ),
    (
        "unknown-action",
        Path("NomosUnknownActionCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuityUnknownAction",
            "TLC produced the expected unknown-action counterexample.",
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=("BrokenUnknownActionCanAllow",),
        ),
    ),
    (
        "private-export-without-scoped-claim",
        Path("NomosPrivateExportCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuityPrivateExportWithoutScopedClaim",
            (
                "TLC produced the expected private-export-without-scoped-claim "
                "counterexample."
            ),
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=(
                "BrokenPrivateExportWithoutScopedClaimCanPublish",
            ),
        ),
    ),
    (
        "source-deployment-mismatch",
        Path("NomosSourceDeploymentMismatchCounterexample.cfg"),
        TLCProperty(
            "NomosNonVacuitySourceDeploymentMismatch",
            "TLC produced the expected source/deployment mismatch counterexample.",
            refinement_tests=NONVACUITY_REFINEMENT_TESTS,
            expected_invariants=("BrokenSourceDeploymentMismatchCanAllow",),
        ),
    ),
)
COUNTEREXAMPLE_PROPERTIES = [check[2] for check in NONVACUITY_CHECKS]

PROFILES = {
    "smoke": TLCExecutionProfile(
        name="smoke",
        description=(
            "CI/operator freshness profile using the focused source-lineage "
            "config and PACT-bound visibility property."
        ),
        config=SMOKE_CONFIG,
        out_dir=SMOKE_OUT,
        timeout_seconds=30,
        property_ids=(SOURCE_LINEAGE_PACT_PROPERTY_ID,),
        expected_property_ids=(SOURCE_LINEAGE_PACT_PROPERTY_ID,),
        full_strength=False,
        ci_smoke=True,
        min_distinct_states=270,
    ),
    "full": TLCExecutionProfile(
        name="full",
        description=(
            "Release/PACT evidence profile over the full Nomos control-plane "
            "configuration."
        ),
        config=DEFAULT_CONFIG,
        out_dir=DEFAULT_OUT,
        timeout_seconds=900,
        property_ids=DEFAULT_REPORT_PROPERTY_IDS,
        expected_property_ids=DEFAULT_REPORT_PROPERTY_IDS,
        full_strength=True,
        ci_smoke=False,
        min_distinct_states=380000,
    ),
    "pins": TLCExecutionProfile(
        name="pins",
        description=(
            "Focused publish-pin/rotation profile: bundle universe >= 2 with "
            "RotateBundle over a minimal request universe, proving "
            "GenerationPinMatchesPublishBundle as a distinguishing claim "
            "without multiplying the full control-plane state space."
        ),
        config=PINS_CONFIG,
        out_dir=PINS_OUT,
        timeout_seconds=60,
        property_ids=(PINS_PACT_PROPERTY_ID,),
        expected_property_ids=(PINS_PACT_PROPERTY_ID,),
        full_strength=False,
        ci_smoke=False,
        min_distinct_states=322,
    ),
    "iam": TLCExecutionProfile(
        name="iam",
        description=(
            "Focused institutional IAM claim-isolation profile: seven "
            "otherwise-grantable requests, one per claim check, proving each "
            "*CannotGrant invariant non-vacuously without multiplying the "
            "full control-plane request universe."
        ),
        config=IAM_CONFIG,
        out_dir=IAM_OUT,
        timeout_seconds=120,
        property_ids=(IAM_PACT_PROPERTY_ID,),
        expected_property_ids=(IAM_PACT_PROPERTY_ID,),
        full_strength=False,
        ci_smoke=False,
        min_distinct_states=50570,
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
        help="TLA+ module path; relative paths resolve under formal/nomos",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="TLC config path; relative paths resolve under formal/nomos",
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
        "--expect-counterexample",
        action="store_true",
        help=(
            "return success only when TLC reports the expected Nomos "
            "counterexample invariant and formal-report.yaml is written"
        ),
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

    if args.expect_counterexample:
        return _run_counterexample_suite(args)

    profile = PROFILES[args.profile]
    out_dir = args.out or profile.out_dir
    property_ids = args.property_ids or list(profile.property_ids)
    config = args.config or _default_config_for_properties(property_ids, profile)
    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else profile.timeout_seconds
    )
    properties = COUNTEREXAMPLE_PROPERTIES if args.expect_counterexample else PROPERTIES
    selected_properties = select_properties(properties, property_ids)
    expected_property_ids = (
        property_ids
        if args.property_ids
        else list(profile.expected_property_ids or profile.property_ids)
    )
    expected_properties = select_properties(
        properties,
        expected_property_ids,
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
            timeout_seconds=timeout_seconds,
            execution_profile=args.profile,
            profile_timeout_seconds=timeout_seconds,
            min_distinct_states=profile.min_distinct_states,
        )
    except TLCSetupError as exc:
        print(f"formal-nomos: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-nomos: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-nomos: TLC passed; report at {result.report_path}")
    return 0


def _default_config_for_properties(
    property_ids: list[str],
    profile: TLCExecutionProfile,
) -> Path:
    if property_ids == [FINE_GRAINED_ACCESS_CONTROL_PACT_PROPERTY_ID]:
        return FINE_GRAINED_ACCESS_CONTROL_CONFIG
    return profile.config


def _run_counterexample_suite(argparse_args: argparse.Namespace) -> int:
    out_dir = argparse_args.out or (
        REPO_ROOT / ".scratch" / "formal" / "nomos-control-plane-counterexample"
    )
    timeout_seconds = (
        argparse_args.timeout_seconds
        if argparse_args.timeout_seconds is not None
        else 300
    )
    selected = select_properties(
        COUNTEREXAMPLE_PROPERTIES,
        argparse_args.property_ids or [prop.property_id for prop in COUNTEREXAMPLE_PROPERTIES],
    )
    selected_ids = {prop.property_id for prop in selected}
    checks = [
        check for check in NONVACUITY_CHECKS
        if check[2].property_id in selected_ids
    ]
    checker_command = (
        DEFAULT_COUNTEREXAMPLE_CHECKER_COMMAND
        if argparse_args.checker_command is None
        else argparse_args.checker_command
    )
    try:
        results = tuple(
            run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=argparse_args.model,
                config=config,
                out_dir=out_dir / f"nonvacuity-{witness_name}",
                properties=[prop],
                expected_properties=[prop],
                checker_command=checker_command,
                extra_refinement_tests=argparse_args.extra_refinement_tests,
                expect_counterexample=True,
                timeout_seconds=timeout_seconds,
                include_checker_output=True,
            )
            for witness_name, config, prop in checks
        )
    except TLCSetupError as exc:
        print(f"formal-nomos: {exc}", file=sys.stderr)
        return exc.exit_code

    failed = [result for result in results if result.run_failed]
    _write_combined_counterexample_report(out_dir, results)
    _write_counterexample_artifact(out_dir, results)
    if failed:
        print(
            "formal-nomos: counterexample witness failed; see "
            f"{failed[0].stdout_path} and {failed[0].stderr_path}",
            file=sys.stderr,
        )
        return failed[0].exit_code or 1
    print(
        "formal-nomos: expected counterexamples captured; report at "
        f"{out_dir.resolve() / 'formal-report.yaml'}"
    )
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
        "python",
        "scripts/check_formal_nomos.py",
        "--profile",
        args.profile,
    ]
    if _resolve_model_arg(args.model) != DEFAULT_MODEL.resolve():
        parts.extend(["--model", str(args.model)])
    parts.extend(["--config", _command_path(config)])
    parts.extend(["--out", _command_path(out_dir)])
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


def _write_combined_counterexample_report(
    out_dir: Path,
    results: tuple[TLCReportResult, ...],
) -> None:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = [row for result in results for row in result.rows]
    (out_dir / "formal-report.yaml").write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )


def _write_counterexample_artifact(
    out_dir: Path,
    results: tuple[TLCReportResult, ...],
) -> None:
    lines = [
        "TLC counterexample suite result.",
        "",
        "Witnesses:",
    ]
    for result in results:
        row = result.rows[0]
        lines.extend([
            f"- {row['property_id']}",
            f"  command: {' '.join(result.command)}",
            f"  expected: {', '.join(COUNTEREXAMPLE_EXPECTED[row['property_id']])}",
            f"  observed: {', '.join(result.violated_invariants) or '<none>'}",
            f"  stdout: {result.stdout_path}",
            f"  stderr: {result.stderr_path}",
        ])
    out_dir.resolve().mkdir(parents=True, exist_ok=True)
    (out_dir.resolve() / "counterexample.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


COUNTEREXAMPLE_EXPECTED = {
    prop.property_id: prop.expected_invariants
    for _, _, prop in NONVACUITY_CHECKS
}


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
