#!/usr/bin/env python3
"""Model-check deployment restore-point admission and recovery safety."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "deployment_restore_point"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "deployment-restore-point"
DEFAULT_COMMAND = (
    "uv run --extra dev python scripts/check_formal_deployment_restore_point.py "
    "--out .scratch/formal/deployment-restore-point"
)
REFINEMENT = ("tests/pact/test_deployment_restore_point_model_contract.py",)
SAFETY_PROPERTIES = (
    TLCProperty(
        "MutationRequiresExactTargetAndVerifiedRestore",
        "Every database mutation has exact target authority and a verified restore point.",
        refinement_tests=REFINEMENT,
        expected_invariants=("MutationRequiresExactTargetAndVerifiedRestore",),
    ),
    TLCProperty(
        "TargetSubstitutionRequiresExplicitAcknowledgement",
        "A substituted database cannot mutate without the exact planned acknowledgement.",
        refinement_tests=REFINEMENT,
        expected_invariants=("TargetSubstitutionRequiresExplicitAcknowledgement",),
    ),
    TLCProperty(
        "ExternalEffectNeverClaimsReversible",
        "Provider effects stay disclosed as irreversible independently of database restore.",
        refinement_tests=REFINEMENT,
        expected_invariants=("ExternalEffectNeverClaimsReversible",),
    ),
    TLCProperty(
        "UncertainExternalEffectIsNeverReplayed",
        "An interrupted external smoke effect is never replayed without an exact receipt.",
        refinement_tests=REFINEMENT,
        expected_invariants=("UncertainExternalEffectIsNeverReplayed",),
    ),
    TLCProperty(
        "SkipBackupNeverAuthorizesMutation",
        "Skipping archive creation supplies no mutation authority without an exact receipt.",
        refinement_tests=REFINEMENT,
        expected_invariants=("SkipBackupNeverAuthorizesMutation",),
    ),
    TLCProperty(
        "AttemptGuaranteesRecorded",
        "Every started or terminal phase attempt carries its safety guarantees.",
        refinement_tests=REFINEMENT,
        expected_invariants=("AttemptGuaranteesRecorded",),
    ),
    TLCProperty(
        "EffectUncertaintyIsRecorded",
        "Every failed or uncertain effectful phase remains marked as possibly applied.",
        refinement_tests=REFINEMENT,
        expected_invariants=("EffectUncertaintyIsRecorded",),
    ),
)
POSITIVE_HANDOFF_PROPERTY = TLCProperty(
    "IndependentHandoffUnreachable",
    "An exact independent restore receipt plus --skip-backup can reach mutation.",
    refinement_tests=REFINEMENT,
    expected_invariants=("IndependentHandoffUnreachable",),
)
WITNESSES = (
    ("missing-restore", "MissingRestoreCounterexample.cfg", SAFETY_PROPERTIES[0]),
    (
        "target-substitution",
        "TargetSubstitutionCounterexample.cfg",
        SAFETY_PROPERTIES[1],
    ),
    ("external-replay", "ExternalReplayCounterexample.cfg", SAFETY_PROPERTIES[3]),
    ("skip-backup", "SkipBackupCounterexample.cfg", SAFETY_PROPERTIES[4]),
    (
        "external-disclosure",
        "ExternalDisclosureCounterexample.cfg",
        SAFETY_PROPERTIES[2],
    ),
    (
        "independent-handoff-positive",
        "IndependentHandoffPositive.cfg",
        POSITIVE_HANDOFF_PROPERTY,
    ),
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
            model=MODEL_DIR / "DeploymentRestorePoint.tla",
            config=MODEL_DIR / "DeploymentRestorePoint.cfg",
            out_dir=args.out / "safety",
            properties=SAFETY_PROPERTIES,
            expected_properties=SAFETY_PROPERTIES,
            checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=100,
        )
        if safety.run_failed:
            print(
                f"formal-deployment-restore-point: safety failed; see {safety.stdout_path}",
                file=sys.stderr,
            )
            return safety.exit_code or 1
        rows = list(safety.rows)
        for label, config_name, prop in WITNESSES:
            result = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=MODEL_DIR / "DeploymentRestorePointCounterexample.tla",
                config=MODEL_DIR / config_name,
                out_dir=args.out / label,
                properties=(prop,),
                expected_properties=(prop,),
                checker_command=DEFAULT_COMMAND,
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
            )
            if result.run_failed:
                print(
                    f"formal-deployment-restore-point: {label} witness failed; "
                    f"see {result.stdout_path}",
                    file=sys.stderr,
                )
                return result.exit_code or 1
            # Witnesses are executable non-vacuity gates for this checker, but
            # their counterexample module/config digests intentionally differ
            # from the registered safety bindings. Keep their detailed reports
            # in the per-witness directories; the aggregate evidence artifact
            # contains only rows that bind exactly to pact.yaml's formal entries.
    except TLCSetupError as exc:
        print(f"formal-deployment-restore-point: {exc}", file=sys.stderr)
        return exc.exit_code
    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-deployment-restore-point: TLC passed; report at {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
