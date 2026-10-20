#!/usr/bin/env python3
"""Run the entity-merge atomicity model and emit strict PACT evidence."""

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
MODEL_DIR = REPO_ROOT / "formal" / "entity_resolution"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "entity-resolution"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_entity_resolution.py "
    "--out .scratch/formal/entity-resolution"
)
PROPERTIES = [
    TLCProperty(
        "EntityMergeAtomicity",
        "TLC verified sequential two-merge fold, whole-branch token refusal, "
        "identity/provenance safety, Nomos refusal, rollback, and publish coupling.",
        refinement_tests=(
            "tests/substrate/entity_resolution/test_merge_rehome.py",
            "tests/substrate/entity_resolution/test_merge_identity_projection.py",
            "tests/substrate/entity_resolution/test_merge_dedup_provenance.py",
        ),
        expected_invariants=(
            "TypeOK",
            "NoDangling",
            "BatchFold",
            "PrefixPreserved",
            "ReceiptCoupled",
            "PlanIdentityInjective",
            "WholeBranchTokenPinned",
            "StagingIsPrivate",
            "RefusalPreservesPrefix",
            "CandidateProjectionInvariant",
            "CandidatePairSafety",
            "SemanticConfidenceSafety",
            "NomosEnforceNoAlias",
            "PublishedCoupling",
        ),
    )
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--model", type=Path, default=MODEL_DIR / "EntityMerge.tla")
    parser.add_argument("--config", type=Path, default=MODEL_DIR / "EntityMerge.cfg")
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    parser.add_argument(
        "--refinement-test",
        action="append",
        default=[],
        dest="extra_refinement_tests",
    )
    parser.add_argument(
        "--property-id", action="append", default=[], dest="property_ids"
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
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
            min_distinct_states=66,
        )
    except TLCSetupError as exc:
        print(f"formal-entity-resolution: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-entity-resolution: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-entity-resolution: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
