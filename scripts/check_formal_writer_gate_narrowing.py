#!/usr/bin/env python3
"""Run the writer/gate narrowing-equivalence TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "transform"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "writer-gate-narrowing"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_writer_gate_narrowing.py "
    "--out .scratch/formal/writer-gate-narrowing"
)
REFINEMENT_TESTS_BY_PROPERTY = {
    "WriterGateNarrowingEquivalence": [
        "tests/substrate/test_narrowing_equivalence.py::" +
        "test_writer_and_gate_allowed_sets_match_refinement_closure",
        "tests/substrate/test_narrowing_equivalence.py::" +
        "test_writer_gate_allowed_sets_require_same_catalog_version",
        "tests/substrate/test_narrowing_equivalence.py::" +
        "test_transform_gate_noops_narrowing_when_catalog_has_no_rows",
        "tests/substrate/test_narrowing_equivalence.py::" +
        "test_writer_and_gate_agree_under_incomplete_reflexive_closure",
    ],
}
EXPECTED_INVARIANTS = (
    "TypeOK",
    "WriterGateNarrowingEquivalence",
    "AllTuplesEquivalent",
    "CatalogVersionMismatchCanDisagree",
    "InactiveCatalogPermitsAll",
    "AliasRowsDoNotCanonicalizeNarrowing",
    "AllowedTriplesUseCatalogSubtypesOnly",
    "RepresentativeRefinementCases",
    "WriterGateNarrowingContract",
)
PROPERTIES = [
    TLCProperty(
        "WriterGateNarrowingEquivalence",
        "TLC verified writer-path and TransformConsistencyGate narrowing semantics accept the same tuples when they use the same catalog version, including inactive catalogs, subtype-refinement closure, and inert alias rows.",
        refinement_tests=tuple(
            REFINEMENT_TESTS_BY_PROPERTY["WriterGateNarrowingEquivalence"]
        ),
        expected_invariants=EXPECTED_INVARIANTS,
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
        default=MODEL_DIR / "WriterGateNarrowingEquivalence.tla",
        help="TLA+ module path; relative paths resolve under formal/transform",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "WriterGateNarrowingEquivalence.cfg",
        help="TLC config path; relative paths resolve under formal/transform",
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
        print(
            f"formal-writer-gate-narrowing: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-writer-gate-narrowing: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-writer-gate-narrowing: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
