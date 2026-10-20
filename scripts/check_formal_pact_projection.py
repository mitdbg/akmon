#!/usr/bin/env python3
"""Run the PACT graph-projection TLA+ model with TLC."""
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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "pact-projection"
DEFAULT_CHECKER_COMMAND = (
    "uv run python scripts/check_formal_pact_projection.py "
    "--out .scratch/formal/pact-projection"
)
DEFAULT_REFINEMENT_TEST = (
    "tests/pact/test_pact_projection_formal.py::"
    "test_pact_projection_formal_bindings_cover_graph_native_contract"
)
DEFAULT_REPORT_PROPERTY_IDS = ("PactProjectionContract",)
PROPERTIES = [
    TLCProperty(
        "PactProjectionContract",
        "TLC verified the combined PACT projection contract.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
    ),
    TLCProperty(
        "GraphOperationsDoNotMutateFile",
        "TLC verified graph reads and publishes do not mutate pact.yaml state.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
    ),
    TLCProperty(
        "FreshnessStatusReflectsProjection",
        "TLC verified freshness labels reflect local-vs-graph state.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
    ),
    TLCProperty(
        "PublishedProjectionIsStateSnapshot",
        "TLC verified published projection structure is derived from a file snapshot.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
    ),
    TLCProperty(
        "FreshGraphCarriesProjectedDependencies",
        "TLC verified fresh projections carry current dependency edges.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
    ),
    TLCProperty(
        "FreshGraphCarriesProjectedProofs",
        "TLC verified fresh projections carry current proof edges.",
        refinement_tests=(DEFAULT_REFINEMENT_TEST,),
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
        default=MODEL_DIR / "PactProjection.tla",
        help="TLA+ module path; relative paths resolve under formal/pact",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=MODEL_DIR / "PactProjection.cfg",
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
        print(
            f"formal-pact-projection: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-pact-projection: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-pact-projection: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
