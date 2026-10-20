#!/usr/bin/env python3
"""Model-check scholarly BibTeX merge and venue-shape safety."""
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
MODEL_DIR = REPO_ROOT / "formal" / "scholarly"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "scholarly-source-shapes"
DEFAULT_COMMAND = (
    "uv run --extra dev python scripts/check_formal_scholarly_source_shapes.py "
    "--out .scratch/formal/scholarly-source-shapes"
)
REFINEMENT = (
    "tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py",
    "tests/substrate/library/linkers/test_declarative_multivalue.py",
)
SAFETY = TLCProperty(
    "ScholarlySourceShapeSafety",
    "TLC verified lossless replay-safe cite-key union and canonical venue shapes.",
    refinement_tests=REFINEMENT,
    expected_invariants=(
        "TypeOK",
        "NoLostOrInventedKeys",
        "OnlyDatedConferenceEntriesMintConference",
        "OnlyCanonicalVenuesMinted",
        "AliasConfigurationSafe",
        "ReplayIdempotent",
        "ScholarlySourceShapeSafety",
    ),
)
AMBIGUOUS = TLCProperty(
    "ScholarlySourceShapeAmbiguousAliasWitness",
    "Expected counterexample for one normalized alias selecting two canonicals.",
    refinement_tests=REFINEMENT,
    expected_invariants=("AliasConfigurationSafe",),
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
            model=MODEL_DIR / "ScholarlySourceShapes.tla",
            config=MODEL_DIR / "ScholarlySourceShapes.cfg",
            out_dir=args.out / "safety",
            properties=[SAFETY],
            expected_properties=[SAFETY],
            checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=8,
        )
        if safety.run_failed:
            print(
                f"formal-scholarly-source-shapes: safety failed; "
                f"see {safety.stdout_path}",
                file=sys.stderr,
            )
            return safety.exit_code or 1
        witness = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "ScholarlySourceShapes.tla",
            config=MODEL_DIR / "ScholarlySourceShapesAmbiguousAlias.cfg",
            out_dir=args.out / "ambiguous-alias",
            properties=[AMBIGUOUS],
            expected_properties=[AMBIGUOUS],
            checker_command=DEFAULT_COMMAND,
            expect_counterexample=True,
            timeout_seconds=args.timeout_seconds,
        )
        if witness.run_failed:
            print(
                f"formal-scholarly-source-shapes: witness failed; "
                f"see {witness.stdout_path}",
                file=sys.stderr,
            )
            return witness.exit_code or 1
    except TLCSetupError as exc:
        print(f"formal-scholarly-source-shapes: {exc}", file=sys.stderr)
        return exc.exit_code

    rows = [*safety.rows, *witness.rows]
    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False), encoding="utf-8"
    )
    print(f"formal-scholarly-source-shapes: TLC passed; report at {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
