#!/usr/bin/env python3
"""Model-check strict source-registry activation and its guard witness."""
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
MODEL_DIR = REPO_ROOT / "formal" / "library_config"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "library-config-admission"
DEFAULT_COMMAND = (
    "uv run --extra dev python scripts/check_library_config_admission_model.py"
)
REFINEMENT = (
    "tests/substrate/test_source_registry_contract.py",
    "tests/substrate/test_adapter_factory.py",
)
SAFETY = TLCProperty(
    "AllSideEffectsRequireDocumentBarrier",
    "TLC verified that a two-entry strict_v1 document cannot construct, "
    "mutate, or execute until every entry passes schema, semantic, and "
    "binding validation and the document barrier is recorded.",
    refinement_tests=REFINEMENT,
    expected_invariants=(
        "TypeOK",
        "AllSideEffectsRequireDocumentBarrier",
        "RefusedDocumentHasNoSideEffects",
        "ExecutedEntriesUseFrozenStrictBindings",
        "LibraryConfigAdmissionSafety",
    ),
)
COUNTEREXAMPLE = TLCProperty(
    "StrictAdmissionGuardRemovalWitness",
    "TLC produced the expected counterexample when one valid entry may start "
    "before the rest of the strict document has been admitted.",
    refinement_tests=REFINEMENT,
    expected_invariants=("AllSideEffectsRequireDocumentBarrier",),
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
            model=MODEL_DIR / "LibraryConfigAdmission.tla",
            config=MODEL_DIR / "LibraryConfigAdmission.cfg",
            out_dir=args.out / "safety",
            properties=[SAFETY],
            expected_properties=[SAFETY],
            checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=25,
        )
        if safety.run_failed:
            print(
                "formal-library-config-admission: safety failed; see "
                f"{safety.stdout_path}",
                file=sys.stderr,
            )
            return safety.exit_code or 1
        witness = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "LibraryConfigAdmission.tla",
            config=MODEL_DIR / "LibraryConfigAdmissionCounterexample.cfg",
            out_dir=args.out / "counterexample",
            properties=[COUNTEREXAMPLE],
            expected_properties=[COUNTEREXAMPLE],
            checker_command=DEFAULT_COMMAND,
            expect_counterexample=True,
            timeout_seconds=args.timeout_seconds,
        )
        if witness.run_failed:
            print(
                "formal-library-config-admission: counterexample failed; see "
                f"{witness.stdout_path}",
                file=sys.stderr,
            )
            return witness.exit_code or 1
    except TLCSetupError as exc:
        print(f"formal-library-config-admission: {exc}", file=sys.stderr)
        return exc.exit_code

    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump(
            {"results": [*safety.rows, *witness.rows]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print(
        "formal-library-config-admission: TLC passed; "
        f"report at {report}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
