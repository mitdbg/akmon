#!/usr/bin/env python
"""Run the graph-transform-ir commit-boundary property model."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, build_formal_report_rows
from okg.substrate.transform.verify import (
    COMMIT_WITNESS,
    GUARD_DOMINANCE,
    run_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "gtir-commit"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_gtir_commit.py"
)
MIN_STATES_CHECKED = 1000
PROPERTIES = (
    TLCProperty(
        COMMIT_WITNESS,
        (
            "Executable model checked that every durable graph mutation passes "
            "through the DeltaSet commit boundary and resolves to a ledgered "
            "transform program witness."
        ),
        refinement_tests=(
            "tests/substrate/transform/test_verify.py",
            "tests/substrate/transform/test_commit_witness.py",
        ),
    ),
    TLCProperty(
        GUARD_DOMINANCE,
        (
            "Executable model checked that every mutation row has a passing "
            "dominating ASSERT before it can be committed."
        ),
        refinement_tests=(
            "tests/substrate/transform/test_verify.py",
            "tests/substrate/transform/test_commit_witness.py",
        ),
    ),
)


def _validate_report_rows(rows: object) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise ValueError("run_report() must return nonempty result rows")
    expected_ids = {prop.property_id for prop in PROPERTIES}
    actual_ids: set[str] = set()
    validated: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("run_report() rows must be dictionaries")
        property_id = str(row.get("property_id"))
        if property_id in actual_ids:
            raise ValueError(f"run_report() returned duplicate row for {property_id}")
        actual_ids.add(property_id)
        if row.get("status") != "pass":
            raise ValueError(f"{property_id} did not pass")
        states_checked = row.get("states_checked")
        if type(states_checked) is not int:
            raise ValueError(f"{property_id} states_checked must be an integer")
        if states_checked < MIN_STATES_CHECKED:
            raise ValueError(
                f"{property_id} checked {states_checked} states, "
                f"below floor {MIN_STATES_CHECKED}"
            )
        counterexamples = row.get("counterexamples")
        if not isinstance(counterexamples, list) or not counterexamples:
            raise ValueError(f"{property_id} must name non-vacuity counterexamples")
        validated.append(row)
    if actual_ids != expected_ids:
        raise ValueError(
            "run_report() returned invalid property ids "
            f"(expected {sorted(expected_ids)}, got {sorted(actual_ids)})"
        )
    return validated


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "legacy_output",
        nargs="?",
        type=Path,
        help="legacy JSON artifact path; formal-report.yaml is written next to it",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="directory for checker artifact and formal-report.yaml",
    )
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
        help="PACT formal binding checker_command represented by this report",
    )
    args = parser.parse_args(argv)

    if args.out is not None:
        out_dir = args.out.resolve()
    elif args.legacy_output is not None:
        out_dir = args.legacy_output.parent.resolve()
    else:
        out_dir = DEFAULT_OUT.resolve()
    artifact = (
        args.legacy_output.resolve()
        if args.legacy_output is not None
        else out_dir / "gtir-commit-check.json"
    )

    results = run_report()
    try:
        validated = _validate_report_rows(results)
    except ValueError as exc:
        print(f"formal-gtir-commit: {exc}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"ok": True, "results": validated}
    artifact.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    rows = build_formal_report_rows(
        properties=PROPERTIES,
        status="pass",
        checker_command=args.checker_command,
        command=(
            "uv",
            "run",
            "--extra",
            "dev",
            "python",
            "scripts/check_formal_gtir_commit.py",
            "--out",
            str(out_dir),
        ),
        artifact=artifact,
        model_path=REPO_ROOT / "src" / "okg" / "substrate" / "transform" / "verify.py",
        config_path=REPO_ROOT / "scripts" / "check_formal_gtir_commit.py",
    )
    (out_dir / "formal-report.yaml").write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
