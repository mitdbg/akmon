#!/usr/bin/env python3
"""Run the source-retirement/liveness TLA+ model and emit PACT evidence."""

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
MODEL_DIR = REPO_ROOT / "formal" / "source_retirement_liveness"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "source-retirement-liveness"
PROPERTY = TLCProperty(
    "formal.source-retirement-liveness.safety",
    (
        "TLC verified fail-closed PR staging, identity and stale-plan refusals, "
        "deployment binding, chunk and post-terminal recovery, repeat apply, "
        "frontier-scoped any/all cleanup, edge integrity, blocked publication, "
        "and pinned historical retention."
    ),
    refinement_tests=(
        "tests/substrate/test_source_retirement.py",
        "tests/substrate/test_derived_node_liveness_e2e.py",
    ),
    expected_invariants=(
        "TypeOK",
        "UnrelatedContentPreserved",
        "PR1FrontierRefused",
        "StalePlanHasNoEffect",
        "IdentityRefusalHasNoEffect",
        "ProducerCollisionHasNoEffect",
        "ChunkReplayIdempotent",
        "RepeatedApplyIdempotent",
        "TerminalReceiptComplete",
        "ReceiptBeforeRegistryDisable",
        "BlockedPublishInvisible",
        "PublishedSupportSound",
        "NoDanglingPublishedEdges",
        "PublishedPointerAtomic",
        "HistoricalSnapshotRetained",
        "PinnedHistoryPreserved",
        "AnyAndAllSemantics",
        "MissingPolicyNeverDefaults",
    ),
    expected_temporal_properties=("RecoveryStateRemainsExplicit",),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--property-id",
        action="append",
        default=[],
        dest="property_ids",
        help="property_id row to emit; repeat to emit multiple bound rows",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    command_parts = [
        "uv run --extra dev python scripts/check_formal_source_retirement_liveness.py",
        f"--out {args.out}",
    ]
    command_parts.extend(
        f"--property-id {property_id}" for property_id in args.property_ids
    )
    if args.timeout_seconds != 120:
        command_parts.append(f"--timeout-seconds {args.timeout_seconds}")
    command = " ".join(command_parts)
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "SourceRetirementLiveness.tla",
            config=MODEL_DIR / "SourceRetirementLiveness.cfg",
            out_dir=args.out,
            properties=select_properties([PROPERTY], args.property_ids),
            expected_properties=[PROPERTY],
            checker_command=command,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=12,
        )
    except TLCSetupError as exc:
        print(f"formal-source-retirement-liveness: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            f"formal-source-retirement-liveness: failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-source-retirement-liveness: passed; report at "
        f"{result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
