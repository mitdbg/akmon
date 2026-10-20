#!/usr/bin/env python3
"""Model-check Chronos retention compaction and its non-vacuity witnesses."""
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
MODEL_DIR = REPO_ROOT / "formal" / "chronos"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "chronos-retention-compaction"
DEFAULT_COMMAND = (
    "uv run --extra dev python scripts/check_formal_chronos_retention_compaction.py "
    "--out .scratch/formal/chronos-retention-compaction"
)
REFINEMENT_TESTS = ("tests/substrate/test_chronos_compaction.py",)
SAFETY = TLCProperty(
    "RetentionCompactionSafety",
    "TLC verified protected/current read preservation, complete plan binding, "
    "per-table crash-resumable progress, tombstone refusal, and replay.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=(
        "CurrentAndProtectedReadsPreserved",
        "FloorNeverCrossesProtection",
        "PublishAndCompactionMutuallyExclusive",
        "PlanBindingsFreshWhileMutating",
        "TableBatchesAreAtomicAndOrdered",
        "HistoricalReadsAreCompleteOrRefused",
        "EveryActiveReaderHasLease",
        "CrashStateIsValidOrResumable",
        "RetiredGenerationRefusalIsStable",
        "ReplayIsIdempotent",
        "RetentionCompactionSafety",
    ),
)
PROTECTION_WITNESS = TLCProperty(
    "RetentionCompactionProtectionWitness",
    "TLC produced the expected counterexample when protection is ignored.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=("CurrentAndProtectedReadsPreserved",),
)
LOCK_WITNESS = TLCProperty(
    "RetentionCompactionLockWitness",
    "TLC produced the expected counterexample when publish may overlap compaction.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=("PublishAndCompactionMutuallyExclusive",),
)
STALE_PLAN_WITNESS = TLCProperty(
    "RetentionCompactionStalePlanWitness",
    "TLC produced the expected counterexample when stale plan bindings may mutate.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=("PlanBindingsFreshWhileMutating",),
)
READ_INTERLEAVING_WITNESS = TLCProperty(
    "RetentionCompactionReadInterleavingWitness",
    "TLC produced the expected counterexample when an active historical reader "
    "is allowed without a protection lease.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=("HistoricalReadsAreCompleteOrRefused",),
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
            model=MODEL_DIR / "ChronosRetentionCompaction.tla",
            config=MODEL_DIR / "ChronosRetentionCompaction.cfg",
            out_dir=args.out / "safety",
            properties=[SAFETY],
            expected_properties=[SAFETY],
            checker_command=DEFAULT_COMMAND,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=10,
        )
        if safety.run_failed:
            print(f"formal-chronos-retention: safety failed; see {safety.stdout_path}", file=sys.stderr)
            return safety.exit_code or 1
        witnesses = []
        for label, config, prop in (
            (
                "protection-witness",
                MODEL_DIR / "ChronosRetentionCompactionProtectionCounterexample.cfg",
                PROTECTION_WITNESS,
            ),
            (
                "lock-witness",
                MODEL_DIR / "ChronosRetentionCompactionLockCounterexample.cfg",
                LOCK_WITNESS,
            ),
            (
                "stale-plan-witness",
                MODEL_DIR / "ChronosRetentionCompactionStalePlanCounterexample.cfg",
                STALE_PLAN_WITNESS,
            ),
            (
                "read-interleaving-witness",
                MODEL_DIR / "ChronosRetentionCompactionReadInterleavingCounterexample.cfg",
                READ_INTERLEAVING_WITNESS,
            ),
        ):
            result = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=MODEL_DIR / "ChronosRetentionCompaction.tla",
                config=config,
                out_dir=args.out / label,
                properties=[prop],
                expected_properties=[prop],
                checker_command=DEFAULT_COMMAND,
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
            )
            if result.run_failed:
                print(f"formal-chronos-retention: {label} failed; see {result.stdout_path}", file=sys.stderr)
                return result.exit_code or 1
            witnesses.extend(result.rows)
    except TLCSetupError as exc:
        print(f"formal-chronos-retention: {exc}", file=sys.stderr)
        return exc.exit_code

    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump({"results": [*safety.rows, *witnesses]}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-chronos-retention: TLC passed; report at {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
