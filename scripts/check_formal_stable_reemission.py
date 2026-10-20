#!/usr/bin/env python3
"""Check stable source re-emission and its backend-guard counterexample."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "source_sync"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "stable-reemission"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_stable_reemission.py "
    "--out .scratch/formal/stable-reemission"
)
EXPECTED_INVARIANTS = (
    "TypeOK",
    "StableSourceEmission",
    "SubstitutePreservesAuthority",
    "HistoryRestoresMissingSubstitute",
    "DuplicateStableWakeupsDoNotVersion",
    "StableReemission",
)
PROPERTY = TLCProperty(
    "StableReemission",
    (
        "TLC verified dirty-state and execution-id changes cannot alter fixed "
        "git-history emissions, explicit substitutes preserve an authoritative "
        "owner through takeover/retraction/recovery, and graph-bypassing exact "
        "replays cannot grow interval versions; isolated witnesses refuted "
        "each source, ownership, and backend guard removal."
    ),
    refinement_tests=(
        "tests/substrate/library/sources/test_git_graph_history.py::"
        "test_fixed_refs_are_stable_across_worktree_and_run_ids",
        "tests/substrate/test_chronos_graph_store.py::"
        "test_identical_upserts_do_not_add_interval_versions",
        "tests/substrate/test_chronos_graph_store.py::"
        "test_concurrent_identical_changed_upserts_add_one_interval_version",
        "tests/substrate/test_chronos_graph_store.py::"
        "test_substitute_node_preserves_concurrent_authoritative_owner",
        "tests/substrate/test_git_history_reemission_e2e.py::"
        "test_repeated_git_history_reconcile_has_zero_interval_delta",
    ),
    expected_invariants=EXPECTED_INVARIANTS,
)
WITNESSES = (
    (
        "no-backend-guard",
        "StableReemissionNoBackendGuardWitness.cfg",
        TLCProperty(
            "DuplicateStableWakeupsDoNotVersion",
            "TLC refuted version idempotence with only the backend guard disabled.",
            expected_invariants=("DuplicateStableWakeupsDoNotVersion",),
        ),
    ),
    (
        "worktree-membership",
        "StableReemissionWorktreeMembershipWitness.cfg",
        TLCProperty(
            "StableSourceEmission",
            "TLC refuted stable emission when membership consulted worktree dirt.",
            expected_invariants=("StableSourceEmission",),
        ),
    ),
    (
        "run-revision",
        "StableReemissionRunRevisionWitness.cfg",
        TLCProperty(
            "StableSourceEmission",
            "TLC refuted stable emission when revisions used execution identity.",
            expected_invariants=("StableSourceEmission",),
        ),
    ),
    (
        "substitute-clobber",
        "StableReemissionClobberWitness.cfg",
        TLCProperty(
            "SubstitutePreservesAuthority",
            "TLC refuted ownership safety when substitute clobbering was enabled.",
            expected_invariants=("SubstitutePreservesAuthority",),
        ),
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    args = parser.parse_args(argv)

    try:
        for witness_name, config_name, witness_property in WITNESSES:
            witness = run_tlc_formal_report(
                repo_root=REPO_ROOT,
                model_dir=MODEL_DIR,
                model=MODEL_DIR / "StableReemission.tla",
                config=MODEL_DIR / config_name,
                out_dir=args.out / f"nonvacuity-{witness_name}",
                properties=[witness_property],
                checker_command=args.checker_command,
                expected_properties=[witness_property],
                expect_counterexample=True,
                timeout_seconds=args.timeout_seconds,
                write_report=False,
            )
            if witness.run_failed:
                print(
                    f"formal-stable-reemission: {witness_name} witness failed; "
                    f"see {witness.stdout_path}",
                    file=sys.stderr,
                )
                return witness.exit_code or 1

        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "StableReemission.tla",
            config=MODEL_DIR / "StableReemission.cfg",
            out_dir=args.out,
            properties=[PROPERTY],
            checker_command=args.checker_command,
            expected_properties=[PROPERTY],
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=16,
        )
    except TLCSetupError as exc:
        print(f"formal-stable-reemission: {exc}", file=sys.stderr)
        return exc.exit_code

    if result.run_failed:
        print(
            f"formal-stable-reemission: TLC failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-stable-reemission: TLC and all guard-removal witnesses passed; "
        f"report at {result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
