#!/usr/bin/env python3
"""Run archived Console framework TLA+ models with TLC.

These models describe the archived legacy Console, not the current operator app.
They are intentionally non-PACT helpers: a run writes ``archived-formal-report.yaml``
and never leaves an ingestible ``formal-report.yaml`` behind.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "console"
ARCHIVED_REPORT_NAME = "archived-formal-report.yaml"
TLC_ARTIFACT_DIR_NAME = "tlc-artifacts"

# (model-name, .tla file, .cfg file, expect_counterexample, property -> summary)
MODELS = {
    "vocabulary-lookup": (
        "VocabularyLookup.tla",
        "VocabularyLookup.cfg",
        False,
        [
            (
                "LookupIsDeterministic",
                "TLC verified the two-tier lookup result is deterministic.",
            ),
            (
                "DeploymentOverridesProfile",
                "TLC verified deployment overrides beat profile defaults.",
            ),
            (
                "NoSilentFallbackToSubstrateTerm",
                "TLC verified missing translations return ERROR, never the substrate term.",
            ),
        ],
    ),
    "console-focus": (
        "ConsoleFocus.tla",
        "ConsoleFocus.cfg",
        False,
        [
            (
                "AtMostOneTopmostModal",
                "TLC verified at most one modal is topmost at any reachable state.",
            ),
            (
                "EveryModalHasVisibleDismiss",
                "TLC verified every mounted modal advertises a visible dismiss target.",
            ),
            (
                "FocusReflectsTopmost",
                "TLC verified focus reflects the topmost modal or a non-modal focusable.",
            ),
            (
                "EscapeIsEnabledWheneverAModalIsOpen",
                "TLC verified Escape is enabled whenever a modal is open.",
            ),
        ],
    ),
    "offline-replay": (
        "OfflineReplay.tla",
        "OfflineReplay.cfg",
        False,
        [
            (
                "WritesAreUniquelyClassified",
                "TLC verified writes never appear in queue, committed, and triaged simultaneously.",
            ),
            (
                "WritesReplayInCommitOrder",
                "TLC verified writes replay from queue head in FIFO order.",
            ),
            (
                "ConflictsRouteToTriage",
                "TLC verified conflicting writes route to Triage; none drop silently.",
            ),
            (
                "DegradedBannerReflectsState",
                "TLC verified the degradation banner is visible iff offline or queue nonempty.",
            ),
            (
                "ConsoleNeverBlocksIndefinitely",
                "TLC verified the Console shows the banner whenever the substrate is unreachable.",
            ),
            (
                "OnlineHeadWriteEventuallyLeavesQueue",
                "TLC verified online head writes eventually leave the queue unless the Console goes offline.",
            ),
        ],
    ),
    "offline-replay-counterexample": (
        "OfflineReplayCounterexample.tla",
        "OfflineReplayCounterexample.cfg",
        True,
        [
            (
                "offline-replay-counterexample.replay-never-happens",
                "TLC produced the expected counterexample showing replay is reachable.",
            ),
        ],
    ),
    "vocabulary-lookup-counterexample": (
        "VocabularyLookupCounterexample.tla",
        "VocabularyLookupCounterexample.cfg",
        True,
        [
            (
                "vocabulary-lookup-counterexample.broken-profile-always-wins",
                "TLC produced the expected counterexample to a deliberately-false invariant.",
            ),
        ],
    ),
}
# The `archive/ui/legacy-viz-2026-06-17/...` node ids below name tests that
# PACT layout-history-openspec deleted with the rest of `archive/`; they
# resolve only at git commit 3253beb069ba. The report is archive-only either way.
REFINEMENT_TESTS_BY_PROPERTY = {
    "LookupIsDeterministic": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "DeploymentOverridesProfile": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "NoSilentFallbackToSubstrateTerm": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "AtMostOneTopmostModal": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "EveryModalHasVisibleDismiss": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "FocusReflectsTopmost": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "EscapeIsEnabledWheneverAModalIsOpen": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "WritesAreUniquelyClassified": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "WritesReplayInCommitOrder": [
        "archive/ui/legacy-viz-2026-06-17/tests/console/" +
        "test_offline_replay_formal_mapping.py::" +
        "test_online_head_progress_is_mapped_to_runtime_replay_transitions",
    ],
    "ConflictsRouteToTriage": [
        "tests/pact/test_formal_report_producers.py::" +
        "test_console_report_producers_emit_bound_invariant_ids",
    ],
    "OnlineHeadWriteEventuallyLeavesQueue": [
        "archive/ui/legacy-viz-2026-06-17/tests/console/" +
        "test_offline_replay_formal_mapping.py::" +
        "test_online_head_progress_is_mapped_to_runtime_replay_transitions",
    ],
    "offline-replay-counterexample.replay-never-happens": [
        "archive/ui/legacy-viz-2026-06-17/tests/console/" +
        "test_offline_replay_formal_mapping.py::" +
        "test_online_head_progress_is_mapped_to_runtime_replay_transitions",
    ],
}
EXPECTED_INVARIANTS_BY_PROPERTY = {
    "offline-replay-counterexample.replay-never-happens": (
        "BrokenReplayNeverHappens",
    ),
    "vocabulary-lookup-counterexample.broken-profile-always-wins": (
        "BrokenProfileAlwaysWins",
    ),
}
EXPECTED_TEMPORAL_PROPERTIES_BY_PROPERTY = {
    "OnlineHeadWriteEventuallyLeavesQueue": (
        "OnlineHeadWriteEventuallyLeavesQueue",
    ),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--archived-helper",
        action="store_true",
        help=(
            "acknowledge that this legacy Console model is archive-only and "
            "must not emit PACT formal_check evidence"
        ),
    )
    parser.add_argument(
        "--model",
        choices=sorted(MODELS),
        required=True,
        help="which Console TLA+ model to check",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "directory for TLC logs and archived-formal-report.yaml "
            "(default .scratch/formal/console-<model>)"
        ),
    )
    parser.add_argument(
        "--checker-command",
        default=None,
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

    out_dir = (
        args.out.resolve()
        if args.out is not None
        else (REPO_ROOT / ".scratch" / "formal" / f"console-{args.model}").resolve()
    )
    _remove_ingestible_report(out_dir)
    tlc_out_dir = out_dir / TLC_ARTIFACT_DIR_NAME
    _remove_ingestible_report(tlc_out_dir)
    if not args.archived_helper:
        print(
            "check_formal_console: formal/console is archived design history; "
            "rerun with --archived-helper to write non-PACT archived output",
            file=sys.stderr,
        )
        return 2

    model_file, config_file, expect_counterexample, properties = MODELS[args.model]
    checker_command = args.checker_command or _checker_command(args)
    try:
        all_properties = _tlc_properties(properties)
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=Path(model_file),
            config=Path(config_file),
            out_dir=tlc_out_dir,
            properties=select_properties(all_properties, args.property_ids),
            expected_properties=all_properties,
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            expect_counterexample=expect_counterexample,
            timeout_seconds=args.timeout_seconds,
            write_report=False,
        )
    except TLCSetupError as exc:
        print(f"check_formal_console[{args.model}]: {exc}", file=sys.stderr)
        return exc.exit_code

    archived_report_path = _write_archived_report(args.model, result, out_dir)
    _remove_ingestible_report(result.report_path.parent)
    if result.run_failed:
        print(
            f"check_formal_console[{args.model}]: TLC outcome unexpected; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        f"check_formal_console[{args.model}]: TLC outcome as expected; "
        f"archived report at {archived_report_path}"
    )
    return 0


def _remove_ingestible_report(out_dir: Path) -> None:
    (out_dir / "formal-report.yaml").unlink(missing_ok=True)


def _tlc_properties(properties: list[tuple[str, str]]) -> list[TLCProperty]:
    return [
        TLCProperty(
            prop_id,
            summary,
            refinement_tests=tuple(REFINEMENT_TESTS_BY_PROPERTY.get(prop_id, [])),
            expected_invariants=EXPECTED_INVARIANTS_BY_PROPERTY.get(prop_id, ()),
            expected_temporal_properties=EXPECTED_TEMPORAL_PROPERTIES_BY_PROPERTY.get(
                prop_id,
                (),
            ),
        )
        for prop_id, summary in properties
    ]


def _checker_command(args: argparse.Namespace) -> str:
    parts = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "scripts/check_formal_console.py",
        "--archived-helper",
        "--model",
        args.model,
    ]
    for property_id in args.property_ids:
        parts.extend(["--property-id", property_id])
    if args.out is not None:
        parts.extend(["--out", str(args.out)])
    for refinement_test in args.extra_refinement_tests:
        parts.extend(["--refinement-test", refinement_test])
    if args.timeout_seconds != 300:
        parts.extend(["--timeout-seconds", str(args.timeout_seconds)])
    return " ".join(parts)


def _write_archived_report(model: str, result, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / ARCHIVED_REPORT_NAME
    report_path.write_text(
        yaml.safe_dump(
            {
                "schema": "okg.formal.archived-helper.v1",
                "active_pact_evidence": False,
                "reason": (
                    "formal/console models describe archived legacy UI state "
                    "machines and do not refine the current operator app"
                ),
                "model": model,
                "status": "fail" if result.run_failed else "pass",
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "invariant_violated": result.invariant_violated,
                "tlc_stdout": str(result.stdout_path),
                "tlc_stderr": str(result.stderr_path),
                "properties": [
                    {
                        "property_id": row["property_id"],
                        "archived": True,
                        "summary": row.get("summary"),
                    }
                    for row in result.rows
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return report_path


if __name__ == "__main__":
    raise SystemExit(main())
