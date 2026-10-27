#!/usr/bin/env python3
"""Run the AttributeState TLA+ model with TLC."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from okg.substrate.formal_tlc import (
    TLCExecutionProfile,
    TLCProperty,
    TLCSetupError,
    run_tlc_formal_report,
    select_properties,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "attributes"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "attribute-state"
SMOKE_OUT = REPO_ROOT / ".scratch" / "formal" / "attribute-state-smoke"
DEFAULT_MODEL = MODEL_DIR / "AttributeState.tla"
DEFAULT_CONFIG = MODEL_DIR / "AttributeState.cfg"
SMOKE_CONFIG = MODEL_DIR / "AttributeStateSmoke.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_attribute_state.py "
    "--profile full --config formal/attributes/AttributeState.cfg "
    "--out .scratch/formal/attribute-state "
    "--property-id AttributeStateSafety "
    "--property-id AttributeReadSurfaceSafety "
    "--property-id AttributeRetentionSafety "
    "--property-id AttributePromotionLifecycleSafety "
    "--property-id AttributeConflictSafety --timeout-seconds 600"
)
REFINEMENT_TESTS = (
    "tests/substrate/test_mcp_tools.py::test_get_node_returns_at_pin",
    "tests/substrate/external_backed/test_inspect_hydration.py::"
    "test_inspect_node_overlay_and_read_purity",
    "tests/pact/test_formal_report_producers.py::"
    "test_attribute_state_report_producer_emits_strict_rows",
)
PROPERTIES = [
    TLCProperty(
        "AttributeStateSafety",
        "TLC verified that effective attrs only expose accepted materialized "
        "observations; rejected, superseded, stale, and external-live values "
        "stay out of default reads; raw source observations remain immutable; "
        "user-graph promotion is explicit; reads resolve off the maintained "
        "overlay-range read model, which equals the ground-truth effective "
        "projection whenever it is marked fresh; and the read, retention, "
        "promotion, view-maintenance, and overwrite risk contracts hold.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "NodeEdgeSymmetry",
            "EffectiveKnown",
            "EffectiveMatchesAttribute",
            "EffectiveOnlyAcceptedProjection",
            "PinnedSnapshotKnown",
            "PinnedSnapshotOnlyMaterializedProjection",
            "PinnedSnapshotNeverUsesExternalLive",
            "RejectedNeverEffective",
            "SupersededNeverEffective",
            "StaleNeverEffective",
            "ExternalLiveMatchesAttribute",
            "ExternalLiveNeverEffective",
            "RawSourceObservationImmutable",
            "SupersededKeepsHistory",
            "AcceptedOverlayIsEffectiveOrSuperseded",
            "ReservedEffectiveOnlyRaw",
            "ReservedPinnedEffectiveOnlyRaw",
            "OneEffectivePerAttribute",
            "PromotedOnlyAcceptedMaterialized",
            "PromotedOnlyPromotable",
            "TerminalObservationRetained",
            "ReadSurfaceCoverage",
            "PinnedSnapshotKnown",
            "PinnedSnapshotOnlyMaterializedProjection",
            "PinnedSnapshotNeverUsesExternalLive",
            "LastReadUsesEffective",
            "LastReadOnlyAcceptedProjection",
            "LastReadNeverUsesExternalLive",
            "ReservedPinnedEffectiveOnlyRaw",
            "RetainedObservationsKnown",
            "AuditCriticalRetained",
            "UnretainedOnlyNonAudit",
            "ViewMatchesEffectiveWhenFresh",
            "ViewOnlyHoldsMaterialized",
            "ViewMatchesAttribute",
            "ViewNeverUsesExternalLive",
            "AttributeReadSurfaceSafety",
            "AttributeRetentionSafety",
            "AttributePromotionLifecycleSafety",
            "AttributeConflictSafety",
            "AttributeViewMaintenanceSafety",
            "AttributeStateSafety",
        ),
    ),
    TLCProperty(
        "AttributeReadSurfaceSafety",
        "TLC verified that every modeled AKMON read surface reads the effective "
        "projection, not raw history or external live overlays.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "ReadSurfaceCoverage",
            "PinnedSnapshotKnown",
            "PinnedSnapshotOnlyMaterializedProjection",
            "PinnedSnapshotNeverUsesExternalLive",
            "LastReadUsesEffective",
            "LastReadOnlyAcceptedProjection",
            "LastReadNeverUsesExternalLive",
            "ReservedPinnedEffectiveOnlyRaw",
            "AttributeReadSurfaceSafety",
        ),
    ),
    TLCProperty(
        "AttributeRetentionSafety",
        "TLC verified that audit-critical attribute observations remain "
        "retained while pruning is limited to non-critical observations.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "RetainedObservationsKnown",
            "AuditCriticalRetained",
            "UnretainedOnlyNonAudit",
            "AttributeRetentionSafety",
        ),
    ),
    TLCProperty(
        "AttributePromotionLifecycleSafety",
        "TLC verified that only promotable accepted materialized observations "
        "can be promoted and stale or superseded observations leave the "
        "promoted set.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "PromotedOnlyAcceptedMaterialized",
            "PromotedOnlyPromotable",
            "AttributePromotionLifecycleSafety",
        ),
    ),
    TLCProperty(
        "AttributeConflictSafety",
        "TLC verified that accepted overlays are not silently overwritten by "
        "raw or derived writes; they remain effective or are explicitly "
        "superseded.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "RawSourceObservationImmutable",
            "AcceptedOverlayIsEffectiveOrSuperseded",
            "ReservedEffectiveOnlyRaw",
            "OneEffectivePerAttribute",
            "AttributeConflictSafety",
        ),
    ),
]

SMOKE_PROPERTY_IDS = (
    "AttributeReadSurfaceSafety",
    "AttributeRetentionSafety",
)

PROFILES = {
    "smoke": TLCExecutionProfile(
        name="smoke",
        description=(
            "CI/operator freshness profile over a reduced observation and read "
            "surface universe, covering read-surface and retention contracts."
        ),
        config=SMOKE_CONFIG,
        out_dir=SMOKE_OUT,
        timeout_seconds=30,
        property_ids=SMOKE_PROPERTY_IDS,
        expected_property_ids=SMOKE_PROPERTY_IDS,
        full_strength=False,
        ci_smoke=True,
        min_distinct_states=23609,
    ),
    "full": TLCExecutionProfile(
        name="full",
        description=(
            "Release/PACT evidence profile over the full AttributeState config "
            "and aggregate property set."
        ),
        config=DEFAULT_CONFIG,
        out_dir=DEFAULT_OUT,
        timeout_seconds=600,
        property_ids=tuple(prop.property_id for prop in PROPERTIES),
        full_strength=True,
        ci_smoke=False,
        min_distinct_states=3550777,
    ),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="full",
        help="execution profile to apply when config/out/timeout are not supplied",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="list available execution profiles and exit",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable output for --list-profiles",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="directory for TLC logs and formal-report.yaml",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL,
        help="TLA+ module path; relative paths resolve under formal/attributes",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="TLC config path; relative paths resolve under formal/attributes",
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
        default=None,
        help="maximum seconds to allow TLC to run",
    )
    args = parser.parse_args(argv)

    if args.list_profiles:
        _emit_profiles(json_output=args.json)
        return 0

    profile = PROFILES[args.profile]
    out_dir = args.out or profile.out_dir
    config = args.config or profile.config
    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else profile.timeout_seconds
    )
    property_ids = args.property_ids or list(profile.property_ids)
    selected_properties = select_properties(PROPERTIES, property_ids)
    expected_properties = select_properties(
        PROPERTIES,
        list(profile.expected_property_ids or profile.property_ids),
    )
    checker_command = args.checker_command or _checker_command(
        args,
        out_dir=out_dir,
        config=config,
        timeout_seconds=timeout_seconds,
        property_ids=property_ids,
    )

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=config,
            out_dir=out_dir,
            properties=selected_properties,
            expected_properties=expected_properties,
            checker_command=checker_command,
            extra_refinement_tests=args.extra_refinement_tests,
            timeout_seconds=timeout_seconds,
            execution_profile=args.profile,
            profile_timeout_seconds=timeout_seconds,
            min_distinct_states=profile.min_distinct_states,
        )
    except TLCSetupError as exc:
        print(
            f"formal-attribute-state: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-attribute-state: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-attribute-state: TLC passed; report at "
        f"{result.report_path}"
    )
    return 0


def _checker_command(
    args: argparse.Namespace,
    *,
    out_dir: Path,
    config: Path,
    timeout_seconds: int,
    property_ids: list[str],
) -> str:
    parts = [
        "uv",
        "run",
        "--extra",
        "dev",
        "python",
        "scripts/check_formal_attribute_state.py",
        "--profile",
        args.profile,
    ]
    if _resolve_model_arg(args.model) != DEFAULT_MODEL.resolve():
        parts.extend(["--model", str(args.model)])
    parts.extend(["--config", _command_path(config)])
    parts.extend(["--out", _command_path(out_dir)])
    for property_id in property_ids:
        parts.extend(["--property-id", property_id])
    for refinement_test in args.extra_refinement_tests:
        parts.extend(["--refinement-test", refinement_test])
    parts.extend(["--timeout-seconds", str(timeout_seconds)])
    return " ".join(parts)


def _emit_profiles(*, json_output: bool) -> None:
    payload = {
        "default_profile": "full",
        "profiles": [
            PROFILES[name].as_dict(repo_root=REPO_ROOT)
            for name in sorted(PROFILES)
        ],
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for profile in payload["profiles"]:
        print(
            f"{profile['name']}: config={profile['config']} "
            f"out={profile['out']} timeout={profile['timeout_seconds']}s"
        )


def _resolve_model_arg(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (MODEL_DIR / path).resolve()


def _command_path(path) -> str:
    """checker_command is a durable, repo-portable string: relativize so a
    recorded command survives other checkouts. Paths under the model dir
    relativize to it (the checker resolves --config against MODEL_DIR);
    other repo paths (e.g. --out) relativize to the repo root."""
    from pathlib import Path
    resolved = Path(path).resolve()
    for base in (MODEL_DIR, REPO_ROOT):
        try:
            return str(resolved.relative_to(base.resolve()))
        except ValueError:
            continue
    return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
