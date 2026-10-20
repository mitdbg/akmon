#!/usr/bin/env python3
"""Run the TransformGateDirtyScope TLA+ model with TLC."""
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
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "transform-gate-dirty-scope"
SMOKE_OUT = REPO_ROOT / ".scratch" / "formal" / "transform-gate-dirty-scope-smoke"
DEFAULT_MODEL = MODEL_DIR / "TransformGateDirtyScope.tla"
DEFAULT_CONFIG = MODEL_DIR / "TransformGateDirtyScope.cfg"
SMOKE_CONFIG = MODEL_DIR / "TransformGateDirtyScopeSmoke.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_transform_gate_dirty_scope.py "
    "--profile full --config formal/consistency/TransformGateDirtyScope.cfg "
    "--out .scratch/formal/transform-gate-dirty-scope "
    "--property-id TransformGateDirtyScopeSoundness --timeout-seconds 900"
)
REFINEMENT_TESTS = (
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_dirty_scope_extraction_covers_node_edge_changes",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_dirty_scope_unavailable_forces_full_work",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_scoped_additive_delta_blocks_new_violation",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_scoped_dirty_node_subtype_change_checks_incident_edges",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_does_not_expand_incident_edges_for_additive_nodes",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chronos_gate_incremental_fallback_reasons_run_full_gate",
    "tests/substrate/test_chronos_narrowing_gate.py::"
    "test_chunk_retraction_cannot_publish_dangling_enricher_citation",
)
PROPERTIES = [
    TLCProperty(
        "TransformGateDirtyScopeSoundness",
        "TLC verified transform gate candidate selection, fallback, and invalid-edge blocking.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "CandidateSelectionMatchesDirtyScope",
            "PreviousCleanDeltaCoversInvalidEdges",
            "InvalidDirtyEdgeCannotPublish",
            "TopologyDirtyIncidentEdgesInScope",
            "AdditiveNodesDoNotExpandIncidentEdges",
            "UnsafeDirtyScopeFallsBackFull",
            "NoInvalidPostEdgePublishes",
            "MissingSourceEndpointCannotPublish",
            "MissingDestinationEndpointCannotPublish",
            "CleanPostEdgesPass",
            "TransformGateDirtyScopeSoundness",
        ),
    ),
]

PROFILES = {
    "smoke": TLCExecutionProfile(
        name="smoke",
        description=(
            "CI/operator freshness profile with a constrained dirty-scope "
            "universe that preserves dirty edge, incident topology, additive "
            "node, retraction, and fallback cases."
        ),
        config=SMOKE_CONFIG,
        out_dir=SMOKE_OUT,
        timeout_seconds=30,
        property_ids=("TransformGateDirtyScopeSoundness",),
        full_strength=False,
        ci_smoke=True,
        min_distinct_states=3750,
    ),
    "full": TLCExecutionProfile(
        name="full",
        description=(
            "Release/PACT evidence profile over the full dirty-scope universe, "
            "with one independently source/destination-missing endpoint edge."
        ),
        config=DEFAULT_CONFIG,
        out_dir=DEFAULT_OUT,
        timeout_seconds=900,
        property_ids=tuple(prop.property_id for prop in PROPERTIES),
        full_strength=True,
        ci_smoke=False,
        min_distinct_states=5475600,
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
        help="TLA+ module path; relative paths resolve under formal/consistency",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="TLC config path; relative paths resolve under formal/consistency",
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
            f"formal-transform-gate-dirty-scope: {exc}",
            file=sys.stderr,
        )
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-transform-gate-dirty-scope: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(
        "formal-transform-gate-dirty-scope: TLC passed; report at "
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
        "scripts/check_formal_transform_gate_dirty_scope.py",
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
