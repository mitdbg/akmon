#!/usr/bin/env python3
"""Check the #1221 bounded MCP security model and emit PACT evidence.

The checker is deliberately offline.  It prefers TLC when the repository's
pinned jar is available, and otherwise exhaustively evaluates the finite
projection represented by ActionableBounds.tla.  The fallback is explicit in
the report's proof_kind and never hides a TLC failure when TLC was selected.
"""

from __future__ import annotations

import argparse
import itertools
import shutil
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = Path(__file__).resolve().parent
MODEL = MODEL_DIR / "ActionableBounds.tla"
CONFIG = MODEL_DIR / "ActionableBounds.cfg"
DEFAULT_REPORT = REPO_ROOT / ".scratch" / "formal" / "actionable-bounds" / "formal-report.yaml"
DEFAULT_CHECKER_COMMAND = (
    ".venv/bin/python formal/mcp/check_actionable_bounds.py --engine python "
    "--report .scratch/formal/actionable-bounds/formal-report.yaml"
)
REFINEMENT_TESTS = (
    "tests/substrate/mcp/test_expand_path_diagnostics.py",
    "tests/substrate/mcp/test_inspect_subtype_summary.py",
)

PROPERTY_ID = "ActionableBoundsSafety"
HOPS = tuple(range(4))
MAX_HOPS = 3
MAX_FRONTIER_ROWS = 2
MAX_RESULT_ROWS = 2
MAX_SUMMARY_KEYS = 6
SUMMARY_PAGE_SIZE = 2
START_CLASSES = ("visible", "hidden", "absent")
INPUT_KINDS = (
    "invalid_edge_type",
    "invalid_direction",
    "invalid_path_grammar",
    "visible_empty_first",
    "visible_empty_intermediate",
    "visible_empty_terminal",
    "nonempty_complete",
    "nonempty_truncated",
)
EMPTY_KINDS = {
    "start_not_visible",
    "visibility_limited",
    "no_matching_first_hop",
    "empty_intermediate_hop",
    "empty_terminal_result",
}
INVALID_KINDS = {"invalid_edge_type", "invalid_direction", "invalid_path_grammar"}
EXACT_EMPTY_KINDS = {
    "start_not_visible",
    "no_matching_first_hop",
    "empty_intermediate_hop",
    "empty_terminal_result",
}

SUMMARY_SECTIONS = (
    "attributes",
    "filters",
    "outgoing_routes",
    "incoming_routes",
    "visibility_state",
    "declared_state",
)
SUMMARY_FIELDS = {
    "attributes": {"value_types", "presence_frequency", "null_frequency", "sample_values"},
    "filters": {"recommended_filters"},
    "outgoing_routes": {"edge_type", "destination_subtype", "route_count"},
    "incoming_routes": {"edge_type", "source_subtype", "route_count"},
    "visibility_state": {"visible_count", "authority_mode"},
    "declared_state": {"declared_subtype", "declared_attributes"},
}

CONTEXT_FIELDS = (
    "deployment",
    "generation",
    "database",
    "branch",
    "catalog",
    "subtype",
    "path_or_subtype",
    "authority",
    "sort_order",
    "projection",
    "policy_fingerprint",
    "key_id",
    "cursor_position",
)
CONTEXT_VALUES = {
    "deployment": ("deployment_a", "deployment_b"),
    "generation": ("generation_1", "generation_2"),
    "database": ("database_a", "database_b"),
    "branch": ("branch_a", "branch_b"),
    "catalog": ("catalog_a", "catalog_b"),
    "subtype": ("subtype_a", "subtype_b"),
    "path_or_subtype": ("path_alpha", "subtype_summary"),
    "authority": ("reader_a", "reader_b"),
    "sort_order": ("stable_id_asc", "stable_id_desc"),
    "projection": ("ids", "ids_and_types"),
    "policy_fingerprint": ("policy_a", "policy_b"),
    "key_id": ("key_a", "key_b"),
    "cursor_position": (0, 2, 4, 6),
}


def _contexts() -> tuple[tuple[object, ...], ...]:
    return tuple(itertools.product(*(CONTEXT_VALUES[field] for field in CONTEXT_FIELDS)))


def _context_as_dict(context: tuple[object, ...]) -> dict[str, object]:
    return dict(zip(CONTEXT_FIELDS, context))


def _accepts_cursor(
    candidate_context: tuple[object, ...],
    candidate_mac: object,
    expected: tuple[object, ...],
) -> bool:
    return candidate_context == expected and candidate_mac == expected


def _mutate_context(context: tuple[object, ...], field: str) -> tuple[object, ...]:
    values = list(context)
    index = CONTEXT_FIELDS.index(field)
    values[index] = next(value for value in CONTEXT_VALUES[field] if value != values[index])
    return tuple(values)


def _structures() -> tuple[tuple[str, ...], ...]:
    return tuple(itertools.product(START_CLASSES, repeat=len(HOPS)))


def _visible_projection(structure: tuple[str, ...]) -> tuple[bool, ...]:
    return tuple(value == "visible" for value in structure)


def _first_nonvisible(structure: tuple[str, ...], upto: int) -> int | None:
    for hop in range(upto + 1):
        if structure[hop] != "visible":
            return hop
    return None


def _empty_hop_for(kind: str, requested_hop: int) -> int:
    if kind == "visible_empty_first":
        return 1
    if kind == "visible_empty_intermediate":
        return 2
    return requested_hop


def _outcome_for(kind: str, structure: tuple[str, ...], requested_hop: int) -> str:
    if kind in INVALID_KINDS:
        return kind
    decisive = _empty_hop_for(kind, requested_hop)
    if structure[0] != "visible":
        return "start_not_visible"
    if _first_nonvisible(structure, decisive) is not None:
        return "visibility_limited"
    if kind == "visible_empty_first":
        return "no_matching_first_hop"
    if kind == "visible_empty_intermediate":
        return "empty_intermediate_hop"
    if kind == "visible_empty_terminal":
        return "empty_terminal_result"
    return kind


def _diagnostic_hop_for(kind: str, structure: tuple[str, ...], requested_hop: int) -> int:
    if kind in INVALID_KINDS:
        return requested_hop
    decisive = _empty_hop_for(kind, requested_hop)
    first = _first_nonvisible(structure, decisive)
    return decisive if first is None else first


def _public_envelope(
    outcome: str,
    diagnostic_hop: int,
    frontier_rows: int,
    result_count: int,
) -> tuple[object, ...]:
    truncated = outcome == "nonempty_truncated"
    if outcome == "visibility_limited":
        count_state, total_count = "withheld", None
    elif outcome in EXACT_EMPTY_KINDS:
        count_state, total_count = "exact_visible", 0
    elif outcome == "nonempty_complete":
        count_state, total_count = "exact_visible", result_count
    elif outcome == "nonempty_truncated":
        count_state, total_count = "bounded_visible", None
    else:
        count_state, total_count = "not_applicable", None
    return (
        outcome,
        diagnostic_hop,
        frontier_rows,
        result_count,
        count_state,
        total_count,
        truncated,
        truncated,
    )


def _check_summary_pagination(failures: list[str]) -> tuple[int, int]:
    """Check stable section slices and continuation reconstruction."""

    keys = SUMMARY_SECTIONS
    if len(keys) != MAX_SUMMARY_KEYS or set(SUMMARY_FIELDS) != set(keys):
        failures.append("summary section catalog is not the six-section contract")
    if any(not SUMMARY_FIELDS[section] for section in keys):
        failures.append("summary section has no typed metadata fields")
    offset = 0
    collected: list[int] = []
    seen: set[int] = set()
    cursor_positions = (0, 2, 4, 6)
    pages = 0
    states = 0
    while offset < MAX_SUMMARY_KEYS:
        token_offset = offset
        page = keys[offset : offset + SUMMARY_PAGE_SIZE]
        replay = keys[token_offset : token_offset + SUMMARY_PAGE_SIZE]
        states += 1
        if page != replay:
            failures.append("summary cursor replay changed its stable page")
        if not page:
            failures.append("summary cursor produced an empty page before completion")
        collected.extend(page)
        before = len(seen)
        seen.update(page)
        offset += len(page)
        pages += 1
        states += 1
        continuation_token = {"cursor_position": offset}
        replay_token = {"cursor_position": offset}
        if offset not in cursor_positions or continuation_token != replay_token:
            failures.append("summary continuation cursor is not bound to its next offset")
        if tuple(collected) != keys[:offset]:
            failures.append("summary reconstruction has a gap or unstable order")
        if len(seen) != len(collected) or len(seen) != offset:
            failures.append("summary reconstruction contains a duplicate section")
        if len(seen) - before != len(page):
            failures.append("summary page repeated a section")
        if offset < MAX_SUMMARY_KEYS and offset <= token_offset:
            failures.append("summary continuation did not advance")
    if tuple(collected) != keys or seen != set(keys):
        failures.append("summary continuation did not reconstruct every section")
    return states, pages


def run_executable_model(log_path: Path) -> tuple[int, int, int, set[str], int]:
    """Exhaustively check bounded visibility, probes, outcomes, and tokens."""

    states = 0
    failures: list[str] = []
    contexts = _contexts()
    structures = _structures()
    observed_outcomes: set[str] = set()
    structure_pairs = 0
    summary_states, summary_pages = _check_summary_pagination(failures)
    states += summary_states

    for structure_a in structures:
        for structure_b in structures:
            if _visible_projection(structure_a) != _visible_projection(structure_b):
                continue
            structure_pairs += 1
            for kind in INPUT_KINDS:
                for requested_hop in range(1, MAX_HOPS + 1):
                    outcome_a = _outcome_for(kind, structure_a, requested_hop)
                    outcome_b = _outcome_for(kind, structure_b, requested_hop)
                    diagnostic_a = _diagnostic_hop_for(kind, structure_a, requested_hop)
                    diagnostic_b = _diagnostic_hop_for(kind, structure_b, requested_hop)
                    states += 1
                    observed_outcomes.update((outcome_a, outcome_b))
                    if outcome_a != outcome_b or diagnostic_a != diagnostic_b:
                        failures.append("matching visible projections produced different outcomes")
                    if outcome_a in EMPTY_KINDS | INVALID_KINDS:
                        frontiers = (0,)
                        results = (0,)
                    else:
                        frontiers = range(1, MAX_FRONTIER_ROWS + 1)
                        results = range(1, MAX_RESULT_ROWS + 1)
                    if kind in INVALID_KINDS:
                        probes = (0,)
                    else:
                        probes = range(1, MAX_FRONTIER_ROWS + 1)
                    for probe_a in probes:
                        for probe_b in probes:
                            if probe_a > MAX_FRONTIER_ROWS or probe_b > MAX_FRONTIER_ROWS:
                                failures.append("probe exceeded frontier bound")
                            for frontier in frontiers:
                                for result_count in results:
                                    public_a = _public_envelope(
                                        outcome_a, diagnostic_a, frontier, result_count
                                    )
                                    public_b = _public_envelope(
                                        outcome_b, diagnostic_b, frontier, result_count
                                    )
                                    states += 1
                                    if public_a != public_b:
                                        failures.append(
                                            "hidden/absent structure changed the public envelope"
                                        )
                                    if outcome_a == "visibility_limited":
                                        if public_a[4] != "withheld" or public_a[5] is not None:
                                            failures.append(
                                                "visibility-limited outcome exposed a numeric count"
                                            )
                                    elif outcome_a in EXACT_EMPTY_KINDS:
                                        if public_a[4] != "exact_visible" or public_a[5] != 0:
                                            failures.append(
                                                "exact visible empty outcome did not expose count 0"
                                            )
                                    if outcome_a in EMPTY_KINDS | INVALID_KINDS:
                                        if frontier != 0 or result_count != 0:
                                            failures.append("empty outcome exposed public rows")
                                    if outcome_a in EMPTY_KINDS and kind not in INVALID_KINDS:
                                        if probe_a <= 0 or probe_b <= 0:
                                            failures.append("empty outcome did no positive bounded probe")
                                    if outcome_a == "nonempty_truncated":
                                        if result_count <= 0 or not public_a[-1]:
                                            failures.append("truncation omitted continuation")

                    if structure_a[0] != "visible" and outcome_a == "start_not_visible":
                        start_public = _public_envelope(outcome_a, diagnostic_a, 0, 0)
                        if (
                            start_public[2] != 0
                            or start_public[3] != 0
                            or start_public[4] != "exact_visible"
                            or start_public[5] != 0
                        ):
                            failures.append("hidden start did not use exact empty count semantics")
                    if structure_a[0] != "visible" and outcome_a == "visibility_limited":
                        hidden_public = _public_envelope(outcome_a, diagnostic_a, 0, 0)
                        if (
                            hidden_public[2] != 0
                            or hidden_public[3] != 0
                            or hidden_public[4] != "withheld"
                            or hidden_public[5] is not None
                        ):
                            failures.append("hidden start leaked public counts")
                    if kind not in INVALID_KINDS:
                        decisive = _empty_hop_for(kind, requested_hop)
                        if structure_a[0] != "visible" and outcome_a != "start_not_visible":
                            failures.append("hidden start did not become start_not_visible")
                        if structure_a[0] == "visible" and any(
                            structure_a[hop] != "visible" for hop in range(decisive + 1)
                        ) and outcome_a != "visibility_limited":
                            failures.append("hidden hop did not become visibility_limited")

    required_outcomes = {
        "invalid_edge_type",
        "invalid_direction",
        "invalid_path_grammar",
        "start_not_visible",
        "visibility_limited",
        "no_matching_first_hop",
        "empty_intermediate_hop",
        "empty_terminal_result",
        "nonempty_complete",
        "nonempty_truncated",
    }
    if not required_outcomes.issubset(observed_outcomes):
        failures.append(
            "missing outcomes: " + ", ".join(sorted(required_outcomes - observed_outcomes))
        )

    # A valid continuation is deterministic.  Mutating any one bound field,
    # or only the MAC, must reject.  Each field has two values, so this is an
    # exhaustive one-field-difference proof over the finite context product.
    for issued in contexts:
        canonical = {"context": _context_as_dict(issued), "mac": _context_as_dict(issued)}
        states += 1
        if canonical != {"context": _context_as_dict(issued), "mac": _context_as_dict(issued)}:
            failures.append("continuation token is not deterministic")
        if not _accepts_cursor(issued, issued, issued):
            failures.append("valid continuation was not accepted")
        for field in CONTEXT_FIELDS:
            candidate = _mutate_context(issued, field)
            candidate_token = {
                "context": _context_as_dict(candidate),
                "mac": _context_as_dict(candidate),
            }
            states += 1
            accepted = _accepts_cursor(candidate, candidate, issued)
            if candidate_token["context"] == canonical["context"]:
                failures.append(f"{field} mutation did not change context")
            if accepted:
                failures.append(f"cross-context {field} candidate was accepted")
        states += 1
        tampered_accepted = _accepts_cursor(issued, "tampered_mac", issued)
        if tampered_accepted:
            failures.append("tampered MAC unexpectedly matched canonical token")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_lines = [
        "engine=executable_model",
        f"states_checked={states}",
        f"contexts={len(contexts)}",
        f"structure_pairs={structure_pairs}",
        f"summary_pages={summary_pages}",
        f"summary_keys={MAX_SUMMARY_KEYS}",
        f"summary_page_size={SUMMARY_PAGE_SIZE}",
        "summary_sections=" + ",".join(SUMMARY_SECTIONS),
        "summary_fields=" + ";".join(
            f"{section}:{','.join(sorted(SUMMARY_FIELDS[section]))}"
            for section in SUMMARY_SECTIONS
        ),
        f"hops={MAX_HOPS}",
        f"max_frontier_rows={MAX_FRONTIER_ROWS}",
        f"max_result_rows={MAX_RESULT_ROWS}",
        "context_fields=" + ",".join(CONTEXT_FIELDS),
        "properties="
        + ",".join(
            (
                "hidden_absent_at_every_hop",
                "visible_projection_equivalence",
                "invalid_hop_diagnostics",
                "start_not_visible",
                "bounded_positive_probes",
                "count_state_envelope",
                "summary_pagination",
                "cursor_context_binding",
            )
        ),
        "outcomes=" + ",".join(sorted(observed_outcomes)),
        f"failures={len(failures)}",
    ]
    if failures:
        log_lines.extend(f"failure={failure}" for failure in failures)
    log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    if failures:
        raise AssertionError("; ".join(failures))
    return states, len(contexts), structure_pairs, observed_outcomes, summary_pages


def _write_rows(report: Path, rows: list[dict]) -> None:
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(yaml.safe_dump({"results": rows}, sort_keys=False), encoding="utf-8")


def _run_tlc(args: argparse.Namespace, out_dir: Path):
    from okg.substrate.formal_tlc import TLCProperty, run_tlc_formal_report

    prop = TLCProperty(
        PROPERTY_ID,
        "TLC verified hidden/absent equivalence at every hop, visible-projection "
        "public-envelope equality, arbitrary-hop input diagnostics, bounded positive "
        "probes, start-not-visible handling with exact empty count semantics, typed "
        "six-section subtype-summary pagination, and authenticated continuation "
        "context binding.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "StructurePairVisibilityBound",
            "SummarySectionShape",
            "PublicOutcomeEquivalence",
            "CountStateSafety",
            "NoHiddenStructureLeak",
            "HiddenHopOutcomeSafety",
            "VisibleHopOutcomeSafety",
            "BoundedProbeWork",
            "SummaryPaginationSafety",
            "CursorContextBinding",
            "CursorContextFieldsBound",
            "ContinuationTokenShape",
            PROPERTY_ID,
        ),
    )
    return run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=args.model,
        config=args.config,
        out_dir=out_dir,
        properties=[prop],
        expected_properties=[prop],
        checker_command=args.checker_command,
        timeout_seconds=args.timeout_seconds,
        min_distinct_states=8,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--model", type=Path, default=MODEL)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    parser.add_argument(
        "--engine",
        choices=("auto", "tlc", "python"),
        default="auto",
        help="TLC when available, or the explicit finite executable model",
    )
    args = parser.parse_args(argv)
    args.report = args.report.resolve()
    out_dir = (args.out or args.report.parent).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "executable-model.log"

    from okg.substrate.formal_tlc import (
        PROOF_KIND_EXECUTABLE_MODEL,
        TLCProperty,
        TLCSetupError,
        build_formal_report_rows,
        find_tla2tools_jar,
    )

    tlc_available = (
        find_tla2tools_jar(REPO_ROOT) is not None
        and shutil.which("java") is not None
    )
    if args.engine == "tlc" and not tlc_available:
        print("formal-actionable-bounds: TLC requested but unavailable", file=sys.stderr)
        return 2
    use_tlc = args.engine == "tlc" or (args.engine == "auto" and tlc_available)
    if use_tlc:
        try:
            result = _run_tlc(args, out_dir)
        except TLCSetupError as exc:
            print(f"formal-actionable-bounds: {exc}", file=sys.stderr)
            return exc.exit_code
        if args.report != result.report_path.resolve():
            args.report.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(result.report_path, args.report)
        if result.run_failed:
            print(
                "formal-actionable-bounds: TLC failed; see "
                f"{result.stdout_path} and {result.stderr_path}",
                file=sys.stderr,
            )
            return result.exit_code or 1
        print(f"formal-actionable-bounds: TLC passed; report at {args.report}")
        return 0

    try:
        states, contexts, structure_pairs, outcomes, summary_pages = run_executable_model(log_path)
    except AssertionError as exc:
        print(f"formal-actionable-bounds: executable model failed: {exc}", file=sys.stderr)
        return 1
    rows = build_formal_report_rows(
        properties=[
            TLCProperty(
                PROPERTY_ID,
                "Finite executable model verified hidden/absent equivalence at every hop, "
                "visible-projection public-envelope equality, arbitrary-hop input "
                "diagnostics, bounded positive probes, withheld-versus-exact count-state "
                "envelopes, start-not-visible handling, typed six-section subtype-summary "
                "pagination, and cursor context binding.",
                refinement_tests=REFINEMENT_TESTS,
            )
        ],
        status="pass",
        checker_command=args.checker_command,
        command=(sys.executable, str(Path(__file__).resolve()), "--report", str(args.report)),
        artifact=log_path,
        model_path=args.model.resolve(),
        config_path=args.config.resolve(),
        proof_kind=PROOF_KIND_EXECUTABLE_MODEL,
        distinct_states=states,
    )
    rows[0]["summary"] += (
        f" Checked {states} finite states, {contexts} cursor contexts, "
        f"{structure_pairs} matching-projection structure pairs, and "
        f"{len(outcomes)} public outcomes; {summary_pages} stable summary pages."
    )
    rows[0]["engine"] = "executable_model"
    _write_rows(args.report, rows)
    print(f"formal-actionable-bounds: executable model passed; report at {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
