#!/usr/bin/env python3
"""Check successful-watermark backlog drainage and both guard-off witnesses."""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    build_formal_report_rows,
    find_tla2tools_jar,
    run_tlc_formal_report,
    tlc_jvm_args,
    tlc_workers,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "consistency"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "enricher-backlog-drain"
DEFAULT_MODEL = MODEL_DIR / "EnricherBacklogDrain.tla"
POSITIVE_CONFIG = MODEL_DIR / "EnricherBacklogDrain.cfg"
PRE_REUSE_COUNTEREXAMPLE_CONFIG = (
    MODEL_DIR / "EnricherBacklogDrainPreReuseStrandedCounterexample.cfg"
)
POST_REUSE_COUNTEREXAMPLE_CONFIG = (
    MODEL_DIR / "EnricherBacklogDrainPostReuseUnsafeCounterexample.cfg"
)
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_enricher_backlog_drain.py "
    "--out .scratch/formal/enricher-backlog-drain"
)
MIN_DISTINCT_STATES = 4
MIN_PRE_REUSE_WITNESS_STATES = 3
MIN_POST_REUSE_WITNESS_STATES = 2

REFINEMENT_TESTS = (
    "tests/substrate/test_publisher.py::"
    "test_pending_enricher_backlog_prevents_noop_reuse_until_dag_drains",
    "tests/substrate/test_publisher.py::"
    "test_enricher_backlog_probe_ignores_unread_subtypes",
    "tests/substrate/test_publisher.py::"
    "test_unprovable_enricher_cleanliness_prevents_early_noop_reuse",
    "tests/substrate/test_publisher.py::"
    "test_pending_enricher_backlog_prevents_post_enrichment_noop_reuse",
    "tests/substrate/test_publisher.py::"
    "test_matching_retraction_is_pending_enricher_backlog",
)
POSITIVE_PROPERTIES = [
    TLCProperty(
        "EveryEligibleInputEventuallyProcessed",
        "TLC exhausted the finite acyclic two-stage model under weak fairness "
        "on publish cycles. With both generation-reuse sites consulting "
        "successful watermark lag, the downstream stage eventually consumes "
        "the upstream fact and publishes its graph output; neither reuse site "
        "returns while eligible backlog exists.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "WatermarkWithinFrontier",
            "ReuseOnlyWhenNoEligibleBacklog",
        ),
        expected_temporal_properties=(
            "EveryEligibleInputEventuallyProcessed",
        ),
    ),
]
PRE_REUSE_COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "EnricherBacklogPreReuseStrandedCounterexample",
        "TLC produced the pre-reuse-guard-disabled stranded-backlog lasso "
        "under the same fair specification while the post-reuse guard stayed "
        "enabled: the upstream fact checkpoints, source traffic remains "
        "quiescent, and pre-enrichment reuse skips downstream forever.",
        refinement_tests=REFINEMENT_TESTS,
        expected_temporal_properties=(
            "EveryEligibleInputEventuallyProcessed",
        ),
    ),
]
POST_REUSE_COUNTEREXAMPLE_PROPERTIES = [
    TLCProperty(
        "EnricherBacklogPostReuseUnsafeCounterexample",
        "TLC independently disabled only the post-enrichment guard and "
        "refuted ReuseOnlyWhenNoEligibleBacklog: a graph-idempotent upstream "
        "emission creates watermark lag after the phase-start snapshot, then "
        "the final fast path unsafely reuses the generation.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("ReuseOnlyWhenNoEligibleBacklog",),
    ),
]

_TEMPORAL_VIOLATION = "Temporal properties were violated"
_COUNTEREXAMPLE_TRACE = "The following behavior constitutes a counter-example"
_INVARIANT_VIOLATION = (
    "Invariant ReuseOnlyWhenNoEligibleBacklog is violated"
)
_HARD_ERROR_MARKERS = (
    "Parsing or semantic analysis failed",
    "TLC threw an unexpected exception",
    "Semantic errors:",
    "java.lang.",
    "Exception in thread",
)


def _distinct_states(stdout: str) -> int | None:
    matches = re.findall(r"(\d+)\s+distinct states found", stdout)
    return int(matches[-1]) if matches else None


def _report_line(label: str, ok: bool, states: int | None, wall: float) -> str:
    verdict = (
        ("proved clean" if label == "positive" else "counterexample produced")
        if ok
        else "FAILED"
    )
    states_text = (
        f"{states} distinct states"
        if states is not None
        else "distinct states: unparsed"
    )
    return (
        f"formal-enricher-backlog: [{label:10s}] {verdict:24s} "
        f"{states_text:24s} {wall:5.1f}s"
    )


def _run_positive(
    args: argparse.Namespace,
    out_dir: Path,
) -> tuple[list[dict], int]:
    started = time.monotonic()
    result = run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=args.model,
        config=POSITIVE_CONFIG,
        out_dir=out_dir / "positive",
        properties=POSITIVE_PROPERTIES,
        expected_properties=POSITIVE_PROPERTIES,
        checker_command=args.checker_command,
        timeout_seconds=args.timeout_seconds,
        min_distinct_states=MIN_DISTINCT_STATES,
    )
    wall = time.monotonic() - started
    print(
        _report_line(
            "positive",
            not result.run_failed,
            result.distinct_states,
            wall,
        ),
        flush=True,
    )
    if result.run_failed:
        print(
            "formal-enricher-backlog: positive check failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return [], result.exit_code or 1
    return list(result.rows), 0


def _run_counterexample(
    args: argparse.Namespace,
    out_dir: Path,
    *,
    label: str,
    config: Path,
    properties: list[TLCProperty],
    expected_exit_code: int,
    expected_violation: str,
    temporal: bool,
    min_distinct_states: int,
) -> tuple[list[dict], int]:
    jar = find_tla2tools_jar(REPO_ROOT)
    if jar is None:
        raise TLCSetupError(
            "TLC is required but no tla2tools.jar was found. Set "
            "TLA2TOOLS_JAR or run scripts/bootstrap_formal_admission.py."
        )
    java = shutil.which("java")
    if java is None:
        raise TLCSetupError("Java is required to run TLC but was not found.")

    witness_dir = (out_dir / label).resolve()
    witness_dir.mkdir(parents=True, exist_ok=True)
    metadir = witness_dir / "tlc-meta"
    metadir.mkdir(parents=True, exist_ok=True)
    stdout_path = witness_dir / "tlc.stdout.log"
    stderr_path = witness_dir / "tlc.stderr.log"
    model_path = (
        args.model
        if args.model.is_absolute()
        else (MODEL_DIR / args.model).resolve()
    )
    workers = tlc_workers()
    cmd = (
        java,
        *tlc_jvm_args(workers),
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-config",
        str(config),
        "-deadlock",
        "-workers",
        workers,
        "-metadir",
        str(metadir),
        str(model_path),
    )
    started = time.monotonic()
    timed_out = False
    returncode: int | None = None
    try:
        proc = subprocess.run(
            list(cmd),
            cwd=str(MODEL_DIR),
            text=True,
            capture_output=True,
            check=False,
            timeout=args.timeout_seconds,
        )
        stdout, stderr = proc.stdout, proc.stderr
        returncode = int(proc.returncode)
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = (
            (exc.stderr if isinstance(exc.stderr, str) else "")
            + f"\nTLC timed out after {args.timeout_seconds} seconds.\n"
        )
    wall = time.monotonic() - started
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")

    states = _distinct_states(stdout)
    hard_errors = [
        marker
        for marker in _HARD_ERROR_MARKERS
        if marker in stdout or marker in stderr
    ]
    invariant_violated = "Invariant" in stdout and "is violated" in stdout
    state_space_complete = (
        states is not None
        and states >= min_distinct_states
        and "states generated" in stdout
        and "0 states left on queue" in stdout
        and "Finished in " in stdout
    )
    violation_is_exact = (
        _TEMPORAL_VIOLATION in stdout
        and _COUNTEREXAMPLE_TRACE in stdout
        and not invariant_violated
        if temporal
        else (
            expected_violation in stdout
            and invariant_violated
            and _TEMPORAL_VIOLATION not in stdout
        )
    )
    ok = (
        not timed_out
        and returncode == expected_exit_code
        and expected_violation in stdout
        and violation_is_exact
        and state_space_complete
        and not hard_errors
        and not stderr.strip()
    )
    print(_report_line(label, ok, states, wall), flush=True)
    if not ok:
        print(
            "formal-enricher-backlog: counterexample validation failed "
            f"(label={label}, returncode={returncode}, states={states}, "
            f"timed_out={timed_out}, stderr_nonempty={bool(stderr.strip())}); "
            f"see {stdout_path}",
            file=sys.stderr,
        )
        return [], 1

    rows = build_formal_report_rows(
        properties=properties,
        status="pass",
        checker_command=args.checker_command,
        command=cmd,
        artifact=stdout_path,
        model_path=model_path,
        config_path=config,
        summary_override=(
            properties[0].summary
        ),
        exit_code=0,
        distinct_states=states,
    )
    return rows, 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument(
        "--checker-command",
        default=DEFAULT_CHECKER_COMMAND,
    )
    parser.add_argument(
        "--property-id",
        action="append",
        default=[],
        dest="property_ids",
        help="property_id row to emit; repeat to emit multiple bound rows",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)

    out_dir = args.out.resolve()
    try:
        positive_rows, rc = _run_positive(args, out_dir)
        if rc:
            return rc
        pre_reuse_rows, rc = _run_counterexample(
            args,
            out_dir,
            label="pre_reuse_stranded",
            config=PRE_REUSE_COUNTEREXAMPLE_CONFIG,
            properties=PRE_REUSE_COUNTEREXAMPLE_PROPERTIES,
            expected_exit_code=13,
            expected_violation=_TEMPORAL_VIOLATION,
            temporal=True,
            min_distinct_states=MIN_PRE_REUSE_WITNESS_STATES,
        )
        if rc:
            return rc
        post_reuse_rows, rc = _run_counterexample(
            args,
            out_dir,
            label="post_reuse_unsafe",
            config=POST_REUSE_COUNTEREXAMPLE_CONFIG,
            properties=POST_REUSE_COUNTEREXAMPLE_PROPERTIES,
            expected_exit_code=12,
            expected_violation=_INVARIANT_VIOLATION,
            temporal=False,
            min_distinct_states=MIN_POST_REUSE_WITNESS_STATES,
        )
        if rc:
            return rc
    except TLCSetupError as exc:
        print(f"formal-enricher-backlog: {exc}", file=sys.stderr)
        return exc.exit_code

    rows = [*positive_rows, *pre_reuse_rows, *post_reuse_rows]
    if args.property_ids:
        # `repair_formal_evidence.py` names the bindings it is refreshing, and
        # every other checker honours that. All three configs still run — the
        # selection narrows what is reported, not what is proven.
        requested = set(args.property_ids)
        known = {row["property_id"] for row in rows}
        unknown = sorted(requested - known)
        if unknown:
            print(
                "formal-enricher-backlog: unknown --property-id: "
                + ", ".join(unknown),
                file=sys.stderr,
            )
            return 2
        rows = [row for row in rows if row["property_id"] in requested]

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-enricher-backlog: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
