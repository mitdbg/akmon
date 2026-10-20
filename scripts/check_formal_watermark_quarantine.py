#!/usr/bin/env python3
"""Check item quarantine safety, crash rollback, and two falsifications."""

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
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "watermark-quarantine"
DEFAULT_MODEL = MODEL_DIR / "WatermarkQuarantine.tla"
POSITIVE_CONFIG = MODEL_DIR / "WatermarkQuarantine.cfg"
SYSTEMIC_HELD_CONFIG = MODEL_DIR / "WatermarkQuarantineSystemicHeld.cfg"
CRASH_CONFIG = MODEL_DIR / "WatermarkQuarantineCrash.cfg"
SYSTEMIC_WITNESS_CONFIG = MODEL_DIR / "WatermarkQuarantineSystemicFailure.cfg"
SNAPSHOT_WITNESS_CONFIG = MODEL_DIR / "WatermarkQuarantineSnapshotLoss.cfg"
DEFAULT_CHECKER_COMMAND = (
    "uv run --extra dev python scripts/check_formal_watermark_quarantine.py "
    "--out .scratch/formal/watermark-quarantine"
)

REFINEMENT_TESTS = (
    "tests/substrate/test_enricher_input_failure_holds_watermark.py",
    "tests/substrate/extraction/test_enricher_input_quarantine.py",
)

POSITIVE_PROPERTIES = [
    TLCProperty(
        "WatermarkQuarantineSafety",
        "TLC exhausted item/signature retry and complete-snapshot replacement. "
        "Committed failures remain bounded, terminal quarantine cannot occur "
        "early, record/snapshot/progress release is one transition, prior "
        "poison-item output is retained, successful siblings replace, and a "
        "changed signature keeps the old finding active until success.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "DispatchBudgetBound",
            "NoPrematureQuarantine",
            "QuarantineHasDurableRecord",
            "ActiveRecordBackedByHistory",
            "ChangedSignatureKeepsHistory",
            "OldFindingActiveDuringChangedRetry",
            "SuccessfulChangedSignatureSupersedes",
            "QuarantinedOutputRetained",
            "SuccessfulSiblingReplaced",
            "TerminalRecordAtomicWithRelease",
            "WatermarkReleaseIsTerminal",
            "HeldStatusUsesFailedNamespace",
            "NoSystemicQuarantine",
            "ClosedCycleHasNoWorkingState",
        ),
        expected_temporal_properties=("EveryProcessableItemEventuallyTerminal",),
    ),
]
SYSTEMIC_HELD_PROPERTIES = [
    TLCProperty(
        "SystemicFailureHeld",
        "TLC positively exercised a correctly classified repeated provider "
        "outage. Its reservation is released, committed attempts stay zero, "
        "no quarantine is created, and progress remains in failed:% hold.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "NoSystemicQuarantine",
            "SystemicCommittedAttemptsStayZero",
            "SystemicReservationReleased",
            "HeldStatusUsesFailedNamespace",
            "ClosedCycleHasNoWorkingState",
        ),
    ),
]
CRASH_PROPERTIES = [
    TLCProperty(
        "CrashRollbackSafety",
        "TLC explored process crashes before call completion, after failure, "
        "and before terminal commit. The publisher transaction rolls working "
        "attempts and terminal preparation back together; no early quarantine "
        "or split terminal-record/progress state survives.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=(
            "TypeOK",
            "DispatchBudgetBound",
            "NoPrematureQuarantine",
            "QuarantineHasDurableRecord",
            "TerminalRecordAtomicWithRelease",
            "QuarantinedOutputRetained",
            "ClosedCycleHasNoWorkingState",
        ),
        expected_temporal_properties=("EveryProcessableItemEventuallyTerminal",),
    ),
]
SYSTEMIC_WITNESS_PROPERTIES = [
    TLCProperty(
        "SystemicFailureMisclassificationWitness",
        "TLC produced the required counterexample: treating a shared outage "
        "as item-deterministic consumes the budget and quarantines the item.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("SystemicFailureMisclassificationWitness",),
    ),
]
SNAPSHOT_WITNESS_PROPERTIES = [
    TLCProperty(
        "SnapshotOmissionRetractionWitness",
        "TLC produced the required counterexample: treating a quarantined "
        "item's omission as complete-snapshot authority retracts its prior "
        "published output.",
        refinement_tests=REFINEMENT_TESTS,
        expected_invariants=("SnapshotOmissionRetractionWitness",),
    ),
]

_COUNTEREXAMPLE_TRACE = "The behavior up to this point is:"
_HARD_ERRORS = (
    "Parsing or semantic analysis failed",
    "Semantic errors:",
    "TLC threw an unexpected exception",
    "java.lang.",
    "Exception in thread",
)


def _distinct_states(stdout: str) -> int | None:
    matches = re.findall(r"(\d+)\s+distinct states found", stdout)
    return int(matches[-1]) if matches else None


def _run_proof(
    args: argparse.Namespace,
    out_dir: Path,
    *,
    label: str,
    config: Path,
    properties: list[TLCProperty],
    min_distinct_states: int,
) -> tuple[list[dict], int]:
    started = time.monotonic()
    result = run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=args.model,
        config=config,
        out_dir=out_dir / label,
        properties=properties,
        expected_properties=properties,
        checker_command=args.checker_command,
        timeout_seconds=args.timeout_seconds,
        min_distinct_states=min_distinct_states,
    )
    wall = time.monotonic() - started
    print(
        f"formal-watermark-quarantine: [{label}] "
        f"{'proved clean' if not result.run_failed else 'FAILED'}; "
        f"{result.distinct_states} distinct states; {wall:.1f}s",
        flush=True,
    )
    if result.run_failed:
        print(
            f"formal-watermark-quarantine: {label} failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return [], result.exit_code or 1
    return list(result.rows), 0


def _run_witness(
    args: argparse.Namespace,
    out_dir: Path,
    *,
    label: str,
    config: Path,
    property_id: str,
    properties: list[TLCProperty],
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
        args.model if args.model.is_absolute() else (MODEL_DIR / args.model).resolve()
    )
    workers = tlc_workers()
    command = (
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
    try:
        proc = subprocess.run(
            list(command),
            cwd=str(MODEL_DIR),
            text=True,
            capture_output=True,
            check=False,
            timeout=args.timeout_seconds,
        )
        stdout, stderr = proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr += f"\nTLC timed out after {args.timeout_seconds} seconds.\n"
    wall = time.monotonic() - started
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")

    states = _distinct_states(stdout)
    expected = f"Invariant {property_id} is violated"
    hard_errors = [
        marker for marker in _HARD_ERRORS if marker in stdout or marker in stderr
    ]
    ok = (
        not timed_out
        and expected in stdout
        and _COUNTEREXAMPLE_TRACE in stdout
        and not hard_errors
    )
    print(
        f"formal-watermark-quarantine: [{label}] "
        f"{'counterexample produced' if ok else 'FAILED'}; "
        f"{states} distinct states; {wall:.1f}s",
        flush=True,
    )
    if not ok:
        print(
            f"formal-watermark-quarantine: {label} failed; see "
            f"{stdout_path} and {stderr_path}",
            file=sys.stderr,
        )
        return [], 1

    rows = build_formal_report_rows(
        properties=properties,
        status="pass",
        checker_command=args.checker_command,
        command=command,
        artifact=stdout_path,
        model_path=model_path,
        config_path=config,
        summary_override=(
            f"Expected safety counterexample produced: {property_id} violated."
        ),
        exit_code=0,
        distinct_states=states,
    )
    return rows, 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--checker-command", default=DEFAULT_CHECKER_COMMAND)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    out_dir = args.out.resolve()

    rows: list[dict] = []
    try:
        proof_specs = (
            ("positive", POSITIVE_CONFIG, POSITIVE_PROPERTIES, 20),
            ("systemic-held", SYSTEMIC_HELD_CONFIG, SYSTEMIC_HELD_PROPERTIES, 3),
            ("crash", CRASH_CONFIG, CRASH_PROPERTIES, 6),
        )
        for label, config, properties, floor in proof_specs:
            new_rows, rc = _run_proof(
                args,
                out_dir,
                label=label,
                config=config,
                properties=properties,
                min_distinct_states=floor,
            )
            if rc:
                return rc
            rows.extend(new_rows)
        witness_specs = (
            (
                "systemic-witness",
                SYSTEMIC_WITNESS_CONFIG,
                "SystemicFailureMisclassificationWitness",
                SYSTEMIC_WITNESS_PROPERTIES,
            ),
            (
                "snapshot-witness",
                SNAPSHOT_WITNESS_CONFIG,
                "SnapshotOmissionRetractionWitness",
                SNAPSHOT_WITNESS_PROPERTIES,
            ),
        )
        for label, config, property_id, properties in witness_specs:
            new_rows, rc = _run_witness(
                args,
                out_dir,
                label=label,
                config=config,
                property_id=property_id,
                properties=properties,
            )
            if rc:
                return rc
            rows.extend(new_rows)
    except TLCSetupError as exc:
        print(f"formal-watermark-quarantine: {exc}", file=sys.stderr)
        return exc.exit_code

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "formal-report.yaml"
    report_path.write_text(
        yaml.safe_dump({"results": rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-watermark-quarantine: TLC passed; report at {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
