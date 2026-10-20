#!/usr/bin/env python3
"""Run the integrated PACT agent lifecycle TLA+ model."""
from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "pact"
REFINEMENT_TESTS = (
    "tests/pact/test_fsm.py::TestDeriveState::test_tier1_skips_proposed",
    "tests/pact/test_fsm.py::TestDeriveState::test_tier2_waits_at_proposed",
    "tests/pact/test_fsm.py::TestDeriveState::test_tier2_approved_proceeds",
    "tests/pact/test_fsm.py::TestDeriveState::test_done_work_awaits_review",
    "tests/pact/test_fsm.py::TestLegalTransition::test_approve_at_review_retires",
    "tests/pact/test_fsm.py::TestLegalTransition::test_revise_at_review_goes_in_progress",
    "tests/pact/test_v4_lifecycle_authority.py::test_undigested_decision_does_not_approve_v4_intent",
    "tests/pact/test_v4_lifecycle_authority.py::test_proposal_revision_holds_draft_until_intent_digest_changes",
    "tests/pact/test_v4_lifecycle_authority.py::test_compact_post_edit_preserves_authoritative_state_and_successor",
    "tests/pact/test_v4_lifecycle_authority.py::test_status_and_bootstrap_suppress_tasks_until_proposal_decision",
)
STATE_PROPERTY = TLCProperty(
    "PactAgentLifecycleStateContract",
    "TLC verified the authoritative tier, proposal, proof, review, and retirement states.",
    refinement_tests=REFINEMENT_TESTS,
)
APPROVAL_PROPERTY = TLCProperty(
    "PactAgentLifecycleApprovalContract",
    "TLC verified the digest-bound proposal and final approval path.",
    refinement_tests=REFINEMENT_TESTS,
)
WITNESS = TLCProperty(
    "PactAgentLifecycleReachabilityWitness",
    "TLC produced the expected retirement counterexample, proving the positive path is reachable.",
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=("BrokenRetirementNeverOccurs",),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out", type=Path,
        default=REPO_ROOT / ".scratch" / "formal" / "pact-agent-lifecycle",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    refinement = _run_refinement_tests(
        REFINEMENT_TESTS,
        out_dir=args.out / "refinement",
        timeout_seconds=args.timeout_seconds,
    )
    if refinement["status"] != "pass":
        print(
            "formal-pact-agent-lifecycle: refinement tests failed; see "
            f"{refinement['artifact']}",
            file=sys.stderr,
        )
        return 1
    checker = "uv run --extra dev python scripts/check_formal_pact_agent_lifecycle.py"
    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "PactAgentLifecycle.tla",
            config=MODEL_DIR / "PactAgentLifecycle.cfg",
            out_dir=args.out / "safety",
            properties=(STATE_PROPERTY, APPROVAL_PROPERTY),
            expected_properties=(STATE_PROPERTY, APPROVAL_PROPERTY),
            checker_command=checker,
            timeout_seconds=args.timeout_seconds,
        )
        if result.run_failed:
            print(f"formal-pact-agent-lifecycle: failed; see {result.stdout_path}", file=sys.stderr)
            return result.exit_code or 1
        witness = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "PactAgentLifecycle.tla",
            config=MODEL_DIR / "PactAgentLifecycleWitness.cfg",
            out_dir=args.out / "witness",
            properties=(WITNESS,),
            expected_properties=(WITNESS,),
            checker_command=checker,
            expect_counterexample=True,
            timeout_seconds=args.timeout_seconds,
        )
    except TLCSetupError as exc:
        print(f"formal-pact-agent-lifecycle: {exc}", file=sys.stderr)
        return exc.exit_code
    if witness.run_failed:
        print(f"formal-pact-agent-lifecycle: witness failed; see {witness.stdout_path}", file=sys.stderr)
        return witness.exit_code or 1
    for row in result.rows:
        artifact = Path(str(row["artifact"]))
        try:
            row["artifact"] = str(artifact.relative_to(args.out.resolve()))
        except ValueError:
            row["artifact"] = str(artifact)
        row["refinement_execution"] = refinement["tests"]
        row["refinement_report"] = refinement["artifact"]
        row["non_vacuity_witness"] = {
            "status": "pass",
            "property_id": WITNESS.property_id,
            "artifact": str(
                witness.stdout_path.relative_to(args.out.resolve())
            ),
            "distinct_states": witness.rows[0].get("distinct_states") if witness.rows else None,
        }
    _sanitize_artifact_texts(args.out)
    portable_rows = _portable_value(list(result.rows))
    args.out.mkdir(parents=True, exist_ok=True)
    report = args.out / "formal-report.yaml"
    report.write_text(
        yaml.safe_dump({"results": portable_rows}, sort_keys=False),
        encoding="utf-8",
    )
    print(f"formal-pact-agent-lifecycle: passed; report at {report}")
    return 0


def _portable_value(value: object) -> object:
    """Remove checkout-specific paths from durable formal evidence."""
    if isinstance(value, dict):
        return {key: _portable_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_portable_value(item) for item in value]
    if not isinstance(value, str):
        return value
    replacements = {
        str(REPO_ROOT): ".",
        sys.executable: "python",
    }
    tla_jar = os.environ.get("TLA2TOOLS_JAR")
    if tla_jar:
        replacements[tla_jar] = "$TLA2TOOLS_JAR"
        replacements[str(Path(tla_jar).expanduser().resolve())] = "$TLA2TOOLS_JAR"
    for source, replacement in replacements.items():
        value = value.replace(source, replacement)
    value = re.sub(
        r"(?P<prefix>(?:^|\s)-(?:cp|classpath)\s+)"
        r"(?:['\"])?(?P<jar>.*?tla2tools\.jar)(?:['\"])?"
        r"(?=\s+tlc2\.TLC\b)",
        lambda match: f"{match.group('prefix')}$TLA2TOOLS_JAR",
        value,
    )
    return re.sub(
        r"(?<![\w$])(?:[A-Za-z]:)?(?:[^\s'\"=;]+[/\\])+tla2tools\.jar",
        "$TLA2TOOLS_JAR",
        value,
    )


def _sanitize_artifact_texts(out_dir: Path) -> None:
    for path in out_dir.rglob("*"):
        if not path.is_file() or path.suffix not in {".yaml", ".log", ".xml"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        portable = _portable_value(text)
        if portable != text:
            path.write_text(str(portable), encoding="utf-8")


def _run_refinement_tests(
    node_ids: tuple[str, ...],
    *,
    out_dir: Path,
    timeout_seconds: int,
) -> dict[str, object]:
    """Execute every bound pytest node and reject skipped or uncollected tests."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for index, node_id in enumerate(node_ids):
        junit = out_dir / f"{index:03d}.xml"
        stdout_path = out_dir / f"{index:03d}.stdout.log"
        stderr_path = out_dir / f"{index:03d}.stderr.log"
        command = (
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--disable-warnings",
            f"--junitxml={junit}",
            node_id,
        )
        try:
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            return_code = completed.returncode
            stdout = completed.stdout
            stderr = completed.stderr
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            return_code = 124
            stdout = _as_text(exc.stdout)
            stderr = _as_text(exc.stderr)
            timed_out = True
            stderr = f"{stderr}\npytest timed out after {timeout_seconds} seconds.\n"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        cases, skipped, failed = _junit_counts(junit)
        status = (
            "pass"
            if return_code == 0 and cases > 0 and skipped == 0 and failed == 0
            else "fail"
        )
        rows.append(
            {
                "node_id": node_id,
                "status": status,
                "return_code": return_code,
                "timed_out": timed_out,
                "cases": cases,
                "skipped": skipped,
                "failed": failed,
                "command": shlex.join(command),
                "junit_artifact": str(junit),
                "stdout_artifact": str(stdout_path),
                "stderr_artifact": str(stderr_path),
            }
        )
    report = out_dir / "refinement-report.yaml"
    payload = {
        "status": "pass" if all(row["status"] == "pass" for row in rows) else "fail",
        "tests": rows,
    }
    report.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return {
        "status": payload["status"],
        "tests": rows,
        "artifact": str(report),
    }


def _junit_counts(path: Path) -> tuple[int, int, int]:
    if not path.is_file():
        return 0, 0, 0
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return 0, 0, 0
    cases = list(root.iter("testcase"))
    skipped = sum(1 for case in cases if case.find("skipped") is not None)
    failed = sum(
        1
        for case in cases
        if case.find("failure") is not None or case.find("error") is not None
    )
    return len(cases), skipped, failed


def _as_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
