#!/usr/bin/env python3
"""Run the AKMON reviewer quickstart and every claim script, with no keys.

One command for artifact reviewers. It removes every API key, token and
model-provider variable from the environment (printing the names it
removed, never the values), then runs, each in its own process:

  1. reviewer_quickstart.py          publish the bundled synthetic records
  2. claim_admission_rejection.py    typed admission verdict
  3. claim_fork_merge_readmission.py fork, gate, merge back
  4. claim_lens_round_trip.py        lens edit into the next generation

It stops at the first step that exits non-zero, or exits 0 without printing
its PASS line, and exits 1. Everything printed is also written to a
transcript file.

The terminal shows each step's own lines and its PASS or FAIL line. What the
steps write to stderr -- okg's progress messages and its crash-evidence
records -- goes to a log file instead (default: <workdir>/reproduce.log),
because on a passing run it is dozens of lines of noise. When a step fails,
the last lines that step wrote to stderr and the log's path are printed
before the FAIL line, so a failure is never hidden.

Usage:
    uv run python scripts/akmon/reproduce.py \
        --dsn "$OKG_QUICKSTART_DSN"
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import platform
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import NamedTuple, Sequence, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    run_main,
    DSN_ENV,
    REPO_ROOT,
    STATE_FILE_NAME,
    keyless_environment,
    redact,
    resolve_dsn,
)


HERE = Path(__file__).resolve().parent
QUICKSTART = ("reviewer_quickstart.py", "QUICKSTART PASS")
# Each claim script and the start of the PASS line it must print.
CLAIMS = (
    ("claim_admission_rejection.py", "CLAIM admission-rejection PASS"),
    ("claim_fork_merge_readmission.py", "CLAIM fork-merge-readmission PASS"),
    ("claim_lens_round_trip.py", "CLAIM lens-round-trip PASS"),
)
LOG_FILE_NAME = "reproduce.log"
# How many of a failed step's last stderr lines are printed with its FAIL line.
STDERR_TAIL_LINES = 20


class _Tee:
    def __init__(self, path: Path) -> None:
        self._file = path.open("w", encoding="utf-8")

    def line(self, text: str) -> None:
        print(text, flush=True)
        self._file.write(text + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()


def _git_head() -> str:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "unavailable (not a git checkout)"


class Step(NamedTuple):
    label: str
    command: list[str]
    pass_prefix: str  # the step passes only if a stdout line starts with this


class _StepResult(NamedTuple):
    code: int
    passed_line: bool       # a stdout line started with the step's pass prefix
    stderr_tail: list[str]  # the last STDERR_TAIL_LINES stderr lines
    stderr_lines: int       # how many stderr lines the step wrote in all


def _run(tee: _Tee, log: TextIO, step: Step, env: dict[str, str]) -> _StepResult:
    """Run one step: stdout to the terminal and transcript, stderr to the log.

    The log receives every stderr line (database passwords redacted); the
    last STDERR_TAIL_LINES of them are kept for the failure report.
    """
    label = step.label
    tee.line(f"=== {label}")
    log.write(f"=== {label}\n")
    log.flush()
    process = subprocess.Popen(
        step.command, cwd=str(REPO_ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", errors="replace",
    )
    assert process.stdout is not None and process.stderr is not None
    tail: collections.deque[str] = collections.deque(maxlen=STDERR_TAIL_LINES)
    counted = [0]

    def drain_stderr() -> None:
        for raw in process.stderr:
            text = redact(raw.rstrip("\n"))
            log.write(text + "\n")
            log.flush()
            tail.append(text)
            counted[0] += 1

    # Read stderr on its own thread so neither pipe can fill and stall the child.
    reader = threading.Thread(target=drain_stderr, daemon=True)
    reader.start()
    passed_line = False
    for raw in process.stdout:
        text = raw.rstrip("\n")
        tee.line(text)
        passed_line = passed_line or text.startswith(step.pass_prefix)
    code = process.wait()
    reader.join()
    log.write(f"=== {label} exit={code}\n")
    log.flush()
    tee.line(f"=== {label} exit={code}")
    return _StepResult(code, passed_line, list(tail), counted[0])


def run_steps(
    steps: Sequence[Step],
    *,
    env: dict[str, str],
    tee: _Tee,
    log: TextIO,
    log_path: Path,
    transcript: Path,
) -> int:
    """Run the steps in order; stop at the first failure and report it.

    A step fails when it exits non-zero or exits 0 without printing its PASS
    line; either way its last stderr lines and the log's path are shown.
    """
    for step in steps:
        result = _run(tee, log, step, env)
        if result.code == 0 and result.passed_line:
            continue
        if result.code == 0:
            tee.line(f"--- {step.label} exited 0 but printed no '{step.pass_prefix}' line")
            reason = f"exit 0, no '{step.pass_prefix}' line"
        else:
            reason = f"exit {result.code}"
        if result.stderr_lines:
            tee.line(
                f"--- last {len(result.stderr_tail)} of {result.stderr_lines} "
                f"stderr lines from {step.label} (full log: {log_path}):"
            )
            for text in result.stderr_tail:
                tee.line(f"  | {text}")
        else:
            tee.line(f"--- {step.label} wrote nothing to stderr (log: {log_path})")
        tee.line(
            f"REPRODUCE FAIL at {step.label} ({reason}); "
            f"transcript: {transcript}; stderr log: {log_path}"
        )
        return 1
    tee.line(f"REPRODUCE PASS steps={len(steps)}/{len(steps)} transcript={transcript}")
    return 0


def _log_path_problem(path: Path, transcript: Path) -> str | None:
    """Why ``path`` cannot hold the stderr log, or None when it can."""
    if path == transcript:
        return "the stderr log and the transcript must be different files"
    if path.is_relative_to(REPO_ROOT):
        ignored = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "check-ignore", "-q", str(path)],
            capture_output=True, check=False,
        ).returncode == 0
        if not ignored:
            # Every publish records the checkout's commit and refuses while it
            # has untracked files, so a log here would fail the quickstart.
            return (
                "the stderr log is inside this checkout and not git-ignored; "
                "every publish refuses a checkout with untracked files"
            )
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", help=f"PostgreSQL URL of an empty database (or set {DSN_ENV})")
    parser.add_argument("--workdir", help="empty directory (default: a new temporary directory)")
    parser.add_argument("--transcript", help="transcript path (default: <workdir>/transcript.txt)")
    parser.add_argument(
        "--log",
        help=f"where the steps' stderr goes (default: <workdir>/{LOG_FILE_NAME})",
    )
    args = parser.parse_args(argv)

    dsn = resolve_dsn(args.dsn)
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="akmon-reviewer-")).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    transcript = Path(args.transcript).resolve() if args.transcript else workdir / "transcript.txt"
    log_path = Path(args.log).resolve() if args.log else workdir / LOG_FILE_NAME
    tee = _Tee(transcript)
    env, removed = keyless_environment()
    env.pop(DSN_ENV, None)

    tee.line("AKMON reviewer reproduction")
    tee.line(f"started: {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}")
    tee.line(f"checkout: {REPO_ROOT} at {_git_head()}")
    tee.line(f"python: {platform.python_version()} on {platform.system()} {platform.machine()}")
    tee.line(f"workdir: {workdir}")
    tee.line(
        "removed credential variables: "
        + (", ".join(removed) if removed else "none were set")
    )
    tee.line(f"stderr log: {log_path} (the last lines are shown here if a step fails)")
    problem = _log_path_problem(log_path, transcript)
    if problem is not None:
        tee.line(f"REPRODUCE FAIL at setup: {problem}")
        tee.close()
        return 1
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("w", encoding="utf-8")
    except OSError as exc:
        tee.line(f"REPRODUCE FAIL at setup: cannot write the stderr log: {exc}")
        tee.close()
        return 1

    python = sys.executable
    script, prefix = QUICKSTART
    steps = [Step(script, [
        python, str(HERE / script),
        "--dsn", dsn, "--workdir", str(workdir / "quickstart"),
    ], prefix)]
    state = workdir / "quickstart" / STATE_FILE_NAME
    for script, prefix in CLAIMS:
        steps.append(Step(
            script,
            [python, str(HERE / script), "--state", str(state), "--dsn", dsn],
            prefix,
        ))

    try:
        return run_steps(
            steps, env=env, tee=tee, log=log, log_path=log_path, transcript=transcript,
        )
    finally:
        log.close()
        tee.close()


if __name__ == "__main__":
    run_main("reproduce", main)
