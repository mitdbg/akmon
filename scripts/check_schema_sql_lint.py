#!/usr/bin/env python3
"""Run the pinned PostgreSQL schema SQL lint gate.

This is intentionally warning-only by default. The current schema audit found
historical DDL debt, so the first useful gate is a stable, concise Squawk
summary for migration files that does not fail the existing baseline.

Default scope:
  - included: src/okg/substrate/db/migrations/*.sql
  - excluded: src/okg/substrate/db/schema.sql

The bootstrap schema snapshot is excluded by default because it contains known
historical noise and is not a new migration review surface. Pass
``--include-schema-snapshot`` when refreshing the audit baseline.
"""
from __future__ import annotations

import argparse
from collections import Counter
import glob
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SQUAWK_VERSION = "2.58.0"
DEFAULT_SQUAWK_COMMAND = f"uvx --from squawk-cli=={SQUAWK_VERSION} squawk"
DEFAULT_MIGRATION_GLOB = "src/okg/substrate/db/migrations/*.sql"
SCHEMA_SNAPSHOT = "src/okg/substrate/db/schema.sql"


def _repo_relative(path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        return candidate.as_posix()
    try:
        return candidate.relative_to(ROOT).as_posix()
    except ValueError:
        return candidate.as_posix()


def _has_glob_magic(path: str) -> bool:
    return any(char in path for char in "*?[")


def build_targets(
    paths: list[str],
    *,
    include_schema_snapshot: bool,
) -> list[str]:
    specs = list(paths) if paths else [DEFAULT_MIGRATION_GLOB]
    if include_schema_snapshot and SCHEMA_SNAPSHOT not in specs:
        specs.insert(0, SCHEMA_SNAPSHOT)

    targets: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        path = Path(spec)
        if _has_glob_magic(spec):
            pattern = spec if path.is_absolute() else str(ROOT / spec)
            matches = [Path(match) for match in glob.glob(pattern)]
        else:
            matches = [path if path.is_absolute() else ROOT / path]

        for match in sorted(matches):
            repo_path = _repo_relative(match)
            if repo_path in seen:
                continue
            seen.add(repo_path)
            targets.append(repo_path)
    return targets


def parse_squawk_payload(stdout: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(stdout or "[]")
    except json.JSONDecodeError as exc:
        raise ValueError(f"Squawk did not emit valid JSON: {exc}") from exc

    if isinstance(payload, list):
        return [issue for issue in payload if isinstance(issue, dict)]
    if isinstance(payload, dict):
        issues = payload.get("issues")
        if isinstance(issues, list):
            return [issue for issue in issues if isinstance(issue, dict)]
    raise ValueError("Squawk JSON must be a list or an object with an issues list")


def _sorted_counts(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def summarize_issues(
    issues: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    by_rule: Counter[str] = Counter()
    by_file: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []

    for issue in issues:
        rule = str(issue.get("rule_name") or issue.get("rule") or "unknown")
        file = _repo_relative(str(issue.get("file") or "unknown"))
        by_rule[rule] += 1
        by_file[file] += 1
        if len(samples) < sample_limit:
            samples.append(
                {
                    "file": file,
                    "line": issue.get("line"),
                    "column": issue.get("column"),
                    "level": issue.get("level"),
                    "rule_name": rule,
                    "message": issue.get("message"),
                }
            )

    return {
        "warning_count": len(issues),
        "by_rule": _sorted_counts(by_rule),
        "by_file": _sorted_counts(by_file),
        "samples": samples,
    }


def _command_for_display(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def run_squawk(
    *,
    squawk_command: str,
    targets: list[str],
) -> tuple[list[str], subprocess.CompletedProcess[str]]:
    command = shlex.split(squawk_command) + ["--reporter", "json", *targets]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    return command, completed


def _print_human(payload: dict[str, Any]) -> None:
    mode = payload["mode"]
    warning_count = payload["warning_count"]
    file_count = len(payload["by_file"])
    target_count = len(payload["targets"])
    print(
        "schema SQL lint: "
        f"{warning_count} Squawk warning(s) across {file_count} file(s) "
        f"({mode})"
    )
    print(f"squawk: {payload['squawk_version']}; targets: {target_count}")
    print(f"command: {_command_for_display(payload['command'])}")
    print(
        "scope: migration SQL by default; "
        "schema.sql requires --include-schema-snapshot"
    )

    if payload.get("stderr"):
        print("stderr:")
        print(payload["stderr"].rstrip())

    if warning_count == 0:
        return

    print("by rule:")
    for rule, count in payload["by_rule"].items():
        print(f"  {rule}: {count}")

    print("by file:")
    for file, count in payload["by_file"].items():
        print(f"  {file}: {count}")

    if payload["samples"]:
        print("samples:")
    for sample in payload["samples"]:
        location = sample["file"]
        if sample.get("line") is not None:
            location = f"{location}:{sample['line']}"
        message = sample.get("message") or ""
        print(f"  {location} {sample['rule_name']}: {message}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run pinned Squawk SQL lint for OKG schema migration files. "
            "Warnings are reported but accepted unless --fail-on-warnings is set."
        )
    )
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        help=(
            "SQL file or glob to lint. Repeatable. Defaults to "
            f"{DEFAULT_MIGRATION_GLOB}."
        ),
    )
    parser.add_argument(
        "--include-schema-snapshot",
        action="store_true",
        help=(
            "Also lint src/okg/substrate/db/schema.sql. This is excluded by "
            "default because it is a historical bootstrap snapshot."
        ),
    )
    parser.add_argument(
        "--fail-on-warnings",
        action="store_true",
        help="Exit 1 when Squawk reports warnings.",
    )
    parser.add_argument(
        "--squawk-command",
        default=os.environ.get("OKG_SQUAWK_COMMAND", DEFAULT_SQUAWK_COMMAND),
        help=(
            "Command used to invoke Squawk before '--reporter json'. "
            f"Default: {DEFAULT_SQUAWK_COMMAND!r}."
        ),
    )
    parser.add_argument(
        "--sample-limit",
        default=10,
        type=int,
        help="Maximum warning samples to include in output.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit a machine-readable wrapper summary.",
    )
    parser.add_argument(
        "--list-targets",
        action="store_true",
        help="Print the resolved lint target list and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    targets = build_targets(
        args.path,
        include_schema_snapshot=args.include_schema_snapshot,
    )
    if args.list_targets:
        print("\n".join(targets))
        return 0
    if not targets:
        print("schema SQL lint: no SQL targets matched", file=sys.stderr)
        return 2

    try:
        command, completed = run_squawk(
            squawk_command=args.squawk_command,
            targets=targets,
        )
    except FileNotFoundError as exc:
        print(f"schema SQL lint: failed to execute Squawk: {exc}", file=sys.stderr)
        return 2

    try:
        issues = parse_squawk_payload(completed.stdout)
    except ValueError as exc:
        print(f"schema SQL lint: {exc}", file=sys.stderr)
        if completed.stderr:
            print(completed.stderr.rstrip(), file=sys.stderr)
        return 2

    summary = summarize_issues(issues, sample_limit=args.sample_limit)
    warning_return = completed.returncode == 1 and bool(issues)
    runtime_error = completed.returncode != 0 and not warning_return
    fail_on_warnings = bool(issues) and args.fail_on_warnings
    payload = {
        "ok": not runtime_error and not fail_on_warnings,
        "mode": "fail-on-warnings" if args.fail_on_warnings else "warning-only",
        "squawk_version": SQUAWK_VERSION,
        "command": command,
        "targets": targets,
        "returncode": completed.returncode,
        "stderr": completed.stderr,
        **summary,
    }

    if args.json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_human(payload)

    if runtime_error:
        return 2
    if fail_on_warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
