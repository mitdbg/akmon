#!/usr/bin/env python3
"""Re-prove and re-ingest stale PACT formal evidence.

For every change under pact/changes/, find formal bindings whose every
fresh-claiming evidence row is stale against the current model/config/checker
bytes (the same check the freshness sweep test enforces), re-run each
binding's checker, and ingest the fresh formal-report.yaml back into the
change.

Binding commands recorded before the execution-profile split no longer match
what checkers reproduce today (checkers now always stamp --profile and
--timeout-seconds), and a report must contain rows for exactly the change's
bound properties. The repair therefore:

  1. groups a change's stale bindings by their recorded checker_command;
  2. builds a modern run: the recorded args, plus --property-id for each
     bound property not already selected, plus --profile <name> when the
     recorded --config matches a checker profile's config (asked via
     --list-profiles --json);
  3. runs it, rewrites each binding's checker_command to the command the
     report actually stamps (a recorded mapping repair — module, config, and
     property are unchanged), and ingests the report.

Failures are loud and terminal for the affected binding: a checker that no
longer passes leaves its binding stale, is listed in the summary, and makes
this script exit non-zero. Downgrading such a binding (waiver or task-status
correction) is a manual, recorded decision in the affected manifest — never
something this script does silently.

Usage:
    uv run python scripts/repair_formal_evidence.py [--change <id>] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from okg.substrate.pact.operations import ingest_formal_report  # noqa: E402
from okg.substrate.pact.store import (  # noqa: E402
    load_manifest,
    load_manifest_file,
    manifest_write_lock,
    save_manifest,
)
from okg.substrate.pact.validation import (  # noqa: E402
    _formal_binding_evidence_stale_reasons,
    formal_bindings_without_fresh_evidence,
)

CHANGES_DIR = REPO_ROOT / "pact" / "changes"


def _flag_value(parts: list[str], flag: str) -> str | None:
    if flag not in parts:
        return None
    return parts[parts.index(flag) + 1]


def _flag_values(parts: list[str], flag: str) -> list[str]:
    return [parts[i + 1] for i, part in enumerate(parts) if part == flag]


def _script_path(parts: list[str]) -> str | None:
    for part in parts:
        if part.startswith("scripts/check_formal_") and part.endswith(".py"):
            return part
    return None


_PROFILE_CACHE: dict[str, list[dict]] = {}


def _checker_profiles(script: str) -> list[dict]:
    if script not in _PROFILE_CACHE:
        proc = subprocess.run(
            ["uv", "run", "python", script, "--list-profiles", "--json"],
            cwd=str(REPO_ROOT), text=True, capture_output=True,
        )
        try:
            _PROFILE_CACHE[script] = json.loads(proc.stdout)["profiles"]
        except (json.JSONDecodeError, KeyError):
            _PROFILE_CACHE[script] = []
    return _PROFILE_CACHE[script]


def _build_run_command(
    command: str,
    property_ids: list[str],
    *,
    out_suffix: str | None = None,
) -> list[str]:
    """The recorded args, plus missing --property-id selections, plus
    --profile when the recorded --config identifies a checker profile, plus
    a per-change --out suffix when several changes record the same out dir
    (a shared dir makes each change's run clobber the previous change's
    artifact, staling its just-ingested artifact digest)."""
    parts = shlex.split(command)
    script = _script_path(parts)
    if script and "--profile" not in parts:
        config = _flag_value(parts, "--config")
        if config:
            config_name = Path(config).name
            for profile in _checker_profiles(script):
                if Path(profile["config"]).name == config_name:
                    parts.extend(["--profile", profile["name"]])
                    break
    selected = set(_flag_values(parts, "--property-id"))
    for property_id in property_ids:
        if property_id not in selected:
            parts.extend(["--property-id", property_id])
    if out_suffix and "--out" in parts:
        out_index = parts.index("--out") + 1
        parts[out_index] = f"{parts[out_index]}-{out_suffix}"
    return parts


def _report_path(parts: list[str]) -> Path | None:
    out = _flag_value(parts, "--out")
    if out is None:
        return None
    out_dir = Path(out)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    return out_dir / "formal-report.yaml"


def _stamped_checker_command(report: Path) -> str | None:
    rows = (yaml.safe_load(report.read_text(encoding="utf-8")) or {}).get(
        "results", []
    )
    commands = {row.get("checker_command") for row in rows}
    if len(commands) == 1:
        return next(iter(commands))
    return None


def _rewrite_binding_commands(
    change_id: str, binding_ids: list[str], new_command: str,
) -> None:
    with manifest_write_lock(REPO_ROOT, change_id):
        manifest = load_manifest(REPO_ROOT, change_id)
        for binding in manifest.formal:
            if binding.id in binding_ids and binding.checker_command != new_command:
                print(f"repair: {change_id}: updating {binding.id} "
                      "checker_command to the modern stamped form")
                binding.checker_command = new_command
        save_manifest(REPO_ROOT, manifest)


def _downgrade_superseded_rows(manifest_paths: list[Path]) -> int:
    """For every binding that HAS a currently-valid fresh row, mark its stale
    sibling rows fresh: false with a recorded reason. A stale row may not keep
    claiming fresh: true once the binding is honestly re-proven; bindings with
    NO valid row are left alone (they are the rerun/failure surface, and a
    downgrade there would silently erase the only proof claim)."""
    downgraded = 0
    for manifest_path in manifest_paths:
        change_id = manifest_path.parent.name
        with manifest_write_lock(REPO_ROOT, change_id):
            manifest = load_manifest(REPO_ROOT, change_id)
            changed = False
            for binding in manifest.formal:
                rows = [
                    evidence
                    for evidence in manifest.evidence
                    if evidence.kind in {"formal", "formal_check"}
                    and binding.id in evidence.refs
                    and (evidence.data or {}).get("fresh") is True
                ]
                if not rows:
                    continue
                reasons_by_row = {
                    evidence.id: _formal_binding_evidence_stale_reasons(
                        manifest, binding, evidence,
                        repo_root_path=REPO_ROOT,
                    )
                    for evidence in rows
                }
                if all(reasons_by_row.values()):
                    continue  # no valid row; leave for rerun/failure report
                for evidence in rows:
                    reasons = reasons_by_row[evidence.id]
                    if not reasons:
                        continue
                    evidence.data["fresh"] = False
                    evidence.data["superseded"] = (
                        "downgraded by scripts/repair_formal_evidence.py: "
                        "stale ("
                        + ", ".join(sorted(set(reasons)))
                        + ") and superseded by a fresh re-proof of "
                        + binding.id
                    )
                    downgraded += 1
                    changed = True
            if changed:
                save_manifest(REPO_ROOT, manifest)
                print(f"repair: {change_id}: downgraded superseded stale rows")
    return downgraded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--change", help="repair only this change id")
    parser.add_argument("--dry-run", action="store_true",
                        help="list stale bindings and the commands that "
                             "would run, without running them")
    args = parser.parse_args(argv)

    manifests = sorted(CHANGES_DIR.glob("*/pact.yaml"))
    if args.change:
        manifests = [m for m in manifests if m.parent.name == args.change]
        if not manifests:
            print(f"repair: unknown change {args.change}", file=sys.stderr)
            return 2

    # change_id -> recorded checker_command -> (binding_ids, property_ids)
    work: dict[str, dict[str, tuple[list[str], list[str]]]] = {}
    for manifest_path in manifests:
        manifest = load_manifest_file(manifest_path)
        stale = formal_bindings_without_fresh_evidence(
            manifest, repo_root_path=REPO_ROOT,
        )
        groups: dict[str, tuple[list[str], list[str]]] = {}
        for row in stale:
            command = row["checker_command"]
            if not command:
                print(f"repair: {manifest.change.id}: binding "
                      f"{row['binding_id']} has no checker_command; "
                      "needs a manual downgrade", file=sys.stderr)
                continue
            binding_ids, property_ids = groups.setdefault(command, ([], []))
            binding_ids.append(row["binding_id"])
            property_ids.append(row["property_id"])
            print(f"stale: {manifest.change.id}: {row['binding_id']} "
                  f"({', '.join(row['newest_reasons'])})")
        if groups:
            work[manifest.change.id] = groups

    if not work:
        downgraded = _downgrade_superseded_rows(manifests)
        if downgraded:
            print(f"repair: no reruns needed; downgraded {downgraded} "
                  "superseded stale row(s)")
        else:
            print("repair: no stale formal bindings; nothing to do")
        return 0
    runs = [
        (change_id, command, binding_ids, property_ids)
        for change_id, groups in work.items()
        for command, (binding_ids, property_ids) in groups.items()
    ]
    # Out dirs recorded by more than one change get a per-change suffix so
    # one change's rerun cannot stale another change's artifact digest. The
    # collision map spans ALL manifests' bindings (not just the stale set):
    # a fresh change sharing an out dir with a stale one must not have its
    # artifact clobbered by the rerun.
    out_dir_changes: dict[str, set[str]] = {}
    for manifest_path in sorted(CHANGES_DIR.glob("*/pact.yaml")):
        manifest = load_manifest_file(manifest_path)
        for binding in manifest.formal:
            out = _flag_value(
                shlex.split(binding.checker_command or ""), "--out",
            )
            if out:
                out_dir_changes.setdefault(out, set()).add(manifest.change.id)

    def _suffix_for(change_id: str, command: str) -> str | None:
        out = _flag_value(shlex.split(command), "--out")
        if out and len(out_dir_changes.get(out, set())) > 1:
            return change_id
        return None

    print(f"repair: {len(work)} change(s), {len(runs)} checker run(s)")
    if args.dry_run:
        for change_id, command, _binding_ids, property_ids in runs:
            parts = _build_run_command(
                command, property_ids,
                out_suffix=_suffix_for(change_id, command),
            )
            print(f"would run [{change_id}]: {' '.join(parts)}")
        return 0

    run_cache: dict[str, tuple[bool, str]] = {}
    failures: list[str] = []
    for change_id, command, binding_ids, property_ids in runs:
        parts = _build_run_command(
            command, property_ids,
            out_suffix=_suffix_for(change_id, command),
        )
        cache_key = " ".join(parts)
        if cache_key not in run_cache:
            print(f"repair: running: {cache_key}", flush=True)
            proc = subprocess.run(parts, cwd=str(REPO_ROOT), text=True,
                                  capture_output=True)
            tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-6:])
            run_cache[cache_key] = (proc.returncode == 0, tail)
        ok, tail = run_cache[cache_key]
        if not ok:
            failures.append(f"{change_id}: checker failed: {cache_key}\n{tail}")
            continue
        report = _report_path(parts)
        if report is None or not report.is_file():
            failures.append(
                f"{change_id}: checker succeeded but no formal report at "
                f"{report}: {cache_key}"
            )
            continue
        stamped = _stamped_checker_command(report)
        if stamped is None:
            failures.append(
                f"{change_id}: report rows carry inconsistent checker "
                f"commands: {report}"
            )
            continue
        try:
            _rewrite_binding_commands(change_id, binding_ids, stamped)
            ingest_formal_report(REPO_ROOT, change_id, report)
            print(f"repair: ingested {report} into {change_id}")
        except Exception as exc:  # noqa: BLE001 - reported and terminal
            failures.append(f"{change_id}: ingest failed: {exc}")

    downgraded = _downgrade_superseded_rows(manifests)
    if downgraded:
        print(f"repair: downgraded {downgraded} superseded stale row(s)")
    if failures:
        print(f"\nrepair: {len(failures)} FAILURE(S) — these bindings remain "
              "stale and need a recorded manual downgrade:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print("repair: all stale bindings re-proven and re-ingested")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
