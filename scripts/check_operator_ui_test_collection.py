#!/usr/bin/env python3
"""Refuse an operator-UI test file that the runner never collects.

Issue #1035. `vitest.operator-ui.config.ts` included `**/*.test.tsx` only, so
`healthTruth.test.ts` was not skipped — it was absent from collection. It
reported nowhere, failed nothing, and appeared in no run summary, so the suite
showed 31 files passing while 32 existed on disk and nothing said one was
missing. An uncollected test is invisible in a way a skipped test is not.

The comparison asks vitest what it ACTUALLY collected (`vitest list
--filesOnly --json`) rather than re-deriving the include globs here. Parsing
the patterns out of the TypeScript config would be a second, independent guess
at collection — it would miss `exclude` patterns, workspace/project configs,
and any computed include list, and it would disagree with the runner exactly
when the disagreement matters.

Fails closed: a missing node toolchain, a non-zero vitest exit, unparseable
JSON, or zero files on either side is a failure, never a pass. A check that
cannot run must not report clean — that is the defect this file exists to
prevent, one level up.

Usage:
    python3 scripts/check_operator_ui_test_collection.py [--json]
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / "vitest.operator-ui.config.ts"

# The tree the vitest include glob is rooted at. `web/audit/` is deliberately
# outside it: those are Playwright specs and a `node --test` file driven by
# src/okg/operator_ui/web/audit/ci.sh, not vitest.
UI_TREE = REPO_ROOT / "src" / "okg" / "operator_ui" / "web" / "src"

# Deliberately a SUPERSET of the repo's `*.test.{ts,tsx}` convention: vitest's
# own default include, `**/*.{test,spec}.?(c|m)[jt]s?(x)`. If the on-disk side
# restated the same two extensions the config uses, both sides would be built
# from one assumption and the comparison could only catch a narrowing WITHIN
# it — a new `foo.spec.ts` or `layout.test.mts` would be absent from both sets
# and the check would report clean while the file ran nowhere. Anything a
# contributor would reasonably name a test is compared; a narrower repo
# convention has to be an explicit, reviewed exclusion below.
TEST_NAME_RE = re.compile(r"\.(test|spec)\.(c|m)?[jt]sx?$")

# Files matching the superset that are intentionally NOT vitest's to run. Each
# entry is a reviewed exclusion, not an omission.
NON_VITEST_TEST_FILES: dict[str, str] = {}


class CollectionCheckError(RuntimeError):
    """The check could not be evaluated. Never treated as a pass."""


def discover_test_files(tree: Path = UI_TREE) -> set[str]:
    """Every file under `tree` that anyone would reasonably call a test."""
    found = {
        str(path.relative_to(REPO_ROOT))
        for path in tree.rglob("*")
        if path.is_file() and TEST_NAME_RE.search(path.name)
    }
    return found - set(NON_VITEST_TEST_FILES)


def collected_files(config: Path = CONFIG) -> set[str]:
    """Ask vitest which files it actually collects."""
    if shutil.which("npx") is None:
        raise CollectionCheckError(
            "npx is not on PATH, so collection cannot be verified; install Node "
            "in this job rather than skipping the check"
        )
    try:
        result = subprocess.run(
            [
                "npx",
                "vitest",
                "list",
                "--config",
                str(config.relative_to(REPO_ROOT)),
                "--filesOnly",
                "--json",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as exc:
        raise CollectionCheckError(f"vitest list timed out: {exc}") from exc
    if result.returncode != 0:
        raise CollectionCheckError(
            f"vitest list exited {result.returncode}: {result.stderr.strip()[:500]}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise CollectionCheckError(
            f"vitest list did not return JSON: {exc}; stdout begins "
            f"{result.stdout[:200]!r}"
        ) from exc
    if not isinstance(payload, list):
        raise CollectionCheckError(
            f"vitest list returned {type(payload).__name__}, expected a list"
        )
    files: set[str] = set()
    for entry in payload:
        if not isinstance(entry, dict) or "file" not in entry:
            raise CollectionCheckError(f"unrecognized vitest list entry: {entry!r}")
        path = Path(str(entry["file"]))
        try:
            files.add(str(path.relative_to(REPO_ROOT)))
        except ValueError:
            files.add(str(path))
    return files


def compare(on_disk: set[str], collected: set[str]) -> dict:
    """Report orphans. Empty sets are a refusal, not agreement."""
    refusals: list[str] = []
    if not on_disk:
        refusals.append("no operator-UI test files found on disk")
    if not collected:
        refusals.append("vitest collected no files")
    orphaned = sorted(on_disk - collected)
    unexpected = sorted(collected - on_disk)
    return {
        "ok": not orphaned and not unexpected and not refusals,
        "orphaned": orphaned,
        "collected_but_not_on_disk": unexpected,
        "refusals": refusals,
        "on_disk_count": len(on_disk),
        "collected_count": len(collected),
    }


def check() -> dict:
    try:
        return compare(discover_test_files(), collected_files())
    except CollectionCheckError as exc:
        return {
            "ok": False,
            "orphaned": [],
            "collected_but_not_on_disk": [],
            "refusals": [str(exc)],
            "on_disk_count": None,
            "collected_count": None,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = check()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    elif report["ok"]:
        print(
            f"ok: {report['collected_count']} operator-UI test files collected, "
            f"matching {report['on_disk_count']} on disk"
        )
    else:
        for refusal in report["refusals"]:
            print(f"REFUSED: {refusal}", file=sys.stderr)
        for orphan in report["orphaned"]:
            print(
                f"UNCOLLECTED: {orphan} exists on disk but vitest never runs it",
                file=sys.stderr,
            )
        for extra in report["collected_but_not_on_disk"]:
            print(f"UNEXPECTED: {extra} collected but not a known test file",
                  file=sys.stderr)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
