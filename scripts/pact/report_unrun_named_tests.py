#!/usr/bin/env python3
"""List recorded PACT evidence that proves a requirement whose named test never ran.

Ingest now refuses to mark a requirement proven when a plan entry naming one
specific test is absent from the report or skipped in it. Evidence recorded
BEFORE that check existed was accepted under the old rule, and this reads it back
to say which of those rest on a test that did not run.

Derived entirely from what was recorded — the junit test ids and skipped test ids
already stored on each evidence row — not from re-running anything. The point is
to describe what was actually accepted at the time.

This reports. It does not rewrite evidence. A row with no JUnit identities is
reported as unattested rather than silently omitted: the canonical row remains
visible, while the validator independently refuses to count it as proof.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from okg.substrate.pact.evidence_kinds import load_registry


def _plan_by_requirement(manifest: dict) -> dict[str, list[tuple[str, str]]]:
    """Requirement -> [(plan entry id, the single pytest test it names)].

    Keyed by requirement, not by entry id, because the whole point is to notice
    entries the evidence never mentions. Only pytest node ids count: plan entries
    also use `design::`, `graph::` and similar schemes for non-pytest evidence,
    which no report can contain.
    """
    index: dict[str, list[tuple[str, str]]] = {}
    for entry in manifest.get("test_plan") or []:
        test_id = str(entry.get("test_id") or "")
        if ".py::" not in test_id:
            continue
        index.setdefault(str(entry.get("requirement")), []).append(
            (str(entry.get("id")), test_id)
        )
    return index


def _key(value: str) -> tuple[str, str]:
    """(file key, test name) for a plan id or a junit id, comparably spelled."""
    path, *selectors = value.split("::")
    path = path.replace("/", ".")
    if path.endswith(".py"):
        path = path[: -len(".py")]
    normalized_selectors = [selector.split("[")[0] for selector in selectors]
    if len(normalized_selectors) > 1:
        path = ".".join([path, *normalized_selectors[:-1]])
    return path, normalized_selectors[-1]


def _matches(named: str, junit_ids: list[str]) -> bool:
    # File AND name. Matching on the bare function name would call a same-named
    # test in another file a match, which is the failure being looked for.
    want_file, want_name = _key(named)
    for junit_id in junit_ids:
        got_file, got_name = _key(junit_id)
        if got_name != want_name:
            continue
        if got_file == want_file or got_file.startswith(f"{want_file}."):
            return True
    return False


def scan(root: Path) -> list[dict]:
    findings: list[dict] = []
    registry = load_registry(root)
    for pact in sorted((root / "pact" / "changes").glob("*/pact.yaml")):
        manifest = yaml.safe_load(pact.read_text(encoding="utf-8")) or {}
        plan = _plan_by_requirement(manifest)
        if not plan:
            continue
        acceptance_owner = {
            str(criterion.get("id")): str(requirement.get("id"))
            for requirement in manifest.get("requirements") or []
            for criterion in requirement.get("acceptance") or []
            if criterion.get("id") and requirement.get("id")
        }
        task_requirements = {
            str(task.get("id")): [str(ref) for ref in task.get("requirements") or []]
            for task in manifest.get("tasks") or []
            if task.get("id")
        }
        for row in manifest.get("evidence") or []:
            if registry.canonicalize(str(row.get("kind"))) != "executed_test":
                continue
            if row.get("status") not in {"pass", "waived"}:
                continue
            data = row.get("data") or {}
            junit_ids = list(data.get("junit_test_ids") or [])
            if data.get("junit_test_id"):
                junit_ids.append(str(data["junit_test_id"]))
            skipped = list(data.get("skipped_test_ids") or [])
            failed = list(data.get("failed_test_ids") or [])
            # Every named test of every requirement this row claims to prove —
            # NOT the row's own `planned_tests`, which by construction holds only
            # entries that matched a case in the report and therefore can never
            # contain the absent ones this exists to find.
            scoped_requirements: set[str] = set()
            task_scoped_requirements: set[str] = set()
            for ref in row.get("refs") or []:
                ref = str(ref)
                if ref in plan:
                    scoped_requirements.add(ref)
                if ref in acceptance_owner:
                    scoped_requirements.add(acceptance_owner[ref])
                task_scoped_requirements.update(task_requirements.get(ref, []))
            if not scoped_requirements:
                scoped_requirements.update(task_scoped_requirements)
            for requirement in sorted(scoped_requirements):
                for entry_id, named in plan.get(str(requirement), []):
                    if _matches(named, skipped):
                        reason = "skipped"
                    elif _matches(named, failed):
                        reason = "failed"
                    elif not _matches(named, junit_ids):
                        reason = "absent" if junit_ids else "unattested"
                    else:
                        continue
                    findings.append(
                        {
                            "change": pact.parent.name,
                            "evidence": row.get("id"),
                            "requirement": str(requirement),
                            "planned_test": entry_id,
                            "named_test": named,
                            "reason": reason,
                        }
                    )
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    findings = scan(args.root)
    if args.json:
        json.dump(findings, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        if not findings:
            print("no recorded evidence rests on a named test that did not run")
        for item in findings:
            print(
                f"{item['change']}\n"
                f"    {item['requirement']} <- {item['evidence']}\n"
                f"    {item['named_test']} ({item['reason']})"
            )
        print(f"\n{len(findings)} finding(s)")
    # Reporting only: a non-zero exit would turn a list for the operator to
    # judge into a gate, and which of these matter is exactly the judgement
    # this script cannot make.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
