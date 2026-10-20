"""Read-only PACT hygiene census and disposition ledger.

The census is deliberately separate from the gate.  It measures the exact
three hygiene predicates over both active and archived manifests, then records
what an operator may do next.  It never changes a manifest and never treats a
claim, prose report, or generated artifact as proof.
"""

from __future__ import annotations

import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from okg.substrate.pact.store import load_manifest_file, load_yaml_mapping
from okg.substrate.pact.validation import (
    _task_valid_evidence_refs,
    pact_hygiene_issues,
)

Disposition = Literal[
    "active_work",
    "stale_generated_state",
    "terminal_state_mismatch",
    "missing_proof",
    "genuinely_unverifiable_history",
]

DISPOSITIONS = frozenset({
    "active_work",
    "stale_generated_state",
    "terminal_state_mismatch",
    "missing_proof",
    "genuinely_unverifiable_history",
})

# This is the read-only baseline shipped by the prior census.  It is kept as a
# comparison point, not as current truth and never used to suppress findings.
HISTORICAL_BASELINE = {
    "source": "pact/changes/remediate-audit-2026-07/reports/pact-hygiene-sweep.md",
    "changes": {
        "scanned": 242,
        "change_status_stale_all_tasks_done": {"changes": 97, "tasks": 0, "findings": 97},
        "task_done_empty_evidence": {"changes": 20, "tasks": 71, "findings": 71},
        "task_missing_status": {"changes": 0, "tasks": 0, "findings": 0},
    },
    "archive": {
        "scanned": 462,
        "change_status_stale_all_tasks_done": {"changes": 5, "tasks": 0, "findings": 5},
        "task_done_empty_evidence": {"changes": 13, "tasks": 41, "findings": 41},
        "task_missing_status": {"changes": 0, "tasks": 0, "findings": 0},
    },
}


def _git(root: Path, *args: str) -> tuple[str | None, str | None]:
    try:
        return subprocess.check_output(
            ["git", *args], cwd=root, text=True, stderr=subprocess.DEVNULL,
        ).strip() or None, None
    except (OSError, subprocess.CalledProcessError) as exc:
        return None, str(exc)[:200]


def _subject(issue_code: str, path: str, manifest: Any) -> tuple[str, str | None]:
    if issue_code == "change_status_stale_all_tasks_done":
        return "change", None
    if path.startswith("tasks["):
        try:
            index = int(path.split("[", 1)[1].split("]", 1)[0])
            return "task", manifest.tasks[index].id
        except (IndexError, ValueError):
            pass
    return "path", path


def _disposition(tree: str, issue_code: str, *, finding: bool) -> Disposition:
    if not finding:
        return "active_work"
    if issue_code == "task_done_empty_evidence":
        return (
            "missing_proof"
            if tree == "changes"
            else "genuinely_unverifiable_history"
        )
    if issue_code == "task_missing_status":
        return (
            "stale_generated_state"
            if tree == "changes"
            else "genuinely_unverifiable_history"
        )
    # A proposal in the active tree disagrees with its completed tasks.  In
    # the archive the location is terminal, so the authored status is stale
    # generated state rather than a reason to rewrite history.
    return (
        "terminal_state_mismatch"
        if tree == "changes"
        else "stale_generated_state"
    )


def _next_action(
    disposition: Disposition,
    change: str,
    *,
    code: str | None = None,
    related_codes: set[str] | None = None,
) -> str:
    if disposition == "active_work":
        return "no action; keep using the authored task flow"
    if disposition == "terminal_state_mismatch":
        if code in {"change_status_stale_all_tasks_done", "completed_unarchived"} and (
            related_codes
            and (
                "task_done_empty_evidence" in related_codes
                or "malformed_task_evidence" in related_codes
            )
        ):
            return (
                f"do not approve {change}: resolve its missing proof first; then use "
                "the legal lifecycle/decision-ledger review path or archive it"
            )
        return (
            f"review the derived lifecycle for {change}; use only a legal "
            "decision-ledger approve/revise/reject/defer transition or archive "
            "path, and never manufacture proof"
        )
    if disposition == "stale_generated_state":
        if code == "task_missing_status" and change:
            return (
                f"edit the authored task status in {change} to an explicit legal "
                "value (todo, in_progress, blocked, or done), then run the post-edit gate"
            )
        return (
            f"preserve {change} exactly; use only a legal owner-approved waiver or "
            "archive path, and never rewrite immutable history"
        )
    if disposition == "missing_proof":
        return (
            f"attach the actually produced proof for {change} with `okg pact "
            "evidence attach`, or record an owner-approved `okg pact evidence waive`; "
            "never infer evidence"
        )
    return (
        "keep immutable archive history unchanged; record a legal owner-approved "
        "waiver or leave the row unresolved rather than fabricating proof"
    )


def _rule_counts(rows: list[dict[str, Any]], manifests: int) -> dict[str, Any]:
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("finding"):
            by_code[str(row["code"])].append(row)
    result: dict[str, Any] = {"scanned": manifests}
    for code in (
        "change_status_stale_all_tasks_done",
        "task_done_empty_evidence",
        "task_missing_status",
    ):
        code_rows = by_code.get(code, [])
        result[code] = {
            "changes": len({str(row["change"]) for row in code_rows}),
            "tasks": sum(row.get("subject_kind") == "task" for row in code_rows),
            "findings": len(code_rows),
        }
    supplemental = [
        row for row in rows
        if row.get("finding") and row.get("code") not in {
            "change_status_stale_all_tasks_done",
            "task_done_empty_evidence",
            "task_missing_status",
        }
    ]
    result["supplemental_findings"] = {
        "changes": len({str(row["change"]) for row in supplemental}),
        "findings": len(supplemental),
        "by_code": dict(sorted(Counter(str(row["code"]) for row in supplemental).items())),
    }
    return result


def _census_tree(root: Path, tree: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    paths = sorted((root / "pact" / tree).glob("*/pact.yaml"))
    unreadable: list[dict[str, str]] = []
    active_observations = 0
    issue_codes_by_change: dict[str, set[str]] = defaultdict(set)
    parsed: list[tuple[Path, Any, list[Any]]] = []
    for path in paths:
        change = path.parent.name
        try:
            manifest = load_manifest_file(path)
            raw = load_yaml_mapping(path)
        except Exception as exc:  # pragma: no cover - exercised by integration users
            unreadable.append({"change": change, "error": str(exc)[:240]})
            rows.append({
                "finding": True,
                "change": change,
                "tree": tree,
                "code": "unreadable_manifest",
                "path": "pact.yaml",
                "subject_kind": "path",
                "subject_id": "pact.yaml",
                "disposition": "genuinely_unverifiable_history",
                "next_action": "preserve the record and obtain an operator-readable copy",
            })
            continue
        issues = pact_hygiene_issues(manifest, raw_document=raw)
        parsed.append((path, manifest, issues))
        issue_codes_by_change[change].update(issue.code for issue in issues)
    for path, manifest, issues in parsed:
        change = path.parent.name
        all_tasks_done = bool(manifest.tasks) and all(
            task.status == "done" for task in manifest.tasks
        )
        completed_unarchived = (
            tree == "changes" and all_tasks_done
            and manifest.change.status != "proposal"
        )
        evidence_by_id = {evidence.id: evidence for evidence in manifest.evidence}
        for task in manifest.tasks:
            if task.status != "done":
                continue
            valid_refs = _task_valid_evidence_refs(manifest, task)
            malformed = [
                evidence_id
                for evidence_id in task.evidence
                if evidence_id not in evidence_by_id
                or evidence_by_id[evidence_id].status not in {"pass", "waived"}
                or not (
                    set(evidence_by_id[evidence_id].refs) & valid_refs
                )
            ]
            if malformed:
                issue_codes_by_change[change].add("malformed_task_evidence")
                rows.append({
                    "finding": True,
                    "change": change,
                    "tree": tree,
                    "code": "malformed_task_evidence",
                    "path": "tasks[].evidence",
                    "subject_kind": "task",
                    "subject_id": task.id,
                    "disposition": (
                        "missing_proof"
                        if tree == "changes"
                        else "genuinely_unverifiable_history"
                    ),
                    "next_action": (
                        (
                            "preserve immutable archive history exactly; do not attach "
                            "or rewrite evidence; record a legal owner-approved "
                            "historical waiver or leave this row unresolved"
                        )
                        if tree == "archive"
                        else (
                            f"do not treat {malformed} as proof for {change}; attach a "
                            "resolved, passing, in-scope evidence row or use an "
                            "owner-approved waiver"
                        )
                    ),
                    "severity": "warning",
                })
        if completed_unarchived:
            rows.append({
                "finding": True,
                "change": change,
                "tree": tree,
                "code": "completed_unarchived",
                "path": "change.status",
                "subject_kind": "change",
                "subject_id": change,
                "disposition": "terminal_state_mismatch",
                "next_action": _next_action(
                    "terminal_state_mismatch",
                    change,
                    code="completed_unarchived",
                    related_codes=issue_codes_by_change[change],
                ),
                "severity": "warning",
            })
        if not issues and tree == "changes" and any(
            task.status != "done" for task in manifest.tasks
        ):
            active_observations += 1
            rows.append({
                "finding": False,
                "change": change,
                "tree": tree,
                "code": "active_work",
                "path": "tasks",
                "subject_kind": "change",
                "subject_id": change,
                "disposition": "active_work",
                "next_action": _next_action("active_work", change),
            })
        for issue in issues:
            subject_kind, subject_id = _subject(issue.code, issue.path, manifest)
            disposition = _disposition(tree, issue.code, finding=True)
            rows.append({
                "finding": True,
                "change": change,
                "tree": tree,
                "code": issue.code,
                "path": issue.path,
                "subject_kind": subject_kind,
                "subject_id": subject_id,
                "disposition": disposition,
                "next_action": _next_action(
                    disposition,
                    change,
                    code=issue.code,
                    related_codes=issue_codes_by_change[change],
                ),
                "severity": issue.severity,
            })
    return {
        "scanned": len(paths),
        "active_work_observations": active_observations,
        "unreadable": unreadable,
        "rule_counts": _rule_counts(rows, len(paths)),
    }, rows


def build_census(root: Path) -> dict[str, Any]:
    """Build a deterministic, read-only census ledger for ``root``."""
    generated_at = datetime.now(timezone.utc).isoformat()
    tree_reports: dict[str, Any] = {}
    findings: list[dict[str, Any]] = []
    for tree in ("changes", "archive"):
        report, rows = _census_tree(root, tree)
        tree_reports[tree] = report
        findings.extend(rows)

    disposition_counts = Counter(
        str(row["disposition"]) for row in findings
    )
    current = {
        tree: tree_reports[tree]["rule_counts"] for tree in ("changes", "archive")
    }
    source_commit, source_commit_error = _git(root, "rev-parse", "HEAD")
    source_ref, source_ref_error = _git(root, "branch", "--show-current")
    baseline_commit, baseline_error = _git(root, "rev-parse", "origin/dev")
    working_tree, working_tree_error = _git(root, "status", "--porcelain")
    provenance_errors = {
        "source_commit": source_commit_error,
        "source_ref": source_ref_error,
        "baseline_commit": baseline_error,
        "working_tree": working_tree_error,
    }
    provenance_errors = {
        key: value for key, value in provenance_errors.items() if value
    }
    return {
        "schema": "okg.pact.hygiene_census.v1",
        "generated_at": generated_at,
        "source_commit": source_commit,
        "source_ref": source_ref,
        "baseline_ref": "origin/dev",
        "baseline_commit": baseline_commit,
        "working_tree": (
            "unavailable" if working_tree_error
            else "dirty" if working_tree else "clean"
        ),
        "provenance_errors": provenance_errors,
        "read_only": True,
        "repair_actions_applied": [],
        "comparison": {
            "before": HISTORICAL_BASELINE,
            "after": {
                "source": "current working tree exact hygiene predicates",
                "changes": current["changes"],
                "archive": current["archive"],
            },
        },
        "trees": tree_reports,
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "findings": [row for row in findings if row["finding"]],
        "observations": [row for row in findings if not row["finding"]],
        "unresolved": [row for row in findings if row["finding"]],
    }


def write_census(root: Path, output: Path) -> dict[str, Any]:
    """Write a census ledger, refusing accidental overwrite of an archive."""
    resolved = output.resolve()
    archive_root = (root / "pact" / "archive").resolve()
    changes_root = (root / "pact" / "changes").resolve()
    if resolved.is_relative_to(archive_root) or (
        resolved.is_relative_to(changes_root)
        and resolved.name in {"pact.yaml", "pact.yaml.lock", "pact.md"}
    ):
        raise ValueError(
            "census output cannot write under immutable pact/archive or overwrite "
            "an authored pact.yaml/generated PACT artifact"
        )
    ledger = build_census(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(ledger, sort_keys=False), encoding="utf-8") if output.suffix in {".yaml", ".yml"} else output.write_text(
        __import__("json").dumps(ledger, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ledger
