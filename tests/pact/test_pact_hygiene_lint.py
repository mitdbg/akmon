"""Unit tests for the PACT-hygiene lint (task.audit-2026-07.enforce-pact-hygiene, #699).

Each banned pattern (status no longer discriminates, done-with-no-evidence,
missing schema-required status) must be FLAGGED on a fixture that embodies it,
and a compliant fixture must pass. These tests fail without the lint.
"""

from __future__ import annotations

from typing import Any

import yaml

from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import _gate_validation_report, post_edit_check
from okg.substrate.pact.validation import pact_hygiene_issues, validate_manifest
from tests.pact.test_cli import _seed_root


def _base_raw(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": "demo", "title": "Demo", "status": "proposal", "tier": 1},
        "requirements": [
            {
                "id": "req.demo",
                "statement": "Demo requirement.",
                "evidence_policy": {"required": ["pytest"]},
            }
        ],
        "tasks": [],
        "evidence": [],
    }
    raw.update(overrides)
    return raw


def _codes(raw: dict[str, Any]) -> set[str]:
    manifest = PactManifest.model_validate(raw)
    return {issue.code for issue in pact_hygiene_issues(manifest, raw_document=raw)}


# --- Rule 1: all tasks done but change.status still 'proposal' -----------------


def test_all_tasks_done_but_status_proposal_is_flagged() -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "proposal"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            },
            {
                "id": "task.demo.two",
                "title": "Two",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.two"],
            },
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]},
            {"id": "ev.two", "kind": "pytest", "status": "pass", "refs": ["task.demo.two"]},
        ],
    )
    assert "change_status_stale_all_tasks_done" in _codes(raw)


def test_all_tasks_done_with_advanced_status_passes() -> None:
    raw = _base_raw(
        change={
            "id": "demo",
            "title": "Demo",
            "status": "verification",
            "tier": 1,
        },
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    assert "change_status_stale_all_tasks_done" not in _codes(raw)


def test_partially_done_proposal_is_not_flagged_by_rule_one() -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "proposal"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            },
            {
                "id": "task.demo.two",
                "title": "Two",
                "status": "todo",
                "verification": "v",
            },
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    assert "change_status_stale_all_tasks_done" not in _codes(raw)


def test_zero_task_proposal_is_not_flagged_by_rule_one() -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "proposal"},
        tasks=[],
    )
    assert "change_status_stale_all_tasks_done" not in _codes(raw)


# --- Rule 2: task status:done with evidence:[] --------------------------------


def test_done_task_with_empty_evidence_is_flagged() -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": [],
            }
        ],
    )
    assert "task_done_empty_evidence" in _codes(raw)


def test_done_task_with_evidence_ref_passes_rule_two() -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    assert "task_done_empty_evidence" not in _codes(raw)


def test_done_task_with_change_level_evidence_passes_rule_two() -> None:
    # The canonical `okg pact evidence attach` verb writes a CHANGE-LEVEL
    # evidence record (refs=[task_id]); task.evidence stays empty. Rule 2 must
    # NOT flag such a task -- it is evidenced by the completion gate's predicate.
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "requirements": ["req.demo"],
                "evidence": [],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    assert "task_done_empty_evidence" not in _codes(raw)


def test_done_task_with_change_level_evidence_via_requirement_passes() -> None:
    # A change-level record that refs the task's REQUIREMENT (not the task id
    # directly) also evidences it -- _task_valid_evidence_refs expands to the
    # requirement/acceptance/test-plan/formal ids in scope.
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "requirements": ["req.demo"],
                "evidence": [],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["req.demo"]}
        ],
    )
    assert "task_done_empty_evidence" not in _codes(raw)


def test_done_task_with_failing_change_level_evidence_is_flagged() -> None:
    # A change-level record that refs the task but is NOT passing does not
    # evidence it -- Rule 2 still fires.
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "requirements": ["req.demo"],
                "evidence": [],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "fail", "refs": ["task.demo.one"]}
        ],
    )
    assert "task_done_empty_evidence" in _codes(raw)


def test_todo_task_with_empty_evidence_is_not_flagged() -> None:
    raw = _base_raw(
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "todo",
                "verification": "v",
                "evidence": [],
            }
        ],
    )
    assert "task_done_empty_evidence" not in _codes(raw)


# --- Rule 3: task missing the schema-required status field ---------------------


def test_task_missing_status_field_is_flagged() -> None:
    raw = _base_raw(
        tasks=[
            {
                # no "status" key at all -- pydantic silently defaults it to
                # "todo", so this rule must read the raw document.
                "id": "task.demo.one",
                "title": "One",
                "verification": "v",
            }
        ],
    )
    assert "task_missing_status" in _codes(raw)


def test_task_with_explicit_status_passes_rule_three() -> None:
    raw = _base_raw(
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "todo",
                "verification": "v",
            }
        ],
    )
    assert "task_missing_status" not in _codes(raw)


def test_rule_three_dormant_without_raw_document() -> None:
    # Without the raw document the parsed manifest already carries the default
    # status, so the rule cannot fire (documented limitation, not a false pass:
    # the gate always threads the raw document).
    raw = _base_raw(
        tasks=[{"id": "task.demo.one", "title": "One", "verification": "v"}]
    )
    manifest = PactManifest.model_validate(raw)
    codes = {issue.code for issue in pact_hygiene_issues(manifest)}
    assert "task_missing_status" not in codes


# --- Wiring: hygiene warnings surface through the gate boundary ----------------
#
# The hygiene lint is enforced at the gate (post_edit_check / _gate_validation_
# report), NOT inside the universal validate_manifest whose .ok many internal
# callers depend on. v1 ships the three rules at severity="warning" (visible,
# non-blocking) so the enforcing gate does not red-wall the ~97 done-but-proposal
# + ~71 unevidenced existing PACTs before an operator-gated tree-sweep clears
# them; they flip to severity="error" in a follow-up. These tests pin that
# boundary: the gate report SURFACES the hygiene code as a warning but stays ok,
# and the raw validate_manifest report does not carry the hygiene codes at all.


def test_gate_report_surfaces_hygiene_warning(tmp_path) -> None:
    raw = _base_raw(
        change={
            "id": "demo",
            "title": "Demo",
            "status": "verification",
            "tier": 1,
        },
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": [],
            }
        ],
    )
    manifest = PactManifest.model_validate(raw)
    report = _gate_validation_report(manifest, tmp_path, raw)
    # v1: warning-severity — the hygiene code is surfaced for operator
    # visibility and is never itself a reason to refuse.
    assert any(
        issue.code == "task_done_empty_evidence"
        for issue in report.issues
        if issue.severity == "warning"
    )
    assert not any(
        issue.code == "task_done_empty_evidence"
        for issue in report.issues
        if issue.severity == "error"
    )
    # The gate DOES refuse this manifest, for the proof checks rather than the
    # lint: a done task that links no requirement closes against nothing
    # (`task_requirements_link` + `task_done_missing_evidence`,
    # pact-vacuous-gate-close). Pinned exactly, so a later change cannot let
    # the hygiene lint start doing the blocking by the back door.
    assert {
        issue.code for issue in report.issues if issue.severity == "error"
    } == {"task_requirements_link", "task_done_missing_evidence"}


def test_universal_validate_manifest_stays_clean_of_hygiene() -> None:
    # The broad validate_manifest path (edit-guard, review, retire) must NOT
    # flip on hygiene rot, or the whole closure workflow breaks.
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "proposal"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": [],
            }
        ],
    )
    manifest = PactManifest.model_validate(raw)
    hygiene_codes = {
        "change_status_stale_all_tasks_done",
        "task_done_empty_evidence",
        "task_missing_status",
    }
    report = validate_manifest(manifest)
    assert not any(issue.code in hygiene_codes for issue in report.issues)


def test_gate_report_clean_manifest_has_no_hygiene_error(tmp_path) -> None:
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "verification"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    manifest = PactManifest.model_validate(raw)
    report = _gate_validation_report(manifest, tmp_path, raw)
    hygiene_codes = {
        "change_status_stale_all_tasks_done",
        "task_done_empty_evidence",
        "task_missing_status",
    }
    assert not any(issue.code in hygiene_codes for issue in report.issues)


# --- End-to-end: `gate post-edit` machinery rejects a rotted authored file -----


def _write_pact(root, change_id: str, raw: dict) -> None:
    change_dir = root / "pact" / "changes" / change_id
    change_dir.mkdir(parents=True, exist_ok=True)
    (change_dir / "pact.yaml").write_text(
        yaml.safe_dump(raw, sort_keys=False), encoding="utf-8"
    )


def test_post_edit_gate_warns_missing_task_status(tmp_path) -> None:
    # The missing-status rule can only fire against the AUTHORED file (the
    # parser backfills 'todo'); this pins raw-document threading through
    # post_edit_check. v1: warning-severity, so the gate reports it as a
    # warning without blocking (result["ok"] stays True).
    root = _seed_root(tmp_path)
    raw = _base_raw(
        tasks=[{"id": "task.demo.one", "title": "One", "verification": "v"}]
    )
    _write_pact(root, "demo", raw)
    result = post_edit_check(root, "demo")
    warn_codes = {e["code"] for e in result["validation"]["warnings"]}
    err_codes = {e["code"] for e in result["validation"]["errors"]}
    assert "task_missing_status" in warn_codes
    assert "task_missing_status" not in err_codes


def test_post_edit_gate_warns_all_done_but_proposal(tmp_path) -> None:
    root = _seed_root(tmp_path)
    raw = _base_raw(
        change={"id": "demo", "title": "Demo", "status": "proposal"},
        tasks=[
            {
                "id": "task.demo.one",
                "title": "One",
                "status": "done",
                "verification": "v",
                "evidence": ["ev.one"],
            }
        ],
        evidence=[
            {"id": "ev.one", "kind": "pytest", "status": "pass", "refs": ["task.demo.one"]}
        ],
    )
    _write_pact(root, "demo", raw)
    result = post_edit_check(root, "demo")
    warn_codes = {e["code"] for e in result["validation"]["warnings"]}
    err_codes = {e["code"] for e in result["validation"]["errors"]}
    assert "change_status_stale_all_tasks_done" in warn_codes
    assert "change_status_stale_all_tasks_done" not in err_codes
