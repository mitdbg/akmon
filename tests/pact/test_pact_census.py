"""Regression tests for the read-only #978 hygiene census."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.pact.census import DISPOSITIONS, build_census, write_census


def _raw(
    change_id: str,
    *,
    status: str = "verification",
    task_status: str = "todo",
    include_status: bool = True,
    evidence: list[dict] | None = None,
) -> dict:
    task: dict = {
        "id": f"task.{change_id}.one",
        "title": "One",
        "verification": "pytest tests/pact/test_pact_census.py -q",
    }
    if include_status:
        task["status"] = task_status
    else:
        task_status = "todo"
    if task_status == "done":
        task["requirements"] = [f"req.{change_id}"]
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": change_id, "title": change_id, "status": status},
        "requirements": [{
            "id": f"req.{change_id}",
            "statement": "The census has a durable, checkable result.",
            "evidence_policy": {"required": ["pytest"]},
        }],
        "tasks": [task],
        "test_plan": [],
        "formal": [],
        "waivers": [],
        "evidence": evidence or [],
    }


def _write(root: Path, tree: str, change_id: str, raw: dict) -> Path:
    path = root / "pact" / tree / change_id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _pass_evidence(change_id: str) -> list[dict]:
    return [{
        "id": f"ev.{change_id}.one",
        "kind": "pytest",
        "status": "pass",
        "refs": [f"task.{change_id}.one", f"req.{change_id}"],
    }]


def test_census_assigns_one_disposition_per_finding(tmp_path: Path) -> None:
    # Active work is an observation, not a hygiene finding.
    _write(tmp_path, "changes", "active", _raw("active", task_status="todo"))
    _write(
        tmp_path, "changes", "terminal-mismatch",
        _raw("terminal-mismatch", status="proposal", task_status="done",
             evidence=_pass_evidence("terminal-mismatch")),
    )
    _write(
        tmp_path, "changes", "missing-proof",
        _raw("missing-proof", task_status="done"),
    )
    _write(
        tmp_path, "changes", "stale-status",
        _raw("stale-status", task_status="done", include_status=False,
             evidence=_pass_evidence("stale-status")),
    )
    _write(
        tmp_path, "archive", "archived-stale",
        _raw("archived-stale", status="proposal", task_status="done",
             evidence=_pass_evidence("archived-stale")),
    )
    _write(
        tmp_path, "archive", "archived-history",
        _raw("archived-history", task_status="done"),
    )

    ledger = build_census(tmp_path)
    rows = ledger["findings"]
    assert rows
    assert all(row["disposition"] in DISPOSITIONS for row in rows)
    assert len({id(row) for row in rows}) == len(rows)
    assert {row["disposition"] for row in rows} == {
        "terminal_state_mismatch",
        "missing_proof",
        "stale_generated_state",
        "genuinely_unverifiable_history",
    }
    assert {row["disposition"] for row in ledger["observations"]} == {"active_work"}
    assert any(
        row["code"] == "completed_unarchived"
        and row["disposition"] == "terminal_state_mismatch"
        for row in ledger["findings"]
    )
    unproven_terminal = _write(
        tmp_path, "changes", "unproven-terminal",
        _raw("unproven-terminal", status="implementation", task_status="done"),
    )
    assert unproven_terminal.exists()
    rerun = build_census(tmp_path)
    row = next(
        row for row in rerun["findings"]
        if row["change"] == "unproven-terminal" and row["code"] == "completed_unarchived"
    )
    assert "resolve its missing proof first" in row["next_action"]
    assert ledger["trees"]["changes"]["rule_counts"][
        "change_status_stale_all_tasks_done"
    ]["findings"] == 1
    assert ledger["trees"]["changes"]["rule_counts"][
        "task_done_empty_evidence"
    ]["tasks"] == 1
    assert ledger["trees"]["changes"]["rule_counts"][
        "task_missing_status"
    ]["tasks"] == 1


def test_census_is_read_only_and_preserves_archived_history(tmp_path: Path) -> None:
    path = _write(
        tmp_path, "archive", "history", _raw("history", task_status="done")
    )
    before = path.read_bytes()
    ledger = build_census(tmp_path)
    assert path.read_bytes() == before
    assert ledger["read_only"] is True
    assert ledger["repair_actions_applied"] == []
    assert ledger["findings"][0]["disposition"] == "genuinely_unverifiable_history"
    assert "fabricating proof" in ledger["findings"][0]["next_action"]


def test_census_writes_machine_readable_ledger_with_before_after_counts(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "changes", "active", _raw("active", task_status="todo"))
    output = tmp_path / "reports" / "census.json"
    ledger = write_census(tmp_path, output)
    decoded = json.loads(output.read_text(encoding="utf-8"))
    assert decoded["schema"] == "okg.pact.hygiene_census.v1"
    assert decoded["comparison"]["before"]["source"].endswith("pact-hygiene-sweep.md")
    assert decoded["comparison"]["after"]["changes"]["scanned"] == 1
    assert decoded["baseline_ref"] == "origin/dev"
    assert decoded["source_ref"] is None
    assert ledger["comparison"]["after"] == decoded["comparison"]["after"]


def test_census_refuses_output_under_immutable_archive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="immutable pact/archive"):
        write_census(tmp_path, tmp_path / "pact" / "archive" / "bad.json")


def test_census_refuses_overwriting_authored_manifest(tmp_path: Path) -> None:
    manifest = _write(tmp_path, "changes", "demo", _raw("demo"))
    with pytest.raises(ValueError, match="authored pact.yaml"):
        write_census(tmp_path, manifest)


def test_census_surfaces_dangling_task_evidence_as_missing_proof(tmp_path: Path) -> None:
    raw = _raw("dangling", task_status="done")
    raw["tasks"][0]["evidence"] = ["ev.does-not-exist"]
    _write(tmp_path, "changes", "dangling", raw)
    ledger = build_census(tmp_path)
    # The hygiene predicate is intentionally shared with the gate and regards
    # a non-empty authored task.evidence list as an existing claim. The ledger
    # still exposes that malformed claim as an unresolved proof risk.
    row = next(row for row in ledger["findings"] if row["code"] == "malformed_task_evidence")
    assert row["disposition"] == "missing_proof"
    paired = next(
        row for row in ledger["findings"]
        if row["code"] == "completed_unarchived"
    )
    assert "resolve its missing proof first" in paired["next_action"]


def test_census_preserves_archive_malformed_evidence_action(tmp_path: Path) -> None:
    raw = _raw("archived-dangling", task_status="done")
    raw["tasks"][0]["evidence"] = ["ev.does-not-exist"]
    _write(tmp_path, "archive", "archived-dangling", raw)
    ledger = build_census(tmp_path)
    row = next(
        row for row in ledger["findings"]
        if row["code"] == "malformed_task_evidence"
    )
    assert row["disposition"] == "genuinely_unverifiable_history"
    assert "preserve immutable archive history exactly" in row["next_action"]
    assert "attach a resolved" not in row["next_action"]


def test_census_marks_git_provenance_unavailable_instead_of_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.pact import census

    monkeypatch.setattr(census, "_git", lambda *_args: (None, "git unavailable"))
    ledger = census.build_census(tmp_path)
    assert ledger["working_tree"] == "unavailable"
    assert ledger["provenance_errors"]
