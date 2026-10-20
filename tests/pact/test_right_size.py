"""Per `right-size-pact-authoring`: diff-vs-scope drift, evidence staleness
revocation, and the authoring tier policy."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact import scope_drift as pact_scope_drift
from okg.substrate.pact.cli import _status
from okg.substrate.pact.fsm import DecisionEvent
from okg.substrate.pact.models import Evidence
from okg.substrate.pact.scope_drift import drift_report, staleness_report
from okg.substrate.pact.store import intent_digest, load_manifest, save_manifest

from tests.private_policy import private_policy_path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _git(root: Path, *args: str, env: dict | None = None) -> None:
    import os

    merged = {**os.environ, **(env or {})}
    subprocess.run(
        ["git", "-C", str(root), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True, capture_output=True, env=merged,
    )


def _seed_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    _git(root, "init", "-q", "-b", "dev")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "seed")
    _git(root, "checkout", "-q", "-b", "work")
    return root


def _declare_scope(root: Path, change_id: str, patterns: list[str]) -> None:
    import yaml

    path = root / "pact" / "changes" / change_id / "pact.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["change"]["affected_code"] = patterns
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")


def _new_change(root: Path, change_id: str) -> None:
    assert main([
        "pact", "--root", str(root), "new", change_id,
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, change_id, req_id="req.rs", title="RS",
        statement="Right-size must hold.", evidence=["executed_test"],
    )
    pact_operations.add_task(
        root, change_id, task_id="task.rs", title="Do RS",
        verification="pytest -q", requirements=["req.rs"],
    )


def test_pre_merge_reports_uncovered_paths(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    _declare_scope(root, "rs", ["src/covered/*"])
    (root / "src" / "covered").mkdir(parents=True)
    (root / "src" / "covered" / "in_scope.py").write_text("x = 1\n")
    (root / "src" / "rogue.py").write_text("y = 2\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "work")

    report = drift_report(root, load_manifest(root, "rs"), base_branch="dev")
    assert "src/rogue.py" in report["uncovered"]
    assert "src/covered/in_scope.py" not in report["uncovered"]
    assert "scope drift" in report["warning"]

    capsys.readouterr()
    main(["pact", "--root", str(root), "gate", "pre-merge", "rs", "--json"])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    gate = json.loads(lines[-1])
    assert "src/rogue.py" in gate["scope_drift"]["uncovered"]
    # the same warning text appears in the decision brief
    capsys.readouterr()
    assert main([
        "pact", "--root", str(root), "view", "rs", "--format=brief", "--json",
    ]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    brief = json.loads(lines[-1])
    assert brief["operator_brief"]["scope_drift_warning"] == report["warning"]


def test_stale_evidence_revokes_task_satisfaction(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    _declare_scope(root, "rs", ["src/covered/*"])
    (root / "src" / "covered").mkdir(parents=True)
    (root / "src" / "covered" / "mod.py").write_text("x = 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "covered code v1")
    pact_operations.attach_evidence(
        root, "rs", "req.rs",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    # covered code changes AFTER the evidence (future committer date makes
    # the ordering deterministic despite 1s git timestamp granularity)
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    (root / "src" / "covered" / "mod.py").write_text("x = 2\n")
    _git(root, "add", "-A")
    _git(
        root, "commit", "-q", "-m", "covered code v2",
        env={"GIT_COMMITTER_DATE": future, "GIT_AUTHOR_DATE": future},
    )

    staleness = staleness_report(root, load_manifest(root, "rs"), "req.rs")
    assert staleness["revoked"] is True
    assert staleness["stale"]

    gate = pact_operations.task_completion_gate(root, "rs", "task.rs")
    assert gate["ok"] is False
    assert "req.rs:stale_evidence" in gate["missing_proof"]
    assert gate["stale_evidence"]["req.rs"]["stale"]

    # evidence newer than the code change restores satisfaction
    newer = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    pact_operations.attach_evidence(
        root, "rs", "req.rs",
        kind="executed_test", ref="tests/x.py", status="pass",
        data={"attached_at": newer},
    )
    staleness = staleness_report(root, load_manifest(root, "rs"), "req.rs")
    assert staleness["revoked"] is False
    gate = pact_operations.task_completion_gate(root, "rs", "task.rs")
    assert "req.rs:stale_evidence" not in gate["missing_proof"]


def _prepare_status_fixture(root: Path) -> None:
    _new_change(root, "rs")
    pact_operations.require_test(root, "rs", "req.rs", "tests/x.py")
    _declare_scope(root, "rs", ["src/covered/*"])
    (root / "src" / "covered").mkdir(parents=True)
    (root / "src" / "covered" / "mod.py").write_text("x = 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "covered code v1")


def _attach_and_finish(
    root: Path,
    requirement_id: str,
    task_id: str,
    *,
    attached_at: str | None = None,
) -> str:
    manifest = pact_operations.attach_evidence(
        root,
        "rs",
        requirement_id,
        kind="executed_test",
        ref="tests/x.py",
        status="pass",
        data=({"attached_at": attached_at} if attached_at else None),
    )
    evidence_id = manifest.evidence[-1].id
    pact_operations.update_task_status(
        root,
        "rs",
        task_id,
        status="done",
        evidence_ref=evidence_id,
    )
    return evidence_id


def _status_without_ledger(root: Path) -> dict:
    manifest = load_manifest(root, "rs")
    return _status(
        root,
        "rs",
        include_graph=False,
        decision_events=[
            DecisionEvent(
                change_id="rs",
                verdict="approve",
                principal="riley",
                from_state="proposed",
                to_state="in_progress",
                intent_digest=intent_digest(manifest),
            )
        ],
        ledger_consulted=True,
    )


def test_status_reports_pre_change_evidence_as_actionable_gap(
    tmp_path: Path,
) -> None:
    root = _seed_root(tmp_path)
    _prepare_status_fixture(root)
    manifest = load_manifest(root, "rs")
    manifest.requirements[0].evidence_policy.required = []
    manifest.requirements[0].evidence_policy.required_any = [
        "pytest",
        "manual_visual",
    ]
    save_manifest(root, manifest)
    evidence_id = _attach_and_finish(root, "req.rs", "task.rs")
    untimestamped_id = "ev.executed-test.req-rs.untimestamped"
    manifest = load_manifest(root, "rs")
    manifest.evidence.append(Evidence(
        id=untimestamped_id,
        kind="executed_test",
        status="pass",
        refs=["req.rs"],
    ))
    save_manifest(root, manifest)
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    (root / "src" / "covered" / "mod.py").write_text("x = 2\n")
    _git(root, "add", "-A")
    _git(
        root,
        "commit",
        "-q",
        "-m",
        "covered code v2",
        env={"GIT_COMMITTER_DATE": future, "GIT_AUTHOR_DATE": future},
    )

    status = _status_without_ledger(root)
    gap = status["evidence_gaps"][0]

    assert status["state"] == "in_progress"
    assert "tasks terminal but evidence gaps remain" in status["derived_state"]["reason"]
    assert status["missing_evidence"] == status["evidence_gaps"]
    assert gap["id"] == "req.rs"
    assert gap["missing"] == ["any:pytest|manual_visual"]
    assert gap["gap_type"] == "scope_drift"
    assert gap["reason"] == "code_changed"
    assert gap["revoked_evidence_ids"] == [evidence_id, untimestamped_id]
    assert gap["affected_code"] == ["src/covered/*"]
    assert gap["code_change_boundary"] == {
        "status": "known",
        "code_changed_at": gap["code_changed_at"],
    }
    assert gap["code_changed_at"].startswith(future[:19])
    assert "Rerun and attach fresh evidence" in gap["action"]


def test_status_keeps_post_change_evidence_satisfied(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _prepare_status_fixture(root)
    attached_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    _attach_and_finish(
        root,
        "req.rs",
        "task.rs",
        attached_at=attached_at,
    )

    status = _status_without_ledger(root)

    assert status["state"] == "awaiting_review"
    assert status["evidence_gaps"] == []
    assert status["missing_evidence"] == []


def test_status_reports_each_requirement_sharing_scope(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _prepare_status_fixture(root)
    pact_operations.add_requirement(
        root,
        "rs",
        req_id="req.rs.second",
        title="Second shared-scope requirement",
        statement="The second requirement must stay proven.",
        evidence=["executed_test"],
    )
    pact_operations.add_task(
        root,
        "rs",
        task_id="task.rs.second",
        title="Prove the second requirement",
        verification="pytest -q",
        requirements=["req.rs.second"],
    )
    pact_operations.require_test(root, "rs", "req.rs.second", "tests/y.py")
    first_evidence = _attach_and_finish(root, "req.rs", "task.rs")
    second_evidence = _attach_and_finish(
        root,
        "req.rs.second",
        "task.rs.second",
    )
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    (root / "src" / "covered" / "mod.py").write_text("x = 2\n")
    _git(root, "add", "-A")
    _git(
        root,
        "commit",
        "-q",
        "-m",
        "shared scope changed",
        env={"GIT_COMMITTER_DATE": future, "GIT_AUTHOR_DATE": future},
    )

    status = _status_without_ledger(root)
    gaps = {gap["id"]: gap for gap in status["evidence_gaps"]}

    assert set(gaps) == {"req.rs", "req.rs.second"}
    assert gaps["req.rs"]["revoked_evidence_ids"] == [first_evidence]
    assert gaps["req.rs.second"]["revoked_evidence_ids"] == [second_evidence]
    assert (
        gaps["req.rs"]["code_change_boundary"]
        == gaps["req.rs.second"]["code_change_boundary"]
    )


def test_status_reports_invalid_proof_identity_revocation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _prepare_status_fixture(root)
    evidence_id = _attach_and_finish(root, "req.rs", "task.rs")
    monkeypatch.setattr(
        pact_scope_drift,
        "staleness_report",
        lambda *_args, **_kwargs: {
            "checked": True,
            "freshness_authority": "invalid_code_proof",
            "proof_identity": {
                "ok": False,
                "error": {"code": "code_proof_tuple_digest_mismatch"},
            },
            "stale": [
                {
                    "evidence_id": evidence_id,
                    "reason": "code_proof_receipt_invalid",
                }
            ],
            "fresh_count": 0,
            "revoked": True,
        },
    )

    gap = _status_without_ledger(root)["evidence_gaps"][0]

    assert gap["reason"] == "invalid_code_proof"
    assert gap["revoked_evidence_ids"] == [evidence_id]
    assert gap["code_change_boundary"] == {
        "status": "invalid_proof_identity",
        "error": {"code": "code_proof_tuple_digest_mismatch"},
    }
    assert gap["action"].startswith("Repair the code-proof receipt")


def test_status_reports_missing_git_authority_revocation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _prepare_status_fixture(root)
    evidence_id = _attach_and_finish(root, "req.rs", "task.rs")
    monkeypatch.setattr(
        pact_scope_drift,
        "staleness_report",
        lambda *_args, **_kwargs: {
            "checked": False,
            "reason": "captured Git authority has no affected-code timestamp",
            "authority_missing": True,
            "revoked": True,
            "head_sha": "a" * 40,
        },
    )

    gap = _status_without_ledger(root)["evidence_gaps"][0]

    assert gap["reason"] == "git_authority_missing"
    assert gap["revoked_evidence_ids"] == [evidence_id]
    assert gap["code_change_boundary"] == {
        "status": "unavailable",
        "reason": "captured Git authority has no affected-code timestamp",
        "head_sha": "a" * 40,
    }
    assert gap["action"].startswith("Restore Git authority")


def test_no_declared_scope_means_no_staleness_signal(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    pact_operations.attach_evidence(
        root, "rs", "req.rs",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    staleness = staleness_report(root, load_manifest(root, "rs"), "req.rs")
    assert staleness["checked"] is False


def test_bare_new_emits_fast_shape(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    # `--shape fast` keeps the v4 writer; bare `new` now writes v5.
    assert main(["pact", "--root", str(root), "new", "tiny-fix", "--shape", "fast", "--json"]) == 0
    manifest = load_manifest(root, "tiny-fix")
    assert len(manifest.requirements) == 1
    assert len(manifest.tasks) == 1
    assert len(manifest.test_plan) == 1
    # --template implies the tier-2+ full shape
    assert main([
        "pact", "--root", str(root), "new", "bigger", "--template", "feature",
        "--json",
    ]) == 0
    full = load_manifest(root, "bigger")
    assert full.change.rationale  # full template carries the scaffold prose


def test_pre_commit_warns_on_large_diff(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    big = "\n".join(f"line_{n} = {n}" for n in range(300)) + "\n"
    (root / "src").mkdir(exist_ok=True)
    (root / "src" / "big.py").write_text(big, encoding="utf-8")
    _git(root, "add", "-A")

    capsys.readouterr()
    main(["pact", "--root", str(root), "gate", "pre-commit", "rs", "--json"])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    gate = json.loads(lines[-1])
    tier = gate["tier_warning"]
    assert tier["staged_lines"] > tier["threshold"]
    assert "Pact-Id" in tier["warning"]


def test_naive_timestamps_do_not_crash_the_gate(tmp_path: Path) -> None:
    # Agents edit pact.yaml directly; PyYAML turns unquoted timestamps into
    # naive datetimes. The gate must normalize, not TypeError.
    import yaml

    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    _declare_scope(root, "rs", ["src/covered/*"])
    (root / "src" / "covered").mkdir(parents=True)
    (root / "src" / "covered" / "mod.py").write_text("x = 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "covered")
    path = root / "pact" / "changes" / "rs" / "pact.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["evidence"] = [{
        "id": "ev.executed-test.req-rs.hand",
        "kind": "executed_test",
        "status": "pass",
        "refs": ["req.rs"],
        "artifact": "tests/x.py",
        "data": {"attached_at": "2026-07-01 10:00:00"},
    }]
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    staleness = staleness_report(root, load_manifest(root, "rs"), "req.rs")
    assert staleness["checked"] is True  # normalized to UTC, no TypeError
    gate = pact_operations.task_completion_gate(root, "rs", "task.rs")
    assert isinstance(gate["ok"], bool)


def test_unresolvable_merge_base_is_loud_not_clean(tmp_path: Path) -> None:
    # Shallow CI clones have no base ref; that must never read drift-clean.
    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    report = drift_report(root, load_manifest(root, "rs"), base_branch="nonexistent")
    assert report["ok"] is False
    assert report["base_unresolved"] is True
    assert "NOT evaluated" in report["warning"]


def test_staleness_feeds_the_demotion_guard(tmp_path: Path) -> None:
    # req.rightsize.consistency: revocation feeds the awaiting_review ->
    # in_progress demotion via manifest_condition's evidence_satisfied.
    from okg.substrate.pact.review import manifest_condition

    root = _seed_root(tmp_path)
    _new_change(root, "rs")
    _declare_scope(root, "rs", ["src/covered/*"])
    (root / "src" / "covered").mkdir(parents=True)
    (root / "src" / "covered" / "mod.py").write_text("x = 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "covered v1")
    pact_operations.attach_evidence(
        root, "rs", "req.rs",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    cond = manifest_condition(load_manifest(root, "rs"), repo_root=root)
    assert cond["evidence_satisfied"] is True
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    (root / "src" / "covered" / "mod.py").write_text("x = 2\n")
    _git(root, "add", "-A")
    _git(
        root, "commit", "-q", "-m", "covered v2",
        env={"GIT_COMMITTER_DATE": future, "GIT_AUTHOR_DATE": future},
    )
    cond = manifest_condition(load_manifest(root, "rs"), repo_root=root)
    assert cond["evidence_satisfied"] is False


def test_placeholders_warn_until_replaced(tmp_path: Path, capsys) -> None:
    from okg.substrate.pact.validation import validate_manifest

    root = _seed_root(tmp_path)
    assert main(["pact", "--root", str(root), "new", "tiny", "--shape", "fast", "--json"]) == 0
    report = validate_manifest(load_manifest(root, "tiny"), repo_root=root)
    codes = {i.code for i in report.issues if i.severity == "warning"}
    assert "placeholder_content" in codes


def test_agents_md_documents_tiers_briefly() -> None:
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    start = text.index("## When to create a PACT: the tiers")
    end = text.index("##", start + 5)
    section = [
        ln for ln in text[start:end].splitlines() if ln.strip()
    ]
    assert len(section) <= 20, "tier policy must stay under 20 lines"
    assert "reasonably disagree" in text
    for tier in ("0", "1", "2", "3"):
        assert f"**{tier} —" in text
