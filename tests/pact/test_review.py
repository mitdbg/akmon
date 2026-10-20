"""PACT v4 review loop: decide verb, ledger, archive path, review queue.

DB-backed tests use an explicit ``OKG_TEST_DSN`` or the workflow's
``OKG_TEST_DSN_TEMPLATE`` expanded for the current xdist worker. They fall
back to the project test Postgres and skip cleanly when it is unreachable.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import decisions as ledger
from okg.substrate.pact.fsm import DecisionEvent
from okg.substrate.pact.home_repository import home_repository
from okg.substrate.pact.operations import ingest_pytest_report, task_completion_gate
from okg.substrate.pact.review import build_queue, decide_change

# These tests run cluster-global DDL (CREATE ROLE okg_mcp / GRANT). The module
# resolves a per-worker database in xdist, but the role operations themselves
# remain cluster-global. Mark it serial_db so the database coverage lane keeps
# the module in its serial step; the partitioned fast lane keeps the whole file
# on one worker with ``--dist=loadfile``.
pytestmark = pytest.mark.serial_db

# `postgres` FIRST, so the no-database fast lane DESELECTS these classes
# instead of collecting and skipping them. That skip was silent green: the
# affected-test gate names these nodes as required, `lane:fast/pytest` was
# their only receipted per-PR owner, and every one of them reported
# "test Postgres unreachable" in that lane's JUnit. The skipif is retained
# only for a developer checkout with no database; when a lane DECLARES a DSN,
# `conftest._resolve_postgres_availability` raises instead of skipping.
# Postgres-backed: the no-database fast lane DESELECTS these, and
# `conftest.pytest_runtest_setup` decides at setup whether an unusable database
# is a developer-checkout skip or a lane fault worth failing. That skip used to
# be unconditional and silent -- all 35 of these reported "test Postgres
# unreachable" in `lane:fast/pytest`, the only lane the affected-test gate
# believed was running them.
pg = pytest.mark.postgres


def _seed_change(
    root: Path,
    change_id: str,
    *,
    task_status: str = "done",
    tier_label: str | None = None,
) -> Path:
    d = root / "pact" / "changes" / change_id
    d.mkdir(parents=True)
    manifest = {
        "pact_version": 0,
        "change": {
            "id": change_id,
            "title": f"Test change {change_id}",
            "status": "proposal",
            "rationale": "test",
            "root_cause": "test",
            **({"labels": [tier_label]} if tier_label else {}),
        },
        "requirements": [
            {
                "id": f"req.{change_id}.r1",
                "title": "r1",
                "statement": "MUST test.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        "tasks": [
            {
                "id": f"task.{change_id}.t1",
                "title": "t1",
                "status": task_status,
                "verification": "pytest",
                "requirements": [f"req.{change_id}.r1"],
                "evidence": [f"ev.{change_id}.t1"],
            }
        ],
        "evidence": [
            {
                "id": f"ev.{change_id}.t1",
                "kind": "executed_test",
                "status": "pass",
                "refs": [f"req.{change_id}.r1"],
                "summary": "fixture evidence",
            }
        ],
    }
    (d / "pact.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
    return d


class _QueueConnection:
    def close(self) -> None:
        return None


def _stub_queue_ledger(
    monkeypatch: pytest.MonkeyPatch,
    decisions: dict[str, list[DecisionEvent]],
) -> None:
    monkeypatch.setattr(
        ledger,
        "require_ledger_connection",
        lambda **_kwargs: _QueueConnection(),
    )
    monkeypatch.setattr(
        ledger,
        "load_all_decisions",
        lambda _conn: decisions,
    )
    monkeypatch.setattr(ledger, "load_intents", lambda _conn: [])


def _approval(change_id: str) -> DecisionEvent:
    return DecisionEvent(
        change_id=change_id,
        verdict="approve",
        principal="riley",
        from_state="awaiting_review",
        to_state="retired",
    )


def test_terminal_only_queue_reports_zero_decisions(tmp_path, monkeypatch) -> None:
    change_id = "rvw-terminal-only"
    _seed_change(tmp_path, change_id)
    _stub_queue_ledger(monkeypatch, {change_id: [_approval(change_id)]})

    queue = build_queue(tmp_path)

    assert queue["text"].startswith("review queue: 0 change(s) awaiting a decision")
    assert queue["count"] == 1
    assert queue["action_count"] == 1
    assert queue["decision_count"] == 0
    assert queue["rework_count"] == 0
    assert queue["archive_count"] == 1
    assert queue["queue"][0]["decision"] == "archive"
    assert queue["queue"][0]["action"] == "archive"


def test_mixed_queue_separates_decisions_from_archive(
    tmp_path,
    monkeypatch,
) -> None:
    review_id = "rvw-mixed-review"
    terminal_id = "rvw-mixed-terminal"
    _seed_change(tmp_path, review_id)
    _seed_change(tmp_path, terminal_id)
    broken = tmp_path / "pact" / "changes" / "rvw-mixed-broken"
    broken.mkdir(parents=True)
    (broken / "pact.yaml").write_text(
        "change: [not a mapping",
        encoding="utf-8",
    )
    _stub_queue_ledger(monkeypatch, {terminal_id: [_approval(terminal_id)]})

    queue = build_queue(tmp_path)
    rows = {row["change"]: row for row in queue["queue"]}

    assert queue["count"] == 3
    assert queue["action_count"] == 3
    assert queue["action_counts"] == {
        "decision": 1,
        "rework": 1,
        "archive": 1,
    }
    assert queue["decision_count"] == 1
    assert queue["rework_count"] == 1
    assert queue["archive_count"] == 1
    assert rows[review_id]["action"] == "decision"
    assert rows[review_id]["decision"] == "review"
    assert rows[terminal_id]["action"] == "archive"
    assert rows[terminal_id]["decision"] == "archive"
    assert rows["rvw-mixed-broken"]["action"] == "rework"
    assert rows["rvw-mixed-broken"]["decision"] == "repair"


def test_worktree_local_env_dsn(tmp_path, monkeypatch):
    """Finding #2 fix: the main checkout's .okg/local.env is resolved from
    a linked worktree via git rev-parse --git-common-dir."""
    main_repo = tmp_path / "mainrepo"
    (main_repo / ".okg").mkdir(parents=True)
    (main_repo / ".okg" / "local.env").write_text(
        "# deployment env\nOKG_DSN='postgres://u:p@localhost/db'\n",
        encoding="utf-8",
    )

    class FakeProc:
        stdout = str(main_repo / ".git") + "\n"

    monkeypatch.setattr(
        "subprocess.run", lambda *a, **kw: FakeProc(),
    )
    assert ledger.worktree_local_env_dsn() == "postgres://u:p@localhost/db"


def test_decide_refuses_without_ledger(tmp_path, monkeypatch):
    _seed_change(tmp_path, "rvw-noledger")
    monkeypatch.setattr(
        ledger, "resolve_ledger_dsn",
        lambda **kw: {"dsn": None, "reason": "unit test"},
    )
    with pytest.raises(SubstrateError) as exc:
        decide_change(tmp_path, "rvw-noledger", verdict="approve")
    assert "cannot DECIDE in degraded mode" in exc.value.message


def test_escalate_requires_recipient(tmp_path):
    _seed_change(tmp_path, "rvw-esc")
    with pytest.raises(SubstrateError) as exc:
        decide_change(tmp_path, "rvw-esc", verdict="escalate")
    assert exc.value.data["violated_guard"] == "escalation_target"


@pg
class TestDecideLive:
    def test_approve_at_review_retires_and_archives(self, tmp_path, clean_ledger):
        _seed_change(tmp_path, "rvw-approve")
        out = decide_change(
            tmp_path, "rvw-approve", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        rec = out["recorded"]
        assert out["coordination_event"]["writer_role"] == "okg_mcp"
        assert (rec["from_state"], rec["to_state"]) == ("awaiting_review", "retired")
        assert out["archived"] is True
        assert out["echo"].startswith(
            "recorded: rvw-approve approve (awaiting_review -> retired) by riley"
        )
        # req.deploy.single-authority: the echo names WHICH database
        # recorded the verdict - the 2026-06-10 incident guard.
        database = out["ledger_target"]["database"]
        assert database.startswith("okg_test")
        assert f"[db={database}@" in out["echo"]
        assert rec["intent_digest"].startswith("sha256:")
        assert not (tmp_path / "pact" / "changes" / "rvw-approve").exists()
        archived = tmp_path / "pact" / "archive" / "rvw-approve" / "pact.yaml"
        assert archived.is_file()
        status = yaml.safe_load(archived.read_text())["change"]["status"]
        assert status == "verification"
        import psycopg

        with psycopg.connect(clean_ledger) as conn:
            events = ledger.load_coordination_events(conn, "rvw-approve")
        assert [event["event_type"] for event in events] == ["verdict"]
        assert events[0]["payload"]["verdict"] == "approve"
        assert events[0]["payload"]["decision_id"] == rec["decision_id"]
        assert events[0]["principal"] == "riley"

    def test_v4_terminal_verdict_binds_state_intent_and_code_receipts(
        self, tmp_path, clean_ledger
    ):
        from okg.substrate.pact.proof_identity import canonical_digest
        from okg.substrate.pact.store import (
            intent_digest,
            load_manifest_file,
            state_digest,
        )

        change_id = "rvw-final-receipt"
        _seed_change(tmp_path, change_id, tier_label="tier-2")
        active = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
        data = yaml.safe_load(active.read_text(encoding="utf-8"))
        data["pact_version"] = 4
        data["target_pact_version"] = 4
        data["change"]["tier"] = 2
        active.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )

        manifest = load_manifest_file(active)
        receipt = {
            "schema": "okg.pact_code_proof_receipt/v1",
            "repository": home_repository(),
            "base_sha": "a" * 40,
            "base_tree": "b" * 40,
            "proven_code_sha": "c" * 40,
            "proven_code_tree": "d" * 40,
            "pact_intent_digest": intent_digest(manifest),
            "closure_allowlist": [
                f"pact/archive/{change_id}/pact.yaml",
                f"pact/changes/{change_id}/pact.yaml",
            ],
            "code_tuples": [],
            "code_digest": canonical_digest([]),
        }
        receipt["receipt_digest"] = canonical_digest(receipt)
        data = yaml.safe_load(active.read_text(encoding="utf-8"))
        data["proof_receipts"] = {"code": [receipt]}
        active.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

        out = decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )

        archived = tmp_path / "pact" / "archive" / change_id / "pact.yaml"
        terminal_manifest = load_manifest_file(archived)
        import psycopg

        with psycopg.connect(clean_ledger) as conn:
            events = ledger.load_coordination_events(conn, change_id)
        final = events[-1]
        assert out["coordination_event"]["event_id"] == final["event_id"]
        assert set(final["payload"]) == {
            "command",
            "verdict",
            "from_state",
            "to_state",
            "decision_id",
            "note",
            "target",
            "state_digest",
            "intent_digest",
            "code_proof_receipt_digests",
        }
        assert final["payload"]["target"] == change_id
        assert final["payload"]["state_digest"] == state_digest(terminal_manifest)
        assert final["payload"]["intent_digest"] == intent_digest(terminal_manifest)
        assert final["payload"]["code_proof_receipt_digests"] == [
            receipt["receipt_digest"]
        ]

        # Re-signing a mutated tracked receipt does not preserve the external
        # verdict binding: state and receipt digests both change.
        from okg.substrate.pact.cli import _final_verdict_event_guard

        archived_data = yaml.safe_load(archived.read_text(encoding="utf-8"))
        mutated = archived_data["proof_receipts"]["code"][0]
        mutated["closure_allowlist"].append(
            f"pact/archive/{change_id}/reports/later.json"
        )
        mutated["closure_allowlist"] = sorted(mutated["closure_allowlist"])
        mutated_without_digest = dict(mutated)
        mutated_without_digest.pop("receipt_digest")
        mutated["receipt_digest"] = canonical_digest(mutated_without_digest)
        archived.write_text(
            yaml.safe_dump(archived_data, sort_keys=False),
            encoding="utf-8",
        )
        tampered_manifest = load_manifest_file(archived)
        with psycopg.connect(clean_ledger) as conn:
            decision_row = [
                row
                for row in ledger.load_decision_rows(conn)
                if row["decision_id"] == out["recorded"]["decision_id"]
            ][0]
        guard = _final_verdict_event_guard(
            tampered_manifest,
            decision_row,
            events,
        )
        assert guard["ok"] is False
        assert "payload.state_digest" in guard["mismatches"]
        assert "payload.code_proof_receipt_digests" in guard["mismatches"]

    def test_approve_in_progress_refused_with_alternatives(
        self, tmp_path, clean_ledger
    ):
        _seed_change(tmp_path, "rvw-early", task_status="todo")
        with pytest.raises(SubstrateError) as exc:
            decide_change(
                tmp_path, "rvw-early", verdict="approve",
                explicit_dsn=clean_ledger,
            )
        assert exc.value.data["violated_guard"] == "legal_transition"
        assert "supersede" in exc.value.data["legal_verdicts_here"]

    def test_revise_returns_to_in_progress(self, tmp_path, clean_ledger):
        _seed_change(tmp_path, "rvw-revise")
        out = decide_change(
            tmp_path, "rvw-revise", verdict="revise",
            note="tighten the verification", explicit_dsn=clean_ledger,
        )
        assert out["recorded"]["to_state"] == "in_progress"
        assert out["archived"] is False
        assert (tmp_path / "pact" / "changes" / "rvw-revise").exists()

    def test_reopen_restores_from_archive(self, tmp_path, clean_ledger):
        _seed_change(tmp_path, "rvw-reopen")
        decide_change(
            tmp_path, "rvw-reopen", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        assert not (tmp_path / "pact" / "changes" / "rvw-reopen").exists()
        out = decide_change(
            tmp_path, "rvw-reopen", verdict="reopen",
            principal="riley", explicit_dsn=clean_ledger,
        )
        assert out["restored_from_archive"] is True
        assert out["recorded"]["to_state"] == "awaiting_review"
        assert (tmp_path / "pact" / "changes" / "rvw-reopen" / "pact.yaml").is_file()

    def test_supersede_requires_target(self, tmp_path, clean_ledger):
        _seed_change(tmp_path, "rvw-sup", task_status="todo")
        with pytest.raises(SubstrateError) as exc:
            decide_change(
                tmp_path, "rvw-sup", verdict="supersede",
                explicit_dsn=clean_ledger,
            )
        assert exc.value.data["violated_guard"] == "target_exists"
        out = decide_change(
            tmp_path, "rvw-sup", verdict="supersede", target="rvw-other",
            explicit_dsn=clean_ledger,
        )
        assert out["recorded"]["to_state"] == "superseded"
        assert out["archived"] is True

    def test_defer_records_without_transition(self, tmp_path, clean_ledger):
        _seed_change(tmp_path, "rvw-defer")
        out = decide_change(
            tmp_path, "rvw-defer", verdict="defer", explicit_dsn=clean_ledger,
        )
        assert out["recorded"]["to_state"] is None
        assert "recorded only" in out["echo"]

    def test_decision_history_drives_derived_state(self, tmp_path, clean_ledger):
        """A retry repairs an active-file projection without a second decision."""
        _seed_change(tmp_path, "rvw-twice")
        decide_change(
            tmp_path, "rvw-twice", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        # restore the dir to simulate a stale worktree retrying the verdict
        (tmp_path / "pact" / "archive" / "rvw-twice").rename(
            tmp_path / "pact" / "changes" / "rvw-twice"
        )
        out = decide_change(
            tmp_path, "rvw-twice", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        assert out["recovered_projection"] is True
        assert not (tmp_path / "pact" / "changes" / "rvw-twice").exists()
        assert (tmp_path / "pact" / "archive" / "rvw-twice" / "pact.yaml").is_file()
        import psycopg

        with psycopg.connect(clean_ledger) as conn:
            assert len(ledger.load_decisions(conn, "rvw-twice")) == 1

    def test_approve_archived_changed_digest_records_fresh_approval(
        self, tmp_path, clean_ledger
    ):
        _seed_change(tmp_path, "rvw-refresh")
        decide_change(
            tmp_path, "rvw-refresh", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / "rvw-refresh" / "pact.yaml"
        data = yaml.safe_load(archived.read_text(encoding="utf-8"))
        data["requirements"][0]["statement"] = "MUST test the refreshed digest."
        archived.write_text(yaml.safe_dump(data), encoding="utf-8")

        out = decide_change(
            tmp_path, "rvw-refresh", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )

        assert out["archived"] is False
        assert out["restored_from_archive"] is False
        assert out["recorded"]["from_state"] == "retired"
        assert out["recorded"]["to_state"] is None
        assert "recorded only" in out["echo"]
        assert archived.is_file()
        assert not (tmp_path / "pact" / "changes" / "rvw-refresh").exists()

    def test_v4_archived_intent_change_reopens_proof_and_proposal(
        self, tmp_path, clean_ledger
    ):
        change_id = "rvw-v4-refresh"
        _seed_change(tmp_path, change_id, tier_label="tier-2")
        active = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
        data = yaml.safe_load(active.read_text(encoding="utf-8"))
        data["pact_version"] = 4
        data["target_pact_version"] = 4
        data["change"]["tier"] = 2
        active.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
        decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / change_id / "pact.yaml"
        data = yaml.safe_load(archived.read_text(encoding="utf-8"))
        data["requirements"][0]["statement"] = "MUST prove the changed intent again."
        archived.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

        out = decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )

        assert out["restored_changed_intent"] is True
        assert out["recorded"]["from_state"] == "proposed"
        assert out["recorded"]["to_state"] == "in_progress"
        assert out["state"] == "in_progress"
        assert not archived.exists()
        restored = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
        restored_data = yaml.safe_load(restored.read_text(encoding="utf-8"))
        assert restored_data["tasks"][0]["status"] == "todo"
        assert restored_data["tasks"][0]["evidence"] == []

    def test_archive_rewrites_repo_relative_formal_artifact_path(
        self, tmp_path, clean_ledger
    ):
        change_id = "rvw-formal-path"
        _seed_change(tmp_path, change_id)
        active = tmp_path / "pact" / "changes" / change_id / "pact.yaml"
        data = yaml.safe_load(active.read_text(encoding="utf-8"))
        data["evidence"][0]["data"] = {
            "artifact_path": (
                f"pact/changes/{change_id}/reports/formal/safety/tlc.stdout.log"
            )
        }
        intent_path = f"pact/changes/{change_id}/designs/design.md"
        data["requirements"][0]["statement"] = f"MUST preserve {intent_path}."
        active.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

        out = decide_change(
            tmp_path,
            change_id,
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )

        assert out["archived"] is True
        archived = tmp_path / "pact" / "archive" / change_id / "pact.yaml"
        archived_data = yaml.safe_load(archived.read_text(encoding="utf-8"))
        assert archived_data["evidence"][0]["data"]["artifact_path"] == (
            f"pact/archive/{change_id}/reports/formal/safety/tlc.stdout.log"
        )
        assert archived_data["requirements"][0]["statement"] == (
            f"MUST preserve {intent_path}."
        )


@pg
class TestPreMergeDecisionGate:
    """The merge boundary: v4 changes need a ledger approval for their
    CURRENT intent digest (design.md section 5)."""

    def _gate(self, root: Path) -> dict:
        from okg.substrate.pact.cli import _ledger_decision_gate

        return _ledger_decision_gate(root, "rvw-merge")

    def _fresh_graph_projection(self, monkeypatch):
        from okg.substrate.pact import cli as pact_cli
        from okg.substrate.pact.store import state_digest

        def fresh_graph(manifest, *_args, **_kwargs):
            return {
                "ok": True,
                "status": "fresh",
                "stale": False,
                "projected_state_digest": state_digest(manifest),
                "recommended_commands": [],
            }

        monkeypatch.setattr(pact_cli, "_graph_projection_status", fresh_graph)

    def _seed_v4(self, root: Path) -> None:
        d = _seed_change(root, "rvw-merge")
        data = yaml.safe_load((d / "pact.yaml").read_text(encoding="utf-8"))
        data["target_pact_version"] = 4
        data["change"]["tier"] = 1
        (d / "pact.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")

    def test_refuses_without_any_decision(self, tmp_path, clean_ledger, monkeypatch):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._seed_v4(tmp_path)
        gate = self._gate(tmp_path)
        assert gate["ok"] is False
        assert gate["status"] == "no_approval_recorded"
        assert gate["workflow_authority"]["accepted"] is False
        # The ledger ANSWERED; what is missing is a decision. Reporting
        # `ledger_not_fresh` here named a broken database and routed the
        # operator to a remedy with no command (issue #1443).
        blockers = gate["workflow_authority"]["blockers"]
        assert "no_ledger_approval" in blockers
        assert "ledger_not_fresh" not in blockers
        assert "ledger_dsn_not_configured" not in blockers
        assert "pact_coordination_disagreement" not in blockers

    def test_passes_with_matching_approval(self, tmp_path, clean_ledger, monkeypatch):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._fresh_graph_projection(monkeypatch)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        gate = self._gate(tmp_path)
        assert gate["ok"] is True
        assert gate["status"] == "approved"
        assert gate["principal"] == "riley"
        assert gate["workflow_authority"]["accepted"] is True
        assert gate["workflow_authority"]["blockers"] == []
        assert gate["workflow_authority"]["graph_projection"]["projected_generation_id"] is None

    def test_refuses_matching_approval_when_graph_projection_is_stale(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        from okg.substrate.pact import cli as pact_cli

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": True,
                "status": "stale",
                "stale": True,
                "recommended_commands": ["uv run okg pact gate graph-freshness rvw-merge --refresh"],
            },
        )
        monkeypatch.setattr(ledger, "current_worktree_identity", lambda: None)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        assert gate["status"] == "approved"
        assert "graph_not_fresh" in gate["workflow_authority"]["blockers"]

    def test_pre_merge_accepts_fresh_event_plane_intent_when_projection_missing(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        import psycopg

        from okg.substrate.pact import cli as pact_cli
        from okg.substrate.pact.store import intent_digest, load_manifest_file
        from okg.substrate.pact.workflow_authority import GitEvidence

        worktree = {
            "path": str(tmp_path),
            "suffix": "w" * 10,
            "branch": "codex/worktree-pact",
            "head_sha": "a" * 40,
            "session_id": "session-1",
        }
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(ledger, "current_worktree_identity", lambda: worktree)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": False,
                "status": "missing",
                "stale": True,
                "reason": "unit test projection missing",
                "recommended_commands": [
                    "uv run okg pact gate graph-freshness rvw-merge --refresh"
                ],
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda _root: {
                "mode": "foreign_checkout",
                "event_plane_authority_allowed": True,
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "git_evidence_for_manifest",
            lambda _root, _manifest: GitEvidence(live_checked=True),
        )
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        digest = intent_digest(load_manifest_file(archived))
        with psycopg.connect(clean_ledger) as conn:
            ledger.register_intent(
                conn,
                change_id="rvw-merge",
                intent_digest=digest,
                derived_state="retired",
                worktree=worktree,
            )

        gate = self._gate(tmp_path)

        assert gate["ok"] is True
        authority = gate["workflow_authority"]
        assert authority["accepted"] is True
        assert authority["graph_projection"]["authority_scope"] == (
            "event_plane_intent"
        )
        assert authority["graph_projection"]["file_projection"]["status"] == "missing"
        assert authority["graph_projection"]["event_plane_intent"]["ok"] is True

    def test_pre_merge_refuses_sync_root_event_plane_bypass_when_projection_missing(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        import psycopg

        from okg.substrate.pact import cli as pact_cli
        from okg.substrate.pact.store import intent_digest, load_manifest_file

        worktree = {
            "path": str(tmp_path),
            "suffix": "s" * 10,
            "branch": "main",
            "head_sha": "a" * 40,
            "session_id": "session-1",
        }
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(ledger, "current_worktree_identity", lambda: worktree)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": False,
                "status": "missing",
                "stale": True,
                "reason": "unit test projection missing",
                "recommended_commands": [],
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda _root: {
                "mode": "sync_root",
                "event_plane_authority_allowed": False,
            },
        )
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        digest = intent_digest(load_manifest_file(archived))
        with psycopg.connect(clean_ledger) as conn:
            ledger.register_intent(
                conn,
                change_id="rvw-merge",
                intent_digest=digest,
                derived_state="retired",
                worktree=worktree,
            )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        authority = gate["workflow_authority"]
        assert "event_plane_not_allowed" in authority["blockers"]
        assert authority["graph_projection"]["authority_scope"] == (
            "event_plane_intent"
        )
        assert authority["graph_projection"]["event_plane_authority"]["mode"] == (
            "sync_root"
        )

    def test_pre_merge_refuses_event_plane_when_git_cleanliness_not_live_checked(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        import psycopg

        from okg.substrate.pact import cli as pact_cli
        from okg.substrate.pact.store import intent_digest, load_manifest_file
        from okg.substrate.pact.workflow_authority import GitEvidence

        worktree = {
            "path": str(tmp_path),
            "suffix": "g" * 10,
            "branch": "codex/worktree-pact",
            "head_sha": "a" * 40,
            "session_id": "session-1",
        }
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(ledger, "current_worktree_identity", lambda: worktree)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": False,
                "status": "missing",
                "stale": True,
                "reason": "unit test projection missing",
                "recommended_commands": [],
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda _root: {
                "mode": "foreign_checkout",
                "event_plane_authority_allowed": True,
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "git_evidence_for_manifest",
            lambda _root, _manifest: GitEvidence(live_checked=False),
        )
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        digest = intent_digest(load_manifest_file(archived))
        with psycopg.connect(clean_ledger) as conn:
            ledger.register_intent(
                conn,
                change_id="rvw-merge",
                intent_digest=digest,
                derived_state="retired",
                worktree=worktree,
            )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        authority = gate["workflow_authority"]
        assert "worktree_not_clean" in authority["blockers"]
        assert authority["graph_projection"]["authority_scope"] == (
            "event_plane_intent"
        )

    def test_pre_merge_reports_stale_event_plane_intent_digest(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        from okg.substrate.pact import cli as pact_cli

        worktree = {
            "path": str(tmp_path),
            "suffix": "d" * 10,
            "branch": "codex/worktree-pact",
            "head_sha": "a" * 40,
            "session_id": "session-1",
        }
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(ledger, "current_worktree_identity", lambda: worktree)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": False,
                "status": "missing",
                "stale": True,
                "reason": "unit test projection missing",
                "recommended_commands": [],
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda _root: {
                "mode": "foreign_checkout",
                "event_plane_authority_allowed": True,
            },
        )
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        archived = tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        data = yaml.safe_load(archived.read_text(encoding="utf-8"))
        data["change"]["rationale"] = "changed after intent registration"
        archived.write_text(yaml.safe_dump(data), encoding="utf-8")

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        assert gate["status"] == "approval_stale_intent_changed"
        authority = gate["workflow_authority"]
        assert "intent_digest_mismatch" in authority["blockers"]
        assert authority["graph_projection"]["authority_scope"] == (
            "event_plane_intent"
        )
        assert authority["graph_projection"]["event_plane_intent"]["status"] == "stale"

    def test_pre_merge_refuses_stale_event_plane_intent_head(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        from okg.substrate.pact import cli as pact_cli

        initial_worktree = {
            "path": str(tmp_path),
            "suffix": "h" * 10,
            "branch": "codex/worktree-pact",
            "head_sha": "a" * 40,
            "session_id": "session-1",
        }
        current_worktree = {
            **initial_worktree,
            "head_sha": "b" * 40,
        }
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setattr(
            pact_cli,
            "_graph_projection_status",
            lambda _manifest, *_args, **_kwargs: {
                "ok": False,
                "status": "missing",
                "stale": True,
                "reason": "unit test projection missing",
                "recommended_commands": [],
            },
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda _root: {
                "mode": "foreign_checkout",
                "event_plane_authority_allowed": True,
            },
        )
        monkeypatch.setattr(
            ledger,
            "current_worktree_identity",
            lambda: initial_worktree,
        )
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        monkeypatch.setattr(
            ledger,
            "current_worktree_identity",
            lambda: current_worktree,
        )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        assert gate["status"] == "approved"
        authority = gate["workflow_authority"]
        assert "worktree_head_mismatch" in authority["blockers"]
        assert authority["graph_projection"]["authority_scope"] == (
            "event_plane_intent"
        )

    def test_pre_merge_gate_reads_archived_approved_manifest(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        from okg.substrate.cli.main import main
        from okg.substrate.pact.store import intent_digest, load_manifest

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._fresh_graph_projection(monkeypatch)
        self._seed_v4(tmp_path)
        path = tmp_path / "pact" / "changes" / "rvw-merge" / "pact.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        data["test_plan"] = [{
            "id": "test.rvw-merge",
            "requirement": "req.rvw-merge.r1",
            "test_id": "tests/pact/test_review.py",
            "command": "pytest tests/pact/test_review.py",
            "evidence": ["executed_test"],
        }]
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        manifest = load_manifest(tmp_path, "rvw-merge")
        data.setdefault("evidence", []).insert(0, {
            "id": "ev.rvw-merge.intent-review",
            "kind": "intent_review",
            "status": "pass",
            "refs": ["rvw-merge"],
            "summary": "operator approved intent",
            "data": {
                "decision": "approve",
                "reviewer": "riley",
                "reviewed_digest": intent_digest(manifest),
            },
        })
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )

        assert not path.exists()
        assert not (
            tmp_path / "pact" / "archive" / "rvw-merge" / "pact.md"
        ).exists()
        assert main([
            "pact", "--root", str(tmp_path), "gate", "pre-merge",
            "rvw-merge", "--json",
        ]) == 0

    def test_pre_merge_gate_accepts_legacy_archived_approval_without_v4_lifecycle_fields(
        self, tmp_path, clean_ledger, monkeypatch, capsys
    ):
        from okg.substrate.cli.main import main

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._fresh_graph_projection(monkeypatch)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        import psycopg

        with psycopg.connect(clean_ledger) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {ledger.LEDGER_TABLE}
                    SET from_state = NULL, to_state = NULL
                    WHERE change_id = %s AND verdict = 'approve'
                    """,
                    ("rvw-merge",),
                )

        assert not (tmp_path / "pact" / "changes" / "rvw-merge").exists()
        assert (
            tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        ).is_file()
        rc = main([
            "pact", "--root", str(tmp_path), "gate", "pre-merge",
            "rvw-merge", "--json",
        ])
        payload = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert rc == 0
        assert payload["ok"] is True
        assert payload["decision_gate"]["status"] == "approved"
        assert payload["review_view"]["status"] == "on-demand"

    def test_refuses_when_intent_changed_after_approval(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        # Drift: edit a requirement statement after the approval. The
        # archive move happened on approve, so edit in place at the
        # archived location.
        path = tmp_path / "pact" / "archive" / "rvw-merge" / "pact.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        data["requirements"][0]["statement"] = "MUST test something ELSE."
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        (tmp_path / "pact" / "archive" / "rvw-merge").rename(
            tmp_path / "pact" / "changes" / "rvw-merge"
        )
        gate = self._gate(tmp_path)
        assert gate["ok"] is False
        assert gate["status"] == "approval_stale_intent_changed"
        blockers = gate["workflow_authority"]["blockers"]
        assert "no_ledger_approval" in blockers
        assert "ledger_not_fresh" not in blockers
        assert "pact_coordination_disagreement" not in blockers

    def test_refuses_matching_approval_after_reopen(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._fresh_graph_projection(monkeypatch)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        decide_change(
            tmp_path, "rvw-merge", verdict="reopen",
            principal="riley", explicit_dsn=clean_ledger,
        )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        assert gate["status"] == "approval_superseded_by_later_decision"
        assert gate["latest_decision"] == "reopen"
        blockers = gate["workflow_authority"]["blockers"]
        assert "no_ledger_approval" in blockers
        assert "ledger_not_fresh" not in blockers
        assert "pact_coordination_disagreement" not in blockers

    def test_refuses_matching_approval_when_manifest_is_active(
        self, tmp_path, clean_ledger, monkeypatch
    ):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._fresh_graph_projection(monkeypatch)
        self._seed_v4(tmp_path)
        decide_change(
            tmp_path, "rvw-merge", verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        (tmp_path / "pact" / "archive" / "rvw-merge").rename(
            tmp_path / "pact" / "changes" / "rvw-merge"
        )

        gate = self._gate(tmp_path)

        assert gate["ok"] is False
        assert gate["status"] == "approval_not_terminal_archive"
        authority = gate["workflow_authority"]
        blockers = authority["blockers"]
        # An approval DOES bind this digest, so `no_ledger_approval` would be
        # false and the decide remedy inert (the FSM-legal verdict here is
        # `defer`). The repair is the archive step, and the check says so.
        assert "approval_not_archived" in blockers
        assert "no_ledger_approval" not in blockers
        assert "ledger_not_fresh" not in blockers
        assert "pact_coordination_disagreement" not in blockers
        assert "archive the approved change" in authority["required_live_checks"]
        assert (
            "record an operator decision with okg pact decide"
            not in authority["required_live_checks"]
        )

    def test_refuses_when_ledger_unreachable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("OKG_DSN", raising=False)
        monkeypatch.setattr(
            ledger, "resolve_ledger_dsn",
            lambda **kw: {"dsn": None, "reason": "unit test"},
        )
        self._seed_v4(tmp_path)
        gate = self._gate(tmp_path)
        assert gate["ok"] is False
        assert gate["status"] == "ledger_unreachable"
        # The control for issue #1443: a ledger that could not be READ keeps
        # the ledger blocker, and never claims to know that no approval exists.
        authority = gate["workflow_authority"]
        assert "ledger_not_fresh" in authority["blockers"]
        assert "no_ledger_approval" not in authority["blockers"]
        assert "refresh decision ledger" in authority["required_live_checks"]


@pg
class TestUnapprovedChangeAcrossSurfaces:
    """Issue #1443's acceptance: for a v4 change whose work is finished but
    which nobody has decided, the three operator surfaces must agree that it is
    waiting for a decision — and none of them may blame the ledger."""

    CHANGE = "rvw-unapproved"

    def _seed(self, root: Path) -> None:
        d = _seed_change(root, self.CHANGE)
        path = d / "pact.yaml"
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        data["target_pact_version"] = 4
        data["change"]["tier"] = 1
        path.write_text(yaml.safe_dump(data), encoding="utf-8")

    def _premerge(self, root: Path, capsys) -> dict:
        from okg.substrate.cli.main import main

        rc = main([
            "pact", "--root", str(root), "gate", "pre-merge", self.CHANGE, "--json",
        ])
        payload = json.loads(capsys.readouterr().out.splitlines()[-1])
        assert rc != 0, "an undecided change must not pass the merge boundary"
        return payload

    def test_three_surfaces_agree_without_a_decision(
        self, tmp_path, clean_ledger, monkeypatch, capsys
    ):
        from okg.substrate.pact.cli import _status

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._seed(tmp_path)

        # (i) the local operator surface: every task is terminal and the
        # evidence is satisfied, yet the change is NOT retired.
        status = _status(tmp_path, self.CHANGE, include_graph=False)
        assert status["retired"] is False
        assert status["state"] == "awaiting_review"
        assert status["derived_state"]["state"] == "awaiting_review"

        # (ii) the repo-wide review queue.
        row = {r["change"]: r for r in build_queue(tmp_path)["queue"]}[self.CHANGE]
        assert row["operator_status"]["raw_state"] == "awaiting_review"
        assert row["decision"] == "review"

        # (iii) the merge boundary refuses, and names the MISSING DECISION
        # rather than a ledger it read perfectly well.
        payload = self._premerge(tmp_path, capsys)
        assert payload["ok"] is False
        assert payload["decision_gate"]["status"] == "no_approval_recorded"
        causes = payload["blocked_by"]
        assert "no_ledger_approval" in causes
        assert "ledger_not_fresh" not in causes
        assert "ledger_dsn_not_configured" not in causes
        assert "pact_coordination_disagreement" not in causes
        # The remedy is a command, not an instruction to repair a database.
        assert any(
            "pact decide" in command for command in payload["next_commands"]
        ), payload["next_commands"]

    def test_unarchived_approval_refuses_without_an_inert_decide_command(
        self, tmp_path, clean_ledger, monkeypatch, capsys
    ):
        """The other side of the routing: an approval binds this digest, so the
        refusal must NOT publish a decide command. The FSM-legal verdict in
        this state is `defer`, which records a defer and leaves the gate
        refusing identically — the inert-command loop."""
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        self._seed(tmp_path)
        decide_change(
            tmp_path, self.CHANGE, verdict="approve",
            principal="riley", explicit_dsn=clean_ledger,
        )
        # Approve archives the change; move it back so the approval is current
        # but the manifest is still active.
        (tmp_path / "pact" / "archive" / self.CHANGE).rename(
            tmp_path / "pact" / "changes" / self.CHANGE
        )

        payload = self._premerge(tmp_path, capsys)
        assert payload["decision_gate"]["status"] == "approval_not_terminal_archive"
        causes = payload["blocked_by"]
        assert "approval_not_archived" in causes
        assert "no_ledger_approval" not in causes
        assert "ledger_not_fresh" not in causes
        # The decide plan is still REPORTED for the reader...
        assert payload["decide_plan"]["state"] is not None
        # ...but never RECOMMENDED, because no verdict clears this refusal.
        for command in payload.get("next_commands") or []:
            assert "pact decide" not in command, command

    def test_unreachable_ledger_still_reports_a_ledger_fault(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.delenv("OKG_DSN", raising=False)
        monkeypatch.setattr(
            ledger, "resolve_ledger_dsn",
            lambda **kw: {"dsn": None, "reason": "unit test"},
        )
        self._seed(tmp_path)

        payload = self._premerge(tmp_path, capsys)
        assert payload["decision_gate"]["status"] == "ledger_unreachable"
        causes = payload["blocked_by"]
        assert "ledger_not_fresh" in causes
        assert "no_ledger_approval" not in causes


@pg
class TestQueueLive:
    def test_queue_rows_and_repair(self, tmp_path, clean_ledger, monkeypatch):
        monkeypatch.setenv("OKG_DSN", clean_ledger)
        _seed_change(tmp_path, "rvw-q-review")              # awaiting_review
        _seed_change(tmp_path, "rvw-q-go", tier_label="tier-2")
        _seed_change(tmp_path, "rvw-q-wip", task_status="todo")  # in_progress
        _seed_change(tmp_path, "rvw-q-archive")
        decide_change(
            tmp_path,
            "rvw-q-archive",
            verdict="approve",
            principal="riley",
            explicit_dsn=clean_ledger,
        )
        (tmp_path / "pact" / "archive" / "rvw-q-archive").rename(
            tmp_path / "pact" / "changes" / "rvw-q-archive"
        )
        broken = tmp_path / "pact" / "changes" / "rvw-q-broken"
        broken.mkdir(parents=True)
        (broken / "pact.yaml").write_text("change: [not a mapping", encoding="utf-8")

        q = build_queue(tmp_path)
        rows = {r["change"]: r for r in q["queue"]}
        assert q["ok"] is True
        assert rows["rvw-q-review"]["decision"] == "review"
        assert rows["rvw-q-review"]["operator_status"] == {
            "surface": "review_queue",
            "authoritative": "decision_queue",
            "queue_state": "needs_review",
            "decision": "review",
            "raw_state": "awaiting_review",
            "task_state": "awaiting_review",
            "flags": [],
        }
        assert rows["rvw-q-broken"]["decision"] == "repair"
        assert rows["rvw-q-broken"]["operator_status"]["queue_state"] == "needs_repair"
        assert rows["rvw-q-broken"]["operator_status"]["authoritative"] == "decision_queue"
        assert rows["rvw-q-go"]["decision"] == "go-ahead"
        assert rows["rvw-q-go"]["operator_status"]["queue_state"] == "needs_go_ahead"
        assert rows["rvw-q-go"]["operator_status"]["raw_state"] == "proposed"
        assert rows["rvw-q-archive"]["decision"] == "archive"
        assert rows["rvw-q-archive"]["flags"] == ["unarchived-terminal"]
        assert rows["rvw-q-archive"]["operator_status"]["queue_state"] == "needs_archive"
        assert rows["rvw-q-archive"]["operator_status"]["raw_state"] == "retired"
        assert "rvw-q-wip" not in rows  # no pending decision -> not in queue
        # repair rows rank before review rows
        assert q["queue"][0]["decision"] == "repair"

    def test_queue_warns_without_ledger(self, tmp_path, monkeypatch):
        monkeypatch.delenv("OKG_DSN", raising=False)
        monkeypatch.setattr(
            ledger, "resolve_ledger_dsn",
            lambda **kw: {"dsn": None, "reason": "unit test"},
        )
        _seed_change(tmp_path, "rvw-q-warn")
        q = build_queue(tmp_path)
        assert q["ok"] is True
        assert any("ledger unavailable" in w for w in q["warnings"])


@pg
class TestIntentPlane:
    """Tier E: in-flight intent is an event on the shared plane
    (worktree-access-tiers req.tiers.event-plane)."""

    def test_register_upsert_and_cross_worktree_visibility(
        self, tmp_path, clean_ledger, monkeypatch
    ) -> None:
        import psycopg

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        conn = psycopg.connect(clean_ledger)
        with conn.cursor() as cur:
            cur.execute(ledger._DDL)
            cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS okg.source_watermarks (
                  source TEXT PRIMARY KEY,
                  cutover_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
                  last_applied_rev JSONB NOT NULL DEFAULT '{}'::jsonb,
                  last_published_rev JSONB NOT NULL DEFAULT '{}'::jsonb,
                  projection_cursor BIGINT NOT NULL DEFAULT 0,
                  last_reconcile_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                    (source, last_reconcile_at)
                VALUES
                    ('okg-workspace.agent-sessions', now() - interval '15 seconds'),
                    ('okg-workspace.codex-sessions', now() - interval '30 seconds')
                ON CONFLICT (source) DO UPDATE
                  SET last_reconcile_at = EXCLUDED.last_reconcile_at
                """
            )
        conn.commit()
        other = {
            "path": "/elsewhere/wt-b",
            "suffix": "b" * 10,
            "branch": "claude/agent-b",
            "head_sha": "a" * 40,
            "session_id": "session-b",
        }
        first_event = ledger.register_intent(
            conn, change_id="rvw-intent", intent_digest="sha256:aaa",
            derived_state="draft", worktree=other,
        )
        # Upsert: same (change, worktree) advances state + digest.
        second_event = ledger.register_intent(
            conn, change_id="rvw-intent", intent_digest="sha256:bbb",
            derived_state="in_progress", worktree=other,
        )
        rows = ledger.load_intents(conn)
        events = ledger.load_coordination_events(conn, "rvw-intent")
        conn.close()
        assert len(rows) == 1
        assert rows[0]["intent_digest"] == "sha256:bbb"
        assert rows[0]["derived_state"] == "in_progress"
        assert rows[0]["worktree_suffix"] == "b" * 10
        assert rows[0]["head_sha"] == "a" * 40
        assert rows[0]["session_id"] == "session-b"
        assert rows[0]["claim_freshness"] == "fresh"
        assert rows[0]["transcript_watermark_status"] == "available"
        assert rows[0]["transcript_watermark_age_seconds"] is not None
        assert rows[0]["transcript_watermark_age_seconds"] < 120
        assert rows[0]["transcript_watermark_last_reconcile_at"] is not None
        assert {
            row["source"] for row in rows[0]["transcript_watermarks"]
        } >= {
            "okg-workspace.agent-sessions",
            "okg-workspace.codex-sessions",
        }
        assert first_event["writer_role"] == "okg_mcp"
        assert second_event["writer_role"] == "okg_mcp"
        assert [event["event_type"] for event in events] == ["intent", "intent"]
        assert events[-1]["payload"]["derived_state"] == "in_progress"
        assert events[-1]["head_sha"] == "a" * 40
        assert events[-1]["session_id"] == "session-b"

        # The queue in THIS checkout surfaces the other worktree's
        # intent as in-flight, marked absent-here (no local manifest).
        _seed_change(tmp_path, "rvw-local")
        q = build_queue(tmp_path)
        flight = {f["change"]: f for f in q["in_flight"]}
        assert "rvw-intent" in flight
        assert flight["rvw-intent"]["divergence"] == "absent-here"
        assert flight["rvw-intent"]["branch"] == "claude/agent-b"
        assert flight["rvw-intent"]["head_sha"] == "a" * 40
        assert flight["rvw-intent"]["session_id"] == "session-b"
        assert flight["rvw-intent"]["claim_freshness"] == "fresh"
        assert flight["rvw-intent"]["operator_status"] == {
            "surface": "review_queue",
            "authoritative": "intent_claim",
            "queue_state": "in_flight",
            "raw_state": "in_progress",
            "task_state": "in_progress",
            "claim_freshness": "fresh",
            "claim_expired": False,
            "divergence": "absent-here",
        }
        assert flight["rvw-intent"]["transcript_watermark_status"] == "available"
        assert (
            flight["rvw-intent"]["transcript_watermark_age_seconds"] is not None
        )

    def test_own_worktree_and_same_digest_intents_filtered(
        self, tmp_path, clean_ledger, monkeypatch
    ) -> None:
        import psycopg

        from okg.substrate.pact.store import intent_digest as digest_of
        from okg.substrate.pact.store import load_manifest

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        conn = psycopg.connect(clean_ledger)
        with conn.cursor() as cur:
            cur.execute(ledger._DDL)
        conn.commit()
        _seed_change(tmp_path, "rvw-same")
        same_digest = digest_of(load_manifest(tmp_path, "rvw-same"))
        ledger.register_intent(
            conn, change_id="rvw-same", intent_digest=same_digest,
            derived_state="in_progress",
            worktree={"path": "/elsewhere", "suffix": "c" * 10,
                      "branch": "x"},
        )
        here = ledger.current_worktree_identity()
        ledger.register_intent(
            conn, change_id="rvw-own", intent_digest="sha256:ddd",
            derived_state="draft", worktree=here,
        )
        conn.close()
        q = build_queue(tmp_path)
        flights = {f["change"] for f in q["in_flight"]}
        assert "rvw-same" not in flights  # merged reality adds nothing
        assert "rvw-own" not in flights  # own worktree filtered

    def test_claim_expires_from_last_event_age(
        self, tmp_path, clean_ledger, monkeypatch
    ) -> None:
        import psycopg

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setenv("OKG_PACT_CLAIM_EXPIRY_SECONDS", "60")
        conn = psycopg.connect(clean_ledger)
        with conn.cursor() as cur:
            cur.execute(ledger._DDL)
        conn.commit()
        ledger.register_intent(
            conn,
            change_id="rvw-expired",
            intent_digest="sha256:expired",
            derived_state="in_progress",
            worktree={
                "path": "/elsewhere/expired",
                "suffix": "e" * 10,
                "branch": "stale",
                "head_sha": "e" * 40,
                "session_id": "dead-session",
            },
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.pact_intents
                   SET last_event_at = now() - interval '2 minutes'
                 WHERE change_id = 'rvw-expired'
                """
            )
        conn.commit()
        rows = ledger.load_intents(conn)
        conn.close()
        assert rows[0]["claim_freshness"] == "expired"
        assert rows[0]["claim_expired"] is True

        q = build_queue(tmp_path)
        flight = {f["change"]: f for f in q["in_flight"]}
        assert flight["rvw-expired"]["claim_freshness"] == "expired"
        assert flight["rvw-expired"]["claim_expired"] is True
        assert flight["rvw-expired"]["claim_expiry_seconds"] == 60
        assert flight["rvw-expired"]["operator_status"]["queue_state"] == "stale_claim"
        assert (
            flight["rvw-expired"]["operator_status"]["authoritative"]
            == "intent_claim"
        )
        assert flight["rvw-expired"]["operator_status"]["claim_freshness"] == "expired"

    def test_decide_all_stale_defer_removes_only_expired_intent_claims(
        self, tmp_path, clean_ledger, monkeypatch, capsys
    ) -> None:
        import psycopg

        from okg.substrate.cli.main import main

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setenv("OKG_PACT_CLAIM_EXPIRY_SECONDS", "60")
        conn = psycopg.connect(clean_ledger)
        with conn.cursor() as cur:
            cur.execute(ledger._DDL)
        conn.commit()
        _seed_change(tmp_path, "rvw-local")
        ledger.register_intent(
            conn,
            change_id="rvw-stale",
            intent_digest="sha256:stale",
            derived_state="in_progress",
            worktree={
                "path": "/elsewhere/stale",
                "suffix": "s" * 10,
                "branch": "codex/stale",
                "head_sha": "s" * 40,
                "session_id": "stale-session",
            },
        )
        ledger.register_intent(
            conn,
            change_id="rvw-fresh",
            intent_digest="sha256:fresh",
            derived_state="in_progress",
            worktree={
                "path": "/elsewhere/fresh",
                "suffix": "f" * 10,
                "branch": "codex/fresh",
                "head_sha": "f" * 40,
                "session_id": "fresh-session",
            },
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.pact_intents
                   SET last_event_at = now() - interval '2 minutes'
                 WHERE change_id = 'rvw-stale'
                """
            )
        conn.commit()
        conn.close()

        rc = main([
            "pact", "--root", str(tmp_path), "decide",
            "--all-stale", "--verdict", "defer", "--json",
        ])
        payload = json.loads(capsys.readouterr().out.splitlines()[-1])

        assert rc == 0
        assert payload["removed"] == 1
        assert payload["eligible"] == 1
        assert payload["skipped"] == 1
        assert payload["events_recorded"] == 1
        assert payload["claims"][0]["change"] == "rvw-stale"

        with psycopg.connect(clean_ledger) as conn:
            intents = ledger.load_intents(conn)
            stale_events = ledger.load_coordination_events(conn, "rvw-stale")
            fresh_events = ledger.load_coordination_events(conn, "rvw-fresh")

        assert [row["change_id"] for row in intents] == ["rvw-fresh"]
        assert [event["event_type"] for event in stale_events] == [
            "intent",
            "stale_cleanup",
        ]
        cleanup = stale_events[-1]
        assert cleanup["worktree_suffix"] == "s" * 10
        assert cleanup["payload"]["command"] == "decide --all-stale"
        assert cleanup["payload"]["verdict"] == "defer"
        assert [event["event_type"] for event in fresh_events] == ["intent"]


@pg
class TestVerbCoordinationEventsLive:
    def test_evidence_ingest_and_task_gate_emit_events(
        self, tmp_path, clean_ledger, monkeypatch
    ) -> None:
        import psycopg

        monkeypatch.setenv("OKG_DSN", clean_ledger)
        monkeypatch.setenv("OKG_AGENT_SESSION_ID", "session-proof")
        change_dir = _seed_change(tmp_path, "rvw-events", task_status="todo")
        manifest_path = change_dir / "pact.yaml"
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        data["test_plan"] = [
            {
                "id": "test.rvw-events.pytest",
                "requirement": "req.rvw-events.r1",
                "test_id": "tests/test_events.py::test_emits",
                "command": "pytest tests/test_events.py::test_emits",
            }
        ]
        manifest_path.write_text(yaml.safe_dump(data), encoding="utf-8")
        junit = tmp_path / "pytest.xml"
        junit.write_text(
            """
            <testsuite tests="1">
              <testcase classname="tests/test_events.py" name="test_emits" time="0.1"/>
            </testsuite>
            """,
            encoding="utf-8",
        )

        ingest_pytest_report(tmp_path, "rvw-events", junit)
        gate = task_completion_gate(tmp_path, "rvw-events", "task.rvw-events.t1")
        assert gate["coordination_event"]["status"] == "recorded"

        with psycopg.connect(clean_ledger) as conn:
            events = ledger.load_coordination_events(conn, "rvw-events")
        event_types = [event["event_type"] for event in events]
        assert event_types == ["proof", "progress"]
        proof, progress = events
        assert proof["payload"]["command"] == "evidence ingest pytest"
        assert proof["payload"]["evidence_count"] == 1
        assert proof["session_id"] == "session-proof"
        assert proof["head_sha"]
        assert progress["task_id"] == "task.rvw-events.t1"
        assert progress["payload"]["command"] == "gate task-done"
        assert progress["payload"]["ok"] is True
        assert progress["payload"]["missing_proof"] == []
