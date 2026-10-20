from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact import review as pact_review
from tests.pact.conftest import ingest_passing_named_plan


def test_brief_text_leads_with_decision_and_choices(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=brief"])

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("Decision needed:")
    assert "Derived state:" in text
    assert "Next action:" in text
    # `demo` is a tier-1 change with an open task, so the FSM state is
    # `in_progress`, where `approve` and `revise` are both illegal. This
    # assertion used to demand `approve` here, which pinned the very loop
    # issue #1476 reports: the brief named a verdict `decide` then refused.
    from okg.substrate.pact.fsm import legal_verdicts

    assert "`in_progress`" in text
    for verdict in ("approve", "revise"):
        assert verdict not in legal_verdicts("in_progress")
        assert verdict not in text.split("Proof state:")[0]
    assert "supersede" in text
    assert "Proof state:" in text
    # v4 brief contract: loud unavailability + recommendation-first ending.
    assert "WARNING: graph/ledger unavailable" in text
    assert "Recommendation:" in text
    assert "Against:" in text
    # budget (req.review.brief-budget)
    from okg.substrate.pact.cli import BRIEF_MAX_BYTES, BRIEF_MAX_LINES

    assert len(text.splitlines()) <= BRIEF_MAX_LINES
    assert len(text.encode()) <= BRIEF_MAX_BYTES


def test_brief_json_matches_markdown_operator_brief_values(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)

    assert (
        main(
            ["pact", "--root", str(root), "view", "demo", "--format=markdown", "--json"]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=brief", "--json"])
        == 0
    )
    payload = _json_out(capsys)
    brief = payload["operator_brief"]
    text = (root / "pact" / "changes" / "demo" / "pact.md").read_text(encoding="utf-8")

    assert f"- Decision needed: {brief['decision_needed']}." in text
    assert f"- Current state: `{brief['current_state']}`;" in text
    assert f"- Next action: {brief['next_action']}" in text
    assert f"- Proof state: {brief['proof_state']}" in text


def test_brief_json_includes_agent_guidance_and_graph_projection(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    capsys.readouterr()

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=brief", "--json"])
        == 0
    )
    payload = _json_out(capsys)

    assert payload["ok"] is True
    assert payload["change"] == "demo"
    assert payload["operator_brief"]["current_state"] == "proposal"
    assert payload["operator_brief"]["next_action"] == "`task.demo`: Do demo"
    assert payload["agent_guidance"]["state"] == "ready"
    assert payload["agent_guidance"]["next_action"] == "run_task"
    assert payload["agent_guidance"]["recommended_commands"]
    assert payload["review_view"]["status"] == "on-demand"
    assert payload["graph_projection"]["status"] == "unavailable"
    assert "graph_projection" not in payload["operator_brief"]


def test_chat_review_inbox_selected_item_is_decision_card(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=chat-review"])

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("Decision Needed")
    assert "Outcome: continue implementation" in text
    assert "Current state:" in text
    assert "Work:" in text
    assert "My recommendation:" in text
    assert "Why:" in text
    assert "What would change:" in text
    assert "The part I want you to judge:" in text
    assert "Concern:" in text
    assert "Missing proof:" in text
    assert "How we will know it worked:" in text
    assert "Reply with one:" in text
    assert "show proof" in text
    assert "show risks" in text
    assert "show details" in text
    assert "show raw YAML" in text

    assert "req." not in text
    assert "task." not in text
    assert "Derived state:" not in text
    assert "proposal" not in text
    assert "in_progress" not in text
    assert "pact.yaml" not in text

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=chat-review",
                "--json",
            ]
        )
        == 0
    )
    payload = _json_out(capsys)
    packet = payload["chat_review"]
    assert payload["format"] == "chat-review"
    assert packet["work"] == "Demo"
    assert packet["outcome"] == "continue implementation"
    assert packet["current_state"]
    assert packet["reply_options"][-1] == "show raw YAML"
    assert packet["missing_proof"] == [
        "Demo needs executed test, acceptance:acc.req-demo."
    ]
    assert "in_progress" not in json.dumps(packet)


def test_chat_review_completed_work_says_how_proof_passed(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    manifest = ingest_passing_named_plan(root, "demo")
    pact_operations.update_task_status(
        root,
        "demo",
        "task.demo",
        status="done",
        evidence_ref=manifest.evidence[-1].id,
    )
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=chat-review"])

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("Decision Needed")
    assert "My recommendation: approve completed work" in text
    assert "How we will know it worked:" in text
    assert "No unsatisfied requirement evidence gaps." in text
    assert "No next task is available." not in text


@pytest.mark.parametrize(
    ("terminal_state", "recommendation"),
    [
        ("retired", "already approved and archived"),
        ("rejected", "already rejected"),
        ("superseded", "already superseded"),
    ],
)
def test_chat_review_terminal_work_does_not_request_a_decision(
    tmp_path: Path,
    capsys,
    monkeypatch,
    terminal_state: str,
    recommendation: str,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    manifest = ingest_passing_named_plan(root, "demo")
    pact_operations.update_task_status(
        root,
        "demo",
        "task.demo",
        status="done",
        evidence_ref=manifest.evidence[-1].id,
    )
    monkeypatch.setattr(
        pact_cli,
        "_derived_state_payload",
        lambda root_arg, change_id: {
            "state": terminal_state,
            "pending_decision": None,
            "reason": f"ledger records terminal outcome {terminal_state}",
            "ledger_consulted": True,
            "decision_count": 1,
        },
    )
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=chat-review"])

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("No Decision Needed")
    assert f"My recommendation: {recommendation}" in text
    assert "No decision is needed unless you want to reopen this work." in text
    assert "continue implementation" not in text


def test_chat_review_inbox_summary_is_bucketed_and_actionable(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    broken = root / "pact" / "changes" / "broken-review"
    broken.mkdir(parents=True)
    (broken / "pact.yaml").write_text("change: [not a mapping", encoding="utf-8")
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "--format=chat-review-queue"])

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("Review Inbox")
    assert "1 urgent blocker" in text
    assert "Summary:" in text
    assert "Urgent blockers: 1" in text
    assert "Needs human judgment:" in text
    assert "Low-risk completed review:" in text
    assert "Best next move: review urgent." in text
    assert "Urgent:" in text
    assert "1. Repair first" in text
    assert "cannot be reviewed until it loads cleanly" in text
    assert "Reply with one:" in text
    assert "batch low-risk" in text
    assert "what blocks agents?" in text
    assert "Showing 1 of 1" not in text
    assert "operator_status" not in text
    assert "queue_state" not in text
    assert "raw_state" not in text
    assert len(text.splitlines()) <= 20

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--json",
            ]
        )
        == 0
    )
    payload = _json_out(capsys)
    packet = payload["chat_review_queue"]
    assert payload["format"] == "chat-review-queue"
    assert packet["heading"] == "Review Inbox"
    assert packet["mode"] == "summary"
    assert packet["shown"] == 1
    assert packet["summary_lines"][0] == "Urgent blockers: 1"
    assert packet["rows"][0]["recommendation"] == "Repair first"
    assert packet["rows"][0]["bucket"] == "urgent"
    assert packet["rows"][0]["change"] == "broken-review"
    assert packet["rows"][0]["priority_reason"]


def test_terminal_archive_stays_visible_without_becoming_a_decision(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda _root: {
            "ok": True,
            "queue": [
                {
                    "change": "terminal-active",
                    "title": "Finished but active",
                    "decision": "archive",
                    "action": "archive",
                    "flags": ["unarchived-terminal"],
                    "priority": 1,
                    "priority_reason": "Archive cleanup is required.",
                    "age_days": 1.0,
                }
            ],
            "count": 1,
            "action_count": 1,
            "decision_count": 0,
            "rework_count": 0,
            "archive_count": 1,
            "in_flight": [],
            "warnings": [],
        },
    )
    capsys.readouterr()

    assert main([
        "pact",
        "--root",
        str(root),
        "view",
        "--format=chat-review-queue",
        "--json",
    ]) == 0
    packet = _json_out(capsys)["chat_review_queue"]

    assert packet["total"] == 1
    assert packet["shown"] == 1
    assert packet["decision_total"] == 0
    assert packet["rework_total"] == 0
    assert packet["archive_total"] == 1
    assert packet["rows"][0]["decision_type"] == "archive"
    assert packet["rows"][0]["action"] == "archive"
    assert any(
        "queue actions: 0 decision, 0 rework, 1 archive" in line
        for line in packet["summary_lines"]
    )

    assert main([
        "pact",
        "--root",
        str(root),
        "view",
        "--format=chat-review-queue",
        "--query",
        "what changed",
    ]) == 0
    changed_text = capsys.readouterr().out
    assert "Current decisions: 0" in changed_text
    assert "Current decisions: 1" not in changed_text


def test_revoked_evidence_gap_chat_names_refresh_action(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    manifest = pact_operations.load_manifest(root, "demo")
    action = (
        "Rerun and attach fresh evidence after "
        "2026-08-26T12:00:00+00:00."
    )

    lines = pact_cli._proof_gap_lines(
        manifest,
        {
            "evidence_gaps": [
                {
                    "id": "req.demo",
                    "missing": ["executed_test"],
                    "gap_type": "scope_drift",
                    "action": action,
                }
            ]
        },
    )

    assert lines == [f"Demo: {action}"]
    assert "needs ." not in lines[0]


def test_chat_review_queue_uses_explicit_priority_not_alphabetical_order(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_done_change(root, "a-newer-review", title="Zulu same priority")
    _build_done_change(root, "m-older-review", title="Middle older review")
    _build_done_change(root, "z-tied-review", title="Alpha same priority")
    broken = root / "pact" / "changes" / "b-broken-review"
    broken.mkdir(parents=True)
    (broken / "pact.yaml").write_text("change: [not a mapping", encoding="utf-8")

    now = 1_000_000_000
    os.utime(broken / "pact.yaml", (now, now))
    os.utime(
        root / "pact" / "changes" / "m-older-review" / "pact.yaml",
        (now - 300, now - 300),
    )
    for change_id in ["a-newer-review", "z-tied-review"]:
        os.utime(
            root / "pact" / "changes" / change_id / "pact.yaml",
            (now - 100, now - 100),
        )
    capsys.readouterr()

    queue = pact_review.build_queue(root)["queue"]
    assert [row["change"] for row in queue] == [
        "b-broken-review",
        "m-older-review",
        "z-tied-review",
        "a-newer-review",
    ]
    assert queue[0]["priority_reason"].startswith("Needs repair")
    assert queue[1]["priority_reason"].startswith("Completed work")
    assert queue[2]["tie_breaker"] == "title then change id"

    rc = main(["pact", "--root", str(root), "view", "--format=chat-review-queue"])

    assert rc == 0
    text = capsys.readouterr().out
    assert "1. Repair first" in text
    assert "Showing 4 of 4." not in text
    assert "Rows are ordered by explicit priority" not in text
    assert "Priority: Needs repair before review" not in text

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--query",
                "why is 1 first?",
            ]
        )
        == 0
    )
    ranked_text = capsys.readouterr().out
    assert ranked_text.startswith("Ranked Review Queue")
    assert (
        "I understood your question as: explain the displayed queue order."
        in ranked_text
    )
    assert "Showing 4 of 4." in ranked_text
    assert "Rows are ordered by explicit priority" in ranked_text
    assert "1. Repair first" in ranked_text
    assert "Priority: Needs repair before review" in ranked_text
    assert ranked_text.index("Middle older review") < ranked_text.index(
        "Alpha same priority"
    )
    assert ranked_text.index("Alpha same priority") < ranked_text.index(
        "Zulu same priority"
    )
    assert "title only breaks exact ties" in ranked_text
    assert "operator_status" not in ranked_text
    assert "queue_state" not in ranked_text

    (broken / "pact.yaml").unlink()
    capsys.readouterr()
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--query",
                "why is 1 first?",
            ]
        )
        == 0
    )
    equal_priority_text = capsys.readouterr().out
    assert "Visible rows share the same priority" in equal_priority_text
    assert "numbers are handles, not recommendations" in equal_priority_text


def test_chat_review_inbox_summary_does_not_dump_low_risk_rows(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda root_arg: {
            "ok": True,
            "queue": [
                {
                    "change": f"done-{i}",
                    "title": f"Docs wording cleanup {i}",
                    "decision": "review",
                    "state": "awaiting_review",
                    "age_days": i,
                    "flags": [],
                }
                for i in range(8)
            ],
            "count": 8,
            "in_flight": [],
            "warnings": [],
        },
    )
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "--format=chat-review-queue"])

    assert rc == 0
    text = capsys.readouterr().out
    assert "Nothing urgent is blocking active work." in text
    assert "Low-risk completed review: 8" in text
    assert "Best next move: batch low-risk." in text
    assert "Docs wording cleanup 0" not in text
    assert "Docs wording cleanup 7" not in text
    assert "Showing" not in text


def test_chat_review_inbox_summary_does_not_dump_judgment_rows(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda root_arg: {
            "ok": True,
            "queue": [
                {
                    "change": f"judgment-{i}",
                    "title": f"Workflow judgment {i}",
                    "decision": "go-ahead",
                    "state": "proposed",
                    "age_days": i,
                    "flags": [],
                }
                for i in range(8)
            ],
            "count": 8,
            "in_flight": [],
            "warnings": [],
        },
    )
    capsys.readouterr()

    rc = main(["pact", "--root", str(root), "view", "--format=chat-review-queue"])

    assert rc == 0
    text = capsys.readouterr().out
    assert "Nothing urgent is blocking active work." in text
    assert "Needs human judgment: 8" in text
    assert "Best next move: review judgment." in text
    assert "Workflow judgment 0" not in text
    assert "Workflow judgment 7" not in text
    assert "Showing" not in text


def test_chat_review_inbox_batch_low_risk_previews_exact_changes(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda root_arg: {
            "ok": True,
            "queue": [
                {
                    "change": f"done-safe-{i}",
                    "title": f"Docs wording cleanup {i}",
                    "decision": "review",
                    "state": "awaiting_review",
                    "age_days": 1,
                    "flags": [],
                }
                for i in range(7)
            ]
            + [
                {
                    "change": "needs-judgment",
                    "title": "Workflow judgment",
                    "decision": "go-ahead",
                    "state": "proposed",
                    "age_days": 2,
                    "flags": [],
                },
                {
                    "change": "broken",
                    "title": "Broken manifest",
                    "decision": "repair",
                    "state": "draft",
                    "age_days": 3,
                    "flags": ["invalid"],
                },
            ],
            "count": 9,
            "in_flight": [],
            "warnings": [],
        },
    )
    capsys.readouterr()

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "view",
            "--format=chat-review-queue",
            "--query",
            "batch low-risk",
        ]
    )

    assert rc == 0
    text = capsys.readouterr().out
    assert text.startswith("Batch Low-Risk Preview")
    assert "I can prepare a batch decision for 7 changes." in text
    assert "Included:" in text
    assert "1. Review now: Docs wording cleanup 0" in text
    assert "7. Review now: Docs wording cleanup 6" in text
    assert "Excluded examples:" in text
    assert "Workflow judgment" in text
    assert "Broken manifest" in text
    assert "I will not record anything until you confirm." in text
    assert "Excluded examples are not affected by approve batch." in text
    assert "approve batch" in text


def test_chat_review_inbox_query_modes_are_answer_first(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda root_arg: {
            "ok": True,
            "queue": [
                {
                    "change": "broken",
                    "title": "CI failure",
                    "decision": "repair",
                    "state": "draft",
                    "age_days": 1,
                    "flags": ["invalid"],
                },
                {
                    "change": "product-call",
                    "title": "Review inbox UX",
                    "decision": "go-ahead",
                    "state": "proposed",
                    "age_days": 1,
                    "flags": [],
                },
                {
                    "change": "safe",
                    "title": "Wording cleanup",
                    "decision": "review",
                    "state": "awaiting_review",
                    "age_days": 1,
                    "flags": [],
                },
            ],
            "count": 3,
            "in_flight": [],
            "warnings": [],
        },
    )
    capsys.readouterr()

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--query",
                "show me only stuff where my judgment matters",
            ]
        )
        == 0
    )
    judgment = capsys.readouterr().out
    assert judgment.startswith("Human Judgment")
    assert (
        "I understood your question as: show work where your judgment matters."
        in judgment
    )
    assert "No review item needs product judgment right now." not in judgment
    assert "2. Decide whether to start: Review inbox UX" in judgment
    assert "Hidden mechanical work:" in judgment
    assert "1. Repair first: CI failure" in judgment
    assert "review 2" in judgment
    assert "review 1" not in judgment
    assert "Showing" not in judgment
    assert "operator_status" not in judgment

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--query",
                "what needs my attention before agents can continue?",
            ]
        )
        == 0
    )
    blockers = capsys.readouterr().out
    assert blockers.startswith("Agent Blockers")
    assert (
        "I understood your question as: what stops agents from continuing safely."
        in blockers
    )
    assert "1 urgent item can block agents." in blockers
    assert "Blocking work:" in blockers
    assert "1. Repair first: CI failure" in blockers
    assert "Not blockers:" in blockers
    assert "3. Review now: Wording cleanup" in blockers
    assert "fix blocker" in blockers


def test_chat_review_queue_dedupes_stale_claims_by_change(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    stale_status = {"queue_state": "stale_claim"}
    monkeypatch.setattr(
        pact_review,
        "build_queue",
        lambda root_arg: {
            "ok": True,
            "queue": [],
            "count": 0,
            "in_flight": [
                {
                    "change": "duplicate-work",
                    "divergence": "differs",
                    "operator_status": stale_status,
                },
                {
                    "change": "duplicate-work",
                    "divergence": "absent-here",
                    "operator_status": stale_status,
                },
                {
                    "change": "other-work",
                    "divergence": "absent-here",
                    "operator_status": stale_status,
                },
                {
                    "change": "third-work",
                    "divergence": "absent-here",
                    "operator_status": stale_status,
                },
                {
                    "change": "fourth-work",
                    "divergence": "absent-here",
                    "operator_status": stale_status,
                },
                {
                    "change": "current-work",
                    "operator_status": {"queue_state": "in_flight"},
                },
            ],
            "warnings": [],
        },
    )
    capsys.readouterr()

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "view",
            "--format=chat-review-queue",
            "--json",
        ]
    )

    assert rc == 0
    packet = _json_out(capsys)["chat_review_queue"]
    assert packet["stale_total"] == 4
    assert [row["change"] for row in packet["stale_rows"]] == [
        "duplicate-work",
        "other-work",
        "third-work",
    ]
    assert "fourth-work" not in json.dumps(packet["stale_rows"])

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "--format=chat-review-queue",
                "--query",
                "show stale",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert text.startswith("Stale Cleanup")
    assert "review <number>" not in text
    assert "uv run okg pact decide --all-stale --verdict defer --json" in text
    assert "defer all stale" not in text
    assert "inspect stale work" in text


def _seed_root(root: Path) -> Path:
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
        "pact_schema_version: 0\n",
        encoding="utf-8",
    )
    return root


def _build_demo(root: Path) -> None:
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new-change",
                "demo",
                "--template",
                "protocol",
                "--json",
            ]
        )
        == 0
    )
    manifest = pact_operations.load_manifest(root, "demo")
    manifest.change.tier = 1
    pact_operations.save_manifest(root, manifest)
    pact_operations.add_requirement(
        root,
        "demo",
        req_id="req.demo",
        title="Demo",
        statement="Demo must validate.",
        evidence=["planned_test"],
    )
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.demo",
        title="Do demo",
        verification="pytest tests/pact/test_operator_brief_cli.py -q",
        requirements=["req.demo"],
    )
    pact_operations.require_test(
        root,
        "demo",
        "req.demo",
        "tests/pact/test_operator_brief_cli.py::test_brief_text_leads_with_decision_and_choices",
    )


def _build_done_change(root: Path, change_id: str, *, title: str) -> None:
    pact_operations.create_change(root, change_id, template="feature", title=title)
    compact_id = change_id.replace("-", "")
    req_id = f"req.{compact_id}"
    task_id = f"task.{compact_id}"
    pact_operations.add_requirement(
        root,
        change_id,
        req_id=req_id,
        title=title,
        statement=f"{title} must validate.",
        evidence=["planned_test"],
    )
    pact_operations.add_task(
        root,
        change_id,
        task_id=task_id,
        title=title,
        verification="pytest tests/pact/test_operator_brief_cli.py -q",
        requirements=[req_id],
    )
    test_ref = (
        "tests/pact/test_operator_brief_cli.py::"
        "test_chat_review_queue_uses_explicit_priority_not_alphabetical_order"
    )
    pact_operations.require_test(root, change_id, req_id, test_ref)
    manifest = ingest_passing_named_plan(root, change_id)
    pact_operations.update_task_status(
        root,
        change_id,
        task_id,
        status="done",
        evidence_ref=manifest.evidence[-1].id,
    )
    manifest = pact_operations.load_manifest(root, change_id)
    manifest.change.tier = 1
    pact_operations.save_manifest(root, manifest)


def _json_out(capsys) -> dict:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])
