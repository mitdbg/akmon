from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.markdown_view import check_markdown_view
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_root


REPO_ROOT = Path(__file__).resolve().parents[2]


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_coordination_runbook_keeps_safety_boundaries_concise() -> None:
    text = (REPO_ROOT / "docs/runbooks/agents/coordination.md").read_text()
    prose = " ".join(text.split())

    assert "Wait 30–60 seconds" in prose
    assert "After two unchanged waits" in prose
    assert "deployments/<name>/" in text
    assert "src/okg/substrate/" in text
    assert "published graph version" in text
    assert "before-and-after row or metric counts" in text
    assert "failure-queue state" in text
    assert "keep the change\nunready" in text


def test_agent_can_improve_pact_without_chat_history(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)

    assert main([
        "pact", "--root", str(root), "new-change", "demo", "--shape", "fast", "--json",
    ]) == 0
    _json_out(capsys)
    # The fast-path scaffold is already a complete, runnable agent task. The
    # workflow should improve that authored PACT instead of deleting and
    # rebuilding its requirement/task/test-plan skeleton.
    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)

    assert main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"]) == 0
    status = _json_out(capsys)
    assert status["agent_guidance"]["state"] == "ready"
    assert status["agent_guidance"]["next_action"] == "run_task"
    assert status["next"][0]["id"] == "task.demo"
    assert status["review_view"]["status"] == "on-demand"

    report = validate_manifest(load_manifest(root, "demo"), completion_gate=True)
    assert not report.ok
    error_codes = {issue.code for issue in report.issues}
    assert {
        "completion_gate_unsatisfied",
        "verification_gate_incomplete_task",
    } & error_codes

    pact_operations.add_requirement(
        root, "demo", req_id="req.followup", title="Followup",
        statement="A later trace should be observed.", evidence=["planned_test"],
    )
    stale_view = check_markdown_view(root, "demo")
    assert stale_view["status"] == "stale"

    # Per `simplify-pact-surface` task 3, the `loop record` CLI verb
    # was deleted; the "PACT self-improvement loop" is now the general
    # "write a PACT, attach evidence, commit" flow documented in
    # pact/AGENTS.md. To recover a fresh review-view after the
    # normative edit above, an agent now runs `gate post-edit` (which
    # subsumes render + check-view).
    # The manifest validates, but this tmp root has no decision-ledger
    # deployment, so intent registration is unavailable: the gate defers
    # (exit 3) rather than claiming a durable registration it never made.
    rc = main([
        "pact", "--root", str(root), "gate", "post-edit", "demo", "--full",
        "--json",
    ])
    gated = _json_out(capsys)
    assert rc == 3
    assert gated["verdict"] == "deferred"
    assert gated["ok"] is False
    assert gated["intent_registration"]["status"] == "unavailable"
    assert gated["validation"]["errors"] == []
    assert gated["review_view"]["status"] == "on-demand"
