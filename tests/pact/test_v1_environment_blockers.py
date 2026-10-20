from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _manifest() -> PactManifest:
    # The task carries a linked requirement and its proof: a task linked to
    # nothing closes against nothing, which `task_requirements_link` refuses
    # (pact-vacuous-gate-close). Environment blocking — what these tests
    # measure — is unaffected by the link.
    return PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.publish",
                "statement": "The graph publishes with a configured DSN.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        evidence=[
            {
                "id": "ev.executed-test.req-publish",
                "kind": "executed_test",
                "status": "pass",
                "refs": ["req.publish"],
                "artifact": "tests/pact/test_v1_environment_blockers.py",
            }
        ],
        tasks=[
            {
                "id": "task.publish",
                "title": "Publish graph",
                "verification": "okg generation describe <generation-id>",
                "requirements": ["req.publish"],
                "environment": [
                    {
                        "id": "env.OKG_DSN",
                        "kind": "env_var",
                        "env": "OKG_DSN",
                        "description": "okg-workspace publish requires a DSN",
                    }
                ],
            }
        ],
    )


def test_missing_okg_dsn_blocks_publish_task(monkeypatch) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)

    report = validate_manifest(_manifest())
    task_state = report.alignment["tasks"]["task.publish"]

    assert report.ok
    assert task_state["status"] == "blocked"
    assert task_state["blockers"] == ["env.OKG_DSN"]
    assert task_state["environment_blockers"][0]["name"] == "OKG_DSN"
    assert report.alignment["lifecycle"]["verification"] is False

    gated = validate_manifest(_manifest(), lifecycle_gate="verification")
    assert not gated.ok
    assert any(
        issue.code == "verification_gate_incomplete_task"
        for issue in gated.issues
    )


def test_missing_okg_dsn_does_not_reopen_completed_task(monkeypatch) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    manifest = _manifest()
    manifest.tasks[0].status = "done"

    report = validate_manifest(manifest)
    task_state = report.alignment["tasks"]["task.publish"]

    assert report.ok
    assert task_state["status"] == "complete"
    assert task_state["blockers"] == []
    assert task_state["environment_blockers"] == []


def test_status_reports_environment_blockers_before_next_action(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    save_manifest(root, _manifest())

    rc = main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
    payload = _json_out(capsys)

    assert rc == 0
    assert payload["next"] == []
    assert payload["environment_blockers"][0]["task"] == "task.publish"
    assert payload["blocked_tasks"][0]["blockers"] == ["env.OKG_DSN"]
