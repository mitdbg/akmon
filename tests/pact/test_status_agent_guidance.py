from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_status_explains_next_empty_and_blockers(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must explain blocked next work.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "blocked",
                        "when": ["status runs"],
                        "then": ["the blocker is explicit"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "status explains next=[]",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.publish",
                "title": "Publish",
                "verification": "OKG_DSN=$OKG_DSN okg publish",
                "requirements": ["req.demo"],
                "environment": [
                    {
                        "id": "env.OKG_DSN",
                        "kind": "env_var",
                        "name": "OKG_DSN",
                        "required": True,
                    }
                ],
            },
            {
                "id": "task.followup",
                "title": "Follow up",
                "verification": "pytest tests/pact/test_status_agent_guidance.py -q",
                "requirements": ["req.demo"],
            },
        ],
        dependencies=[
            {
                "from": "task.publish",
                "to": "task.followup",
                "kind": "blocks",
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_status_agent_guidance.py::test_status_explains_next_empty_and_blockers",
            }
        ],
    )
    save_manifest(root, manifest)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=markdown",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
        == 0
    )
    payload = _json_out(capsys)

    assert payload["next"] == []
    assert payload["agent_guidance"]["state"] == "blocked"
    assert payload["agent_guidance"]["next_action"] == "unblock"
    assert payload["agent_guidance"]["next_empty_reason"] == "blocked"
    assert "env.OKG_DSN" in payload["agent_guidance"]["why"]
    assert any(
        "OKG_DSN" in command
        for command in payload["agent_guidance"]["recommended_commands"]
    )
    assert payload["blocked_tasks"][0]["id"] == "task.publish"


def test_status_treats_explicit_blocked_task_as_blocked(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must not run manually blocked work.",
                "evidence_policy": {"required": ["pytest"]},
            }
        ],
        tasks=[
            {
                "id": "task.publish",
                "title": "Publish",
                "status": "blocked",
                "blocked_reason": "publish must run from the registered sync root",
                "verification": "okg deployment ready okg-workspace --checks dogfood --json",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_status_agent_guidance.py::test_status_treats_explicit_blocked_task_as_blocked",
            }
        ],
    )
    save_manifest(root, manifest)

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=status",
                "--json",
            ]
        )
        == 0
    )
    payload = _json_out(capsys)

    assert payload["next"] == []
    assert payload["blocked_tasks"][0]["id"] == "task.publish"
    assert payload["blocked_tasks"][0]["blocked_reason"] == (
        "publish must run from the registered sync root"
    )
    assert payload["agent_guidance"]["state"] == "blocked"
    assert payload["agent_guidance"]["next_action"] == "unblock"
    assert "registered sync root" in payload["agent_guidance"]["why"]


def test_bootstrap_treats_explicit_blocked_task_as_blocked(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo bootstrap must not present blocked work as ready.",
                "evidence_policy": {"required": ["pytest"]},
            }
        ],
        tasks=[
            {
                "id": "task.publish",
                "title": "Publish",
                "status": "blocked",
                "blocked_reason": "publish must run from the registered sync root",
                "verification": "okg deployment ready okg-workspace --checks dogfood --json",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_status_agent_guidance.py::test_bootstrap_treats_explicit_blocked_task_as_blocked",
            }
        ],
    )
    save_manifest(root, manifest)

    # The selected task is blocked, so bootstrap cannot hand the agent
    # runnable work: it refuses (exit 1) instead of reporting a ready task.
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "view",
            "demo",
            "--format=bootstrap",
            "--full",
            "--json",
        ]
    )
    payload = _json_out(capsys)
    assert rc == 1
    assert payload["verdict"] == "refused"
    assert payload["ok"] is False

    assert payload["pact"]["next_task"]["id"] == "task.publish"
    assert payload["pact"]["next_task"]["status"] == "blocked"
    assert payload["task_bundle"]["task_id"] == "task.publish"
    assert payload["task_bundle"]["blockers"] == [
        "publish must run from the registered sync root"
    ]
    assert payload["pact"]["blocked_tasks"][0]["id"] == "task.publish"


def test_status_recommends_lifecycle_advance_when_complete(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must be verified.",
                "evidence_policy": {"required": ["pytest"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "verified",
                        "when": ["status runs"],
                        "then": ["the lifecycle command is suggested"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "pytest evidence",
                        "evidence": ["pytest"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "status": "done",
                "verification": "pytest tests/pact/test_status_agent_guidance.py -q",
                "requirements": ["req.demo"],
                "evidence": ["ev.pytest.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_status_agent_guidance.py::test_status_recommends_lifecycle_advance_when_complete",
            }
        ],
        evidence=[
            Evidence(
                id="ev.pytest.demo",
                kind="pytest",
                status="pass",
                refs=["req.demo", "test.demo", "task.demo"],
                artifact="pytest.xml",
                data={
                    "junit_test_ids": [
                        "tests.pact.test_status_agent_guidance::test_status_recommends_lifecycle_advance_when_complete"
                    ]
                },
            )
        ],
    )
    save_manifest(root, manifest)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=markdown",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
        == 0
    )
    payload = _json_out(capsys)

    assert payload["next"] == []
    assert payload["agent_guidance"]["state"] == "advance_lifecycle"
    # `advance` was deleted (complete-pact-surface-cut); the lifecycle marker
    # is now set by editing pact.yaml, and the canonical action is the
    # verification gate.
    assert payload["agent_guidance"]["next_action"] == "verify"
    assert payload["agent_guidance"]["next_empty_reason"] == "lifecycle_status_lag"
    assert payload["agent_guidance"]["recommended_commands"][0] == (
        "okg pact gate pre-merge demo --json"
    )
