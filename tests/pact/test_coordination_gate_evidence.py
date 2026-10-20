from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.models import Change, PactManifest, Task
from okg.substrate.pact.operations import task_completion_gate
from okg.substrate.pact.store import save_manifest


def _seed_manifest(
    root: Path,
    *,
    generation_id: int | None = None,
    target_pact_version: int = 2,
) -> None:
    (root / "pact").mkdir(parents=True, exist_ok=True)
    save_manifest(
        root,
        PactManifest(
            pact_version=0,
            target_pact_version=target_pact_version,
            change=Change(
                id="demo",
                title="Demo",
                tier=1 if target_pact_version >= 4 else None,
            ),
            # The task's own proof obligation is linked AND met, so the
            # coordination gates are the only variable these tests move. An
            # unlinked task now refuses outright (`task_requirements_link` /
            # `task_proof_obligation_vacuous`, pact-vacuous-gate-close), which
            # would mask the coordination signal.
            requirements=[
                {
                    "id": "req.demo",
                    "statement": "Coordinated work resolves its gates.",
                    "evidence_policy": {"required": ["executed_test"]},
                }
            ],
            evidence=[
                {
                    "id": "ev.executed-test.req-demo",
                    "kind": "executed_test",
                    "status": "pass",
                    "refs": ["req.demo"],
                    "artifact": "tests/pact/test_coordination_gate_evidence.py",
                }
            ],
            tasks=[
                Task(
                    id="task.demo",
                    title="Coordinated task",
                    verification="coordination gates resolve",
                    requirements=["req.demo"],
                    coordination_gates=[
                        "claim.active",
                        "review.approved",
                        "subagents.rolled_up",
                    ],
                    coordination_generation_id=generation_id,
                )
            ],
        ),
    )


def _json_out(capsys) -> dict:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _write_gate_state(root: Path, payload: dict) -> None:
    state_dir = root / ".pact" / "coordination-gates"
    state_dir.mkdir(parents=True)
    (state_dir / "demo.json").write_text(json.dumps(payload), encoding="utf-8")


def test_task_done_explains_missing_and_satisfied_coordination_gates(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_manifest(tmp_path)

    missing = task_completion_gate(tmp_path, "demo", "task.demo")
    assert missing["ok"] is False
    assert missing["missing_proof"] == [
        "coordination_gate:claim.active",
        "coordination_gate:review.approved",
        "coordination_gate:subagents.rolled_up",
    ]
    assert [gate["status"] for gate in missing["coordination_gates"]["details"]] == [
        "missing",
        "missing",
        "missing",
    ]

    _write_gate_state(
        tmp_path,
        {
            "generation_id": 41,
            "tasks": {
                "task.demo": {
                    "gates": {
                        "claim.active": True,
                        "review.approved": {
                            "status": "pass",
                            "evidence_ref": "coordination:review:approved",
                        },
                        "subagents.rolled_up": "satisfied",
                    }
                }
            },
        },
    )
    assert main([
        "pact",
        "--root",
        str(tmp_path),
        "gate",
        "task-done",
        "demo",
        "task.demo",
        "--json",
    ]) == 0
    satisfied = _json_out(capsys)
    assert satisfied["ok"] is True
    assert satisfied["marked"] is True
    assert satisfied["workflow_authority"]["accepted"] is True
    assert satisfied["workflow_authority"]["blockers"] == []
    assert satisfied["coordination_gates"]["missing"] == []
    assert [gate["status"] for gate in satisfied["coordination_gates"]["details"]] == [
        "satisfied",
        "satisfied",
        "satisfied",
    ]


def test_task_done_rejects_stale_coordination_gate_generation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_manifest(tmp_path, generation_id=42, target_pact_version=4)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_ledger_authority",
        lambda _repo_root: {"fresh": True, "status": "available"},
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda _root, _manifest: {
            "ok": True,
            "status": "fresh",
            "stale": False,
            "projected_generation_id": 42,
        },
    )
    _write_gate_state(
        tmp_path,
        {
            "generation_id": 41,
            "tasks": {
                "task.demo": {
                    "gates": {
                        "claim.active": True,
                        "review.approved": True,
                        "subagents.rolled_up": True,
                    }
                }
            },
        },
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert result["missing_proof"] == [
        "coordination_gate:claim.active",
        "coordination_gate:review.approved",
        "coordination_gate:subagents.rolled_up",
    ]
    assert result["workflow_authority"]["accepted"] is False
    assert "generation_pin_mismatch" in result["workflow_authority"]["blockers"]
    assert {gate["status"] for gate in result["coordination_gates"]["details"]} == {
        "stale_generation",
    }


def test_v4_task_done_refuses_stale_graph_authority(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_manifest(tmp_path, generation_id=41, target_pact_version=4)
    _write_gate_state(
        tmp_path,
        {
            "generation_id": 41,
            "tasks": {
                "task.demo": {
                    "gates": {
                        "claim.active": True,
                        "review.approved": True,
                        "subagents.rolled_up": True,
                    }
                }
            },
        },
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_ledger_authority",
        lambda _repo_root: {"fresh": True, "status": "available"},
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda _root, _manifest: {
            "ok": True,
            "status": "stale",
            "stale": True,
            "recommended_commands": ["uv run okg pact gate graph-freshness demo --refresh"],
        },
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert result["workflow_authority"]["accepted"] is False
    assert "graph_not_fresh" in result["workflow_authority"]["blockers"]


def test_v4_task_done_accepts_checked_authority_surfaces(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_manifest(tmp_path, generation_id=41, target_pact_version=4)
    _write_gate_state(
        tmp_path,
        {
            "generation_id": 41,
            "tasks": {
                "task.demo": {
                    "gates": {
                        "claim.active": True,
                        "review.approved": True,
                        "subagents.rolled_up": True,
                    }
                }
            },
        },
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_ledger_authority",
        lambda _repo_root: {"fresh": True, "status": "available"},
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda _root, _manifest: {
            "ok": True,
            "status": "fresh",
            "stale": False,
            "recommended_commands": [],
        },
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is True
    assert result["workflow_authority"]["accepted"] is True
    assert result["workflow_authority"]["blockers"] == []
    assert result["workflow_authority"]["graph_projection"]["projected_generation_id"] is None
