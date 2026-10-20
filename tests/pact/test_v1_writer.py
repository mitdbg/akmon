from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _build_demo, _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_cli_admission_and_direct_edits_share_writer(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    manifest = PactManifest(change=Change(id="demo", title="Demo"))
    save_manifest(root, manifest)

    pact_operations.normalize_manifest(root, "demo")
    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())

    assert data["edits"][0]["command"] == "normalize direct-yaml"
    assert data["edits"][0]["validation"] == "accepted"


def test_edit_plan_duplicate_replay_is_idempotent(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    pact_operations.add_task(
        root, "demo", task_id="task.second", title="Second",
        verification="pytest tests/pact/test_v1_writer.py -q",
        requirements=["req.demo"],
    )

    plan = pact_operations.save_edit_plan(
        root, "demo",
        operations=[{"op": "dep_add", "from": "task.demo", "to": "task.second"}],
    )

    pact_operations.apply_edit_plan(root, plan["plan_id"])
    # Idempotent replay: re-applying the same plan is a no-op, not a
    # double-add (the edits log guards against re-application).
    replay = pact_operations.apply_edit_plan(root, plan["plan_id"])
    deps = [(d.from_task, d.to_task) for d in replay.dependencies]
    assert deps.count(("task.demo", "task.second")) == 1
