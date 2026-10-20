from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.test_cli import _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_fast_path_pact_validates_and_graduates(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    test_id = (
        "tests/pact/test_v3_fast_path.py::"
        "test_fast_path_pact_validates_and_graduates"
    )

    assert main([
        "pact", "--root", str(root), "new", "demo", "--shape=fast",
        "--title", "Demo",
        "--intent", "Demo must use the PACT fast path.",
        "--acceptance", "The fast-path PACT validates and can graduate.",
        "--task-title", "Implement demo",
        "--test-id", test_id,
        "--test-command", f"pytest -q {test_id}",
        "--json",
    ]) == 0
    created = _json_out(capsys)
    assert created["pact_shape"] == "fast_path"
    assert created["requirements"] == 1
    assert created["tasks"] == 1

    assert validate_manifest(load_manifest(root, "demo")).ok

    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)
    view = (root / "pact" / "changes" / "demo" / "pact.md").read_text(
        encoding="utf-8"
    )
    assert "PACT shape: `fast_path`" in view

    before = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert before["target_pact_version"] == 3
    assert before["prose"]["pact_shape"] == "fast_path"
    assert before["requirements"][0]["id"] == "req.demo"
    assert before["tasks"][0]["id"] == "task.demo"

    upgraded = pact_operations.upgrade_fast_path_change(root, "demo")
    assert upgraded.prose.get("pact_shape") == "full"
    assert len(upgraded.requirements) == 1
    assert len(upgraded.tasks) == 1
    assert len(upgraded.test_plan) == 1

    after = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert after["prose"]["pact_shape"] == "full"
    assert after["requirements"] == before["requirements"]
    assert after["tasks"] == before["tasks"]
    assert after["test_plan"] == before["test_plan"]
