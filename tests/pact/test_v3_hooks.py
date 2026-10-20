from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from tests.pact.conftest import ingest_passing_named_plan
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_post_edit_check_normalizes_validates_and_reports_semantic_diff(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    manifest_path = root / "pact" / "changes" / "demo" / "pact.yaml"
    data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    data["change"]["affected_areas"] = ["pact"]
    manifest_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "gate",
                "post-edit",
                "demo",
                "--full",
                "--json",
            ]
        )
        == 0
    )
    payload = _json_out(capsys)

    assert payload["ok"] is True
    assert payload["normalized"] is True
    assert payload["validation"]["ok"] is True
    assert payload["review_view"]["status"] == "fresh"
    assert payload["semantic_diff"]["changed"] is True
    assert payload["semantic_diff"]["command"] == "normalize direct-yaml"


def test_task_completion_gate_requires_evidence_or_waiver(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "migrate-version",
                "demo",
                "--target",
                "3",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "gate",
                "task-done",
                "demo",
                "task.demo",
                "--json",
            ]
        )
        == 1
    )
    blocked = _json_out(capsys)
    assert blocked["ok"] is False
    assert "req.demo:executed_test" in blocked["missing_proof"]
    assert blocked["recommended_commands"]

    ingest_passing_named_plan(root, "demo")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "gate",
                "task-done",
                "demo",
                "task.demo",
                "--json",
            ]
        )
        == 0
    )
    allowed = _json_out(capsys)
    assert allowed["ok"] is True
    assert allowed["missing_proof"] == []
