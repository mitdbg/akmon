from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_ai_and_human_review_surfaces_are_distinct(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    root = _seed_root(tmp_path)
    _build_demo(root)
    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)

    assert main([
        "pact", "--root", str(root), "view", "demo", "--format=packet",
        "--reviewer-kind", "ai", "--json",
    ]) == 0
    ai = _json_out(capsys)

    assert main([
        "pact", "--root", str(root), "view", "demo", "--format=packet",
        "--reviewer-kind", "human", "--json",
    ]) == 0
    human = _json_out(capsys)

    assert ai["reviewer_kind"] == "ai"
    assert ai["structured"] is True
    assert "requirements" in ai
    assert "tasks" in ai
    assert "proof_gaps" in ai
    assert "graph_projection" in ai
    assert ai["graph_projection"]["status"] == "unavailable"

    assert human["reviewer_kind"] == "human"
    assert "operator_brief" in human
    assert "brief_text" in human
    assert "requirements" not in human
    assert "tasks" not in human
    assert "Decision needed:" in human["brief_text"]
    assert ai != human
