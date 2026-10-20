from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_reviewer_packet_prioritizes_risks_diffs_and_checklist(
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
    assert ai["decision_checklist"]
    assert ai["semantic_diff"]["command"]
    assert ai["contradictions"][0]["kind"] in {
        "graph_projection_not_fresh",
        "proof_gaps",
    }
    assert "acceptance" in ai
    assert "hierarchy" in ai

    assert main([
        "pact", "--root", str(root), "view", "demo", "--format=packet",
        "--reviewer-kind", "human", "--json",
    ]) == 0
    human = _json_out(capsys)
    assert "decision_checklist" in human
    assert "semantic_diff" in human
    assert "requirements" not in human
