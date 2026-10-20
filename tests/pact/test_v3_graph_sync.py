from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_pact_watch_syncs_changed_manifest_to_workspace_projection(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    monkeypatch.setenv("OKG_WORKSPACE_SYNC_ROOT", str(root))
    _build_demo(root)

    assert main([
        "pact", "--root", str(root), "gate", "graph-freshness", "demo",
        "--refresh", "--deployment", "okg-workspace", "--dry-run", "--json",
    ]) == 0
    payload = _json_out(capsys)
    assert payload["source"] == "pact"
    assert payload["watch_roots"] == ["pact"]
    assert "pact/changes/demo/pact.yaml" in payload["sync_scope_keys"]
    assert any("--include pact" in command for command in payload["commands"])
    assert payload["recommended_commands"][-1] == "uv run okg pact view demo --format=status --json"
