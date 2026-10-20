from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_hook_install_materializes_local_and_ci_gates(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    assert main([
        "pact", "--root", str(root), "hooks", "install", "demo",
        "--dry-run", "--json",
    ]) == 0
    planned = _json_out(capsys)
    assert planned["dry_run"] is True
    assert planned["installed"] == []
    assert {
        ".pact/hooks/post-edit.sh",
        ".pact/hooks/task-done.sh",
        ".git/hooks/pre-commit",
        ".pact/hooks/pre-merge.sh",
        ".pact/hooks/graph-freshness.sh",
        ".pact/hooks/reviewer-brief.sh",
        ".github/workflows/pact-gates.yml",
    } <= {row["relative_path"] for row in planned["planned"]}
    assert "uv run okg pact gate pre-merge demo --json" in planned["commands"]

    assert main([
        "pact", "--root", str(root), "hooks", "install", "demo", "--json",
    ]) == 0
    installed = _json_out(capsys)
    assert installed["ok"] is True
    pre_commit = root / ".git" / "hooks" / "pre-commit"
    workflow = root / ".github" / "workflows" / "pact-gates.yml"
    assert "# okg-pact-managed" in pre_commit.read_text(encoding="utf-8")
    assert "uv run okg pact gate pre-commit demo --json" in pre_commit.read_text(
        encoding="utf-8"
    )
    assert "uv run okg pact gate pre-merge demo --json" in workflow.read_text(
        encoding="utf-8"
    )

    unmanaged = root / ".pact" / "hooks" / "reviewer-brief.sh"
    unmanaged.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    assert main([
        "pact", "--root", str(root), "hooks", "install", "demo",
        "--target", "local", "--json",
    ]) == 1
    blocked = _json_out(capsys)
    assert blocked["blocked"][0]["reason"] == "existing file is not PACT-managed"
