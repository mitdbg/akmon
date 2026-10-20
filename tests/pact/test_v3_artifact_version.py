from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from tests.pact.test_cli import _build_demo, _seed_v3_root as _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_pact_v3_schema_version_is_explicit_and_migratable(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 3\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 0\n"
        "  target_pact_version: 3\n",
        encoding="utf-8",
    )
    _build_demo(root)

    assert main([
        "pact", "--root", str(root), "migrate-version", "demo",
        "--target", "3", "--json",
    ]) == 0
    compat = _json_out(capsys)
    assert compat["artifact_format_version"] == 0
    assert compat["target_language_version"] == 3
    assert compat["compatibility_mode"] is True
    assert compat["write_artifact_version"] == 3
    assert compat["migration"]["recommended_command"].endswith(
        "--target 3 --artifact-format 3"
    )

    assert main([
        "pact", "--root", str(root), "migrate-version", "demo",
        "--target", "3", "--artifact-format", "3", "--json",
    ]) == 0
    native = _json_out(capsys)
    assert native["artifact_format_version"] == 3
    assert native["target_language_version"] == 3
    assert native["compatibility_mode"] is False
    assert native["read_mode"] == "native"

    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert data["pact_version"] == 3
    assert data["target_pact_version"] == 3
