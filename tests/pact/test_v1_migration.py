from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_v0_to_v1_migration_is_explicit(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    save_manifest(root, PactManifest(pact_version=0, change=Change(id="demo", title="Demo")))

    assert main([
        "pact",
        "--root",
        str(root),
        "migrate-version",
        "demo",
        "--target",
        "1",
        "--json",
    ]) == 0
    payload = _json_out(capsys)

    assert payload["target_pact_version"] == 1
    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())
    assert data["pact_version"] == 0
    assert data["target_pact_version"] == 1
    assert data["edits"][0]["before_digest"] != data["edits"][0]["after_digest"]
