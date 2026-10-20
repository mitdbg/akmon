from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest


def _help_text(argv: list[str], capsys) -> str:
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 0
    return capsys.readouterr().out


def test_migrate_openspec_hidden_from_default_help(capsys) -> None:
    text = _help_text(["pact", "--help"], capsys)

    assert "migrate-openspec" not in text


def test_migrate_openspec_visible_only_in_help_all(capsys) -> None:
    text = _help_text(["pact", "--help-all"], capsys)

    assert "migrate-openspec" in text
    assert "deprecated" in text
    assert "history-import compatibility" in text
    assert "do not use for new" in text


def test_active_change_migration_preserves_content(
    tmp_path: Path,
    capsys,
) -> None:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 0\nproject: okg\n", encoding="utf-8"
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    change = root / "openspec" / "changes" / "demo"
    (change / "specs" / "demo-cap").mkdir(parents=True)
    (change / "proposal.md").write_text(
        "# Change: Demo\n\n## Why\n\nBecause tests need traceability.\n",
        encoding="utf-8",
    )
    (change / "tasks.md").write_text(
        "## 1. Work\n\n"
        "- [ ] 1.1 Implement demo. Verification: pytest tests/pact -q\n",
        encoding="utf-8",
    )
    (change / "specs" / "demo-cap" / "spec.md").write_text(
        "## ADDED Requirements\n\n"
        "### Requirement: Demo validates\n\n"
        "The system SHALL validate demo.\n\n"
        "#### Scenario: validation passes\n\n"
        "- **WHEN** validation runs\n"
        "- **THEN** validation passes\n",
        encoding="utf-8",
    )

    rc = main([
        "pact", "--root", str(root), "migrate-openspec", "demo", "--json",
    ])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["change"] == "demo"
    assert (root / "pact" / "changes" / "demo" / "pact.yaml").is_file()

    report = validate_manifest(load_manifest(root, "demo"))
    assert report.ok

    data = (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    assert "Demo validates" in data
    assert "pytest tests/pact -q" in data


def test_missing_change_refusal_names_the_moved_tree(tmp_path: Path) -> None:
    """The refusal points at history/openspec/, where okg keeps its tree now.

    migrate-openspec still reads only `<repo>/openspec/changes/<id>/` (PACT
    layout-history-openspec chose the refusal over a second lookup), so a
    change that lives only under `history/openspec/` is refused, and the
    refusal says where the tree went.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.pact.operations import migrate_openspec

    moved = tmp_path / "history" / "openspec" / "changes" / "demo"
    moved.mkdir(parents=True)
    (moved / "proposal.md").write_text("# Change: Demo\n", encoding="utf-8")

    with pytest.raises(SubstrateError) as excinfo:
        migrate_openspec(tmp_path, "demo")

    assert excinfo.value.data["path"] == str(
        tmp_path / "openspec" / "changes" / "demo"
    )
    assert "history/openspec/changes/" in excinfo.value.fix_hint
    assert not (tmp_path / "pact" / "changes" / "demo").exists()


def test_example_protocol_change_validates() -> None:
    repo = Path(__file__).resolve().parents[2]
    data = yaml.safe_load(
        (repo / "pact" / "examples" / "changes" / "protocol-change" / "pact.yaml")
        .read_text(encoding="utf-8")
    )
    manifest = PactManifest.model_validate(data)
    report = validate_manifest(manifest)

    assert manifest.change.id == "protocol-change"
    assert report.ok
