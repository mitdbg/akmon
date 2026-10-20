"""`okg deployment gc-scaffolds` — list/remove stale operator-state scaffolds."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployment_scaffold_gc import gc_scaffolds, list_scaffolds


def _make_deployment(root: Path, name: str) -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({"name": name}), encoding="utf-8"
    )
    return d


def _make_incomplete(root: Path, name: str) -> Path:
    """A scaffold with no deployment.yaml (abandoned new/install run)."""
    d = root / name
    d.mkdir(parents=True)
    (d / "NOTES.txt").write_text("partial scaffold\n", encoding="utf-8")
    return d


def test_list_scaffolds_flags_incomplete_and_numbered(tmp_path: Path) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    _make_deployment(root, "okg-workspace")  # valid → keep
    _make_deployment(root, "okg-workspace-1")  # numbered sibling → stale
    _make_incomplete(root, "halfbaked")  # no manifest → stale

    report = list_scaffolds(root=root)
    by_name = {e["name"]: e for e in report["scaffolds"]}
    assert by_name["okg-workspace"]["stale"] is False
    assert by_name["okg-workspace"]["status"] == "deployment"
    assert by_name["okg-workspace-1"]["stale"] is True
    assert by_name["okg-workspace-1"]["status"] == "numbered_throwaway"
    assert by_name["halfbaked"]["stale"] is True
    assert by_name["halfbaked"]["status"] == "incomplete_scaffold"
    assert set(report["stale_names"]) == {"okg-workspace-1", "halfbaked"}


def test_gc_scaffolds_dry_run_removes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    _make_incomplete(root, "halfbaked")

    report = gc_scaffolds(yes=False, root=root)
    assert report["mode"] == "dry_run"
    assert report["would_remove"] == ["halfbaked"]
    assert (root / "halfbaked").is_dir()  # untouched


def test_gc_scaffolds_apply_removes_stale_only(tmp_path: Path) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    _make_deployment(root, "okg-workspace")
    _make_incomplete(root, "halfbaked")
    _make_deployment(root, "okg-workspace-2")  # numbered throwaway

    report = gc_scaffolds(yes=True, root=root)
    assert report["mode"] == "remove"
    assert report["ok"] is True
    assert set(report["removed"]) == {"halfbaked", "okg-workspace-2"}
    # The valid primary deployment is preserved.
    assert (root / "okg-workspace").is_dir()
    assert not (root / "halfbaked").exists()
    assert not (root / "okg-workspace-2").exists()


def test_gc_scaffolds_cli_lists_and_removes(tmp_path: Path, capsys) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    _make_incomplete(root, "halfbaked")

    rc = main([
        "deployment",
        "gc-scaffolds",
        "--root",
        str(root),
        "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["mode"] == "dry_run"
    assert payload["would_remove"] == ["halfbaked"]
    assert (root / "halfbaked").is_dir()

    rc = main([
        "deployment",
        "gc-scaffolds",
        "--root",
        str(root),
        "--yes",
        "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["removed"] == ["halfbaked"]
    assert not (root / "halfbaked").exists()
