from __future__ import annotations

from pathlib import Path

import yaml

from scripts import check_docs_site


ROOT = Path(__file__).resolve().parents[2]


def test_documentation_has_one_strict_build_and_search_index() -> None:
    inventory = yaml.safe_load(
        (ROOT / "documentation.yaml").read_text(encoding="utf-8")
    )
    configs = []
    indexes = []
    build_dirs = []
    for publication in inventory["publications"].values():
        config_path = ROOT / publication["config"]
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert config["strict"] is True
        assert config["docs_dir"] == publication["source_dir"]
        assert config["site_dir"] == publication["build_dir"]
        assert "search" in config["plugins"]
        configs.append(config_path)
        indexes.append(publication["machine_index"])
        build_dirs.append(publication["build_dir"])
    assert len(configs) == 1
    assert len(set(configs)) == 1
    assert len(set(indexes)) == 1
    assert len(set(build_dirs)) == 1
    assert not (ROOT / "handbook").exists()
    assert not (ROOT / "mkdocs.handbook.yml").exists()
    assert not (ROOT / "handbook-llms.txt").exists()

    errors: list[str] = []
    check_docs_site._check_mkdocs_nav(errors)
    assert errors == []


def test_ci_builds_and_packages_one_site_bundle() -> None:
    workflow = (ROOT / ".github/workflows/docs-site.yml").read_text(encoding="utf-8")
    builder = (ROOT / "scripts/build_docs_bundle.py").read_text(encoding="utf-8")
    assert "python scripts/build_docs_bundle.py" in workflow
    assert "tar -C .scratch/docs-bundle" in workflow
    assert 'bundle / "handbook"' not in builder
    assert 'bundle / "release-manifest.json"' in builder
    assert '"site"' in builder
    assert '"handbook"' not in builder
