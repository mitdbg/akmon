from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def _inventory() -> dict:
    return yaml.safe_load((ROOT / "documentation.yaml").read_text(encoding="utf-8"))


def _unpublished(publication: dict) -> tuple[str, ...]:
    return tuple(publication.get("unpublished") or ())


def test_source_tree_matches_the_machine_inventory_exactly() -> None:
    inventory = _inventory()
    for name, publication in inventory["publications"].items():
        root = ROOT / publication["source_dir"]
        unpublished = _unpublished(publication)
        all_pages = {
            path.relative_to(root).as_posix() for path in root.rglob("*.md")
        }
        held_back = {page for page in all_pages if page.startswith(unpublished)}
        actual_pages = all_pages - held_back
        # Every page file is either published (listed in `pages`) or sits under
        # a declared `unpublished` prefix; no page is both, and the ceiling is
        # a ceiling, not a pin on the exact count.
        assert actual_pages == set(publication["pages"]), name
        assert not set(publication["pages"]) & held_back, name
        assert len(actual_pages) <= publication["page_limit"], name
        for prefix in unpublished:
            assert prefix.endswith("/"), prefix
            assert any(page.startswith(prefix) for page in held_back), prefix

        expected_assets = {asset["path"] for asset in publication["assets"]}
        for asset in expected_assets:
            assert (root / asset).is_file(), asset

        expected_dirs = {
            parent.as_posix()
            for relative in [*all_pages, *expected_assets]
            for parent in Path(relative).parents
            if parent.as_posix() != "."
        }
        actual_dirs = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_dir()
        }
        assert actual_dirs == expected_dirs


def test_retired_roots_and_gated_quickstart_are_absent() -> None:
    inventory = _inventory()
    for relative in inventory["retired_roots"]:
        assert not (ROOT / relative).exists(), relative
    for gated in inventory["gated_pages"]:
        assert not (ROOT / gated["path"]).exists(), gated["path"]


def test_unpublished_pages_are_excluded_from_the_build_and_navigation() -> None:
    inventory = _inventory()
    for publication in inventory["publications"].values():
        config = yaml.safe_load(
            (ROOT / publication["config"]).read_text(encoding="utf-8")
        )
        excluded = {
            line.strip()
            for line in str(config.get("exclude_docs") or "").splitlines()
            if line.strip()
        }
        assert excluded == set(_unpublished(publication))
        machine_index = (ROOT / publication["machine_index"]).read_text(
            encoding="utf-8"
        )
        for prefix in _unpublished(publication):
            assert f"/{prefix}" not in machine_index, prefix
