"""Every built-in source type has a docs page that matches the code.

`okg deployment source-types` and docs/use/sources/<type>.md read the same
catalog: the page's generated block is rendered from it, and every page
carries the same headings (public-release-docs, task connect-data).
"""

from __future__ import annotations

from pathlib import Path

from scripts import render_source_type_pages as pages

ROOT = Path(__file__).resolve().parents[2]


def test_every_source_type_has_a_current_page() -> None:
    assert pages.source_types(), "the catalog lists no source types"
    assert pages.problems() == []


def test_a_stale_generated_block_is_reported(tmp_path: Path, monkeypatch) -> None:
    for item in pages.source_types():
        text = pages.page_path(item["type"]).read_text(encoding="utf-8")
        (tmp_path / f"{item['type']}.md").write_text(
            text.replace(item["summary"], "Reads something else."), encoding="utf-8"
        )
    monkeypatch.setattr(pages, "PAGES", tmp_path)
    monkeypatch.setattr(pages, "ROOT", tmp_path)
    found = pages.problems()
    assert len(found) == len(pages.source_types())
    assert all("out of date" in line for line in found)


def _copy_pages(tmp_path: Path, monkeypatch) -> list[dict]:
    items = pages.source_types()
    for item in items:
        text = pages.page_path(item["type"]).read_text(encoding="utf-8")
        (tmp_path / f"{item['type']}.md").write_text(text, encoding="utf-8")
    monkeypatch.setattr(pages, "PAGES", tmp_path)
    monkeypatch.setattr(pages, "ROOT", tmp_path)
    return items


def test_a_missing_page_is_reported(tmp_path: Path, monkeypatch) -> None:
    items = _copy_pages(tmp_path, monkeypatch)
    (tmp_path / f"{items[0]['type']}.md").unlink()
    assert pages.main(["--write"]) == 1
    assert [line for line in pages.problems() if "no page" in line] == [
        f"{items[0]['type']}.md: no page for source type {items[0]['type']!r}"
    ]


def test_a_missing_hand_written_heading_is_reported(tmp_path: Path, monkeypatch) -> None:
    items = _copy_pages(tmp_path, monkeypatch)
    path = tmp_path / f"{items[0]['type']}.md"
    path.write_text(
        path.read_text(encoding="utf-8").replace("## When it fails", "## Failures"),
        encoding="utf-8",
    )
    found = pages.problems()
    assert len(found) == 1 and "missing headings ['## When it fails']" in found[0]


def test_pages_are_published_and_linked_from_the_route_chooser() -> None:
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    chooser = (ROOT / "docs/use/sources/index.md").read_text(encoding="utf-8")
    for item in pages.source_types():
        assert f"use/sources/{item['type']}.md" in nav
        assert f"]({item['type']}.md)" in chooser
