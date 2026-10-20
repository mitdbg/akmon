from __future__ import annotations

from pathlib import Path

from scripts import check_docs_site as checker


def _page() -> str:
    return (
        "---\n"
        "audience: user\n"
        "type: overview\n"
        "canonical_id: page\n"
        "status: active\n"
        "---\n"
        "# Page\n"
    )


def _fixture(monkeypatch, tmp_path: Path) -> dict:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text(_page(), encoding="utf-8")
    (docs / "README.md").write_text(_page(), encoding="utf-8")
    inventory = {
        "version": 1,
        "publications": {
            "product": {
                "source_dir": "docs",
                "page_limit": 1,
                "pages": ["README.md"],
                "assets": [],
            },
        },
        "gated_pages": [
            {"path": "docs/start/quickstart.md", "gate": "distribution"}
        ],
        "retired_roots": [
            "handbook",
            "mkdocs.product.yml",
            "mkdocs.handbook.yml",
            "handbook-llms.txt",
        ],
        "redirects": {},
    }
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(checker, "DOCS", docs)
    monkeypatch.setattr(checker, "_load_inventory", lambda: inventory)
    return inventory


def test_unowned_page_is_rejected(monkeypatch, tmp_path: Path) -> None:
    _fixture(monkeypatch, tmp_path)
    extra = tmp_path / "docs/runbooks/old.md"
    extra.parent.mkdir()
    extra.write_text(_page(), encoding="utf-8")
    errors: list[str] = []
    checker._check_inventory(errors)
    assert any("not owned by documentation.yaml" in error for error in errors)


def test_gated_quickstart_is_rejected_before_promotion(
    monkeypatch, tmp_path: Path
) -> None:
    _fixture(monkeypatch, tmp_path)
    gated = tmp_path / "docs/start/quickstart.md"
    gated.parent.mkdir()
    gated.write_text(_page(), encoding="utf-8")
    errors: list[str] = []
    checker._check_inventory(errors)
    assert any("promotion gate" in error for error in errors)


def test_second_publication_is_rejected(monkeypatch, tmp_path: Path) -> None:
    inventory = _fixture(monkeypatch, tmp_path)
    inventory["publications"]["handbook"] = {
        "source_dir": "handbook",
        "page_limit": 1,
        "pages": ["index.md"],
        "assets": [],
    }
    errors: list[str] = []
    checker._check_inventory(errors)
    assert "documentation.yaml: product must be the only publication" in errors


def test_retired_tree_or_build_artifact_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    _fixture(monkeypatch, tmp_path)
    (tmp_path / "handbook").mkdir()
    (tmp_path / "mkdocs.handbook.yml").write_text("site_name: old\n", encoding="utf-8")
    errors: list[str] = []
    checker._check_retired_paths(errors)
    assert "handbook: retired documentation path has returned" in errors
    assert "mkdocs.handbook.yml: retired documentation path has returned" in errors


def test_retired_publication_link_is_rejected(monkeypatch, tmp_path: Path) -> None:
    _fixture(monkeypatch, tmp_path)
    (tmp_path / "docs/README.md").write_text(
        _page() + "[Old route](/handbook/quality/)\n", encoding="utf-8"
    )
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert any("retired documentation route" in error for error in errors)


def _unpublished_fixture(monkeypatch, tmp_path: Path, *, nav: str, exclude: str) -> dict:
    """The base fixture plus one held-back page under ``internal/``."""

    inventory = _fixture(monkeypatch, tmp_path)
    internal = tmp_path / "docs/internal/notes.md"
    internal.parent.mkdir()
    internal.write_text(_page(), encoding="utf-8")
    product = inventory["publications"]["product"]
    product["unpublished"] = ["internal/"]
    product["config"] = "mkdocs.yml"
    product["build_dir"] = ".scratch/docs-site"
    (tmp_path / "mkdocs.yml").write_text(
        "docs_dir: docs\nsite_dir: .scratch/docs-site\nstrict: true\n"
        "plugins:\n  - search\n"
        f"exclude_docs: |\n  {exclude}\n"
        f"nav:\n{nav}"
        "validation:\n  links:\n    not_found: warn\n    absolute_links: ignore\n"
        "    unrecognized_links: warn\n    anchors: warn\n",
        encoding="utf-8",
    )
    return inventory


_NAV = "  - Home: README.md\n"


def test_unpublished_fixture_is_clean(monkeypatch, tmp_path: Path) -> None:
    """The positive control: each failure case below differs by one mutation."""

    _unpublished_fixture(monkeypatch, tmp_path, nav=_NAV, exclude="internal/")
    errors: list[str] = []
    checker._check_inventory(errors)
    checker._check_mkdocs_nav(errors)
    checker._check_all_active_links(errors)
    assert errors == []


def test_published_page_linking_into_an_unpublished_page_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    _unpublished_fixture(monkeypatch, tmp_path, nav=_NAV, exclude="internal/")
    (tmp_path / "docs/README.md").write_text(
        _page() + "[Notes](internal/notes.md)\n", encoding="utf-8"
    )
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert errors == [
        "docs/README.md: published page links to an unpublished page: "
        "internal/notes.md"
    ]


def test_exclude_docs_must_match_unpublished(monkeypatch, tmp_path: Path) -> None:
    _unpublished_fixture(monkeypatch, tmp_path, nav=_NAV, exclude="other/")
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert errors == [
        "mkdocs.yml: exclude_docs ['other/'] must equal documentation.yaml "
        "unpublished ['internal/']"
    ]


def test_unpublished_page_in_navigation_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    _unpublished_fixture(
        monkeypatch,
        tmp_path,
        nav=_NAV + "  - Notes: internal/notes.md\n",
        exclude="internal/",
    )
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert "mkdocs.yml: unpublished page appears in navigation: internal/notes.md" in errors


def test_redirect_to_an_unpublished_page_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    inventory = _unpublished_fixture(
        monkeypatch, tmp_path, nav=_NAV, exclude="internal/"
    )
    inventory["redirects"] = {"old.md": "internal/notes.md"}
    errors: list[str] = []
    checker._check_inventory(errors)
    assert errors == [
        "documentation.yaml: redirect target is unpublished: "
        "'old.md' -> 'internal/notes.md'"
    ]
