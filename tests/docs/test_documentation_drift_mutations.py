"""Small fixtures prove that documentation drift fails for the right reason."""

from __future__ import annotations

from pathlib import Path

from scripts import check_docs_site as checker


def _page(canonical_id: str, body: str = "") -> str:
    return (
        "---\naudience: user\ntype: how-to\n"
        f"canonical_id: {canonical_id}\nstatus: active\n---\n"
        f"# {canonical_id}\n\n{body}\n"
    )


def _site(monkeypatch, tmp_path: Path) -> Path:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text("[Documentation](docs/README.md)\n", encoding="utf-8")
    mkdocs = tmp_path / "mkdocs.yml"
    mkdocs.write_text(
        "strict: true\nnav:\n  - Start:\n      - Home: README.md\n"
        "validation:\n  links:\n    not_found: warn\n"
        "    absolute_links: warn\n    unrecognized_links: warn\n    anchors: warn\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(checker, "DOCS", docs)
    monkeypatch.setattr(checker, "MKDOCS", mkdocs)
    return docs


def test_orphan_page_is_rejected(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(_page("home"), encoding="utf-8")
    (docs / "orphan.md").write_text(_page("orphan"), encoding="utf-8")
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert any("no route" in error for error in errors)


def test_nav_only_orphan_is_rejected(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(_page("home"), encoding="utf-8")
    (docs / "orphan.md").write_text(_page("orphan"), encoding="utf-8")
    checker.MKDOCS.write_text(
        checker.MKDOCS.read_text(encoding="utf-8").replace(
            "- Start:\n      - Home: README.md",
            "- Start:\n      - Home: README.md\n      - Orphan: orphan.md",
        ),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert any("no route" in error for error in errors)


def test_broken_heading_anchor_is_rejected(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(
        _page("home", "[Task](task.md#missing)"), encoding="utf-8"
    )
    (docs / "task.md").write_text(_page("task"), encoding="utf-8")
    errors: list[str] = []
    checker._check_all_active_links(errors)
    assert any("heading anchor does not exist" in error for error in errors)


def test_duplicate_owner_and_historical_page_are_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(_page("shared"), encoding="utf-8")
    old = _page("shared", "**Audience:** user").replace(
        "status: active", "status: historical"
    )
    (docs / "old.md").write_text(old, encoding="utf-8")
    errors: list[str] = []
    checker._check_front_matter(errors)
    assert any("already belongs" in error for error in errors)
    assert any("historical material" in error for error in errors)
    assert any("repeats audience metadata" in error for error in errors)


def test_reader_facing_meta_commentary_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(
        _page("home", "The site is organized around customer paths."),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_front_matter(errors)
    assert any("maintainer commentary" in error for error in errors)


def test_forbidden_history_audit_milestone_and_compatibility_paths_are_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    _site(monkeypatch, tmp_path)
    forbidden = (
        "docs/history/old.md",
        "docs/architecture/db-schema-review-2026-08.md",
        "docs/architecture/2026-08-review.md",
        "docs/interfaces/m1-publisher.md",
        "docs/interfaces/m8_projector.md",
        "docs/reference/cli-surface-audit-copy.md",
        "docs/runbooks/no-silent-fallbacks-copy.md",
        "docs/operators/README.md",
    )
    for relative in forbidden:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("retired\n", encoding="utf-8")
    errors: list[str] = []
    checker._check_retired_paths(errors)
    assert len(errors) == len(forbidden)


def test_only_the_live_non_published_program_ledger_is_allowed(
    monkeypatch, tmp_path: Path
) -> None:
    _site(monkeypatch, tmp_path)
    audit_dir = tmp_path / "docs/audits"
    audit_dir.mkdir(parents=True, exist_ok=True)
    for relative in checker.LIVE_NON_PUBLISHED_PROGRAM_ARTIFACTS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("findings: []\n", encoding="utf-8")
    retired = tmp_path / "docs/audits/old-review.yaml"
    retired.write_text("findings: []\n", encoding="utf-8")
    errors: list[str] = []
    checker._check_retired_paths(errors)
    assert errors == ["docs/audits/old-review.yaml: retired documentation path has returned"]


def test_forbidden_history_heading_and_compatibility_status_are_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "page.md").write_text(
        _page("page", "## History and audits\n\nold material"), encoding="utf-8"
    )
    (docs / "compat.md").write_text(
        _page("compat").replace("status: active", "status: compatibility"), encoding="utf-8"
    )
    errors: list[str] = []
    checker._check_front_matter(errors)
    assert any("forbidden history" in error for error in errors)
    assert any("compatibility-only" in error for error in errors)


def test_compatibility_placeholder_metadata_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    docs = _site(monkeypatch, tmp_path)
    placeholder = _page("old-route").replace(
        "status: active", "status: active\nvisibility: search-only\nreplacement: docs/new.md"
    )
    (docs / "old-route.md").write_text(placeholder, encoding="utf-8")
    errors: list[str] = []
    checker._check_front_matter(errors)
    assert any("compatibility placeholders" in error for error in errors)


def test_retired_run_sql_call_is_rejected(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "page.md").write_text(_page("page", "Use `run_sql(query)` here."), encoding="utf-8")
    errors: list[str] = []
    checker._check_retired_terms(errors)
    assert any("retired run_sql call" in error for error in errors)


def test_weakening_link_validation_is_rejected(monkeypatch, tmp_path: Path) -> None:
    _site(monkeypatch, tmp_path)
    checker.MKDOCS.write_text(
        checker.MKDOCS.read_text(encoding="utf-8").replace("anchors: warn", "anchors: ignore"),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert any("link validation anchors must remain" in error for error in errors)


def test_retired_section_nested_in_navigation_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    _site(monkeypatch, tmp_path)
    checker.MKDOCS.write_text(
        checker.MKDOCS.read_text(encoding="utf-8").replace(
            "- Home: README.md",
            "- Home: README.md\n      - Reference:\n          - History and audits: README.md",
        ),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert any("retired navigation section" in error for error in errors)


def test_disabling_strict_site_build_is_rejected(monkeypatch, tmp_path: Path) -> None:
    _site(monkeypatch, tmp_path)
    checker.MKDOCS.write_text(
        checker.MKDOCS.read_text(encoding="utf-8").replace("strict: true", "strict: false"),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_mkdocs_nav(errors)
    assert any("strict mode" in error for error in errors)


def test_copied_instruction_block_is_rejected(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    block = "```bash\n" + "uv run okg status --deployment example --json\n" * 4 + "```"
    (docs / "README.md").write_text(
        _page("home", f"[Task](task.md)\n\n{block}"), encoding="utf-8"
    )
    (docs / "task.md").write_text(_page("task", block), encoding="utf-8")
    errors: list[str] = []
    checker._check_duplicate_instructions(errors)
    assert any("copied instruction block" in error for error in errors)


def test_changed_copy_of_a_command_procedure_is_rejected(
    monkeypatch, tmp_path: Path
) -> None:
    docs = _site(monkeypatch, tmp_path)
    first = """```bash
uv run okg deployment env example --json
uv run okg deployment ready example --profile dev --json
uv run okg doctor --deployment example --json
```"""
    changed = """```bash
uv run okg deployment env other --json
uv run okg deployment ready other --profile prod --json
uv run okg doctor --deployment other --verbose --json
```"""
    (docs / "README.md").write_text(_page("home", f"[Task](task.md)\n\n{first}"), encoding="utf-8")
    (docs / "task.md").write_text(_page("task", changed), encoding="utf-8")
    errors: list[str] = []
    checker._check_duplicate_instructions(errors)
    assert any("overlapping command procedure" in error for error in errors)


def test_new_linked_task_passes_without_an_inventory(monkeypatch, tmp_path: Path) -> None:
    docs = _site(monkeypatch, tmp_path)
    (docs / "README.md").write_text(
        _page("home", "[Import a dataset](import.md)"), encoding="utf-8"
    )
    (docs / "import.md").write_text(
        _page("import-dataset", "## Result\n\nA published version."),
        encoding="utf-8",
    )
    errors: list[str] = []
    checker._check_front_matter(errors)
    checker._check_mkdocs_nav(errors)
    checker._check_all_active_links(errors)
    assert errors == []
