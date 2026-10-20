"""Active docs must not cite repository paths or documentation pages that do not exist."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load():
    path = ROOT / "scripts/check_doc_references.py"
    spec = importlib.util.spec_from_file_location("check_doc_references", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = _load()


def _make_docs(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "repo"
    docs = root / "docs"
    source = docs / "runbooks" / "quality" / "source-test-matrix.md"
    source.parent.mkdir(parents=True)
    source.write_text("# Test page\n", encoding="utf-8")
    sources = docs / "use" / "sources"
    sources.mkdir(parents=True)
    (sources / "index.md").write_text("# Sources\n", encoding="utf-8")
    (sources / "page name.md").write_text("# Named page\n", encoding="utf-8")
    assets = docs / "assets"
    assets.mkdir()
    (assets / "diagram.png").write_bytes(b"png")
    (root / "pact").mkdir()
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "README.md").write_text("# Repository\n", encoding="utf-8")
    return root, docs, source


def _validate(
    target: str,
    source: Path,
    docs: Path,
    root: Path,
    *,
    image: bool = False,
):
    destination = CHECK.MarkdownDestination(target, target, image)
    return CHECK._validate_markdown_destination(
        destination, source, docs_root=docs, root=root
    )


def test_no_new_dead_references() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/check_doc_references.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_extracts_supported_commonmark_destinations() -> None:
    cases = {
        "[plain](page.md)": ("page.md", "page.md", False),
        '[title](page.md "A title")': ("page.md", "page.md", False),
        "[angle](<page name.md>)": ("page name.md", "page name.md", False),
        r"[escaped](guide\(old\).md)": (
            r"guide\(old\).md",
            "guide(old).md",
            False,
        ),
        "[balanced](guide(old).md)": ("guide(old).md", "guide(old).md", False),
        '[reference]: page.md "A title"': ("page.md", "page.md", False),
        "![image](images/diagram.png)": (
            "images/diagram.png",
            "images/diagram.png",
            True,
        ),
    }
    for line, expected in cases.items():
        destinations, issues = CHECK._extract_markdown_destinations(line)
        assert not issues, line
        assert [tuple(item) for item in destinations] == [expected]


def test_inline_code_spans_do_not_emit_markdown_links() -> None:
    lines = [
        "`[fake](missing.md)` [real](page.md)",
        "``[fake](missing.md) `nested tick` `` [real](page.md)",
    ]
    for line in lines:
        destinations, issues = CHECK._extract_markdown_destinations(line)
        assert not issues
        assert [item.target for item in destinations] == ["page.md"]


def test_balanced_and_escaped_link_labels_are_recognized() -> None:
    lines = [
        "[outer [inner]](page.md)",
        r"[escaped \]](page.md)",
    ]
    for line in lines:
        destinations, issues = CHECK._extract_markdown_destinations(line)
        assert not issues
        assert [item.target for item in destinations] == ["page.md"]

    destinations, issues = CHECK._extract_markdown_destinations(
        "[outer [inner](missing.md)"
    )
    assert not destinations
    assert len(issues) == 1
    assert issues[0].detail == "unbalanced link label brackets"


def test_fenced_and_indented_code_blocks_are_skipped(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "```md\n"
        "[fenced](missing.md)\n"
        "```\n"
        "    [indented](missing.md)\n"
        "\n"
        "    [continued](missing.md)\n"
        "[real](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(7, "missing.md")]


def test_indentation_respects_paragraph_and_block_boundaries(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "    [document-code](missing.md)\n"
        "\n"
        "paragraph text\n"
        "    [paragraph-continuation](missing.md)\n"
        "\n"
        "# Heading\n"
        "    [after-heading-code](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(4, "missing.md")]


def test_blockquote_container_distinguishes_code_and_links(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        ">     [quoted-code](missing.md)\n> [quoted-link](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(2, "missing.md")]


def test_container_exit_closes_a_fence_instead_of_blinding_the_rest_of_the_file(
    tmp_path: Path,
) -> None:
    """A fence opened inside a container must not swallow the whole page.

    Closing the fence required matching container depth, so once the
    blockquote or list item ended the fence stayed open to end of file and
    every later link was silently treated as code.
    """
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "> ```\n> quoted code\n\n[after-quote](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(4, "missing.md")]

    source.write_text(
        "- ```\n  list code\n\n[after-list](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(4, "missing.md")]

    # A fence whose own line is not indented into the list body is not bound to
    # that item, so its unindented content must stay code.
    source.write_text(
        "- item\n```\n[fenced](missing.md)\n```\n[after](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [(5, "missing.md")]


def test_placeholder_in_query_or_fragment_does_not_excuse_a_dead_page(
    tmp_path: Path,
) -> None:
    """Query and fragment suffixes must not change page existence."""
    root, docs, source = _make_docs(tmp_path)
    for suffix in ("#N", "#a{b}", "?v=*", "#step-1"):
        result = _validate(f"/no/such/route/{suffix}", source, docs, root)
        assert not result.ok, suffix
        assert result.classification == "site-absolute", suffix

    # A placeholder in the path itself is still a template, not a page.
    assert _validate("/no/such/{route}/", source, docs, root).ok


def test_ambiguous_list_prefix_is_reported_rather_than_silently_dropped(
    tmp_path: Path,
) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text("-      [ambiguous](missing.md)\n", encoding="utf-8")
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.kind, item.line, item.detail) for item in issues] == [
        ("syntax", 1, "unsupported ambiguous list prefix")
    ]


def test_list_container_distinguishes_links_continuations_and_code(
    tmp_path: Path,
) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "- [list-link](missing.md)\n"
        "- paragraph text\n"
        "      [list-paragraph-continuation](missing.md)\n"
        "\n"
        "- item\n"
        "\n"
        "      [list-code](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.line, item.target) for item in issues] == [
        (1, "missing.md"),
        (3, "missing.md"),
    ]


def test_backtick_repo_paths_survive_link_code_masking(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "`[inline-link](missing.md)` `tests/definitely_missing_inline_code.py`\n"
        "```md\n"
        "[fenced-link](missing.md)\n"
        "`tests/definitely_missing_fenced_code.py`\n"
        "```\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.kind, item.line, item.target) for item in issues] == [
        ("repository", 1, "tests/definitely_missing_inline_code.py"),
        ("repository", 4, "tests/definitely_missing_fenced_code.py"),
    ]


def test_rejects_unsupported_local_looking_link_syntax() -> None:
    lines = [
        '[broken](../missing.md "title"',
        "[spaced] (../missing.md)",
        "[reference]: ../missing.md trailing",
    ]
    for line in lines:
        destinations, issues = CHECK._extract_markdown_destinations(line)
        assert not destinations
        assert len(issues) == 1, line
        assert "missing.md" in issues[0].raw


def test_accepts_absolute_relative_and_pretty_page_routes(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    targets = [
        "/use/sources/",
        "/use/sources/index.md?x=1#connect-the-source",
        "/use/sources/#connect-the-source",
        "../../use/sources/#connect-the-source",
        "../../use/sources/index.md?x=1#connect-the-source",
        "/use/sources/page%20name.md",
    ]
    for target in targets:
        result = _validate(target, source, docs, root)
        assert result.ok, (target, result)


def test_classification_ignores_external_anchor_and_non_page_assets(
    tmp_path: Path,
) -> None:
    root, docs, source = _make_docs(tmp_path)
    ignored = [
        "https://example.com/page.md",
        "https://[malformed-authority",
        "custom+docs://[malformed-authority",
        "x-docs:local(page).md",
        "mailto:docs@example.com",
        "tel:+12025550123",
        "data:text/plain,page.md",
        "//example.com/page.md",
        "#fragment",
        "../../assets/diagram.png?raw=1#view",
    ]
    for target in ignored:
        result = _validate(target, source, docs, root)
        assert result == CHECK.TargetResult("ignored", True), target

    known_asset = _validate(
        "docs/assets/diagram.png?raw=1#view", source, docs, root, image=True
    )
    assert known_asset == CHECK.TargetResult("repository", True)


def test_readme_docs_pages_and_known_root_refs_are_disjoint(tmp_path: Path) -> None:
    root, docs, _ = _make_docs(tmp_path)
    readme = root / "README.md"
    docs_page = _validate("docs/use/sources/index.md", readme, docs, root)
    repo_ref = _validate("pact/AGENTS.md", readme, docs, root)
    assert docs_page == CHECK.TargetResult("readme-docs", True)
    assert repo_ref == CHECK.TargetResult("repository", True)


def test_missing_doc_pages_do_not_fall_back_to_repository_root(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    (root / "missing.md").write_text("# Wrong root\n", encoding="utf-8")

    dead_route = _validate("/use/sources/connect/", source, docs, root)
    missing_relative = _validate("missing.md", source, docs, root)

    assert dead_route.classification == "site-absolute"
    assert not dead_route.ok
    assert dead_route.detail == "documentation page does not exist"
    assert missing_relative.classification == "source-relative"
    assert not missing_relative.ok
    assert missing_relative.detail == "documentation page does not exist"


def test_diagnostics_keep_original_target_source_and_line(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    source.write_text(
        "[dead](/use/sources/connect/)\n[relative](missing.md)\n",
        encoding="utf-8",
    )
    issues = CHECK.find_reference_issues([source], docs_root=docs, root=root)
    assert [(item.source, item.line, item.target) for item in issues] == [
        ("docs/runbooks/quality/source-test-matrix.md", 1, "/use/sources/connect/"),
        ("docs/runbooks/quality/source-test-matrix.md", 2, "missing.md"),
    ]


def test_rejects_malformed_and_unsafe_local_paths(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    cases = {
        "/use/%ZZ.md": "malformed percent escape",
        "/use/%00.md": "decoded path contains NUL",
        "/use/%5Coutside.md": "decoded path contains a backslash",
        "../../../README.md": "path escapes docs",
        "..%2F..%2F..%2FREADME.md": "path escapes docs",
        "/../README.md": "path escapes docs",
    }
    for target, detail in cases.items():
        result = _validate(target, source, docs, root)
        assert not result.ok, target
        assert result.detail == detail


def test_double_encoded_traversal_decodes_once_and_misses(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    result = _validate("%252e%252e%252fREADME.md", source, docs, root)
    assert result.classification == "source-relative"
    assert not result.ok
    assert result.detail == "documentation page does not exist"


def test_symlinked_page_cannot_escape_docs_or_fall_through(tmp_path: Path) -> None:
    root, docs, source = _make_docs(tmp_path)
    outside = root / "outside.md"
    outside.write_text("# Outside\n", encoding="utf-8")
    (docs / "escape.md").symlink_to(outside)
    (docs / "escape").mkdir()
    (docs / "escape" / "index.md").write_text("# Inside fallback\n", encoding="utf-8")

    result = _validate("/escape/", source, docs, root)
    assert not result.ok
    assert result.detail == "resolved path escapes docs"


def test_repository_path_and_placeholder_checks_are_preserved() -> None:
    assert CHECK._looks_like_path("src/okg/substrate/pact/cli.py")
    assert CHECK._path_exists("src/okg/substrate/pact/cli.py")
    assert not CHECK._path_exists("src/okg/substrate/pact/does_not_exist.py")
    assert not CHECK._path_exists("docs/architecture.md")
    assert CHECK._path_exists("docs/develop/architecture/pipeline.md")
    assert not CHECK._looks_like_path("pact/changes/<id>/pact.yaml")
    assert not CHECK._looks_like_path("https://example.com/x/y")
    assert not CHECK._looks_like_path("docs/reports/audit-YYYY-MM-DD.md")
