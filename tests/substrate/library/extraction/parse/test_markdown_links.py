"""Tests for the markdown link extractor and target resolver.

Per `add-markdown-link-extraction` (2026-04-27).
"""
from __future__ import annotations



# ---------- AST extractor: extract_links() ---------------------------

def test_extracts_inline_link() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = "See [the architecture](architecture.md) for details."
    links = extract_links(md)
    assert len(links) == 1
    assert links[0].href == "architecture.md"
    assert links[0].text == "the architecture"


def test_extracts_multiple_inline_links() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = (
        "Read [STYLE.md](STYLE.md) and "
        "[glossary](docs/reference/glossary.md#publish-cycle) "
        "and [the spec](https://example.com/spec)."
    )
    links = extract_links(md)
    assert len(links) == 3
    hrefs = [link.href for link in links]
    assert hrefs == [
        "STYLE.md",
        "docs/reference/glossary.md#publish-cycle",
        "https://example.com/spec",
    ]


def test_link_text_with_inline_code() -> None:
    """`[`foo.py`](foo.py)` — inline code inside link text."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = "Try [`foo.py`](foo.py) directly."
    links = extract_links(md)
    assert len(links) == 1
    assert links[0].href == "foo.py"
    assert "foo.py" in links[0].text


def test_link_text_with_emphasis() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = "[**bold**](bold.md) and [_italic_](italic.md)"
    links = extract_links(md)
    assert [(link.href, link.text) for link in links] == [
        ("bold.md", "bold"),
        ("italic.md", "italic"),
    ]


def test_skips_image_links() -> None:
    """`![alt](src)` is an image, not an anchor link."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = "An image: ![alt](image.png) and a [link](page.md)."
    links = extract_links(md)
    assert len(links) == 1
    assert links[0].href == "page.md"


def test_skips_links_in_code_blocks() -> None:
    """A `[text](href)` literal inside a fenced code block must
    NOT be emitted as a link — markdown-it correctly returns
    code_block content as opaque text."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = (
        "Some prose with a [real](real.md) link.\n\n"
        "```\n"
        "[fake](fake.md)\n"
        "```\n\n"
        "More prose with [another](another.md).\n"
    )
    links = extract_links(md)
    hrefs = [link.href for link in links]
    assert hrefs == ["real.md", "another.md"]
    assert "fake.md" not in hrefs


def test_empty_input() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    assert extract_links("") == []


def test_no_links() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    assert extract_links("Just prose, no links.") == []


def test_anchor_only_link_extracted_caller_decides() -> None:
    """`[text](#section)` IS extracted by the AST walker; the
    caller (doc_corpus) is responsible for filtering anchor-only
    links since their resolution depends on context."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        extract_links,
    )
    md = "See [section](#publish-cycle) below."
    links = extract_links(md)
    assert len(links) == 1
    assert links[0].href == "#publish-cycle"


# ---------- target resolver: resolve_link_target() -------------------

def test_resolve_sibling_relative() -> None:
    """A same-directory sibling link resolves within that directory:
    `docs/develop/architecture/pipeline.md` -> `graph-model.md` resolves to
    `docs/develop/architecture/graph-model.md`. (`resolve_link_target`
    canonicalizes the path; it does not rewrite to a doc's moved
    location — the caller checks the result against the known docs.)"""
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target(
        "graph-model.md", "docs/develop/architecture/pipeline.md",
    )
    assert rel == "docs/develop/architecture/graph-model.md"
    assert anchor is None


def test_resolve_parent_relative() -> None:
    """A parent-relative link traverses up then back down:
    `docs/runbooks/troubleshooting/X.md` -> `../../reference/glossary.md` resolves to
    `docs/reference/glossary.md`."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target(
        "../../reference/glossary.md", "docs/runbooks/troubleshooting/X.md",
    )
    assert rel == "docs/reference/glossary.md"
    assert anchor is None


def test_resolve_strips_anchor() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target(
        "glossary.md#publish-cycle", "docs/reference/security.md",
    )
    assert rel == "docs/reference/glossary.md"
    assert anchor == "publish-cycle"


def test_resolve_strips_query_string() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target(
        "glossary.md?v=1#section", "docs/reference/security.md",
    )
    assert rel == "docs/reference/glossary.md"
    assert anchor == "section"


def test_resolve_repo_root_absolute() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, _ = resolve_link_target(
        "/docs/develop/architecture/pipeline.md", "anywhere/X.md",
    )
    assert rel == "docs/develop/architecture/pipeline.md"


def test_resolve_external_url_returns_none() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    for href in (
        "http://example.com",
        "https://example.com/path",
        "mailto:foo@example.com",
        "javascript:void(0)",
    ):
        rel, anchor = resolve_link_target(href, "docs/X.md")
        assert rel is None, f"expected None for {href!r}, got {rel!r}"
        assert anchor is None


def test_resolve_anchor_only_returns_none() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target("#section", "docs/X.md")
    assert rel is None
    assert anchor is None


def test_resolve_empty_returns_none() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    assert resolve_link_target("", "X.md") == (None, None)
    assert resolve_link_target("   ", "X.md") == (None, None)


def test_resolve_above_repo_root_refused() -> None:
    """A relative path that walks ABOVE the repo root has no
    valid target; refuse rather than produce `../something`."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, _ = resolve_link_target("../../../etc/passwd", "docs/X.md")
    assert rel is None


def test_resolve_dot_segment_normalizes() -> None:
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, _ = resolve_link_target("./glossary.md", "docs/reference/security.md")
    assert rel == "docs/reference/glossary.md"


def test_resolve_self_reference() -> None:
    """A doc linking to itself by name."""
    from okg.substrate.library.extraction.parse.markdown_links import (
        resolve_link_target,
    )
    rel, anchor = resolve_link_target(
        "security.md#section", "docs/reference/security.md",
    )
    assert rel == "docs/reference/security.md"
    assert anchor == "section"
