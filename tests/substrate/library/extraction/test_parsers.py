"""Parser tests + format-keyed dispatch.

Exercises each Parser backend on a small fixture input and
asserts the response shape. Failure-mode tests cover:
  - parser_for raises ParserNotFound on unknown format
  - sniff_format identifies common signatures
  - PdfTextParser raises on truly corrupt PDF input
"""
from __future__ import annotations

import io
from textwrap import dedent

import pytest


def test_parser_for_raises_on_unknown() -> None:
    from okg.substrate.library.extraction.parse import (
        ParserNotFound, parser_for,
    )
    with pytest.raises(ParserNotFound):
        parser_for("not-a-format")


def test_parser_for_routes_known_formats() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    for fmt in ["pdf", "html", "markdown", "ipynb", "email", "csv", "xlsx", "docx"]:
        p = parser_for(fmt)
        assert hasattr(p, "parse")


def test_sniff_format_identifies_pdf() -> None:
    from okg.substrate.library.extraction.parse import sniff_format
    assert sniff_format(b"%PDF-1.4\n...") == "pdf"


def test_sniff_format_identifies_html() -> None:
    from okg.substrate.library.extraction.parse import sniff_format
    assert sniff_format(b"<!DOCTYPE html>") == "html"
    assert sniff_format(b"<html>") == "html"


def test_sniff_format_returns_none_on_unknown() -> None:
    from okg.substrate.library.extraction.parse import sniff_format
    assert sniff_format(b"random bytes") is None
    assert sniff_format("a string") is None  # type: ignore[arg-type]


def test_html_parser_extracts_title_and_paragraphs() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    html = """
    <html><head><title>Test</title></head>
    <body>
      <h1>Section A</h1>
      <p>first para</p>
      <h2>Subsection</h2>
      <p>second para</p>
    </body></html>
    """
    doc = parser_for("html").parse(html)
    assert doc.metadata["title"] == "Test"
    # Headings are chunks too, in document order.
    assert [(c.chunk_kind, c.text) for c in doc.chunks] == [
        ("heading", "Section A"),
        ("paragraph", "first para"),
        ("heading", "Subsection"),
        ("paragraph", "second para"),
    ]
    assert doc.chunks[3].heading_path == ["Section A", "Subsection"]


def test_markdown_parser_emits_heading_chunks() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    md = dedent("""\
        # Heading

        First paragraph.

        ## Sub

        Second paragraph.
    """)
    doc = parser_for("markdown").parse(md)
    assert len(doc.chunks) >= 2


def test_frontmatter_parser_splits_metadata_and_body() -> None:
    """Per the wrap invariant, frontmatter and Markdown-body
    parsing live in sibling modules; callers compose them by
    running frontmatter first to extract metadata, then handing
    the body to the Markdown parser."""
    from okg.substrate.library.extraction.parse import parser_for
    raw = dedent("""\
        ---
        title: My Doc
        author: Alice
        ---
        # Heading

        Body paragraph.
    """)
    fm_doc = parser_for("frontmatter").parse(raw)
    assert fm_doc.metadata["title"] == "My Doc"
    assert fm_doc.metadata["author"] == "Alice"
    assert "---" not in fm_doc.text
    md_doc = parser_for("markdown").parse(fm_doc.text)
    assert len(md_doc.chunks) >= 1


def test_csv_parser_round_trip() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    csv_text = "a,b,c\n1,2,3\n4,5,6\n"
    doc = parser_for("csv").parse(csv_text)
    assert doc.metadata["row_count"] == 3
    assert doc.metadata["col_count"] == 3
    assert doc.metadata["header"] == ["a", "b", "c"]


def test_email_parser_extracts_headers_and_body() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    eml = (
        "From: alice@example.com\r\n"
        "To: bob@example.com\r\n"
        "Subject: hi\r\n"
        "Date: Mon, 24 Apr 2026 12:00:00 GMT\r\n"
        "\r\n"
        "Hello, Bob.\r\n"
    )
    doc = parser_for("email").parse(eml)
    assert doc.metadata["from"] == "alice@example.com"
    assert doc.metadata["subject"] == "hi"
    assert "Hello, Bob" in doc.text


def test_notebook_parser_round_trip() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    import nbformat
    nb = nbformat.v4.new_notebook()
    nb.metadata.kernelspec = {"name": "python3", "display_name": "Python 3"}
    nb.cells.append(nbformat.v4.new_markdown_cell("# title"))
    nb.cells.append(nbformat.v4.new_code_cell("print(1)"))
    doc = parser_for("ipynb").parse(nbformat.writes(nb))
    assert doc.metadata["kernel"] == "python3"
    assert doc.metadata["code_cells"] == 1
    assert doc.metadata["markdown_cells"] == 1
    assert "# title" in doc.text


def test_pdf_parser_blank_page() -> None:
    """Round-trip a pypdf-generated single-page PDF."""
    from pypdf import PdfWriter

    from okg.substrate.library.extraction.parse import parser_for

    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    buf = io.BytesIO()
    writer.write(buf)
    doc = parser_for("pdf").parse(buf.getvalue())
    assert doc.metadata["pages"] == 1
    # Empty page yields empty/whitespace text.
    assert isinstance(doc.text, str)


def test_pdf_parser_raises_on_corrupt() -> None:
    """A truly corrupt PDF must raise (no silent degraded text)."""
    from okg.substrate.library.extraction.parse import parser_for
    parser = parser_for("pdf")
    with pytest.raises(Exception):
        parser.parse(b"this is not a pdf at all")


def test_python_ast_parser_extracts_metadata() -> None:
    from okg.substrate.library.extraction.parse import parser_for
    src = dedent("""\
        '''Module docstring.'''
        import os
        from typing import Optional

        class Foo:
            '''Class docstring.'''
            def bar(self): pass

        def baz():
            '''Func docstring.'''
            return 42
    """)
    parser = parser_for("code")
    doc = parser.parse(src, language="python")
    assert "os" in doc.metadata["imports"]
    assert "Foo" in doc.metadata["classes"]
    assert "baz" in doc.metadata["defs"]
    assert doc.metadata["docstring"] == "Module docstring."


def test_tree_sitter_grammar_missing_raises() -> None:
    """Unsupported language → GrammarMissing with fix-hint."""
    from okg.substrate.library.extraction.parse import parser_for
    from okg.substrate.library.extraction.parse.code_treesitter import (
        GrammarMissing,
    )
    parser = parser_for("code")
    with pytest.raises(GrammarMissing) as exc:
        parser.parse(b"function foo() {}", language="javascript")
    assert "tree_sitter_javascript" in (exc.value.fix_hint or "")
