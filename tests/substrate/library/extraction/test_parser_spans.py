"""Parsers that build their own document text give each chunk its real span.

A whole-record read decides `complete` from stored spans, so a span must name
exactly the chunk's text inside `ParsedDocument.text`: `text[start:end] ==
chunk.text` for every chunk, with the separators between them left in the
document text only.
"""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest

from okg.substrate.library.extraction._protocols import ParsedDocument


def _assert_spans_locate_chunks(doc: ParsedDocument) -> None:
    assert doc.chunks, "the fixture must produce chunks"
    for chunk in doc.chunks:
        assert chunk.span is not None, chunk
        start, end = chunk.span
        assert doc.text[start:end] == chunk.text, (chunk.span, chunk.text)
    spans = [c.span for c in doc.chunks]
    assert spans == sorted(spans)


def test_pdf_page_spans_leave_only_the_page_separator_between(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pypdf

    from okg.substrate.library.extraction.parse.pdf_text import PdfTextParser

    pages = ["Title page\nhttps://x/y", "", "Body \x00text\x0b é", "End\n"]
    fake = SimpleNamespace(
        pages=[SimpleNamespace(extract_text=lambda t=t: t) for t in pages],
        metadata=None,
    )
    monkeypatch.setattr(pypdf, "PdfReader", lambda _buf: fake)
    doc = PdfTextParser().parse(b"%PDF-1.4 fake")
    _assert_spans_locate_chunks(doc)
    assert doc.text == "Title page\nhttps://x/y\n\n\n\nBody text é\n\nEnd\n"
    covered = "".join(
        doc.text[a:b] for a, b in zip([0] + [c.span[1] for c in doc.chunks],
                                      [c.span[0] for c in doc.chunks] + [len(doc.text)])
    )
    # What lies between the page spans is the page separator and nothing else.
    assert covered.strip("\n") == ""
    assert doc.chunks[2].span == (26, 37)


def test_docx_paragraph_spans_skip_headings() -> None:
    docx = pytest.importorskip("docx")
    from okg.substrate.library.extraction.parse.docx import DocxParser

    document = docx.Document()
    document.add_heading("Intro", level=1)
    document.add_paragraph("First body paragraph.")
    document.add_paragraph("   ")
    document.add_paragraph("Second, with é.")
    document.add_heading("Tail heading", level=2)
    buf = io.BytesIO()
    document.save(buf)
    doc = DocxParser().parse(buf.getvalue())
    _assert_spans_locate_chunks(doc)
    assert doc.text == "Intro\nFirst body paragraph.\nSecond, with é.\nTail heading"
    assert [c.span for c in doc.chunks] == [(6, 27), (28, 43)]


def test_xlsx_sheet_spans_including_an_empty_sheet() -> None:
    openpyxl = pytest.importorskip("openpyxl")
    from okg.substrate.library.extraction.parse.xlsx import XlsxParser

    wb = openpyxl.Workbook()
    first = wb.active
    first.title = "one"
    first.append(["a", 1])
    first.append(["b", None])
    wb.create_sheet("empty")
    third = wb.create_sheet("three")
    third.append(["z"])
    buf = io.BytesIO()
    wb.save(buf)
    doc = XlsxParser().parse(buf.getvalue())
    _assert_spans_locate_chunks(doc)
    assert doc.text == "a\t1\nb\t\nz"
    assert [c.span for c in doc.chunks] == [(0, 6), (6, 6), (7, 8)]


def test_notebook_cell_spans_sit_inside_the_code_fence() -> None:
    nbformat = pytest.importorskip("nbformat")
    from okg.substrate.library.extraction.parse.notebook import NotebookParser

    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_markdown_cell("# title"))
    nb.cells.append(nbformat.v4.new_code_cell("print(1)"))
    nb.cells.append(nbformat.v4.new_markdown_cell("done"))
    doc = NotebookParser().parse(nbformat.writes(nb))
    _assert_spans_locate_chunks(doc)
    assert doc.text == "# title\n\n```python\nprint(1)\n```\n\ndone"
    assert [c.span for c in doc.chunks] == [(0, 7), (19, 27), (33, 37)]


def test_semantic_chunker_spans_are_character_offsets() -> None:
    pytest.importorskip("semantic_text_splitter")
    from okg.substrate.library.extraction.chunk.semantic import SemanticChunker

    text = "".join(
        f"Größe {i}: naïve café résumé — ünïcödé sentence number {i}.\n\n"
        for i in range(40)
    )
    chunker = SemanticChunker(max_chars=200, overlap=40)
    chunks = chunker.chunk(text)
    assert len(chunks) > 5
    for chunk in chunks:
        assert chunk.span is not None
        assert text[chunk.span[0]:chunk.span[1]] == chunk.text
    assert chunker.last_warnings == []


def test_semantic_chunker_keeps_a_piece_it_cannot_place_without_a_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("semantic_text_splitter")
    from okg.substrate.library.extraction.chunk.semantic import SemanticChunker

    chunker = SemanticChunker(max_chars=50)

    class _Wrong:
        def chunk_indices(self, text: str) -> list[tuple[int, str]]:
            return [(0, "alpha"), (3, "gamma")]

    monkeypatch.setattr(chunker, "_get_splitter", lambda: _Wrong())
    chunks = chunker.chunk("alpha gamma")
    assert [c.span for c in chunks] == [(0, 5), None]
    assert chunker.last_warnings == ["semantic_chunk_span_mismatch:3"]
