"""Bounded PDF parse budgets (constrain-graph-file-and-parser-ingestion).

Each PDF parser must refuse an over-cap input with a bounded, contained result
rather than iterating every page / reading unbounded bytes.
"""
from __future__ import annotations

from io import BytesIO

import pytest

from okg.substrate.library.enrichers.scholarly.pdf_structural import (
    default_pypdfium2_parser,
)
from okg.substrate.library.extraction.parse.pdf_tables import PdfTablesParser
from okg.substrate.library.extraction.parse.pdf_text import PdfTextParser


def _make_pdf(pages: int) -> bytes:
    pypdf = pytest.importorskip("pypdf")
    writer = pypdf.PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=72, height=72)
    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()


# ---- byte caps (no valid PDF needed; the cap is checked first) ----


def test_pdf_text_parser_rejects_oversized_bytes():
    doc = PdfTextParser().parse(b"x" * 100, max_pdf_bytes=10)
    assert doc.metadata.get("parse_error") == "pdf_too_large"
    assert doc.text == ""
    assert doc.chunks == []


def test_pdf_tables_parser_rejects_oversized_bytes():
    doc = PdfTablesParser().parse(b"x" * 100, max_pdf_bytes=10)
    assert doc.metadata.get("parse_error") == "pdf_too_large"


def test_pypdfium2_parser_rejects_oversized_file(tmp_path):
    big = tmp_path / "big.pdf"
    big.write_bytes(b"x" * 100)
    result = default_pypdfium2_parser(
        pdf_path=big, paper_id="paper:x", max_bytes=10,
    )
    assert result.reason == "pdf_too_large"
    assert result.confidence == 0.0


# ---- page caps ----


def test_pdf_text_parser_caps_page_iteration():
    content = _make_pdf(5)
    doc = PdfTextParser().parse(content, max_pdf_pages=2)
    assert doc.metadata["pages"] == 5          # total reported
    assert doc.metadata["pages_parsed"] == 2   # but only 2 iterated
    assert doc.metadata.get("truncated") is True
    assert len(doc.chunks) == 2


def test_pdf_text_parser_within_budget_parses_all_pages():
    content = _make_pdf(3)
    doc = PdfTextParser().parse(content)  # generous defaults
    assert doc.metadata["pages"] == 3
    assert doc.metadata["pages_parsed"] == 3
    assert "truncated" not in doc.metadata
    assert len(doc.chunks) == 3
