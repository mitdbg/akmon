"""Bounded budgets for PDF parsing (constrain-graph-file-and-parser-ingestion).

PDF backends (pypdf / pdfplumber / pypdfium2) otherwise iterate every page of an
attacker- or source-supplied PDF with no byte/page ceiling, so an oversized or
object-bomb PDF can exhaust CPU/memory inside a publish cycle. These caps are
generous (real corpora have large multi-hundred-page PDFs) and config-overridable
per parse call; exceeding a cap yields a contained, bounded result rather than
unbounded work.
"""
from __future__ import annotations

#: Default ceiling on a single PDF input (128 MiB).
DEFAULT_MAX_PDF_BYTES = 128 * 1024 * 1024
#: Default ceiling on pages iterated for a single PDF.
DEFAULT_MAX_PDF_PAGES = 5000


def pdf_bytes_within_budget(
    size_bytes: int, max_bytes: int = DEFAULT_MAX_PDF_BYTES,
) -> bool:
    return size_bytes <= max_bytes


def capped_page_count(
    total_pages: int, max_pages: int = DEFAULT_MAX_PDF_PAGES,
) -> tuple[int, bool]:
    """Return (pages_to_parse, truncated)."""
    if total_pages > max_pages:
        return max_pages, True
    return total_pages, False
