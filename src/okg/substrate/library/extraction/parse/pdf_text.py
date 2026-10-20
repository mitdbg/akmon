"""PDF text + metadata parser. Wraps `pypdf` only.

PDF table extraction lives in a separate module
(`parse/pdf_tables.py`, wrapping `pdfplumber`) so the wrap
invariant — ≤1 third-party top-level package per backend module —
holds structurally.

Format key: `"pdf"`.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any

from okg.substrate.library.extraction._pdf_budget import (
    DEFAULT_MAX_PDF_BYTES,
    DEFAULT_MAX_PDF_PAGES,
    capped_page_count,
    pdf_bytes_within_budget,
)
from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
    _strip_control_chars,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


_PAGE_SEPARATOR = "\n\n"


class PdfTextParser:
    """PDF -> ParsedDocument via pypdf (text + metadata)."""

    def parse(self, content: bytes | str, **kwargs: Any) -> ParsedDocument:
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, (bytes, bytearray)):
            raise TypeError(
                f"PdfTextParser.parse expected bytes; got {type(content).__name__}"
            )
        max_bytes = int(kwargs.get("max_pdf_bytes") or DEFAULT_MAX_PDF_BYTES)
        max_pages = int(kwargs.get("max_pdf_pages") or DEFAULT_MAX_PDF_PAGES)
        if not pdf_bytes_within_budget(len(content), max_bytes):
            # Bounded failure: do not parse an over-cap input.
            return ParsedDocument(
                text="",
                metadata={
                    "parse_error": "pdf_too_large",
                    "size_bytes": len(content),
                    "max_bytes": max_bytes,
                    "low_confidence": True,
                },
                chunks=[],
            )

        import pypdf
        reader = pypdf.PdfReader(BytesIO(content))
        total_pages = len(reader.pages)
        pages_to_parse, truncated = capped_page_count(total_pages, max_pages)
        meta = reader.metadata or {}
        metadata: dict[str, Any] = {
            "title": meta.get("/Title"),
            "author": meta.get("/Author"),
            "subject": meta.get("/Subject"),
            "creator": meta.get("/Creator"),
            "producer": meta.get("/Producer"),
            "pages": total_pages,
            "pages_parsed": pages_to_parse,
        }
        if truncated:
            metadata["truncated"] = True
        metadata = {k: v for k, v in metadata.items() if v is not None}

        # Each page chunk carries its real span in `text` (pages joined by
        # `_PAGE_SEPARATOR`), so a whole read can rejoin the pages exactly.
        # Control characters are stripped first (ParsedChunk and
        # ParsedDocument strip them anyway; NUL is one), so each span is
        # measured on the exact string the document text is built from.
        chunks: list[ParsedChunk] = []
        page_texts: list[str] = []
        position = 0
        for i in range(pages_to_parse):
            if i:
                position += len(_PAGE_SEPARATOR)
            page_text = _strip_control_chars(reader.pages[i].extract_text() or "")
            chunks.append(ParsedChunk(
                text=page_text,
                heading_path=[f"page {i + 1}"],
                span=(position, position + len(page_text)),
            ))
            page_texts.append(page_text)
            position += len(page_text)
        text = _PAGE_SEPARATOR.join(page_texts)

        return ParsedDocument(text=text, metadata=metadata, chunks=chunks)


register_parser("pdf", PdfTextParser)


__all__ = ["PdfTextParser"]
