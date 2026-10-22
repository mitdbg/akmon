"""PDF text + metadata parser. Wraps `pypdf` only.

PDF table extraction lives in a separate module
(`parse/pdf_tables.py`, wrapping `pdfplumber`) so the wrap
invariant — ≤1 third-party top-level package per backend module —
holds structurally.

Format key: `"pdf"`.
"""

from __future__ import annotations

import contextlib
import logging
import threading
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


_MAX_EXAMPLE_WARNINGS = 3
_pypdf_capture_lock = threading.Lock()
_pypdf_capture_depth = 0
_pypdf_was_propagating = True


class _PypdfWarnings(logging.Handler):
    """Collects what pypdf logs while one document is read, in this thread.

    pypdf reports a font it cannot fully decode through the `pypdf` logger,
    once per font per page. A physics paper triggers hundreds, which would
    flood the console of `okg deployment add-source`. They are counted here,
    with the first few distinct messages kept, and summarised once on the
    document instead.
    """

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.thread_id = threading.get_ident()
        self.count = 0
        self.examples: list[str] = []
        self.fonttools_missing = False

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self.thread_id:
            return  # another document's parse, in another thread
        self.count += 1
        message = record.getMessage()
        if "fontTools" in message:
            self.fonttools_missing = True
        message = message if len(message) <= 160 else message[:157] + "..."
        if message not in self.examples and len(self.examples) < _MAX_EXAMPLE_WARNINGS:
            self.examples.append(message)


@contextlib.contextmanager
def _capture_pypdf_logs():
    """Attach a collector to the pypdf logger and keep its records off the
    console. Concurrent parses share one propagate switch: the first turns it
    off, the last restores it, under a lock."""
    global _pypdf_capture_depth, _pypdf_was_propagating
    collected = _PypdfWarnings()
    pypdf_logger = logging.getLogger("pypdf")
    with _pypdf_capture_lock:
        if _pypdf_capture_depth == 0:
            _pypdf_was_propagating = pypdf_logger.propagate
            pypdf_logger.propagate = False
        _pypdf_capture_depth += 1
        pypdf_logger.addHandler(collected)
    try:
        yield collected
    finally:
        with _pypdf_capture_lock:
            pypdf_logger.removeHandler(collected)
            _pypdf_capture_depth -= 1
            if _pypdf_capture_depth == 0:
                pypdf_logger.propagate = _pypdf_was_propagating


class PdfTextParser:
    """PDF -> ParsedDocument via pypdf (text + metadata).

    Connectors may call this directly: `PdfTextParser().parse(pdf_bytes)`
    returns the text, one chunk per page, and `metadata["parse_warnings"]`.
    """

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
        with _capture_pypdf_logs() as collected:
            return self._parse_pdf(pypdf, content, max_pages, collected)

    def _parse_pdf(
        self, pypdf: Any, content: bytes, max_pages: int, collected: _PypdfWarnings,
    ) -> ParsedDocument:
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
        parse_warnings: list[str] = []
        if pages_to_parse and not text.strip():
            # A scan: pages parse but carry no text layer. Say so, so the
            # record is not silently empty.
            parse_warnings.append("pdf_no_text_layer")
        if collected.count:
            # pypdf could not decode something exactly (usually a font), so
            # some words in the text may be garbled or split. Say how many
            # times, once, and keep the first few distinct messages.
            parse_warnings.append(f"pdf_reader_warnings:{collected.count}")
            if collected.fonttools_missing:
                parse_warnings.append("pdf_font_encoding_incomplete:fonttools_missing")
            parse_warnings.extend(
                f"pdf_reader_warning_example:{m}" for m in collected.examples
            )
        if parse_warnings:
            metadata["parse_warnings"] = parse_warnings

        return ParsedDocument(text=text, metadata=metadata, chunks=chunks)


register_parser("pdf", PdfTextParser)


__all__ = ["PdfTextParser"]
