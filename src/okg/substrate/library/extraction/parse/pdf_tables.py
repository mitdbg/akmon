"""PDF table extractor. Wraps `pdfplumber` only.

PDF text + metadata extraction lives in `parse/pdf_text.py` (pypdf).
Splitting them satisfies the wrap invariant: ≤1 third-party
top-level package per backend module.

Format key: `"pdf_tables"`. The parser returns a ParsedDocument
whose `metadata["tables"]` is a list of tables (each table is a
list of rows; each row is a list of cell strings). Top-level
`text` is the tab-joined dump for callers who want a flat string.
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
    ParsedDocument,
)
from okg.substrate.library.extraction.pdf_table_vlm import (
    TableRegion,
    TableRegionCandidate,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class PdfTablesParser:
    """PDF -> ParsedDocument via pdfplumber (tables only)."""

    def parse(self, content: bytes | str, **kwargs: Any) -> ParsedDocument:
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, (bytes, bytearray)):
            raise TypeError(
                f"PdfTablesParser.parse expected bytes; got {type(content).__name__}"
            )
        max_bytes = int(kwargs.get("max_pdf_bytes") or DEFAULT_MAX_PDF_BYTES)
        max_pages = int(kwargs.get("max_pdf_pages") or DEFAULT_MAX_PDF_PAGES)
        if not pdf_bytes_within_budget(len(content), max_bytes):
            return ParsedDocument(
                text="",
                metadata={
                    "parse_error": "pdf_too_large",
                    "size_bytes": len(content),
                    "max_bytes": max_bytes,
                    "tables": [],
                    "low_confidence": True,
                },
            )

        import pdfplumber

        tables: list[list[list[str]]] = []
        page_count = 0
        truncated = False
        with pdfplumber.open(BytesIO(content)) as pdf:
            page_count = len(pdf.pages)
            pages_to_parse, truncated = capped_page_count(page_count, max_pages)
            for page in pdf.pages[:pages_to_parse]:
                page_tables = page.extract_tables() or []
                if page_tables:
                    tables.extend(page_tables)

        flat_lines: list[str] = []
        for tbl in tables:
            for row in tbl:
                flat_lines.append("\t".join("" if c is None else str(c) for c in row))

        metadata: dict[str, Any] = {"tables": tables, "pages": page_count}
        if truncated:
            metadata["truncated"] = True
        return ParsedDocument(
            text="\n".join(flat_lines),
            metadata=metadata,
        )


class PdfPlumberTableRegionLocator:
    """Locate table bboxes without trusting geometric cell reconstruction."""

    name = "pdfplumber_find_tables"
    version = "v1"

    def locate(self, pdf_bytes: bytes, *, max_pages: int) -> list[TableRegionCandidate]:
        import pdfplumber

        candidates: list[TableRegionCandidate] = []
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page_index, page in enumerate(pdf.pages[:max_pages]):
                for table in page.find_tables() or ():
                    candidates.append(
                        TableRegionCandidate(
                            page_index=page_index,
                            bbox=tuple(table.bbox),
                            page_width=page.width,
                            page_height=page.height,
                            detector_name=self.name,
                            detector_version=self.version,
                        )
                    )
        return candidates


class PdfPlumberTableCropRenderer:
    """Render one accepted pdfplumber crop to PNG bytes."""

    name = "pdfplumber_page_image"
    version = "v1"

    def render_crop(self, pdf_bytes: bytes, region: TableRegion, *, dpi: int) -> bytes:
        import pdfplumber

        output = BytesIO()
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            page = pdf.pages[region.page_index]
            cropped = page.crop(tuple(float(value) for value in region.bbox))
            image = cropped.to_image(resolution=dpi, antialias=True)
            image.original.save(output, format="PNG")
        return output.getvalue()


register_parser("pdf_tables", PdfTablesParser)


__all__ = [
    "PdfPlumberTableCropRenderer",
    "PdfPlumberTableRegionLocator",
    "PdfTablesParser",
]
