"""Plain-text parser. Wraps stdlib only (no third-party deps).

Decodes bytes via UTF-8 (errors='replace' for unrecoverable),
applies `unicode_clean(NFC)` for canonical form, returns a
`ParsedDocument` with the cleaned text and content_sha256 in
metadata.

Format keys: `text`, `txt`, `rst`. (Restructured-text gets the
plain-text path; structural parsing for RST is out of scope.)

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations

import hashlib
from typing import Any

from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.normalize import unicode_clean
from okg.substrate.library.extraction.parse.dispatch import register_parser


class TextParser:
    """UTF-8 + Unicode-clean. Single chunk = whole document."""

    def parse(
        self, content: bytes | str, **_: Any,
    ) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            byte_count = len(content)
            sha = hashlib.sha256(bytes(content)).hexdigest()
            text = content.decode("utf-8", errors="replace")
        elif isinstance(content, str):
            encoded = content.encode("utf-8")
            byte_count = len(encoded)
            sha = hashlib.sha256(encoded).hexdigest()
            text = content
        else:
            raise TypeError(
                f"TextParser.parse expected str/bytes; got "
                f"{type(content).__name__}"
            )
        cleaned = unicode_clean(text)
        return ParsedDocument(
            text=cleaned,
            metadata={
                "format": "text",
                "byte_count": byte_count,
                "content_sha256": sha,
            },
            chunks=[],
        )


register_parser("text", TextParser)
register_parser("txt", TextParser)
register_parser("rst", TextParser)


__all__ = ["TextParser"]
