"""Strict, source-preserving LaTeX parser.

The parser decodes UTF-8 without replacement, retains the complete raw source,
and delegates bounded structural discovery to ``LatexStructuralChunker``. The
table lane expands only locally-scanned macro definitions under explicit
budgets; it does not compile TeX or follow filesystem references.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


_UNSUPPORTED_C0_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


class LatexControlCharacterError(ValueError):
    """LaTeX source contains a control character unsafe for graph storage."""


class LatexParser:
    """Parse one LaTeX source document while preserving authoritative spans."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            raw_bytes = bytes(content)
            text = raw_bytes.decode("utf-8", errors="strict")
        elif isinstance(content, str):
            text = content
            raw_bytes = text.encode("utf-8")
        else:
            raise TypeError(
                f"LatexParser.parse expected str/bytes; got {type(content).__name__}"
            )
        match = _UNSUPPORTED_C0_RE.search(text)
        if match is not None:
            raise LatexControlCharacterError(
                f"unsupported C0 control character at offset {match.start()}"
            )

        chunker = LatexStructuralChunker()
        chunks = chunker.chunk(text)
        return ParsedDocument(
            text=text,
            metadata={
                "format": "latex",
                "byte_count": len(raw_bytes),
                "content_sha256": hashlib.sha256(raw_bytes).hexdigest(),
                "parse_warnings": list(chunker.last_warnings),
                "macro_definition_spans": [
                    list(definition.span)
                    for definition in chunker.last_macro_definitions
                ],
            },
            chunks=chunks,
        )


register_parser("latex", LatexParser)
register_parser("tex", LatexParser)


__all__ = ["LatexControlCharacterError", "LatexParser"]
