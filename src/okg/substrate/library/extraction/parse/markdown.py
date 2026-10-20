"""Markdown parser. Wraps `markdown-it-py` only.

YAML frontmatter handling lives in the companion module
`parse/frontmatter.py` (wraps `python-frontmatter`). For documents
that carry frontmatter, callers extract metadata via the
frontmatter parser first, then pass the body content to this
parser. The wrap invariant (≤1 third-party top-level package per
backend module) holds structurally.

Format keys: `"markdown"`, `"md"`.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class MarkdownParser:
    """CommonMark Markdown -> ParsedDocument via markdown-it-py.

    Heading-pathed paragraph chunks; no frontmatter handling.
    """

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")
        if not isinstance(content, str):
            raise TypeError(
                f"MarkdownParser.parse expected str/bytes; got "
                f"{type(content).__name__}"
            )

        from markdown_it import MarkdownIt
        md = MarkdownIt("commonmark")
        tokens = md.parse(content)

        chunks: list[ParsedChunk] = []
        heading_path: list[str] = []
        i = 0
        para_buf: list[str] = []
        # Stride A.1 (2026-04-26): flush per paragraph_close
        # (and for fenced/inline code blocks as their own
        # chunks) so chunk identity matches author-visible
        # paragraph boundaries. Pre-fix every paragraph under
        # the same heading collapsed into ONE chunk, which
        # both inflated chunk size beyond chunker budgets AND
        # made content-hash dedup miss intra-section reuse.
        in_paragraph = False

        def _flush() -> None:
            nonlocal para_buf
            if para_buf:
                joined = " ".join(para_buf).strip()
                if joined:
                    chunks.append(ParsedChunk(
                        text=joined,
                        heading_path=list(heading_path),
                    ))
            para_buf = []

        while i < len(tokens):
            tok = tokens[i]
            if tok.type == "heading_open":
                _flush()
                level = int(tok.tag[1])
                inline = tokens[i + 1] if i + 1 < len(tokens) else None
                heading_text = (inline.content if inline else "").strip()
                heading_path = heading_path[: level - 1]
                heading_path.append(heading_text)
                i += 3
                continue
            if tok.type == "paragraph_open":
                _flush()
                in_paragraph = True
                i += 1
                continue
            if tok.type == "paragraph_close":
                _flush()
                in_paragraph = False
                i += 1
                continue
            if tok.type in ("fence", "code_block"):
                # Standalone code block — its own chunk.
                _flush()
                code_text = (tok.content or "").rstrip()
                if code_text:
                    chunks.append(ParsedChunk(
                        text=code_text,
                        heading_path=list(heading_path),
                        chunk_kind="code_block",
                    ))
                i += 1
                continue
            if tok.type == "inline" and in_paragraph:
                para_buf.append(tok.content)
            i += 1
        _flush()

        return ParsedDocument(text=content, metadata={}, chunks=chunks)


register_parser("markdown", MarkdownParser)
register_parser("md", MarkdownParser)


__all__ = ["MarkdownParser"]
