"""Markdown structural chunker. Wraps `markdown-it-py` only.

HTML structural chunking lives in
`chunk/structural_html.py` (lxml). The wrap invariant holds:
≤1 third-party top-level package per backend module.

Each chunk carries its `heading_path` so downstream code can
reason about section context.
"""

from __future__ import annotations

from okg.substrate.library.extraction._protocols import ParsedChunk


class MarkdownStructuralChunker:
    """Heading-bounded chunks over a Markdown body, via
    markdown-it-py."""

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text:
            return []
        from markdown_it import MarkdownIt
        md = MarkdownIt("commonmark")
        tokens = md.parse(text)

        chunks: list[ParsedChunk] = []
        heading_path: list[str] = []
        para_buf: list[str] = []

        def _flush() -> None:
            nonlocal para_buf
            joined = "\n".join(para_buf).strip()
            if joined:
                chunks.append(ParsedChunk(
                    text=joined,
                    heading_path=list(heading_path),
                ))
            para_buf = []

        i = 0
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
            if tok.type == "inline":
                para_buf.append(tok.content)
            i += 1
        _flush()
        return chunks


__all__ = ["MarkdownStructuralChunker"]
