"""HTML structural chunker. Wraps `lxml` only.

Markdown structural chunking lives in
`chunk/structural_markdown.py` (markdown-it-py). The wrap
invariant holds: ≤1 third-party top-level package per backend
module.

Each chunk carries its `heading_path` so downstream code can
reason about section context.
"""

from __future__ import annotations

from okg.substrate.library.extraction._protocols import ParsedChunk


class HtmlStructuralChunker:
    """Heading-bounded chunks over an HTML body, via lxml."""

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text:
            return []
        from lxml import html as _html
        tree = _html.fromstring(text.encode("utf-8"))
        for tag in ("script", "style", "noscript"):
            for el in tree.iter(tag):
                el.drop_tree()
        chunks: list[ParsedChunk] = []
        heading_path: list[str] = []
        for el in tree.iter():
            tag = el.tag if isinstance(el.tag, str) else None
            if tag and tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                level = int(tag[1])
                heading_path = heading_path[: level - 1]
                heading_path.append((el.text_content() or "").strip())
            elif tag == "p":
                txt = (el.text_content() or "").strip()
                if txt:
                    chunks.append(ParsedChunk(
                        text=txt,
                        heading_path=list(heading_path),
                    ))
        return chunks


__all__ = ["HtmlStructuralChunker"]
