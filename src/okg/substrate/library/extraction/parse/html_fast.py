"""HTML parser (fast). Wraps `selectolax` only.

Modest's C backend; faster than lxml for body-text + CSS-selector
queries on large HTML payloads. Does NOT do XPath; deployments
that need XPath use `parse/html_lxml.py` instead.

Format key: `"html_fast"`. Caller picks explicitly — no auto-
sniff that picks between lxml and selectolax.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class HtmlFastParser:
    """HTML -> ParsedDocument via selectolax (CSS-selector body
    extraction; no XPath)."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, (bytes, bytearray)):
            raise TypeError(
                f"HtmlFastParser.parse expected bytes/str; got "
                f"{type(content).__name__}"
            )
        if not content:
            return ParsedDocument(text="", metadata={"empty": True})

        from selectolax.parser import HTMLParser

        tree = HTMLParser(content)
        for selector in ("script", "style", "noscript"):
            for node in tree.css(selector):
                node.decompose()

        title_node = tree.css_first("title")
        title = title_node.text(strip=True) if title_node else None

        chunks: list[ParsedChunk] = []
        for node in tree.css("p"):
            txt = node.text(strip=True)
            if txt:
                chunks.append(ParsedChunk(text=txt))

        body = tree.css_first("body")
        text = body.text(strip=True) if body else tree.text(strip=True)
        import re as _re
        text = _re.sub(r"\s+", " ", text or "").strip()

        metadata: dict[str, Any] = {}
        if title is not None:
            metadata["title"] = title

        return ParsedDocument(text=text, metadata=metadata, chunks=chunks)


register_parser("html_fast", HtmlFastParser)


__all__ = ["HtmlFastParser"]
