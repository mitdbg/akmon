"""YAML-frontmatter parser. Wraps `python-frontmatter` only.

Splits a leading YAML frontmatter block from the body. Callers
typically run this before handing the body to a Markdown / HTML
parser. The wrap invariant (≤1 third-party top-level package per
backend module) is satisfied.

Format key: `"frontmatter"`. The returned ParsedDocument carries
the frontmatter dict in `metadata` and the body in `text`.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class FrontmatterParser:
    """text-with-leading-frontmatter -> ParsedDocument via
    python-frontmatter."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")
        if not isinstance(content, str):
            raise TypeError(
                f"FrontmatterParser.parse expected str/bytes; got "
                f"{type(content).__name__}"
            )

        import frontmatter as _frontmatter
        post = _frontmatter.loads(content)
        return ParsedDocument(
            text=post.content,
            metadata=dict(post.metadata),
        )


register_parser("frontmatter", FrontmatterParser)


__all__ = ["FrontmatterParser"]
