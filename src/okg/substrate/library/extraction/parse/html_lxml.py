"""HTML parser. Wraps `lxml` only.

Provides DOM walking + XPath; produces heading-pathed chunks for every
block of body text and extracts `<title>` into metadata. Companion module
`parse/html_fast.py` (selectolax) ships a CSS-selector-only backend for
callers who need raw speed and don't need lxml's DOM ops.

Chunks, in document order (`<script>`, `<style>`, `<noscript>` and
`<head>` never become chunks):

- `h1`-`h6` -> `heading`; the heading also starts a new heading path.
- `li`, `dt`, `dd` -> `list_item`.
- `pre` (and the obsolete `listing` / `xmp` / `plaintext`) -> `code_block`,
  whitespace kept as written.
- A table whose text all sits in its caption and cells, and whose cells
  hold only inline text -> one `table` chunk: the caption line, then one
  line per row with cells separated by a tab. Any other table (a layout
  table, or one with text between its rows or cells) is walked like any
  other container, each row and cell being a block.
- Every other block (`p`, `div`, `blockquote`, `td`, ...) and text sitting
  directly in `body` -> `paragraph`.

Text belongs to the innermost block around it, so nested blocks never
repeat their text in their parent: `<li>a<ul><li>b</li></ul>c</li>` gives
the chunks `a`, `b`, `c`. A block's text is its text nodes joined as
`text_content()` joins them, stripped, so a `<p>` chunk is the same string
the parser produced when it chunked only paragraphs.

Format key: `"html"`.
"""

from __future__ import annotations

import re
from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser

_SKIPPED_TAGS = ("script", "style", "noscript")
_HEADING_TAGS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})
_LIST_ITEM_TAGS = frozenset({"li", "dt", "dd"})
_PREFORMATTED_TAGS = frozenset({"pre", "listing", "xmp", "plaintext"})
_TABLE_PART_TAGS = frozenset({
    "caption", "colgroup", "col", "thead", "tbody", "tfoot", "tr", "td", "th",
})
# Elements whose text is a block of its own: text inside one never joins the
# text around it. Everything else (a, span, code, em, br, ...) is inline.
_BLOCK_TAGS = frozenset({
    "html", "body", "address", "article", "aside", "blockquote", "center",
    "details", "dialog", "dir", "div", "dl", "fieldset", "figcaption",
    "figure", "footer", "form", "frameset", "header", "hgroup", "hr", "legend",
    "main", "menu", "nav", "noframes", "ol", "p", "search", "section",
    "summary", "table", "ul",
}) | _HEADING_TAGS | _LIST_ITEM_TAGS | _PREFORMATTED_TAGS | _TABLE_PART_TAGS
_WHITESPACE_RE = re.compile(r"\s+")


def _tag(el: Any) -> str | None:
    """Lower-case tag name, or None for comments and processing
    instructions (whose `tag` is not a string)."""
    tag = el.tag
    return tag.lower() if isinstance(tag, str) else None


def _kind_for(tag: str) -> str:
    if tag in _HEADING_TAGS:
        return "heading"
    if tag in _LIST_ITEM_TAGS:
        return "list_item"
    if tag in _PREFORMATTED_TAGS:
        return "code_block"
    return "paragraph"


_CELL_TAGS = frozenset({"caption", "td", "th"})


def _has_text_outside_cells(el: Any) -> bool:
    """True when `el` (the table or a row group, row, or column group
    inside it) holds text that is in no caption or cell: `_table_text`
    reads only captions and cells, so such text would be lost."""
    if (el.text or "").strip():
        return True
    for child in el:
        if (child.tail or "").strip():
            return True
        tag = _tag(child)
        if tag is None or tag in _CELL_TAGS:
            continue
        if tag in _TABLE_PART_TAGS:
            if _has_text_outside_cells(child):
                return True
        elif (child.text_content() or "").strip():
            return True
    return False


def _is_simple_table(table: Any) -> bool:
    """True when every element inside the table is a table part or
    inline, and all its text sits in a caption or a cell: the table is
    data, and its text reads as one grid."""
    for el in table.iterdescendants():
        tag = _tag(el)
        if tag is None or tag in _TABLE_PART_TAGS:
            continue
        if tag in _BLOCK_TAGS:
            return False
    return not _has_text_outside_cells(table)


def _table_text(table: Any) -> str:
    """Caption, then one line per row; cells joined by a tab, each cell's
    whitespace runs collapsed to one space. Rows with no text are left out."""
    lines: list[str] = []
    for el in table.iterdescendants():
        tag = _tag(el)
        if tag == "caption":
            caption = _WHITESPACE_RE.sub(" ", el.text_content()).strip()
            if caption:
                lines.append(caption)
        elif tag == "tr":
            cells = [
                _WHITESPACE_RE.sub(" ", cell.text_content()).strip()
                for cell in el
                if _tag(cell) in {"td", "th"}
            ]
            if any(cells):
                lines.append("\t".join(cells))
    return "\n".join(lines)


class _BlockWalker:
    """One pass over the element tree that turns each block's own text
    into a chunk, in document order."""

    def __init__(self) -> None:
        self.chunks: list[ParsedChunk] = []
        self.heading_path: list[str] = []

    def _emit(self, text: str, kind: str) -> None:
        if not text.strip():
            return
        if kind == "code_block":
            # Keep the first line's indentation and every inner line; drop
            # only the blank lines and trailing spaces around the block.
            text = text.strip("\r\n").rstrip()
        else:
            text = text.strip()
        self.chunks.append(ParsedChunk(
            text=text,
            heading_path=list(self.heading_path),
            chunk_kind=kind,
        ))

    def _heading(self, el: Any, tag: str) -> None:
        level = int(tag[1])
        self.heading_path = self.heading_path[: level - 1]
        self.heading_path.append((el.text_content() or "").strip())
        self._emit(el.text_content() or "", "heading")

    def walk_block(self, el: Any, tag: str) -> None:
        """Emit the chunks of one block element and everything inside it.
        The element's tail belongs to the enclosing block; the caller
        handles it."""
        if tag in _HEADING_TAGS:
            self._heading(el, tag)
            return
        if tag in _PREFORMATTED_TAGS:
            self._emit(el.text_content() or "", "code_block")
            return
        if tag == "table" and _is_simple_table(el):
            self._emit(_table_text(el), "table")
            return
        kind = _kind_for(tag)
        buffer: list[str] = [el.text or ""]
        self._walk_children(el, buffer, kind)
        self._emit("".join(buffer), kind)

    def _walk_children(self, el: Any, buffer: list[str], kind: str) -> None:
        """Append the inline text under `el` to `buffer`; a nested block
        closes the text gathered so far as one chunk, emits its own chunks,
        and the text after it starts a new chunk of the same block."""
        for child in el:
            tag = _tag(child)
            if tag == "head":
                pass
            elif tag is None:
                # Comment or processing instruction: its own text is not
                # document text, its tail is.
                pass
            elif tag in _BLOCK_TAGS:
                self._emit("".join(buffer), kind)
                buffer.clear()
                self.walk_block(child, tag)
            else:
                buffer.append(child.text or "")
                self._walk_children(child, buffer, kind)
            buffer.append(child.tail or "")


class HtmlLxmlParser:
    """HTML -> ParsedDocument via lxml."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, (bytes, bytearray)):
            raise TypeError(
                f"HtmlLxmlParser.parse expected bytes/str; got "
                f"{type(content).__name__}"
            )

        from lxml import html as _html

        if not content:
            return ParsedDocument(text="", metadata={"empty": True})

        tree = _html.fromstring(content)
        for tag in _SKIPPED_TAGS:
            for el in list(tree.iter(tag)):
                el.drop_tree()

        title_els = tree.xpath("//title")
        title = title_els[0].text_content().strip() if title_els else None

        walker = _BlockWalker()
        root_tag = _tag(tree) or "div"
        # `fromstring` on a fragment returns the fragment's own element,
        # which may be inline (`<span>`); its text is still one block.
        walker.walk_block(tree, root_tag if root_tag in _BLOCK_TAGS else "div")
        chunks = walker.chunks

        text = tree.text_content() if tree is not None else ""
        text = _WHITESPACE_RE.sub(" ", text).strip()

        metadata: dict[str, Any] = {}
        if title is not None:
            metadata["title"] = title

        return ParsedDocument(text=text, metadata=metadata, chunks=chunks)


register_parser("html", HtmlLxmlParser)


__all__ = ["HtmlLxmlParser"]
