"""Jupyter notebook parser. Wraps `nbformat`."""

from __future__ import annotations

from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
    _strip_control_chars,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser

_CELL_SEPARATOR = "\n\n"
_CODE_FENCE_OPEN = "```python\n"


class NotebookParser:
    """`.ipynb` -> ParsedDocument via nbformat. Code + markdown
    cells become structural chunks; metadata captures kernel +
    cell counts."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")

        import nbformat
        nb = nbformat.reads(content, as_version=4)

        cells = nb.get("cells", [])
        kernel = (
            nb.get("metadata", {})
            .get("kernelspec", {})
            .get("name")
        )

        # `text` is the cells joined by a blank line, code cells wrapped in a
        # fence; each cell chunk (the raw source, no fence) carries its span
        # in it. The fences are not chunk text, so a whole read of a notebook
        # with code cells says partial rather than complete.
        text_parts: list[str] = []
        chunks: list[ParsedChunk] = []
        n_code = 0
        n_markdown = 0
        position = 0
        for i, cell in enumerate(cells):
            cell_text = cell.get("source", "")
            if isinstance(cell_text, list):
                cell_text = "".join(cell_text)
            cell_text = _strip_control_chars(cell_text)
            cell_type = cell.get("cell_type", "code")
            prefix = ""
            if cell_type == "code":
                n_code += 1
                prefix = _CODE_FENCE_OPEN
                tagged = f"{prefix}{cell_text}\n```"
            elif cell_type == "markdown":
                n_markdown += 1
                tagged = cell_text
            else:
                tagged = cell_text
            if text_parts:
                position += len(_CELL_SEPARATOR)
            start = position + len(prefix)
            text_parts.append(tagged)
            chunks.append(ParsedChunk(
                text=cell_text,
                heading_path=[f"cell {i + 1} ({cell_type})"],
                span=(start, start + len(cell_text)),
                chunk_kind=(
                    "code_block" if cell_type == "code" else "paragraph"
                ),
            ))
            position += len(tagged)

        return ParsedDocument(
            text=_CELL_SEPARATOR.join(text_parts),
            metadata={
                "kernel": kernel,
                "code_cells": n_code,
                "markdown_cells": n_markdown,
            },
            chunks=chunks,
        )


register_parser("ipynb", NotebookParser)
register_parser("notebook", NotebookParser)


__all__ = ["NotebookParser"]
