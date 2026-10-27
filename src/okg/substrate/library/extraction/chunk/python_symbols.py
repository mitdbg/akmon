"""Symbol-aligned chunker for Python source. Stdlib only.

`PythonAstParser` emits one chunk per top-level function or class whose
text is the *docstring only*, so the code itself never reaches the
graph: identifiers used inside a function body are unsearchable and an
agent can read a symbol's signature but never its logic. This chunker
keeps the code.

It sets `prefers_raw_text = True`, so `DocCorpusSource._chunks_from`
bypasses the parser's docstring chunks and runs this chunker on the raw
source (the same hook the Markdown heading chunker uses). Code symbol
nodes are emitted separately by the code extractor and are unaffected.

Chunks follow the code's structure:

- each top-level function: one chunk, `heading_path` = [name];
- each class: one chunk when it fits, otherwise its header (class line,
  docstring, class attributes) plus one chunk per method,
  `heading_path` = [Class] / [Class, method];
- module-level statements between definitions (imports, constants,
  `if __name__ == "__main__"` blocks): grouped, `heading_path` = [];
- anything longer than `max_chars` is split into overlapping windows,
  keeping its heading path.

Every chunk carries `extras` = {`start_line`, `end_line` (1-based,
inclusive), `symbol` (dotted qualified name, "" for module code)}. A
chunk's first window starts at the first non-blank line of its extent, so
`start_line` is the line its text begins on (a reader that counts lines
from `start_line` gets the source's line numbers). That line is the `def`
or `class` line, the first decorator when the symbol has any, or the first
comment line between the previous statement and the symbol, however many
blank lines separate the two. The code-symbol node of the same name records
the `def` line (`ast` `lineno`, decorators excluded), so a chunk can start
a few lines before it. Later windows of a long symbol start wherever the
window falls, possibly on a blank line.

The blank lines between two chunks belong to neither; the document stores
them as separators, up to `_MAX_SEPARATOR_CHARS`. A longer blank run stays
in the chunk that follows it, so the chunks still tile the file and a whole
read stays complete; only then does a chunk start on a blank line.
Source that does not parse (syntax error, or too deeply nested for the
parser) falls back to plain windows with `symbol` "".
"""

from __future__ import annotations

import ast
import bisect
from typing import Optional

from okg.substrate.library.extraction._protocols import ParsedChunk

_KIND = "code_block"
# The longest whitespace gap between two chunks that the document stores as a
# separator (`MAX_STORED_SEPARATOR_CHARS` in the document-corpus source; a test
# keeps the two equal). A longer gap would make a whole read report `partial`.
_MAX_SEPARATOR_CHARS = 256


class PythonSymbolChunker:
    prefers_raw_text: bool = True

    def __init__(self, max_chars: int = 4000, overlap: int = 400) -> None:
        if max_chars <= 0:
            raise ValueError(f"max_chars must be > 0; got {max_chars}")
        if not 0 <= overlap < max_chars:
            raise ValueError(f"overlap must be in [0, max_chars); got {overlap}")
        self.max_chars = max_chars
        self.overlap = overlap

    # -- public -----------------------------------------------------------
    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text or not text.strip():
            return []
        # Line i (1-based) starts at char offset starts[i - 1].
        starts = [0]
        for i, ch in enumerate(text):
            if ch == "\n":
                starts.append(i + 1)
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError, RecursionError, MemoryError):
            # MemoryError / RecursionError: CPython's parser gives up on
            # deeply nested source ("Parser stack overflowed").
            return self._span(text, starts, 1, len(starts), [], "")

        out: list[ParsedChunk] = []
        n_lines = len(starts)
        pending: Optional[tuple[int, int]] = None  # module-level line span
        prev_end = 0  # last line already assigned to a chunk

        def flush_module() -> None:
            nonlocal pending
            if pending is not None:
                out.extend(self._span(text, starts, pending[0], pending[1], [], ""))
                pending = None

        body = tree.body
        for k, node in enumerate(body):
            first, last = _lines(node)
            # Tile the file: comments and blank lines before a statement belong
            # to it (a comment above a def documents the def); lines after the
            # final statement belong to the final chunk.
            first = prev_end + 1
            if k == len(body) - 1:
                last = n_lines
            prev_end = last
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                flush_module()
                out.extend(self._span(text, starts, first, last, [node.name], node.name))
            elif isinstance(node, ast.ClassDef):
                flush_module()
                out.extend(self._class(text, starts, node, first, last))
            else:
                pending = (pending[0], last) if pending else (first, last)
        if not body:
            pending = (1, n_lines)
        flush_module()
        return out

    # -- helpers ----------------------------------------------------------
    def _class(self, text: str, starts: list[int], node: ast.ClassDef,
               first: int, last: int) -> list[ParsedChunk]:
        if _chars(starts, text, first, last) <= self.max_chars:
            return self._span(text, starts, first, last, [node.name], node.name)
        out: list[ParsedChunk] = []
        run: Optional[tuple[int, int]] = None  # class-level non-method lines
        prev_end = first - 1
        kids = node.body
        for k, child in enumerate(kids):
            c_first, c_last = _lines(child)
            c_first = prev_end + 1 if k else first  # header lines go with the first member run
            if k == len(kids) - 1:
                c_last = last
            prev_end = c_last
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if run is not None:
                    out.extend(self._span(text, starts, run[0], run[1], [node.name], node.name))
                    run = None
                if k == 0:  # no docstring: the class line gets its own chunk
                    # The comments above the first method go with the method,
                    # as they do for every later method.
                    head_end = _header_end(text, starts, node.lineno, _lines(child)[0] - 1)
                    if head_end >= c_first:
                        out.extend(self._span(text, starts, c_first, head_end, [node.name], node.name))
                        c_first = head_end + 1
                qual = f"{node.name}.{child.name}"
                out.extend(self._span(text, starts, c_first, c_last, [node.name, child.name], qual))
            else:
                run = (run[0], c_last) if run else (c_first, c_last)
        if run is not None:
            out.extend(self._span(text, starts, run[0], run[1], [node.name], node.name))
        return out

    def _span(self, text: str, starts: list[int], first: int, last: int,
              heading: list[str], symbol: str) -> list[ParsedChunk]:
        # Blank lines before the code are not part of the chunk: its
        # `start_line` is where its text begins.
        # A run longer than the stored-separator limit stays in the chunk, so
        # the file stays fully covered.
        lead = first
        while lead < last and not _line_text(text, starts, lead).strip():
            lead += 1
        if starts[lead - 1] - starts[first - 1] <= _MAX_SEPARATOR_CHARS:
            first = lead
        begin = starts[first - 1]
        end = starts[last] if last < len(starts) else len(text)
        return self._windows(text, begin, end, heading, symbol, starts)

    def _windows(self, text: str, begin: int, end: int, heading: list[str],
                 symbol: str, starts: list[int]) -> list[ParsedChunk]:
        out: list[ParsedChunk] = []
        if not text[begin:end].strip():
            return out
        step = self.max_chars - self.overlap
        i = begin
        while i < end:
            j = min(i + self.max_chars, end)
            piece = text[i:j]
            if piece.strip():
                out.append(ParsedChunk(
                    text=piece,
                    heading_path=list(heading),
                    span=(i, j),
                    chunk_kind=_KIND,
                    extras={
                        "start_line": _line_of(starts, i),
                        "end_line": _line_of(starts, max(i, j - 1)),
                        "symbol": symbol,
                    },
                ))
            if j == end:
                break
            i += step
        return out


def _lines(node: ast.stmt) -> tuple[int, int]:
    """First and last source line of a statement, decorators included."""
    first = node.lineno
    for dec in getattr(node, "decorator_list", []) or []:
        first = min(first, dec.lineno)
    last = getattr(node, "end_lineno", None) or node.lineno
    return first, last


def _header_end(text: str, starts: list[int], first: int, last: int) -> int:
    """Last line in [first, last] holding code, not a comment or blank line.

    Between a class line and its first method there is only the class
    signature (which may span lines) and comments / blank lines."""
    end = first
    for line in range(first, last + 1):
        stop = starts[line] if line < len(starts) else len(text)
        stripped = text[starts[line - 1]:stop].strip()
        if stripped and not stripped.startswith("#"):
            end = line
    return end


def _line_text(text: str, starts: list[int], line: int) -> str:
    stop = starts[line] if line < len(starts) else len(text)
    return text[starts[line - 1]:stop]


def _chars(starts: list[int], text: str, first: int, last: int) -> int:
    end = starts[last] if last < len(starts) else len(text)
    return end - starts[first - 1]


def _line_of(starts: list[int], offset: int) -> int:
    return bisect.bisect_right(starts, offset)


__all__ = ["PythonSymbolChunker"]
