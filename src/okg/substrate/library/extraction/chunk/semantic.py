"""Sentence-aware chunker. Wraps `semantic-text-splitter`.

Units: characters. The optional tokenizer kwarg lets callers
target a specific tokenizer's window (e.g. when feeding into a
specific LLM); when omitted, the splitter operates on character
counts.
"""

from __future__ import annotations

from typing import Optional

from okg.substrate.library.extraction._protocols import ParsedChunk


class SemanticChunker:
    """Sentence-boundary-aware chunker.

    `max_chars` is the upper bound per chunk. The library will
    prefer breaking on sentence/paragraph boundaries within that
    budget.
    """

    def __init__(
        self,
        max_chars: int,
        overlap: int = 0,
        tokenizer: Optional[str] = None,
    ) -> None:
        if max_chars <= 0:
            raise ValueError(f"max_chars must be > 0; got {max_chars}")
        if overlap < 0:
            raise ValueError(f"overlap must be >= 0; got {overlap}")
        self.max_chars = max_chars
        self.overlap = overlap
        self.tokenizer = tokenizer
        self._splitter = None
        self.last_warnings: list[str] = []

    def _get_splitter(self):
        if self._splitter is not None:
            return self._splitter
        from semantic_text_splitter import TextSplitter
        if self.overlap > 0:
            self._splitter = TextSplitter(
                capacity=self.max_chars, overlap=self.overlap,
            )
        else:
            self._splitter = TextSplitter(capacity=self.max_chars)
        return self._splitter

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text:
            return []
        splitter = self._get_splitter()
        # `chunk_indices` returns each piece with its offset in `text`, in
        # Python str code points (checked on semantic-text-splitter 0.32.0
        # with non-ASCII text), so every chunk carries its real span. The
        # older text.find() walk could miss a piece and leave it unplaced.
        # A piece whose offset does not locate it is kept WITHOUT a span
        # and named in `last_warnings` (the doc-corpus source copies those
        # onto the document's parse warnings), so a read of that document
        # says its coverage is unknown instead of trusting a wrong position.
        self.last_warnings = []
        out: list[ParsedChunk] = []
        for start, piece in splitter.chunk_indices(text):
            end = start + len(piece)
            if text[start:end] != piece:
                self.last_warnings.append(f"semantic_chunk_span_mismatch:{start}")
                out.append(ParsedChunk(text=piece))
                continue
            out.append(ParsedChunk(text=piece, span=(start, end)))
        return out


__all__ = ["SemanticChunker"]
