"""Character-window chunker. Stdlib only.

Units are Python `str` characters. Bytes are not supported (no
mixed-unit configuration — pinning chars-only avoids multi-byte
off-by-token bugs).
"""

from __future__ import annotations

from okg.substrate.library.extraction._protocols import ParsedChunk


class WindowChunker:
    """Slide a fixed-size character window over text with overlap.

    `max_chars` and `overlap` are both in characters. Overlap must
    be strictly less than max_chars.
    """

    def __init__(self, max_chars: int, overlap: int = 0) -> None:
        if max_chars <= 0:
            raise ValueError(f"max_chars must be > 0; got {max_chars}")
        if overlap < 0:
            raise ValueError(f"overlap must be >= 0; got {overlap}")
        if overlap >= max_chars:
            raise ValueError(
                f"overlap ({overlap}) must be < max_chars ({max_chars})"
            )
        self.max_chars = max_chars
        self.overlap = overlap

    def chunk(self, text: str) -> list[ParsedChunk]:
        if not text:
            return []
        out: list[ParsedChunk] = []
        step = self.max_chars - self.overlap
        i = 0
        n = len(text)
        while i < n:
            end = min(i + self.max_chars, n)
            out.append(ParsedChunk(text=text[i:end], span=(i, end)))
            if end == n:
                break
            i += step
        return out


__all__ = ["WindowChunker"]
