"""CompositeChunker — try each backend in order, return the first
non-empty result. Empty IS the fallthrough trigger.

Distinct from "first that succeeds" (which would be silent
fallback): a chunker returning [] is a legal explicit signal,
not an exception we paper over.
"""

from __future__ import annotations

from okg.substrate.library.extraction._protocols import (
    Chunker,
    ParsedChunk,
)


class CompositeChunker:
    """Try each backend in order; return the first whose `.chunk()`
    returns a non-empty list. If all return empty, return empty.
    """

    def __init__(self, *backends: Chunker) -> None:
        if not backends:
            raise ValueError("CompositeChunker requires at least one backend")
        self._backends = backends

    def chunk(self, text: str) -> list[ParsedChunk]:
        for b in self._backends:
            chunks = b.chunk(text)
            if chunks:
                return chunks
        return []


__all__ = ["CompositeChunker"]
