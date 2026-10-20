"""Substrate extraction Protocols + dataclasses.

Three Protocols, each with ≥ 2 backends in this proposal:
  - Parser   (8 format-specific backends)
  - Chunker  (3 strategy backends)
  - EntityExtractor (3 backends; Protocol lives at okg.extraction)

Single-backend operations (date parsing, normalization, citation
parsing, identifier validation) ship as plain functions in their
own modules, not Protocols.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable


# C0 control characters minus the legitimate whitespace (tab / newline /
# carriage return). Extracted document text must never carry these: U+0000 is
# unstorable (Postgres text/JSONB rejects it — `U+0000 cannot be converted to
# text`), and the other C0 controls are extraction noise (seen in the wild from
# pypdf / pdfplumber text streams) that can break the COPY into the graph store.
# Stripping them at the parsed-text data model enforces the invariant once for
# every parser + chunker, rather than per backend.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_CHUNK_KIND_RE = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z")

DEFAULT_CHUNK_KIND = "paragraph"
DOCUMENTED_CHUNK_KINDS = frozenset({
    "paragraph",
    "heading",
    "equation",
    "figure_caption",
    "table",
    "list_item",
    "code_block",
    "image_caption",
})


def _strip_control_chars(text: str) -> str:
    """Remove storage-breaking C0 control characters from parsed text.

    Fast path: only allocate a new string when a control character is actually
    present (parsed text is overwhelmingly clean)."""
    if text and _CONTROL_CHARS_RE.search(text):
        return _CONTROL_CHARS_RE.sub("", text)
    return text


# ---- dataclasses --------------------------------------------------

@dataclass(frozen=True)
class ParsedChunk:
    """One structural chunk of a parsed document.

    `extras` is an additive bag for chunker-specific attributes
    that the doc-corpus source splices into the resulting
    `document_chunk` node's attrs. Per
    `add-heading-aware-chunker` (2026-04-26), the heading-aware
    chunker uses it to carry `chunk_index_within_section` /
    `chunk_total_within_section` when an oversized section was
    split. New fields are namespaced by chunker — keys collide
    only when two chunkers genuinely mean the same thing.
    """

    text: str
    heading_path: list[str] = field(default_factory=list)
    span: Optional[tuple[int, int]] = None
    extras: dict[str, Any] = field(default_factory=dict)
    # Kept after the pre-existing fields so positional construction by legacy
    # third-party chunkers remains source-compatible.
    chunk_kind: str = DEFAULT_CHUNK_KIND

    def __post_init__(self) -> None:
        cleaned = _strip_control_chars(self.text)
        if cleaned is not self.text:  # frozen dataclass: bypass on real change
            object.__setattr__(self, "text", cleaned)
        if not isinstance(self.chunk_kind, str):
            raise TypeError(
                "ParsedChunk.chunk_kind must be a string; got "
                f"{type(self.chunk_kind).__name__}"
            )
        normalized_kind = self.chunk_kind.strip().lower().replace("-", "_")
        if not normalized_kind or _CHUNK_KIND_RE.fullmatch(normalized_kind) is None:
            raise ValueError(
                "ParsedChunk.chunk_kind must be non-empty lower snake_case; "
                f"got {self.chunk_kind!r}"
            )
        if normalized_kind != self.chunk_kind:
            object.__setattr__(self, "chunk_kind", normalized_kind)


@dataclass(frozen=True)
class ParsedDocument:
    """Output of any Parser. Carries the flat text + format-specific
    metadata + optional structural chunks (when the format provides
    headings / cells / sections)."""

    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    chunks: list[ParsedChunk] = field(default_factory=list)

    def __post_init__(self) -> None:
        cleaned = _strip_control_chars(self.text)
        if cleaned is not self.text:  # frozen dataclass: bypass on real change
            object.__setattr__(self, "text", cleaned)


@dataclass(frozen=True)
class BibEntry:
    """One bibliographic entry from a BibTeX-style source."""

    title: str
    authors: list[str] = field(default_factory=list)
    year: Optional[int] = None
    doi: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InlineCitation:
    """One inline citation reference with its span in the source
    text. `key` is the matched citation token (e.g., "[1]",
    "(Smith 2020)")."""

    key: str
    span: tuple[int, int]


# ---- Protocols ----------------------------------------------------

@runtime_checkable
class Parser(Protocol):
    """Formats bytes / str into a `ParsedDocument`. Implementations
    must raise rather than return degraded text on malformed input —
    silent fallback is a P0 in this codebase."""

    def parse(self, content: bytes | str, **kwargs: Any) -> ParsedDocument:
        raise NotImplementedError


@runtime_checkable
class Chunker(Protocol):
    """Splits text into chunks. Empty result is a legal output
    (the CompositeChunker uses empty as the fall-through trigger)."""

    def chunk(self, text: str) -> list[ParsedChunk]:
        raise NotImplementedError


# EntityExtractor lives at okg.extraction — substrate-wide. We
# re-export it here so callers can import the three Protocols from
# one module.
from okg.extraction import (
    EntityExtractor as EntityExtractor,  # noqa: F401
    ExtractedEntity as ExtractedEntity,  # noqa: F401
)


__all__ = [
    "BibEntry",
    "Chunker",
    "DEFAULT_CHUNK_KIND",
    "DOCUMENTED_CHUNK_KINDS",
    "EntityExtractor",
    "ExtractedEntity",
    "InlineCitation",
    "Parser",
    "ParsedChunk",
    "ParsedDocument",
]
