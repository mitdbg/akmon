from __future__ import annotations

import hashlib

import pytest

from okg.substrate.library.extraction._protocols import (
    DEFAULT_CHUNK_KIND,
    DOCUMENTED_CHUNK_KINDS,
    ParsedChunk,
)
from okg.substrate.library.sources.doc_corpus import _chunk_node_id


def test_parsed_chunk_defaults_without_breaking_positional_callers() -> None:
    chunk = ParsedChunk("body", ["Heading"], (3, 7), {"source": "legacy"})
    assert chunk.chunk_kind == DEFAULT_CHUNK_KIND
    assert chunk.heading_path == ["Heading"]
    assert chunk.span == (3, 7)
    assert chunk.extras == {"source": "legacy"}


def test_chunk_kind_is_normalized_but_open() -> None:
    assert ParsedChunk("x", chunk_kind=" Figure-Caption ").chunk_kind == "figure_caption"
    assert ParsedChunk("x", chunk_kind="future_structure").chunk_kind == "future_structure"
    assert {
        "paragraph", "heading", "equation", "figure_caption", "table",
        "list_item", "code_block", "image_caption",
    } <= DOCUMENTED_CHUNK_KINDS


@pytest.mark.parametrize("kind", ["", "   ", "not/slashed", "two words", 3, None])
def test_chunk_kind_refuses_invalid_values(kind: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        ParsedChunk("x", chunk_kind=kind)  # type: ignore[arg-type]


def test_paragraph_identity_is_legacy_compatible_and_other_kinds_are_isolated() -> None:
    text = "identical text"
    old = f"chunk:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]}"
    assert _chunk_node_id(text) == old
    assert _chunk_node_id(text, "paragraph") == old
    assert _chunk_node_id(text, "equation") != old
    assert _chunk_node_id(text, "equation") != _chunk_node_id(text, "list_item")
