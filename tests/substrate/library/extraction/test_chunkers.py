"""Chunker tests — three backends + composite."""
from __future__ import annotations

import pytest


def test_window_chunker_basic() -> None:
    from okg.substrate.library.extraction.chunk import WindowChunker
    chunks = WindowChunker(max_chars=5, overlap=0).chunk("abcdefghij")
    assert [c.text for c in chunks] == ["abcde", "fghij"]
    assert chunks[0].span == (0, 5)
    assert chunks[1].span == (5, 10)


def test_window_chunker_with_overlap() -> None:
    from okg.substrate.library.extraction.chunk import WindowChunker
    chunks = WindowChunker(max_chars=5, overlap=2).chunk("abcdefghij")
    # step = 3; chunks at [0:5], [3:8], [6:10]
    texts = [c.text for c in chunks]
    assert texts[0] == "abcde"
    assert texts[1] == "defgh"
    assert texts[2] == "ghij"


def test_window_chunker_rejects_invalid_overlap() -> None:
    from okg.substrate.library.extraction.chunk import WindowChunker
    with pytest.raises(ValueError):
        WindowChunker(max_chars=5, overlap=5)
    with pytest.raises(ValueError):
        WindowChunker(max_chars=0)


def test_window_chunker_empty_input() -> None:
    from okg.substrate.library.extraction.chunk import WindowChunker
    assert WindowChunker(max_chars=5).chunk("") == []


def test_markdown_structural_chunker() -> None:
    from okg.substrate.library.extraction.chunk import (
        MarkdownStructuralChunker,
    )
    md = "# A\nbody-a\n# B\nbody-b\n"
    chunks = MarkdownStructuralChunker().chunk(md)
    assert len(chunks) == 2
    assert chunks[0].heading_path == ["A"]
    assert chunks[1].heading_path == ["B"]


def test_html_structural_chunker() -> None:
    from okg.substrate.library.extraction.chunk import HtmlStructuralChunker
    html = "<h1>A</h1><p>body-a</p><h1>B</h1><p>body-b</p>"
    chunks = HtmlStructuralChunker().chunk(html)
    assert len(chunks) == 2
    assert chunks[0].heading_path == ["A"]
    assert chunks[0].text == "body-a"


def test_semantic_chunker_basic() -> None:
    from okg.substrate.library.extraction.chunk import SemanticChunker
    chunker = SemanticChunker(max_chars=20)
    text = "First sentence. Second sentence. Third one."
    chunks = chunker.chunk(text)
    assert len(chunks) >= 1
    # Every chunk's text appears in the original.
    for c in chunks:
        assert c.text in text


def test_composite_chunker_falls_through_on_empty() -> None:
    from okg.substrate.library.extraction.chunk import CompositeChunker
    from okg.substrate.library.extraction._protocols import ParsedChunk

    class _Empty:
        def chunk(self, text):
            return []

    class _Good:
        def chunk(self, text):
            return [ParsedChunk(text=text)]

    out = CompositeChunker(_Empty(), _Good()).chunk("x")
    assert len(out) == 1
    assert out[0].text == "x"


def test_composite_chunker_first_wins_when_non_empty() -> None:
    from okg.substrate.library.extraction.chunk import CompositeChunker
    from okg.substrate.library.extraction._protocols import ParsedChunk

    class _A:
        def chunk(self, text):
            return [ParsedChunk(text="from-A")]

    class _B:
        def chunk(self, text):
            return [ParsedChunk(text="from-B")]

    out = CompositeChunker(_A(), _B()).chunk("ignored")
    assert len(out) == 1
    assert out[0].text == "from-A"


def test_composite_chunker_all_empty_returns_empty() -> None:
    from okg.substrate.library.extraction.chunk import CompositeChunker

    class _Empty:
        def chunk(self, text):
            return []

    assert CompositeChunker(_Empty(), _Empty()).chunk("x") == []


def test_composite_chunker_requires_at_least_one() -> None:
    from okg.substrate.library.extraction.chunk import CompositeChunker
    with pytest.raises(ValueError):
        CompositeChunker()
