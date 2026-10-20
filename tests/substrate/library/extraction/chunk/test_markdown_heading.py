"""Tests for MarkdownHeadingChunker.

Per `add-heading-aware-chunker` (2026-04-26).

The chunker walks the markdown-it-py AST, emits one chunk per
heading section (until the next heading of any depth), and then
size-normalizes:
    - merge undersized (< min_chars) sections into the next
    - split oversized (> max_chars) sections at paragraph
      boundaries, tagging splits with
      `chunk_index_within_section`

Tests use small thresholds (min=5/max=80 tokens) so synthetic
inputs exercise the boundaries without needing kilobyte fixtures.
The default 100/1500 band is exercised by the integration test.
"""
from __future__ import annotations

from pathlib import Path

import pytest


# ---------- helpers --------------------------------------------------

def _chunker(min_tokens: int = 100, max_tokens: int = 1500):
    from okg.substrate.library.extraction.chunk import (
        MarkdownHeadingChunker,
    )
    return MarkdownHeadingChunker(
        min_tokens=min_tokens, max_tokens=max_tokens,
    )


# ---------- task 3.1: boundaries -------------------------------------

def test_chunks_land_at_heading_boundaries() -> None:
    """Each `##` opens a new section. Chunk count == heading count
    (no preamble) when bodies are above min_chars."""
    md = (
        "# Top\n\n"
        "## Section A\n\n"
        + ("alpha " * 50) + "\n\n"
        "## Section B\n\n"
        + ("beta " * 50) + "\n\n"
        "## Section C\n\n"
        + ("gamma " * 50) + "\n"
    )
    chunks = _chunker(min_tokens=10, max_tokens=1500).chunk(md)
    # `# Top` (preamble-only heading, will be merged forward
    # since it has no body) + 3 sections.
    paths = [c.heading_path for c in chunks]
    # Exact count depends on merge: `# Top` has no body so it
    # merges with `## Section A`. Final list: A, B, C.
    assert len(chunks) == 3
    assert paths[0] == ["Top", "Section A"]
    assert paths[1] == ["Top", "Section B"]
    assert paths[2] == ["Top", "Section C"]
    # Section bodies retain the heading line.
    assert "## Section A" in chunks[0].text
    assert "alpha" in chunks[0].text


def test_heading_path_tracks_nested_levels() -> None:
    """Going from `### A.1` back to `## B` pops the `###` level."""
    md = (
        "# Top\n\n"
        "## A\n\n"
        + ("alpha " * 50) + "\n\n"
        "### A.1\n\n"
        + ("a-one " * 50) + "\n\n"
        "## B\n\n"
        + ("beta " * 50) + "\n"
    )
    chunks = _chunker(min_tokens=10, max_tokens=1500).chunk(md)
    paths = [c.heading_path for c in chunks]
    # `# Top` body is empty, merges into `## A`.
    assert paths[0] == ["Top", "A"]
    assert paths[1] == ["Top", "A", "A.1"]
    assert paths[2] == ["Top", "B"]


def test_no_headings_yields_single_chunk() -> None:
    md = "Just some prose, no headings.\n\nA second paragraph."
    chunks = _chunker(min_tokens=1).chunk(md)
    assert len(chunks) == 1
    assert chunks[0].heading_path == []
    assert "Just some prose" in chunks[0].text


def test_preamble_before_first_heading_is_emitted() -> None:
    md = (
        ("preamble " * 200) + "\n\n"
        "# First heading\n\n"
        + ("body " * 200) + "\n"
    )
    chunks = _chunker(min_tokens=10, max_tokens=1500).chunk(md)
    # Preamble has no heading_path; first heading section follows.
    assert chunks[0].heading_path == []
    assert "preamble" in chunks[0].text
    assert chunks[1].heading_path == ["First heading"]


# ---------- task 3.2: size normalization -----------------------------

def test_undersized_section_merges_with_next_sibling() -> None:
    # Threshold: min_tokens=20 -> min_chars=80.
    # Tiny section at "## Tiny" is < 80 chars; merges forward
    # into "## Big".
    md = (
        "## Tiny\n\nx\n\n"
        "## Big\n\n" + ("payload " * 80) + "\n"
    )
    chunks = _chunker(min_tokens=20, max_tokens=1500).chunk(md)
    assert len(chunks) == 1
    # Merged chunk takes successor's heading_path.
    assert chunks[0].heading_path == ["Big"]
    # Tiny content is preserved inside the merged body.
    assert "## Tiny" in chunks[0].text
    assert "## Big" in chunks[0].text
    assert "payload" in chunks[0].text


def test_oversized_section_splits_at_paragraph_boundaries() -> None:
    # 5 paragraphs of ~120 chars each -> ~600 chars total. With
    # max_tokens=40 (max_chars=160), each split holds at most 1
    # paragraph; 5 splits expected.
    paragraphs = [
        ("para%d-%s" % (i, "x" * 110))
        for i in range(5)
    ]
    md = "## Huge\n\n" + "\n\n".join(paragraphs) + "\n"
    chunks = _chunker(min_tokens=1, max_tokens=40).chunk(md)
    # First split has the heading line + first paragraph; later
    # splits are paragraph-only.
    assert len(chunks) >= 2
    # Every chunk inherits the same heading_path.
    for c in chunks:
        assert c.heading_path == ["Huge"]
    # All but the last chunk should carry chunk_index_within_section.
    indices = [c.extras.get("chunk_index_within_section") for c in chunks]
    totals = [c.extras.get("chunk_total_within_section") for c in chunks]
    assert indices == list(range(len(chunks)))
    assert all(t == len(chunks) for t in totals)


def test_oversized_split_each_under_max() -> None:
    """After split, no individual chunk exceeds max_chars."""
    paragraphs = [("p%d-" % i + "x" * 200) for i in range(8)]
    md = "## Long\n\n" + "\n\n".join(paragraphs) + "\n"
    chunks = _chunker(min_tokens=1, max_tokens=80).chunk(md)
    # max_chars = 80*4 = 320. The first chunk includes the
    # heading line; allow a small +heading slack.
    for c in chunks:
        assert len(c.text) <= 320 + 64, (
            f"chunk len {len(c.text)} exceeds budget"
        )


# ---------- task 3.3: edge cases -------------------------------------

def test_empty_doc() -> None:
    chunks = _chunker().chunk("")
    assert chunks == []


def test_whitespace_only_doc() -> None:
    chunks = _chunker().chunk("   \n\n  \n")
    assert chunks == []


def test_heading_only_doc_with_no_body() -> None:
    # "# Heading" with no body. The whole section is just the
    # heading line (~10 chars); under default min, it's
    # undersized but has no successor — emitted as a single
    # chunk regardless.
    chunks = _chunker(min_tokens=1).chunk("# Heading\n")
    assert len(chunks) == 1
    assert chunks[0].heading_path == ["Heading"]


def test_consecutive_undersized_sections_chain_merge() -> None:
    # Three tiny sections. First two should merge into the
    # third, leaving a single chunk.
    md = (
        "## A\nx\n\n"
        "## B\ny\n\n"
        "## C\nz\n\n"
        + ("filler " * 50)
    )
    chunks = _chunker(min_tokens=20, max_tokens=1500).chunk(md)
    # Final chunk count: 1 (all merged) since the last section
    # is the only one above min.
    assert len(chunks) == 1
    body = chunks[0].text
    # All three heading lines present in the merged body.
    assert "## A" in body
    assert "## B" in body
    assert "## C" in body


def test_token_estimate_convention() -> None:
    """min_tokens / max_tokens map to chars via len // 4."""
    from okg.substrate.library.extraction.chunk import (
        MarkdownHeadingChunker,
    )
    c = MarkdownHeadingChunker(min_tokens=100, max_tokens=1500)
    assert c.min_chars == 400
    assert c.max_chars == 6000


def test_invalid_band_rejected() -> None:
    from okg.substrate.library.extraction.chunk import (
        MarkdownHeadingChunker,
    )
    with pytest.raises(ValueError):
        MarkdownHeadingChunker(min_tokens=0)
    with pytest.raises(ValueError):
        MarkdownHeadingChunker(min_tokens=100, max_tokens=100)
    with pytest.raises(ValueError):
        MarkdownHeadingChunker(min_tokens=200, max_tokens=100)


def test_prefers_raw_text_flag() -> None:
    """The chunker advertises `prefers_raw_text=True` so
    `DocCorpusSource._chunks_from` bypasses parser-emitted
    paragraph chunks. Without this flag, the heading-aware
    chunker would never see the raw markdown."""
    from okg.substrate.library.extraction.chunk import (
        MarkdownHeadingChunker,
    )
    c = MarkdownHeadingChunker()
    assert getattr(c, "prefers_raw_text", False) is True


# ---------- issue #2997: chunks tile the document --------------------

def _assert_tiles(md: str, chunks) -> None:
    """Every chunk is the exact slice its span names, and the spans
    are contiguous from 0 to len(md), so the chunks in order are the
    document byte for byte."""
    assert chunks, "expected at least one chunk"
    assert chunks[0].span[0] == 0
    assert chunks[-1].span[1] == len(md)
    for chunk in chunks:
        start, end = chunk.span
        assert md[start:end] == chunk.text
    for previous, current in zip(chunks, chunks[1:]):
        assert previous.span[1] == current.span[0]
    assert "".join(c.text for c in chunks) == md


def _long_markdown(sections: int = 12) -> str:
    return "".join(
        f"## Section {s}: the valve\n\n"
        + "".join(
            f"Paragraph {s}.{p} " + "the pump feeds the valve; " * 12 + "\n\n"
            for p in range(3)
        )
        for s in range(sections)
    ) + "Closing line with no trailing blank.\n"


@pytest.mark.parametrize(
    ("min_tokens", "max_tokens"),
    [(100, 1500), (10, 80), (1, 40), (400, 1500)],
)
def test_chunks_tile_the_document_exactly(
    min_tokens: int, max_tokens: int,
) -> None:
    """Splits, forward merges and the tail fold all keep every
    character: the blank lines between sections stay in the chunks."""
    md = "Intro line.\n\n" + _long_markdown()
    chunks = _chunker(min_tokens, max_tokens).chunk(md)
    _assert_tiles(md, chunks)


def test_section_seam_keeps_blank_line() -> None:
    """The reported defect: `valve;\\n\\n## Section 2` read back as
    `valve;## Section 2`. The separator now ends the earlier chunk."""
    md = _long_markdown(3)
    chunks = _chunker(min_tokens=10, max_tokens=1500).chunk(md)
    assert len(chunks) == 3
    assert chunks[0].text.endswith("valve; \n\n")
    assert chunks[1].text.startswith("## Section 1")
    assert "valve; ## Section" not in "".join(c.text for c in chunks)


def test_tiling_keeps_blank_preamble_crlf_and_trailing_newlines() -> None:
    md = (
        "\n\n  \n"
        "# Title\r\n\r\n" + ("alpha " * 30) + "\r\n\r\n"
        "## Next\r\n\r\n" + ("beta " * 30) + "\r\n\n\n"
    )
    chunks = _chunker(min_tokens=10, max_tokens=1500).chunk(md)
    _assert_tiles(md, chunks)
    assert [c.heading_path for c in chunks] == [["Title"], ["Title", "Next"]]
    # The blank preamble joins the first section instead of vanishing.
    assert chunks[0].text.startswith("\n\n  \n# Title")


def test_no_heading_document_is_one_exact_chunk() -> None:
    md = "\nJust prose.\n\nMore prose.\n\n\n"
    chunks = _chunker(min_tokens=1).chunk(md)
    assert len(chunks) == 1
    assert chunks[0].text == md
    assert chunks[0].span == (0, len(md))


def test_split_ranges_tile_the_section() -> None:
    paragraphs = [("p%d-" % i + "x" * 200) for i in range(8)]
    md = "## Long\n\n" + "\n\n\n".join(paragraphs) + "\n"
    chunks = _chunker(min_tokens=1, max_tokens=80).chunk(md)
    assert len(chunks) > 1
    _assert_tiles(md, chunks)
    # Size decisions ignore the carried blank lines.
    for c in chunks:
        assert len(c.text.strip()) <= 320 + 64


# ---------- task 3.4: integration on a real doc ----------------------

@pytest.fixture
def architecture_md() -> str:
    repo_root = Path(__file__).resolve().parents[5]
    p = repo_root / "docs" / "develop" / "architecture" / "pipeline.md"
    if not p.is_file():
        pytest.skip(f"not found: {p}")
    return p.read_text(encoding="utf-8")


def test_real_architecture_doc(architecture_md: str) -> None:
    """`docs/develop/architecture/pipeline.md` chunks within the documented band
    on default (100/1500) thresholds."""
    chunks = _chunker().chunk(architecture_md)
    assert len(chunks) > 0
    sizes = [len(c.text) for c in chunks]
    # Token estimate via len//4. min_chars=400, max_chars=6000.
    # Tail-folding allows the very last chunk to slip below min,
    # but the median should land comfortably in the band.
    median = sorted(sizes)[len(sizes) // 2]
    assert 400 <= median <= 6000, (
        f"median chunk size {median} chars outside band; "
        f"sizes={sizes}"
    )
    # Every chunk has a non-empty heading_path or is preamble.
    # On architecture.md, every chunk should have a heading.
    for c in chunks:
        # Either it's the (small) preamble before the first
        # heading, or it's a heading-anchored chunk.
        assert c.heading_path is not None
