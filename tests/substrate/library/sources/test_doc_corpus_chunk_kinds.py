from __future__ import annotations

from dataclasses import dataclass

from okg.substrate.library.extraction._protocols import ParsedChunk, ParsedDocument
from okg.substrate.library.sources.doc_corpus import (
    MAX_STORED_SEPARATOR_CHARS,
    DocCorpusSource,
)


@dataclass
class _RawChunker:
    prefers_raw_text: bool = True

    def chunk(self, text: str) -> list[ParsedChunk]:
        return [ParsedChunk(text=text[2:5], span=(2, 5), chunk_kind="table")]


@dataclass
class _Window:
    max_chars: int = 3

    def chunk(self, text: str) -> list[ParsedChunk]:
        return [
            ParsedChunk(text=text[i:i + 3], span=(i, min(i + 3, len(text))))
            for i in range(0, len(text), 3)
        ]


def test_raw_preferred_chunker_carries_kind_and_full_span() -> None:
    out = DocCorpusSource._chunks_from(ParsedDocument(text="012345"), _RawChunker())
    assert out == [(2, 5, "234", [], "table", {})]


def test_parser_chunk_kind_survives_without_registered_chunker() -> None:
    parsed = ParsedDocument(
        text="xxEQUATIONyy",
        chunks=[ParsedChunk("EQUATION", span=(2, 10), chunk_kind="equation")],
    )
    assert DocCorpusSource._chunks_from(parsed, None) == [
        (2, 10, "EQUATION", [], "equation", {}),
    ]


def test_rechunk_default_children_inherit_nonparagraph_parent_kind() -> None:
    parsed = ParsedDocument(
        text="abcdef",
        chunks=[ParsedChunk("abcdef", span=(10, 16), chunk_kind="code_block")],
    )
    assert DocCorpusSource._chunks_from(parsed, _Window()) == [
        (10, 13, "abc", [], "code_block", {}),
        (13, 16, "def", [], "code_block", {}),
    ]


def test_registered_and_whole_document_fallback_default_to_paragraph() -> None:
    parsed = ParsedDocument(text="abcdef")
    assert [row[4] for row in DocCorpusSource._chunks_from(parsed, _Window())] == [
        "paragraph", "paragraph",
    ]
    assert DocCorpusSource._chunks_from(parsed, None) == [
        (0, 6, "abcdef", [], "paragraph", {}),
    ]


def test_a_chunk_without_a_span_stores_no_position() -> None:
    """HTML paragraphs and Markdown structural chunks carry no span; before
    reparse epoch 4 they were stored at offset 0, which a read took as a
    position."""
    parsed = ParsedDocument(
        text="Para one. Para two.",
        chunks=[ParsedChunk("Para one."), ParsedChunk("Para two.")],
    )
    assert [row[:2] for row in DocCorpusSource._chunks_from(parsed, None)] == [
        (None, None),
        (None, None),
    ]


def test_pieces_of_an_unplaced_chunk_have_no_position_either() -> None:
    """A window over a chunk with no span has offsets relative to that chunk,
    not to the document, so they are not stored as document offsets."""
    parsed = ParsedDocument(text="abcdefg", chunks=[ParsedChunk("abcdef")])
    out = DocCorpusSource._chunks_from(parsed, _Window())
    assert [(row[0], row[1], row[2]) for row in out] == [
        (None, None, "abc"),
        (None, None, "def"),
    ]


def test_pieces_of_a_placed_page_get_document_offsets() -> None:
    parsed = ParsedDocument(
        text="xy\n\nabcdef",
        chunks=[ParsedChunk("xy", span=(0, 2)), ParsedChunk("abcdef", span=(4, 10))],
    )
    out = DocCorpusSource._chunks_from(parsed, _Window())
    assert [(row[0], row[1], row[2]) for row in out] == [
        (0, 2, "xy"),
        (4, 7, "abc"),
        (7, 10, "def"),
    ]


def test_occurrence_layout_stores_whitespace_separators_and_the_length() -> None:
    text = "  page one\n\n\n\npage three\n\nlast\n"
    pieces = [
        (2, 10, "page one"),
        (12, 12, ""),  # an empty page between two separators
        (14, 24, "page three"),
        (26, 30, "last"),
        (None, None, "| table |"),  # a table chunk with no span
    ]
    layout = DocCorpusSource._occurrence_layout(pieces, text)
    assert layout == [
        {"doc_char_length": len(text), "separator_before": "  "},
        {"doc_char_length": len(text)},
        {"doc_char_length": len(text), "separator_before": "\n\n\n\n"},
        {
            "doc_char_length": len(text),
            "separator_before": "\n\n",
            "separator_after": "\n",
        },
        {},
    ]


def test_occurrence_layout_leaves_a_gap_with_text_in_it_unfilled() -> None:
    """A Word heading between two paragraphs is not a separator."""
    text = "Intro\nbody one\nHeading\nbody two"
    pieces = [(6, 14, "body one"), (23, 31, "body two")]
    layout = DocCorpusSource._occurrence_layout(pieces, text)
    assert layout == [{"doc_char_length": 31}, {"doc_char_length": 31}]


def test_occurrence_layout_does_not_store_a_long_whitespace_run() -> None:
    gap = " " * (MAX_STORED_SEPARATOR_CHARS + 1)
    text = "a" + gap + "b"
    layout = DocCorpusSource._occurrence_layout(
        [(0, 1, "a"), (len(text) - 1, len(text), "b")], text
    )
    assert "separator_before" not in layout[1]


def test_occurrence_layout_overlapping_windows_need_no_separator() -> None:
    text = "abcdefghij"
    layout = DocCorpusSource._occurrence_layout(
        [(0, 6, "abcdef"), (4, 10, "efghij")], text
    )
    assert layout == [{"doc_char_length": 10}, {"doc_char_length": 10}]


def test_occurrence_layout_skips_a_span_that_does_not_name_its_text() -> None:
    """A LaTeX table's span is its raw source; its text is the rendering."""
    source = "\\begin{tabular}x\\end{tabular}"
    text = f"before {source} after"
    table_start = text.index(source)
    after_start = text.index("after")
    pieces = [
        (0, 6, "before"),
        (table_start, table_start + len(source), "| x |"),
        (after_start, after_start + 5, "after"),
    ]
    layout = DocCorpusSource._occurrence_layout(pieces, text)
    # No doc_char_length either: a reader must not take the table's span for
    # document text, even when the rendering happens to be as long.
    assert layout[1] == {}
    assert layout[0] == {"doc_char_length": len(text)}
    # The table source sits in no chunk's text, so the gap before "after"
    # holds text and is not a separator.
    assert "separator_before" not in layout[2]
