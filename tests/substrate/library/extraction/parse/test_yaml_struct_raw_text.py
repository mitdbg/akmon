"""YAML keeps the text its author wrote (#2671).

`YamlParser` re-emits a file with sorted keys and no comments, and that
re-emission was all the graph stored, so a comment explaining a value was
unsearchable. The parser now also returns the file as written, and the
doc-corpus source chunks it as `code_block` next to the unchanged re-emission
chunks.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.library.extraction.chunk.window import WindowChunker
from okg.substrate.library.extraction.parse.dispatch import parser_for
from okg.substrate.library.sources.doc_corpus import DocCorpusSource, _chunk_node_id

COMMENTED = (
    "# Keep replicas at three: the quorum_rationale_marker needs a majority.\n"
    "replicas: 3\n"
    "name: tape-reaper  # renamed from reaper in 2025\n"
)


def _write_extractors(extr: Path) -> None:
    base = "okg.substrate.library.extraction"
    extr.mkdir(parents=True, exist_ok=True)
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [
            {"format": "yaml", "class": f"{base}.parse.yaml_struct.YamlParser"},
        ],
        "chunker": [
            {"format": "default", "class": f"{base}.chunk.window.WindowChunker",
             "config": {"max_chars": 1500, "overlap": 200}},
        ],
    }))


def _chunks(tmp_path: Path, name: str, body: str) -> list:
    repo = tmp_path / "repo"
    repo.mkdir(exist_ok=True)
    (repo / name).write_text(body, encoding="utf-8")
    extr = tmp_path / "extr"
    _write_extractors(extr)
    run = DocCorpusSource(
        repo="t", repo_root=repo, non_git_walk=True, extractors_dir=extr,
        include_globs=("**/*",),
    ).run("probe", mode="scope_complete")
    return [f for f in run.facts if getattr(f, "subtype", None) == "document_chunk"]


def _by_kind(chunks: list, kind: str) -> list[str]:
    return [
        str(c.attrs["text"]) for c in chunks if c.attrs.get("chunk_kind") == kind
    ]


def test_the_parser_returns_the_original_text_and_keeps_the_canonical_text() -> None:
    parsed = parser_for("yaml").parse(COMMENTED.encode("utf-8"))

    assert parsed.metadata["original_text"] == COMMENTED
    assert "quorum_rationale_marker" not in parsed.text  # the re-emission drops comments
    assert parsed.text.startswith("name:")  # sorted keys, as before


def test_a_yaml_comment_is_stored_in_an_original_text_chunk(tmp_path: Path) -> None:
    chunks = _chunks(tmp_path, "values.yaml", COMMENTED)

    original = _by_kind(chunks, "code_block")
    canonical = _by_kind(chunks, "paragraph")
    assert original == [COMMENTED]
    # The failure this fixes: the re-emission chunks never held the comment.
    assert canonical and not any("quorum_rationale_marker" in t for t in canonical)


def test_original_chunks_follow_the_canonical_ones_by_offset(tmp_path: Path) -> None:
    """Readers ordering by offset see one form, then the other, never interleaved."""
    body = "# settings\n" + "".join(f"key_{i:04d}: value {i}\n" for i in range(200))
    chunks = _chunks(tmp_path, "big.yaml", body)
    canonical_text = parser_for("yaml").parse(body.encode("utf-8")).text

    paragraph_ends = [
        c.attrs["char_end"] for c in chunks if c.attrs.get("chunk_kind") == "paragraph"
    ]
    original_offsets = [
        c.attrs["char_offset"] for c in chunks if c.attrs.get("chunk_kind") == "code_block"
    ]
    assert original_offsets and min(original_offsets) == len(canonical_text) + 2
    assert max(paragraph_ends) <= len(canonical_text) < min(original_offsets)


@pytest.mark.parametrize("size", [1, 200])
def test_canonical_chunks_keep_their_text_and_ids(tmp_path: Path, size: int) -> None:
    """The re-emission chunks are exactly what a WindowChunker made before."""
    body = "# settings\n" + "".join(f"key_{i:04d}: value {i}\n" for i in range(size))
    chunks = _chunks(tmp_path, "big.yaml", body)
    canonical_text = parser_for("yaml").parse(body.encode("utf-8")).text
    expected = [c.text for c in WindowChunker(1500, 200).chunk(canonical_text)]

    paragraphs = [c for c in chunks if c.attrs.get("chunk_kind") == "paragraph"]
    assert [c.attrs["text"] for c in paragraphs] == expected
    assert [c.node_id for c in paragraphs] == [_chunk_node_id(t) for t in expected]
    if size > 1:
        assert len(expected) > 1  # the oversize case really is windowed
        assert len(_by_kind(chunks, "code_block")) > 1


@pytest.mark.parametrize(
    "body",
    [
        "a: 1\n",  # already canonical: no duplicate chunk
        "",  # empty: no empty chunk
        "   \n",
        "---\n",
        "a: 1\r\nb: 2\r\n",  # canonical but for Windows line endings
    ],
)
def test_no_original_text_chunk_when_it_adds_nothing(tmp_path: Path, body: str) -> None:
    chunks = _chunks(tmp_path, "plain.yaml", body)
    assert _by_kind(chunks, "code_block") == []
    assert all(str(c.attrs["text"]).strip() for c in chunks)
