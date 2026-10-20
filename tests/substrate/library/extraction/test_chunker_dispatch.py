"""Per `refine-doc-corpus-extraction` (2026-04-26) — G1:
per-format chunker dispatch.

Asserts the four shapes the registry must support:
  (a) per-format YAML registers each chunker under its format
      key; missing format falls through to `default`;
  (b) missing default + missing format → `chunker_for_format`
      returns None (caller refuses with a structured error
      from the adapter layer);
  (c) bare-list legacy YAML wraps every entry as a
      `CompositeChunker(*instances)` registered under `default`;
  (d) mixed bare + formatted YAML raises `ExtractorChainMissing`
      at load time pointing operators at one shape or the
      other (no silent demotion).
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.library.extraction.chunk.composite import (
    CompositeChunker,
)
from okg.substrate.library.extraction.chunk.window import WindowChunker
from okg.substrate.library.extraction.chunk.structural_markdown import (
    MarkdownStructuralChunker,
)
from okg.substrate.library.extraction.registry import (
    ExtractorChainMissing,
    load_extractors,
)


def _write_yaml(tmp_path: Path, name: str, doc: dict) -> Path:
    out = tmp_path / name
    out.mkdir()
    (out / "extractors.yaml").write_text(yaml.safe_dump(doc))
    return out


def test_per_format_chunker_picks_correct_backend(tmp_path):
    extr = _write_yaml(tmp_path, "a", {
        "chunker": [
            {
                "format": "markdown",
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "structural_markdown.MarkdownStructuralChunker"
                ),
            },
            {
                "format": "default",
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "window.WindowChunker"
                ),
                "config": {"max_chars": 500, "overlap": 0},
            },
        ],
    })
    bundle = load_extractors(extr)
    assert isinstance(
        bundle.chunker_for_format("markdown"),
        MarkdownStructuralChunker,
    )
    assert isinstance(
        bundle.chunker_for_format("text"),
        WindowChunker,
    )
    # Unknown format falls through to default.
    assert isinstance(
        bundle.chunker_for_format("unknown"),
        WindowChunker,
    )


def test_missing_default_no_format_match_returns_none(tmp_path):
    extr = _write_yaml(tmp_path, "b", {
        "chunker": [{
            "format": "markdown",
            "class": (
                "okg.substrate.library.extraction.chunk."
                "structural_markdown.MarkdownStructuralChunker"
            ),
        }],
    })
    bundle = load_extractors(extr)
    assert bundle.chunker_for_format("markdown") is not None
    # No `default` registered + unknown format → None.
    assert bundle.chunker_for_format("unknown") is None


def test_legacy_bare_list_wraps_into_composite_under_default(
    tmp_path,
):
    extr = _write_yaml(tmp_path, "c", {
        "chunker": [
            {
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "structural_markdown.MarkdownStructuralChunker"
                ),
            },
            {
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "window.WindowChunker"
                ),
                "config": {"max_chars": 500, "overlap": 0},
            },
        ],
    })
    bundle = load_extractors(extr)
    # Both registered under `default` as a CompositeChunker.
    chunker = bundle.chunker_for_format("anything")
    assert isinstance(chunker, CompositeChunker)
    # The legacy `bundle.chunker` attribute returns the same.
    assert bundle.chunker is chunker


def test_mixed_bare_and_format_keyed_raises_load_time(tmp_path):
    extr = _write_yaml(tmp_path, "d", {
        "chunker": [
            {
                "format": "markdown",
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "structural_markdown.MarkdownStructuralChunker"
                ),
            },
            # Missing `format:` → bare entry; mixing with the
            # formatted entry above is a hard error.
            {
                "class": (
                    "okg.substrate.library.extraction.chunk."
                    "window.WindowChunker"
                ),
                "config": {"max_chars": 500, "overlap": 0},
            },
        ],
    })
    with pytest.raises(ExtractorChainMissing) as exc_info:
        load_extractors(extr)
    err = exc_info.value
    assert err.data["format_keyed_count"] == 1
    assert err.data["bare_count"] == 1


def test_single_bare_entry_registers_directly_no_composite(
    tmp_path,
):
    """A single bare entry (the most common legacy shape) lands
    directly under `default` without an extra CompositeChunker
    wrapper."""
    extr = _write_yaml(tmp_path, "e", {
        "chunker": [{
            "class": (
                "okg.substrate.library.extraction.chunk."
                "window.WindowChunker"
            ),
            "config": {"max_chars": 500, "overlap": 0},
        }],
    })
    bundle = load_extractors(extr)
    chunker = bundle.chunker_for_format("anything")
    assert isinstance(chunker, WindowChunker)
    assert not isinstance(chunker, CompositeChunker)
