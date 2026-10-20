from __future__ import annotations

from pathlib import Path

import yaml


def _chunk_attrs() -> dict:
    root = Path(__file__).resolve().parents[4]
    schema = yaml.safe_load(
        (root / "src/okg/substrate/library/ontologies/extraction/schema.yaml")
        .read_text()
    )
    return schema["classes"]["DocumentChunk"]["attributes"]


def test_document_chunk_declares_required_kind_and_raw_end() -> None:
    attrs = _chunk_attrs()
    assert attrs["chunk_kind"]["required"] is True
    assert attrs["char_end"]["range"] == "integer"
    assert "image_caption" in attrs["chunk_kind"]["description"]
