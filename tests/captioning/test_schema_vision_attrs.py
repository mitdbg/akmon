"""Verify that the coarse vision-captioning attributes are declared on DocumentChunk.

req.schema-vision-attrs: chunk_source, caption_model, caption_provider,
category must all be present in extraction/schema.yaml under DocumentChunk.

req.visual-artifacts.caption-surface (first-class-visual-artifacts): a caption
chunk must also be able to point at the VisualArtifact it describes
(`visual_ref`) and carry a recaption-reuse signature (`vision_signature`).
"""
from __future__ import annotations

from pathlib import Path

import yaml


SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / "src/okg/substrate/library/ontologies/extraction/schema.yaml"
)

VISION_ATTRS = {
    "chunk_source",
    "caption_model",
    "caption_provider",
    "category",
    "visual_ref",
    "vision_signature",
}


def _document_chunk_attrs() -> set[str]:
    data = yaml.safe_load(SCHEMA_PATH.read_text())
    return set((data["classes"]["DocumentChunk"].get("attributes") or {}).keys())


def test_chunk_source_declared() -> None:
    assert "chunk_source" in _document_chunk_attrs()


def test_caption_model_declared() -> None:
    assert "caption_model" in _document_chunk_attrs()


def test_caption_provider_declared() -> None:
    assert "caption_provider" in _document_chunk_attrs()


def test_category_declared() -> None:
    assert "category" in _document_chunk_attrs()


def test_visual_ref_declared() -> None:
    assert "visual_ref" in _document_chunk_attrs()


def test_vision_signature_declared() -> None:
    assert "vision_signature" in _document_chunk_attrs()


def test_all_vision_attrs_present() -> None:
    attrs = _document_chunk_attrs()
    missing = VISION_ATTRS - attrs
    assert not missing, f"Missing attrs on DocumentChunk: {missing}"
