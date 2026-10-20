"""Verify the generic VisualArtifact subtype and its narrowings.

req.visual-artifacts.ontology (first-class-visual-artifacts): the extraction
ontology must define a generic visual artifact subtype carrying stable
identity plus locator/provenance metadata, and the active catalog must
authorize document/source/domain edges to and from it so inserts do not
silently fail with narrowing violations.

Task scope: task.visual-artifacts.ontology. This only checks the schema.yaml
declaration (attributes, alias/search fields, narrowings) — discovery
(Task 2), caption-enricher wiring (Task 3), and a full DB publish proof
(Task 4) are out of scope here.
"""
from __future__ import annotations

from pathlib import Path

import yaml


SCHEMA_PATH = (
    Path(__file__).parent.parent.parent
    / "src/okg/substrate/library/ontologies/extraction/schema.yaml"
)

REQUIRED_ATTRS = {
    "visual_id",
    "source_kind",
    "path",
    "source_url",
    "parent_document_ref",
    "source_file_ref",
    "page_number",
    "bbox",
    "image_index",
    "frame",
    "camera",
    "mime_type",
    "width",
    "height",
    "byte_count",
    "content_sha256",
    "render_sha256",
    "renderer_name",
    "renderer_version",
}

# Attributes the PACT statement calls out as `required: true`.
REQUIRED_TRUE_ATTRS = {
    "visual_id",
    "source_kind",
    "parent_document_ref",
    "mime_type",
    "byte_count",
    "content_sha256",
}


def _schema() -> dict:
    return yaml.safe_load(SCHEMA_PATH.read_text())


def _visual_artifact_class() -> dict:
    return _schema()["classes"]["VisualArtifact"]


def _visual_artifact_attrs() -> dict:
    return _visual_artifact_class().get("attributes") or {}


def _edge_narrowings() -> dict:
    return _schema()["classes"]["EdgeNarrowings"].get("attributes") or {}


def test_visual_artifact_class_declared() -> None:
    assert "VisualArtifact" in _schema()["classes"]


def test_visual_artifact_has_all_locator_provenance_attrs() -> None:
    attrs = set(_visual_artifact_attrs().keys())
    missing = REQUIRED_ATTRS - attrs
    assert not missing, f"Missing attrs on VisualArtifact: {missing}"


def test_visual_artifact_identity_and_core_fields_are_required() -> None:
    attrs = _visual_artifact_attrs()
    for name in REQUIRED_TRUE_ATTRS:
        assert attrs[name].get("required") is True, (
            f"Expected VisualArtifact.{name} to be required=true"
        )


def test_visual_artifact_archetype_is_artifact() -> None:
    annotations = _visual_artifact_class().get("annotations") or {}
    assert annotations.get("archetype") == "artifact"
    assert annotations.get("transient") == "false"


def test_visual_artifact_has_alias_field() -> None:
    annotations = _visual_artifact_class().get("annotations") or {}
    assert annotations.get("okg_alias_fields") == "visual_id"


def test_visual_artifact_has_searchable_text_fields() -> None:
    annotations = _visual_artifact_class().get("annotations") or {}
    searchable = annotations.get("okg_searchable_text_fields", "")
    fields = {f.strip() for f in searchable.split(",") if f.strip()}
    assert fields, "VisualArtifact should declare okg_searchable_text_fields"
    assert fields <= {"path", "source_url"}


def _narrowing(src: str, edge_archetype: str, dst: str) -> dict | None:
    for attrs in _edge_narrowings().values():
        annotations = attrs.get("annotations") or {}
        if (
            annotations.get("src_subtypes") == src
            and annotations.get("edge_archetype") == edge_archetype
            and annotations.get("dst_subtypes") == dst
        ):
            return annotations
    return None


def test_document_contains_visual_artifact_narrowing() -> None:
    assert _narrowing("Document", "contains", "VisualArtifact") is not None


def test_visual_artifact_member_of_document_narrowing() -> None:
    assert _narrowing("VisualArtifact", "member_of", "Document") is not None


def test_chunk_derived_from_visual_artifact_narrowing() -> None:
    assert _narrowing("DocumentChunk", "derived_from", "VisualArtifact") is not None


def test_visual_artifact_derived_from_source_file_narrowing() -> None:
    annotations = _narrowing("VisualArtifact", "derived_from", "SourceFile")
    assert annotations is not None
    # Cross-module (git_graph) — must be guarded so deployments without
    # git_graph loaded don't fail to compose.
    assert annotations.get("optional_when_subtypes_missing") == "true"
