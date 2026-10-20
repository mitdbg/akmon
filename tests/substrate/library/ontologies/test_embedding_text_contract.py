"""Embedding text authority is canonical stamped ``attrs.text`` only."""

from __future__ import annotations

import ast
from pathlib import Path

import yaml


REPO = Path(__file__).resolve().parents[4]
ONTOLOGIES = REPO / "src/okg/substrate/library/ontologies"


def _annotation_occurrences(value: object, *, path: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key == "okg_embed_text_field":
                found.append(child_path)
            found.extend(_annotation_occurrences(child, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_annotation_occurrences(child, path=f"{path}[{index}]"))
    return found


def test_no_active_ontology_declares_dead_embedding_text_annotation() -> None:
    offenders: list[str] = []
    for schema in ONTOLOGIES.rglob("schema.yaml"):
        raw = yaml.safe_load(schema.read_text(encoding="utf-8"))
        for occurrence in _annotation_occurrences(raw):
            offenders.append(f"{schema.relative_to(REPO)}:{occurrence}")
    assert offenders == []


def test_embedding_batch_reads_only_canonical_stamped_attrs_text() -> None:
    source_path = REPO / "src/okg/substrate/runtime/embed_workflow.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    batch = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_run_one_embed_batch"
    )
    body = ast.get_source_segment(source, batch) or ""
    assert "attrs->>'text' AS text" in body
    assert "attrs ? 'text'" in body
    assert "okg_embed_text_field" not in body


def test_searchable_text_annotation_is_the_only_ontology_text_selector() -> None:
    source = (REPO / "src/okg/substrate/catalog/modules_compose.py").read_text(
        encoding="utf-8"
    )
    assert 'annotations.get("okg_searchable_text_fields")' in source
    assert "okg_embed_text_field" not in source
