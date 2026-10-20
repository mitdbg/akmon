from __future__ import annotations

import re
from pathlib import Path

from tests.mirror_scope import private_path, shipped


REPO = Path(__file__).resolve().parents[2]


def test_active_embedding_docs_use_canonical_spelling_and_exact_scan() -> None:
    # deployments/okg-workspace/README.md is private; the public copy checks
    # the product docs, and the private page's own claim is checked below.
    files = shipped([
        REPO / "docs/develop/architecture/pipeline.md",
        REPO / "docs/reference/glossary.md",
        REPO / "docs/reference/capabilities.md",
        REPO / "docs/reference/error_codes.yaml",
        REPO / "docs/reference/mcp.md",
        REPO / "deployments/okg-workspace/README.md",
    ])
    joined = "\n".join(path.read_text(encoding="utf-8") for path in files)
    assert "embedding.enabled" not in joined
    assert "embedding.backend" not in joined
    assert "all-MiniLM-L6-v2" not in joined
    assert "exact cosine" in joined
    assert "no ANN index" in joined


def test_workspace_readme_states_there_is_no_2560_dimensional_recipe() -> None:
    text = private_path("deployments/okg-workspace/README.md").read_text(encoding="utf-8")
    assert "no supported\n2560-dimensional production backend recipe" in text


def test_shipped_deployment_yaml_uses_plural_key_only() -> None:
    roots = [REPO / "deployments", REPO / "src/okg/substrate/library"]
    offenders: list[str] = []
    for root in roots:
        for path in root.rglob("*.yaml"):
            if re.search(r"(?m)^embedding:\s*$", path.read_text(encoding="utf-8")):
                offenders.append(str(path.relative_to(REPO)))
    assert offenders == []


def test_no_known_incompatible_production_recipe_or_ann_claim() -> None:
    active = shipped([
        REPO / "docs/develop/architecture/pipeline.md",
        REPO / "docs/reference/mcp.md",
        REPO / "docs/reference/glossary.md",
        REPO / "docs/reference/error_codes.yaml",
        REPO / "deployments/okg-workspace/README.md",
    ])
    text = "\n".join(path.read_text(encoding="utf-8") for path in active)
    assert "all-MiniLM-L6-v2" not in text
    assert "uses a HNSW" not in text
    assert "HNSW index is" not in text
    assert "pin deployment.yaml/embedder.model to a 384-dim model" not in text
    assert "stores and indexes embeddings with IVFFlat or HNSW" not in text


def test_sentence_transformer_adapter_does_not_present_384_as_a_recipe() -> None:
    text = (
        REPO / "src/okg/substrate/embeddings/sentence_transformers.py"
    ).read_text(encoding="utf-8")
    assert "all-MiniLM-L6-v2` (384-dim)" in text
    assert "preflight therefore refuses it" in text
    assert "ships no supported 2560-dimensional production" in text
