from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE = ROOT / "docs/develop/architecture/overview.md"
PIPELINE = ROOT / "docs/develop/architecture/pipeline.md"
GLOSSARY = ROOT / "docs/reference/glossary.md"


def test_architecture_explains_working_branches_and_published_checkpoints() -> None:
    text = ARCHITECTURE.read_text(encoding="utf-8").lower()
    assert "mutable branch" in text
    assert "immutable checkpoint" in text
    assert "blocked publication" in text
    assert "previous published checkpoint" in text


def test_glossary_defines_current_storage_terms_without_migration_history() -> None:
    text = GLOSSARY.read_text(encoding="utf-8")
    assert "## Chronos" in text
    assert "## Generation" in text
    assert "immutable graph checkpoint" in text
    assert "Live-table compatibility history" not in text
    assert "Projector / projection compatibility history" not in text


def test_pipeline_names_current_graph_store_and_extraction_path() -> None:
    text = PIPELINE.read_text(encoding="utf-8")
    assert "ChronosGraphStore.apply_facts" in text
    assert "### Extractor and derived facts" in text
    assert "### Projector" not in text
    assert "## Projection" not in text
