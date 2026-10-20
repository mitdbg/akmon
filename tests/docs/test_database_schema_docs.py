from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "reference" / "database-schema.md"


def test_database_schema_reference_is_a_current_map() -> None:
    text = DOC.read_text(encoding="utf-8")

    for heading in (
        "## Source of truth",
        "## Schema design rules",
        "## Relation families",
        "## Views and read surfaces",
        "## Schema and grant checks",
    ):
        assert heading in text

    for authority in (
        "src/okg/substrate/db/schema.sql",
        "src/okg/substrate/db/views.sql",
        "src/okg/substrate/db/migrations/manifest.yaml",
    ):
        assert authority in text
        assert (ROOT / authority).is_file()

    for landmark in (
        "okg.graph_nodes",
        "okg.graph_edges",
        "okg.graph_generations",
        "okg.node_facts",
        "okg.edge_facts",
        "okg.v_nodes",
        "okg.v_edges",
        "okg.v_runtime_queue_health",
    ):
        assert f"`{landmark}`" in text


def test_database_schema_reference_does_not_duplicate_change_history() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "## Migration Artifacts" not in text
    assert "## Audit Checklist" not in text
    assert "Migration drift" not in text
    assert "0002_remove_job_runtime_subsystem.sql" not in text
    assert len(text.splitlines()) <= 150


def test_database_schema_reference_is_linked_from_build_and_nav_paths() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    build_guide = (ROOT / "docs" / "develop" / "architecture" / "consistency.md").read_text(
        encoding="utf-8",
    )

    assert "Database schema: reference/database-schema.md" in mkdocs
    assert "[Database schema](../../reference/database-schema.md)" in build_guide
