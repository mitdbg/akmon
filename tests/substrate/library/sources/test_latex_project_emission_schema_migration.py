from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.scholarly.bib_references import (
    BibReferencesSource,
    LatexProjectLegacyMismatch,
)
from okg.substrate.library.sources.scholarly.latex_project import LatexProjectSource
from okg.substrate.library.sources.scholarly.our_drafts_source import (
    OurDraftsSource,
    compute_draft_id,
)
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


PROJECT_CONFIG = {
    "repository_authority": "papers",
    "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
}


def _cursor(run) -> dict:
    cursor = dict(run.next_cursor or {})
    assert run.record_set is not None
    cursor["record_set"] = run.record_set.to_cursor()
    return cursor


def _write_project(root: Path) -> tuple[Path, Path]:
    paper = root / "paper"
    paper.mkdir()
    tex = paper / "main.tex"
    bib = paper / "refs.bib"
    tex.write_text(
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}Text \cite{A}.\end{document}",
        encoding="utf-8",
    )
    bib.write_text("@article{A,title={A}}\n", encoding="utf-8")
    return tex, bib


def _strip_record_set_authority(dsn: str, owner: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s",
            (owner,),
        )
        cursor = dict(cur.fetchone()[0])
        for key in (
            "record_set", "record_set_ref", "record_set_summary",
            "record_set_chronos_proof",
        ):
            cursor.pop(key, None)
        cur.execute(
            "UPDATE okg.source_watermarks SET last_applied_rev = %s WHERE source = %s",
            (Jsonb(cursor), owner),
        )
    return cursor


def _owned_nodes(dsn: str, owner: str) -> set[str]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return {
            row["node_id"] for row in store.query(
                "SELECT node_id FROM okg.graph_nodes WHERE source = :source",
                {"source": owner},
            )
        }
    finally:
        store.close()


@pytest.mark.postgres
def test_bibliography_handoff_establishes_legacy_authority_then_scoped_state(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    _tex, bib_path = _write_project(tmp_path)
    owner = "fixture.bib_references"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    runner = SourceRunner(clean_runtime, source_name=owner)
    runner.run(
        BibReferencesSource(bib_paths=[str(bib_path)]),
        mode="scope_complete",
    )
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    assert "paper:bib:a" in _owned_nodes(clean_runtime, owner)
    _strip_record_set_authority(clean_runtime, owner)

    source = BibReferencesSource(
        source_root=tmp_path,
        project_manifest=PROJECT_CONFIG,
        migration={
            "legacy_ownership_id": owner,
            "legacy_bib_paths": [str(bib_path)],
        },
        dsn=clean_runtime,
        source_name=owner,
    )
    run_a = runner.run(source, mode="scope_complete")
    assert run_a.next_cursor["latex_project_migration_stage"] == "legacy_authority"
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    assert "paper:bib:a" in _owned_nodes(clean_runtime, owner)

    run_b = runner.run(source, mode="scope_complete")
    assert run_b.next_cursor["latex_project_migration_stage"] == "project_authority"
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    owned_b = _owned_nodes(clean_runtime, owner)
    assert "paper:bib:a" not in owned_b
    assert any(node_id.startswith("paper:bibscoped:") for node_id in owned_b)

    run_c = runner.run(source, mode="scope_complete")
    assert run_c.nodes_appended == 0
    assert run_c.edges_appended == 0
    assert _owned_nodes(clean_runtime, owner) == owned_b


@pytest.mark.postgres
def test_custom_our_drafts_handoff_is_explicit_and_two_run(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    tex_path, _bib_path = _write_project(tmp_path)
    legacy_id, _ = compute_draft_id(tex_path)
    owner = "fixture.our_drafts"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    runner = SourceRunner(clean_runtime, source_name=owner)
    runner.run(
        OurDraftsSource(draft_paths=[str(tex_path)]),
        mode="scope_complete",
    )
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    assert legacy_id in _owned_nodes(clean_runtime, owner)
    _strip_record_set_authority(clean_runtime, owner)

    original = tex_path.read_text(encoding="utf-8")
    tex_path.write_text(original + "changed", encoding="utf-8")
    mismatched = LatexProjectSource(
        tmp_path,
        PROJECT_CONFIG,
        migration={
            "legacy_ownership_id": owner,
            "legacy_draft_paths": [str(tex_path)],
        },
        source_name=owner,
        dsn=clean_runtime,
    )
    with pytest.raises(LatexProjectLegacyMismatch):
        runner.run(mismatched, mode="scope_complete")
    tex_path.write_text(original, encoding="utf-8")

    source = LatexProjectSource(
        tmp_path,
        PROJECT_CONFIG,
        migration={
            "legacy_ownership_id": owner,
            "legacy_draft_paths": [str(tex_path)],
        },
        source_name=owner,
        dsn=clean_runtime,
    )
    run_a = runner.run(source, mode="scope_complete")
    assert run_a.next_cursor["latex_project_migration_stage"] == "legacy_authority"
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    assert legacy_id in _owned_nodes(clean_runtime, owner)

    run_b = runner.run(source, mode="scope_complete")
    assert run_b.next_cursor["latex_project_migration_stage"] == "project_authority"
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"
    owned_b = _owned_nodes(clean_runtime, owner)
    assert legacy_id not in owned_b
    assert any(node_id.startswith("paper:latex:") for node_id in owned_b)

    run_c = runner.run(source, mode="scope_complete")
    assert run_c.nodes_appended == 0
    assert run_c.edges_appended == 0
    assert _owned_nodes(clean_runtime, owner) == owned_b


def test_fresh_project_enablement_does_not_claim_legacy_deletions(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    source = LatexProjectSource(tmp_path, PROJECT_CONFIG)
    run = source.run("fresh", mode="reconcile")
    assert run.record_set is not None
    assert "latex_project_migration_stage" not in run.next_cursor
    assert all(
        not node_id.startswith("paper:") or node_id.startswith("paper:latex:")
        for record in run.record_set.records.values()
        for node_id in record.nodes_by_id()
        if record.primary is not None and node_id == record.primary.node_id
    )
