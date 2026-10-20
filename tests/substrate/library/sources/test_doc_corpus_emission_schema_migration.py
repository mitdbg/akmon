from __future__ import annotations

import copy
import io
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from PIL import Image as PILImage
from psycopg.types.json import Jsonb

from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.captioning.service import CaptionResult
from okg.substrate.library.sources.doc_corpus import (
    DOC_CORPUS_EMISSION_SCHEMA_VERSION,
    DocCorpusSource,
    _chunk_node_id,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres


def _png() -> bytes:
    image = PILImage.new("RGB", (105, 105), color=(60, 90, 120))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _write_extractors(path: Path) -> None:
    (path / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "text",
            "class": "okg.substrate.library.extraction.parse.text.TextParser",
        }],
        "chunker": [{
            "format": "default",
            "class": (
                "okg.substrate.library.extraction.chunk.window.WindowChunker"
            ),
            "config": {"max_chars": 1000, "overlap": 100},
        }],
        "ner": [],
        "captioning": {
            "enabled": True,
            "mode": "coarse",
            "model": "chatgpt/gpt-4o",
            "min_image_px": 10_000,
        },
    }))


class _CaptionService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def caption(
        self, _image: object, context: str = "", **kwargs: Any
    ) -> list[CaptionResult]:
        self.calls.append({"context": context, **kwargs})
        return [CaptionResult(
            text="legacy migration caption",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


def _downgrade_authoritative_cursor(cursor: dict[str, Any]) -> tuple[str, str]:
    legacy = copy.deepcopy(cursor)
    legacy["doc_corpus_emission_schema_version"] = (
        DOC_CORPUS_EMISSION_SCHEMA_VERSION - 1
    )
    # A cursor produced before the emission schema joined the content-probe
    # config carries a token for the old config shape. Omitting it here is the
    # conservative equivalent: run_probe must report changed and enter adapter
    # migration rather than trusting source bytes alone.
    legacy.pop("change_probe_token", None)
    _, cached_visual = next(iter(legacy["visual_captions"].items()))
    cached = cached_visual["chunks"][0]
    current_id = cached["chunk_id"]
    legacy_id = _chunk_node_id(cached["text"])
    cached["chunk_id"] = legacy_id
    cached["attrs"]["chunk_id"] = legacy_id
    cached["attrs"].pop("chunk_kind", None)

    for record in legacy["record_set"]["records"].values():
        for node in record["descendants"]:
            if node["node_id"] == current_id:
                node["node_id"] = legacy_id
        for edge in record["edges"]:
            if edge["src"] == current_id:
                edge["src"] = legacy_id
            if edge["dst"] == current_id:
                edge["dst"] = legacy_id
    cursor.clear()
    cursor.update(legacy)
    return current_id, legacy_id


def test_source_runner_upgrades_authoritative_legacy_cursor_without_vlm(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_png())
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors(extractors)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    source_name = "test.doc-corpus-emission-schema-migration"
    current_emission_version = DOC_CORPUS_EMISSION_SCHEMA_VERSION
    monkeypatch.setattr(
        doc_corpus_module,
        "DOC_CORPUS_EMISSION_SCHEMA_VERSION",
        current_emission_version - 1,
    )
    first_service = _CaptionService()
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        DocCorpusSource(
            repo="migration-test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors,
            caption_service=first_service,
        ),
        mode="scope_complete",
    )
    assert len(first_service.calls) == 1
    assert first.next_cursor is not None
    assert first.next_cursor["doc_corpus_emission_schema_version"] == (
        current_emission_version - 1
    )
    assert "record_set" in first.next_cursor
    visual_id = next(iter(first.next_cursor["visual_captions"]))
    monkeypatch.setattr(
        doc_corpus_module,
        "DOC_CORPUS_EMISSION_SCHEMA_VERSION",
        current_emission_version,
    )
    current_id, legacy_id = _downgrade_authoritative_cursor(first.next_cursor)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.source_watermarks SET last_applied_rev = %s "
            "WHERE source = %s",
            (Jsonb(first.next_cursor), source_name),
        )
        assert cur.rowcount == 1

    second_service = _CaptionService()
    second_source = DocCorpusSource(
        repo="migration-test",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=second_service,
    )
    second = runner.run(
        second_source,
        mode="scope_complete",
    )
    assert second_service.calls == []
    assert second_source.last_run_stats["scope_status"] == "full"
    assert second_source.last_run_stats["files_changed"] == 1
    assert second_source.last_run_stats["emitted_nodes"] > 0
    assert second.next_cursor is not None
    assert second.next_cursor["doc_corpus_emission_schema_version"] == (
        DOC_CORPUS_EMISSION_SCHEMA_VERSION
    )
    cached = next(iter(second.next_cursor["visual_captions"].values()))["chunks"][0]
    assert cached["chunk_id"] == current_id != legacy_id
    assert cached["attrs"]["chunk_kind"] == "image_caption"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE source = :source AND node_id = :node_id",
            {"source": source_name, "node_id": current_id},
        )
        assert rows[0]["attrs"]["chunk_kind"] == "image_caption"
        visual_rows = store.query(
            "SELECT attrs, source_revision FROM okg.graph_nodes "
            "WHERE source = :source AND node_id = :node_id",
            {"source": source_name, "node_id": visual_id},
        )
        assert visual_rows[0]["attrs"]["caption_status"] == "reused"
        assert visual_rows[0]["source_revision"][
            "doc_corpus_emission_schema_version"
        ] == current_emission_version
    finally:
        store.close()


def test_emission_schema_defeats_git_frontier_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus-git"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_png())
    extractors = tmp_path / "extractors-git"
    extractors.mkdir()
    _write_extractors(extractors)
    frontier = {
        "schema": "okg.doc_corpus.git_frontier.v1",
        "head_sha": "fixture",
        "dirty_paths": [],
    }

    first_service = _CaptionService()
    first_source = DocCorpusSource(
        repo="migration-test",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=first_service,
        git_frontier_skip=True,
    )
    monkeypatch.setattr(first_source, "_git_frontier", lambda: frontier)
    first = first_source.run("first", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    _downgrade_authoritative_cursor(cursor)

    second_service = _CaptionService()
    second_source = DocCorpusSource(
        repo="migration-test",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=second_service,
        git_frontier_skip=True,
    )
    monkeypatch.setattr(second_source, "_git_frontier", lambda: frontier)
    second = second_source.run(
        "second", mode="scope_complete", cursor=cursor
    )
    facts = list(second.facts)
    assert second_service.calls == []
    assert second_source.last_run_stats["scope_status"] == "full"
    assert second_source.last_run_stats["emitted_nodes"] > 0
    assert second.next_cursor["doc_corpus_emission_schema_version"] == (
        DOC_CORPUS_EMISSION_SCHEMA_VERSION
    )
    assert any(
        getattr(fact, "attrs", {}).get("chunk_kind") == "image_caption"
        for fact in facts
    )


# -- Reparse epoch: new emission for unchanged files, no revision rewrite ----


def _write_yaml_extractors(path: Path) -> None:
    base = "okg.substrate.library.extraction"
    (path / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [
            {"format": "yaml", "class": f"{base}.parse.yaml_struct.YamlParser"},
        ],
        "chunker": [
            {"format": "default", "class": f"{base}.chunk.window.WindowChunker",
             "config": {"max_chars": 1500, "overlap": 200}},
        ],
    }))


def _yaml_chunk_kinds(dsn: str, source: str) -> dict[str, list[str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(f"""
            SELECT c.attrs->>'chunk_kind' AS kind, c.attrs->>'text' AS text
              FROM okg.graph_edges e
              JOIN okg.graph_nodes c ON c.node_id = e.src
              JOIN okg.graph_nodes d ON d.node_id = e.dst
             WHERE e.edge_type = 'member_of' AND d.source = '{source}'
               AND d.subtype = 'document'
        """)
    out: dict[str, list[str]] = {}
    for row in rows:
        out.setdefault(str(row["kind"]), []).append(str(row["text"]))
    return {kind: sorted(texts) for kind, texts in out.items()}


def _run_yaml(dsn: str, name: str, corpus: Path, extractors: Path) -> dict:
    source = DocCorpusSource(
        repo="t", repo_root=corpus, non_git_walk=True, extractors_dir=extractors,
    )
    SourceRunner(dsn, source_name=name).run(source, mode="scope_complete")
    return dict(source.last_run_stats or {})


@pytest.mark.parametrize("bump_epoch", [True, False])
def test_the_reparse_epoch_brings_yaml_original_text_to_an_existing_corpus(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bump_epoch: bool,
) -> None:
    """A corpus written before YAML kept its original text gains it once.

    The first run mimics the code before this change: the previous epoch and
    no original-text chunks. The second run, over the same bytes, is the new
    code. With the epoch bumped, the file re-parses and the comment reaches
    the graph. Without the bump (the failure case), the unchanged file is
    reused and the comment stays missing.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    name = f"test.reparse.epoch.{str(bump_epoch).lower()}"
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "values.yaml").write_text(
        "# quorum_rationale_marker: three replicas keep a majority.\nreplicas: 3\n"
    )
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_yaml_extractors(extractors)
    current_epoch = doc_corpus_module.DOC_CORPUS_REPARSE_EPOCH

    with monkeypatch.context() as old_code:
        old_code.setattr(
            doc_corpus_module, "DOC_CORPUS_REPARSE_EPOCH", current_epoch - 1,
        )
        old_code.setattr(
            DocCorpusSource, "_original_text_chunks", staticmethod(lambda *_: []),
        )
        _run_yaml(clean_runtime, name, corpus, extractors)
    before = _yaml_chunk_kinds(clean_runtime, name)
    assert "code_block" not in before and before.get("paragraph"), before

    if not bump_epoch:
        monkeypatch.setattr(
            doc_corpus_module, "DOC_CORPUS_REPARSE_EPOCH", current_epoch - 1,
        )
    stats = _run_yaml(clean_runtime, name, corpus, extractors)
    after = _yaml_chunk_kinds(clean_runtime, name)
    if not bump_epoch:
        assert stats.get("files_changed", 0) == 0, stats
        assert "code_block" not in after, after
        return
    assert stats.get("files_changed") == 1, stats
    assert any("quorum_rationale_marker" in t for t in after["code_block"]), after
    # The re-emission chunks are untouched.
    assert after["paragraph"] == before["paragraph"]
    # Unchanged runs after that re-parse nothing, twice in a row.
    for _ in range(2):
        stats = _run_yaml(clean_runtime, name, corpus, extractors)
        assert stats.get("files_changed", 0) == 0, stats


@pytest.mark.parametrize("bump_epoch", [True, False])
def test_the_reparse_epoch_rewrites_escaped_yaml_text_in_an_existing_corpus(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bump_epoch: bool,
) -> None:
    """A YAML file ingested with escaped text gets readable text once (#2179).

    The first run mimics the code before this change: the previous epoch and
    `yaml.safe_dump`'s escaped, folded re-emission. The second run, over the
    same bytes, is the new code. With the epoch bumped, the file re-parses and
    the stored chunk holds the characters as written; without it, the
    unchanged file is reused and the escaped chunk stays.
    """
    from okg.substrate.library.extraction.parse import yaml_struct

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    name = f"test.reparse.yaml_text.{str(bump_epoch).lower()}"
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "notes.yaml").write_text(
        'note: "A set ∩ is defined here"\n', encoding="utf-8",
    )
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_yaml_extractors(extractors)
    # The epoch before #2179, fixed (not current - 1) so the test fails if
    # the change ever ships without its bump.
    old_epoch = 4

    with monkeypatch.context() as old_code:
        old_code.setattr(doc_corpus_module, "DOC_CORPUS_REPARSE_EPOCH", old_epoch)
        old_code.setattr(
            yaml_struct, "_readable_dump",
            lambda data: yaml.safe_dump(
                data, default_flow_style=False, sort_keys=True,
            ),
        )
        _run_yaml(clean_runtime, name, corpus, extractors)
    before = _yaml_chunk_kinds(clean_runtime, name)
    assert before["paragraph"] == ['note: "A set \\u2229 is defined here"\n'], before

    if not bump_epoch:
        monkeypatch.setattr(doc_corpus_module, "DOC_CORPUS_REPARSE_EPOCH", old_epoch)
    stats = _run_yaml(clean_runtime, name, corpus, extractors)
    after = _yaml_chunk_kinds(clean_runtime, name)
    if not bump_epoch:
        assert stats.get("files_changed", 0) == 0, stats
        assert after["paragraph"] == before["paragraph"], after
        return
    assert stats.get("files_changed") == 1, stats
    assert after["paragraph"] == ["note: A set ∩ is defined here\n"], after
    # The file as written now equals the re-emission up to quoting only, so
    # its as-written chunk is still there and unchanged.
    assert after.get("code_block") == before.get("code_block"), after
    for _ in range(2):
        stats = _run_yaml(clean_runtime, name, corpus, extractors)
        assert stats.get("files_changed", 0) == 0, stats
