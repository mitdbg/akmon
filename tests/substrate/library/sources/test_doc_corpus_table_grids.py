from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import psycopg
import pytest
import yaml

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.pdf_table_vlm import (
    TableRegion,
    TableRegionCandidate,
)
from okg.substrate.library.extraction.registry import ExtractorBundle
from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.base import EdgeFact, NodeFact, RecordSet, SourceRun
from okg.substrate.library.sources.doc_corpus import (
    DOC_CORPUS_EMISSION_SCHEMA_VERSION,
    DocCorpusSource,
    _chunk_node_id,
)
from tests.substrate._db_helpers import seed_catalog_version


_LATEX_EXTRACTORS = """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
  - format: text
    class: okg.substrate.library.extraction.parse.text.TextParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
  - format: default
    class: okg.substrate.library.extraction.chunk.window.WindowChunker
    config: {max_chars: 10, overlap: 0}
ner: []
"""


_TABLE = r"""\begin{table}
\begin{tabular}{lc}
Method & Score \\
Alpha & 10 \\
\end{tabular}
\end{table}"""


def _latex_adapter(tmp_path: Path, source: str) -> DocCorpusSource:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "paper.tex").write_text(source)
    (corpus / "legacy.txt").write_text("Stable legacy paragraph.")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_LATEX_EXTRACTORS)
    return DocCorpusSource(
        repo="table-source",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        include_globs=("**/*.tex", "**/*.txt"),
    )


def _facts(run) -> list[NodeFact | EdgeFact]:
    return list(run.facts)


def test_latex_table_occurrences_are_distinct_atomic_and_fully_provenanced(
    tmp_path: Path,
) -> None:
    malformed = r"""\begin{table}
\begin{tabular}{lc}Method & Score \\ missing \\ \end{tabular}
\end{table}"""
    adapter = _latex_adapter(
        tmp_path,
        r"\begin{document}" + "\n"
        "Intro paragraph.\n\n"
        + _TABLE
        + "\n"
        + _TABLE
        + "\n"
        + malformed
        + "\n"
        + r"\end{document}"
        + "\n",
    )
    run = adapter.run("first", mode="scope_complete")
    facts = _facts(run)
    table_nodes = [
        fact
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "document_chunk"
        and fact.attrs.get("chunk_kind") == "table"
    ]

    assert len(table_nodes) == 3
    assert len({fact.node_id for fact in table_nodes}) == 3
    assert all(fact.node_id.startswith("chunk:table:") for fact in table_nodes)
    parsed = [fact for fact in table_nodes if fact.attrs["table_grid_status"] == "parsed"]
    failed = [fact for fact in table_nodes if fact.attrs["table_grid_status"] != "parsed"]
    assert len(parsed) == 2 and len(failed) == 1
    assert parsed[0].attrs["table_grid"] == parsed[1].attrs["table_grid"]
    assert parsed[0].attrs["text"] == "Method\tScore\nAlpha\t10"
    assert failed[0].attrs["table_grid_error_code"] == "non_rectangular_table"
    assert "table_grid" not in failed[0].attrs
    assert all(fact.attrs["source_locator"].startswith("tex:") for fact in table_nodes)

    memberships = [
        fact
        for fact in facts
        if isinstance(fact, EdgeFact)
        and fact.edge_type == "member_of"
        and fact.src in {node.node_id for node in table_nodes}
    ]
    assert len(memberships) == 3
    by_id = {node.node_id: node for node in table_nodes}
    for edge in memberships:
        node = by_id[edge.src]
        for field in (
            "table_grid_status",
            "table_extraction_lane",
            "source_locator",
            "source_content_sha256",
        ):
            assert edge.attrs[field] == node.attrs[field]

    legacy = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "document_chunk"
        and fact.attrs.get("text") == "Stable leg"
    )
    assert legacy.node_id == _chunk_node_id("Stable leg")
    assert adapter.last_run_stats["tables_seen"] == 3
    assert adapter.last_run_stats["tables_parsed_by_lane"] == {"latex": 2}
    assert adapter.last_run_stats["table_failures_by_code"] == {
        "non_rectangular_table": 1
    }
    assert run.next_cursor is not None
    assert run.next_cursor["doc_corpus_emission_schema_version"] == (
        DOC_CORPUS_EMISSION_SCHEMA_VERSION
    )


def test_unchanged_cycle_reuses_record_set_without_parser_or_table_work(
    tmp_path: Path,
) -> None:
    adapter = _latex_adapter(
        tmp_path, f"\\begin{{document}}\n{_TABLE}\n\\end{{document}}"
    )
    first = adapter.run("first", mode="scope_complete")
    _facts(first)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    second = adapter.run("second", mode="scope_complete", cursor=cursor)
    assert _facts(second) == []
    assert adapter.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert adapter.last_run_stats["tables_seen"] == 0
    assert adapter.last_run_stats["table_provider_invocations"] == 0


class _LegacyTableEmission:
    """Emit one v2-style content-addressed table for migration proof."""

    name = "doc_corpus"
    profile = "discovery_crawl"

    def __init__(self, source: DocCorpusSource) -> None:
        self.source = source
        self.current_id = ""
        self.legacy_id = ""

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: dict[str, Any] | None = None,
    ) -> SourceRun:
        current = self.source.run(run_id, mode=mode, cursor=cursor)
        facts = list(current.facts)
        table = next(
            fact
            for fact in facts
            if isinstance(fact, NodeFact)
            and fact.attrs.get("chunk_kind") == "table"
        )
        self.current_id = table.node_id
        self.legacy_id = _chunk_node_id(table.attrs["text"], "table")

        def legacy_id(value: str) -> str:
            return self.legacy_id if value == self.current_id else value

        legacy_facts: list[NodeFact | EdgeFact] = []
        for fact in facts:
            if isinstance(fact, NodeFact):
                attrs = dict(fact.attrs)
                if fact.node_id == self.current_id:
                    attrs["chunk_id"] = self.legacy_id
                legacy_facts.append(
                    NodeFact(
                        node_id=legacy_id(fact.node_id),
                        subtype=fact.subtype,
                        attrs=attrs,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        op=fact.op,
                    )
                )
            else:
                legacy_facts.append(
                    EdgeFact(
                        src=legacy_id(fact.src),
                        dst=legacy_id(fact.dst),
                        edge_type=fact.edge_type,
                        attrs=fact.attrs,
                        provenance=fact.provenance,
                        confidence=fact.confidence,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        op=fact.op,
                    )
                )

        assert current.record_set is not None
        record_set_cursor = current.record_set.to_cursor()
        for record in record_set_cursor["records"].values():
            primary = record.get("primary")
            if primary is not None:
                primary["node_id"] = legacy_id(primary["node_id"])
            for node in record["descendants"]:
                node["node_id"] = legacy_id(node["node_id"])
            for edge in record["edges"]:
                edge["src"] = legacy_id(edge["src"])
                edge["dst"] = legacy_id(edge["dst"])

        next_cursor = dict(current.next_cursor or {})
        next_cursor["doc_corpus_emission_schema_version"] = 2
        next_cursor.pop("change_probe_token", None)
        return SourceRun(
            facts=legacy_facts,
            completed_scope=True,
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=RecordSet.from_cursor(record_set_cursor),
        )


@pytest.mark.postgres
def test_v2_table_identity_migrates_retracts_and_converges(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    source = _latex_adapter(
        tmp_path,
        f"\\begin{{document}}\n{_TABLE}\n\\end{{document}}",
    )
    source_name = "test.doc-corpus-table-grid-migration"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    runner = SourceRunner(clean_runtime, source_name=source_name)
    legacy = _LegacyTableEmission(source)
    runner.run(legacy, mode="scope_complete")
    assert legacy.current_id.startswith("chunk:table:")
    assert legacy.legacy_id != legacy.current_id

    migrated_source = DocCorpusSource(
        repo=source.repo,
        repo_root=source.repo_root, non_git_walk=True,
        extractors_dir=source.extractors_dir,
        include_globs=source.include_globs,
    )
    migrated = runner.run(migrated_source, mode="scope_complete")
    assert migrated_source.last_run_stats["scope_status"] == "full"
    assert migrated.next_cursor is not None
    assert migrated.next_cursor["doc_corpus_emission_schema_version"] == (
        DOC_CORPUS_EMISSION_SCHEMA_VERSION
    )

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        legacy_rows = store.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE source = :source AND node_id = :node_id",
            {"source": source_name, "node_id": legacy.legacy_id},
        )
        current_rows = store.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE source = :source AND node_id = :node_id",
            {"source": source_name, "node_id": legacy.current_id},
        )
        memberships = store.query(
            "SELECT src, dst FROM okg.graph_edges "
            "WHERE source = :source AND edge_type = 'member_of'",
            {"source": source_name},
        )
        assert legacy_rows == []
        assert current_rows[0]["attrs"]["table_grid_status"] == "parsed"
        assert legacy.legacy_id not in {row["src"] for row in memberships}
        assert legacy.current_id in {row["src"] for row in memberships}
    finally:
        store.close()

    converged_source = DocCorpusSource(
        repo=source.repo,
        repo_root=source.repo_root, non_git_walk=True,
        extractors_dir=source.extractors_dir,
        include_globs=source.include_globs,
    )
    converged = runner.run(converged_source, mode="scope_complete")
    assert (
        converged.nodes_appended
        + converged.edges_appended
        + converged.retractions_node
        + converged.retractions_edge
    ) == 0
    assert converged_source.last_run_stats["scope_status"] in {
        "full_unchanged_noop",
        "probe_short_circuit",
    }
    assert converged_source.last_run_stats.get("table_provider_invocations", 0) == 0


class _PdfParser:
    def parse(self, _content: bytes | str, **_kwargs: Any) -> ParsedDocument:
        return ParsedDocument(text="PDF text", chunks=[])


@dataclass
class _Locator:
    name: str = "recorded-locator"
    version: str = "v1"
    calls: int = 0

    def locate(
        self, _content: bytes, *, max_pages: int
    ) -> Sequence[TableRegionCandidate]:
        self.calls += 1
        assert max_pages == 10
        return [
            TableRegionCandidate(
                page_index=0,
                bbox=(1, 2, 11, 22),
                page_width=100,
                page_height=200,
                detector_name=self.name,
                detector_version=self.version,
            )
        ]


@dataclass
class _Renderer:
    name: str = "recorded-renderer"
    version: str = "v1"
    calls: int = 0

    def render_crop(self, _content: bytes, _region: TableRegion, *, dpi: int) -> bytes:
        self.calls += 1
        assert dpi == 72
        return b"recorded-crop"


@dataclass
class _Provider:
    model: str = "recorded/table-v1"
    external_network: bool = False
    calls: int = 0

    def transcribe(
        self,
        _crop: bytes,
        _prompt: str,
        *,
        fragment_id: str,
        outcome_key_digest: str,
    ) -> str:
        self.calls += 1
        assert fragment_id == "pdf:0:1.000,2.000,11.000,22.000"
        assert len(outcome_key_digest) == 64
        return json.dumps(
            {
                "table_grid": {
                    "schema_version": 1,
                    "column_count": 2,
                    "col_headers": ["Method", "Score"],
                    "row_label_column_index": 0,
                    "rows": [
                        {
                            "row_index": 0,
                            "row_label": "Alpha",
                            "cells": [
                                {
                                    "column_index": 0,
                                    "column_header": "Method",
                                    "text": "Alpha",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                                {
                                    "column_index": 1,
                                    "column_header": "Score",
                                    "text": "10",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                            ],
                        }
                    ],
                }
            }
        )


def _table_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "provider": "injected",
        "model": "recorded/table-v1",
        "prompt_version": "prompt-v1",
        "operator_version": "operator-v1",
        "dpi": 72,
        "max_pages": 10,
        "max_regions_per_page": 5,
        "max_tables_per_run": 10,
        "max_crop_pixels": 100_000,
        "max_crop_bytes": 10_000,
        "timeout_seconds": 30.0,
        "max_output_tokens": 512,
        "retry_count": 0,
        "temperature": 0.0,
        "tracking_mode": "disabled",
    }


def test_pdf_table_service_is_opt_in_cached_in_cursor_and_source_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "paper.pdf").write_bytes(b"recorded-pdf")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        yaml.safe_dump({"table_transcription": _table_config()})
    )
    monkeypatch.setattr(
        doc_corpus_module,
        "load_extractors",
        lambda _path: ExtractorBundle(
            parsers={"pdf": _PdfParser()}, chunker=None, ner=None
        ),
    )
    locator = _Locator()
    renderer = _Renderer()
    provider = _Provider()
    adapter = DocCorpusSource(
        repo="pdf-table-source",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        table_region_locator=locator,
        table_crop_renderer=renderer,
        table_transcription_provider=provider,
    )
    first = adapter.run("first", mode="scope_complete")
    facts = _facts(first)
    table = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.attrs.get("table_extraction_lane") == "pdf_vlm"
    )
    assert table.node_id.startswith("chunk:table:")
    assert table.attrs["source_page_index"] == 0
    assert table.attrs["source_bbox"] == ["1.000", "2.000", "11.000", "22.000"]
    assert table.attrs["table_grid_status"] == "parsed"
    assert adapter.last_run_stats["tables_parsed_by_lane"] == {"pdf_vlm": 1}
    assert adapter.last_run_stats["table_cache_misses"] == 1
    assert adapter.last_run_stats["table_provider_invocations"] == 1
    assert first.next_cursor and first.next_cursor["table_transcription_cache"]

    assert first.record_set is not None
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()
    second = adapter.run("second", mode="scope_complete", cursor=cursor)
    assert _facts(second) == []
    assert (locator.calls, renderer.calls, provider.calls) == (1, 1, 1)
    assert adapter.last_run_stats["table_provider_invocations"] == 0


def test_malformed_enabled_pdf_config_fails_before_any_source_emission(
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "paper.pdf").write_bytes(b"pdf")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    malformed = _table_config()
    malformed.pop("max_crop_bytes")
    (extractors / "extractors.yaml").write_text(
        yaml.safe_dump({"table_transcription": malformed})
    )
    adapter = DocCorpusSource(
        repo="pdf-table-source",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
    )

    with pytest.raises(ValueError, match="must name every bounded field"):
        adapter.run("bad", mode="scope_complete")
