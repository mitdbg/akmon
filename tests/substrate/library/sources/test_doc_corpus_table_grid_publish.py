from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import psycopg
import pytest
import yaml

from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)
from okg.substrate.library.extraction.chunk.window import WindowChunker
from okg.substrate.library.extraction.parse.latex import LatexParser
from okg.substrate.library.extraction.parse.text import TextParser
from okg.substrate.library.extraction.pdf_table_vlm import (
    DEFAULT_TABLE_PROMPT,
    TableCacheSignatureComponents,
    TableRegion,
    TableRegionCandidate,
)
from okg.substrate.library.extraction.registry import ExtractorBundle
from okg.substrate.library.extraction.table_grid import table_occurrence_id
from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.doc_corpus import DocCorpusSource, _chunk_node_id
from okg.substrate.llm.outcomes import prompt_content_hash
from okg.substrate.mcp.run_sql import _expand_composite_views, _rewrite_graph_views
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_TABLE = r"""\begin{table}
\resizebox{\linewidth}{!}{
\begin{tabular}{lc}
\toprule
Method & Score \\
\midrule
\resultrow{Alpha}{10} \\
\bottomrule
\end{tabular}}
\end{table}"""

_MALFORMED = r"""\begin{table}
\begin{tabular}{lc}Method & Score \\ missing \\ \end{tabular}
\end{table}"""

_LATEX = (
    r"\newcommand{\resultrow}[2]{#1 & #2}"
    + "\n"
    + r"\begin{document}"
    + "\n"
    + _TABLE
    + "\n"
    + _TABLE
    + "\n"
    + _MALFORMED
    + "\n"
    + r"\end{document}"
    + "\n"
)

_PDF_BYTES = b"recorded-pdf-table-fixture-v1"
_CROP_BYTES = b"recorded-pdf-table-crop-v1"
_LEGACY_TEXT = "Legacy paragraph identity remains stable."
_COMMAND = (
    "env -u OKG_DSN uv run --extra dev pytest "
    "tests/substrate/library/sources/test_doc_corpus_table_grid_publish.py "
    "--table-grid-report "
    ".scratch/structured-table-extraction/table-grid-publish-report.json"
)


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _graph_counts(store: Any) -> dict[str, int]:
    return {
        "nodes": int(
            store.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
        ),
        "edges": int(
            store.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
        ),
    }


class _PdfParser:
    def parse(self, _content: bytes | str, **_kwargs: Any) -> ParsedDocument:
        return ParsedDocument(text="Recorded PDF table fixture.", chunks=[])


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
        return _CROP_BYTES


@dataclass
class _Provider:
    model: str = "recorded/table-v1"
    external_network: bool = True
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
                                    "text": "42",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                            ],
                        },
                        {
                            "row_index": 1,
                            "row_label": "Beta",
                            "cells": [
                                {
                                    "column_index": 0,
                                    "column_header": "Method",
                                    "text": "Beta",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                                {
                                    "column_index": 1,
                                    "column_header": "Score",
                                    "text": "84",
                                    "row_span": 1,
                                    "column_span": 1,
                                },
                            ],
                        },
                    ],
                }
            }
        )


class _LegacyTableEmission:
    """Translate the first current run into the exact v2 table identity shape."""

    name = "doc_corpus"
    profile = "discovery_crawl"

    def __init__(self, source: DocCorpusSource) -> None:
        self.source = source
        self.current_to_legacy: dict[str, str] = {}

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: dict[str, Any] | None = None,
    ) -> SourceRun:
        current = self.source.run(run_id, mode=mode, cursor=cursor)
        facts = list(current.facts)
        self.current_to_legacy = {
            fact.node_id: _chunk_node_id(fact.attrs["text"], "table")
            for fact in facts
            if isinstance(fact, NodeFact)
            and fact.attrs.get("chunk_kind") == "table"
        }

        def legacy_id(value: str) -> str:
            return self.current_to_legacy.get(value, value)

        legacy_facts: list[NodeFact | EdgeFact] = []
        for fact in facts:
            if isinstance(fact, NodeFact):
                attrs = dict(fact.attrs)
                if fact.node_id in self.current_to_legacy:
                    attrs["chunk_id"] = legacy_id(fact.node_id)
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


def _source(
    *,
    corpus: Path,
    extractors: Path,
    locator: _Locator,
    renderer: _Renderer,
    provider: _Provider,
) -> DocCorpusSource:
    return DocCorpusSource(
        repo="table-proof",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        table_region_locator=locator,
        table_crop_renderer=renderer,
        table_transcription_provider=provider,
    )


def _expected_cell_rows(cache_signature: str) -> list[dict[str, Any]]:
    first = _LATEX.index(_TABLE)
    second = _LATEX.index(_TABLE, first + 1)
    latex_doc = "doc:table-proof:paper.tex"
    pdf_doc = "doc:table-proof:recorded.pdf"
    rows: list[dict[str, Any]] = []
    for offset in (first, second):
        locator = f"tex:{offset}:{offset + len(_TABLE)}"
        occurrence = table_occurrence_id(latex_doc, locator)
        for column_index, (header, text) in enumerate(
            (("Method", "Alpha"), ("Score", "10"))
        ):
            rows.append(
                {
                    "cell_key": f"{occurrence}:0:{column_index}",
                    "table_occurrence_id": occurrence,
                    "doc_id": latex_doc,
                    "path": "paper.tex",
                    "repo": "table-proof",
                    "table_extraction_lane": "latex",
                    "row_index": 0,
                    "row_label": "Alpha",
                    "column_index": column_index,
                    "column_header": header,
                    "cell_text": text,
                    "row_span": 1,
                    "column_span": 1,
                    "source_char_offset": offset,
                    "source_char_end": offset + len(_TABLE),
                    "source_page_index": None,
                    "source_bbox": None,
                    "source_content_sha256": hashlib.sha256(
                        _TABLE.encode()
                    ).hexdigest(),
                    "cache_signature": None,
                }
            )
    pdf_locator = "pdf:0:1.000,2.000,11.000,22.000"
    pdf_occurrence = table_occurrence_id(pdf_doc, pdf_locator)
    for row_index, (label, score) in enumerate((("Alpha", "42"), ("Beta", "84"))):
        for column_index, (header, text) in enumerate(
            (("Method", label), ("Score", score))
        ):
            rows.append(
                {
                    "cell_key": f"{pdf_occurrence}:{row_index}:{column_index}",
                    "table_occurrence_id": pdf_occurrence,
                    "doc_id": pdf_doc,
                    "path": "recorded.pdf",
                    "repo": "table-proof",
                    "table_extraction_lane": "pdf_vlm",
                    "row_index": row_index,
                    "row_label": label,
                    "column_index": column_index,
                    "column_header": header,
                    "cell_text": text,
                    "row_span": 1,
                    "column_span": 1,
                    "source_char_offset": None,
                    "source_char_end": None,
                    "source_page_index": 0,
                    "source_bbox": ["1.000", "2.000", "11.000", "22.000"],
                    "source_content_sha256": hashlib.sha256(_CROP_BYTES).hexdigest(),
                    "cache_signature": cache_signature,
                }
            )
    return sorted(
        rows,
        key=lambda row: (
            row["doc_id"],
            row["table_occurrence_id"],
            row["row_index"],
            row["column_index"],
        ),
    )


def _expected_table_chunks(cache_signature: str) -> list[dict[str, Any]]:
    latex_doc = "doc:table-proof:paper.tex"
    first = _LATEX.index(_TABLE)
    second = _LATEX.index(_TABLE, first + 1)
    expected: list[dict[str, Any]] = []
    for offset in (first, second):
        locator = f"tex:{offset}:{offset + len(_TABLE)}"
        expected.append(
            {
                "node_id": table_occurrence_id(latex_doc, locator),
                "lane": "latex",
                "status": "parsed",
                "error_code": None,
                "text": "Method\tScore\nAlpha\t10",
                "source_locator": locator,
                "source_content_sha256": hashlib.sha256(_TABLE.encode()).hexdigest(),
                "cache_signature": None,
            }
        )
    malformed_offset = _LATEX.index(_MALFORMED)
    malformed_locator = (
        f"tex:{malformed_offset}:{malformed_offset + len(_MALFORMED)}"
    )
    expected.append(
        {
            "node_id": table_occurrence_id(latex_doc, malformed_locator),
            "lane": "latex",
            "status": "parse_failed",
            "error_code": "non_rectangular_table",
            "text": _MALFORMED,
            "source_locator": malformed_locator,
            "source_content_sha256": hashlib.sha256(_MALFORMED.encode()).hexdigest(),
            "cache_signature": None,
        }
    )
    pdf_locator = "pdf:0:1.000,2.000,11.000,22.000"
    expected.append(
        {
            "node_id": table_occurrence_id(
                "doc:table-proof:recorded.pdf", pdf_locator
            ),
            "lane": "pdf_vlm",
            "status": "parsed",
            "error_code": None,
            "text": "Method\tScore\nAlpha\t42\nBeta\t84",
            "source_locator": pdf_locator,
            "source_content_sha256": hashlib.sha256(_CROP_BYTES).hexdigest(),
            "cache_signature": cache_signature,
        }
    )
    return sorted(expected, key=lambda row: row["node_id"])


def test_two_lane_fixture_publishes_exact_immutable_graph_report(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "paper.tex").write_text(_LATEX)
    (corpus / "recorded.pdf").write_bytes(_PDF_BYTES)
    (corpus / "legacy.txt").write_text(_LEGACY_TEXT)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        yaml.safe_dump({"table_transcription": _table_config()})
    )
    window = WindowChunker(max_chars=1_500, overlap=200)
    bundle = ExtractorBundle(
        parsers={
            "latex": LatexParser(),
            "pdf": _PdfParser(),
            "text": TextParser(),
        },
        chunker=window,
        ner=None,
        chunkers_by_format={
            "latex": LatexStructuralChunker(),
            "default": window,
        },
    )
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)

    locator = _Locator()
    renderer = _Renderer()
    provider = _Provider()
    source_name = "test.doc-corpus-table-grid-publish"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = _graph_counts(store)
    finally:
        store.close()

    runner = SourceRunner(clean_runtime, source_name=source_name)
    first_source = _source(
        corpus=corpus,
        extractors=extractors,
        locator=locator,
        renderer=renderer,
        provider=provider,
    )
    legacy = _LegacyTableEmission(first_source)
    runner.run(legacy, mode="scope_complete")
    first_stats = dict(first_source.last_run_stats)
    assert first_stats["table_provider_invocations"] == 1
    assert first_stats["table_external_network_attempts"] == 1
    assert first_stats["table_cache_misses"] == 1
    assert provider.calls == 1

    migration_source = _source(
        corpus=corpus,
        extractors=extractors,
        locator=locator,
        renderer=renderer,
        provider=provider,
    )
    migrated = runner.run(migration_source, mode="scope_complete")
    migration_stats = dict(migration_source.last_run_stats)
    assert migrated.retractions_node > 0
    assert migrated.retractions_edge > 0
    assert migration_stats["table_cache_hits"] == 1
    assert migration_stats["table_provider_invocations"] == 0
    assert migration_stats["table_external_network_attempts"] == 0
    assert provider.calls == 1

    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = published["generation_id"]
    described = describe_generation(clean_runtime, generation_id)
    assert described["status"] == "published", described

    no_change_source = _source(
        corpus=corpus,
        extractors=extractors,
        locator=locator,
        renderer=renderer,
        provider=provider,
    )
    no_change = runner.run(no_change_source, mode="scope_complete")
    no_change_stats = dict(no_change_source.last_run_stats)
    assert (
        no_change.nodes_appended
        + no_change.edges_appended
        + no_change.retractions_node
        + no_change.retractions_edge
    ) == 0
    assert no_change_stats.get("table_provider_invocations", 0) == 0
    assert no_change_stats.get("table_external_network_attempts", 0) == 0
    assert provider.calls == 1

    pdf_locator = "pdf:0:1.000,2.000,11.000,22.000"
    cache_signature = TableCacheSignatureComponents(
        document_content_sha256=hashlib.sha256(_PDF_BYTES).hexdigest(),
        crop_sha256=hashlib.sha256(_CROP_BYTES).hexdigest(),
        source_locator=pdf_locator,
        detector_name=locator.name,
        detector_version=locator.version,
        renderer_name=renderer.name,
        renderer_version=renderer.version,
        dpi=72,
        provider="injected",
        model=provider.model,
        prompt_digest=prompt_content_hash(DEFAULT_TABLE_PROMPT),
        prompt_version="prompt-v1",
        operator_version="operator-v1",
        max_output_tokens=512,
        timeout_seconds=30.0,
        retry_count=0,
        temperature=0.0,
    ).signature()
    expected_cells = _expected_cell_rows(cache_signature)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        after = _graph_counts(generation)
        table_nodes = generation.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE source = :source AND subtype = 'document_chunk'",
            {"source": source_name},
        )
        table_nodes = [
            row for row in table_nodes if row["attrs"].get("chunk_kind") == "table"
        ]
        table_nodes.sort(key=lambda row: row["node_id"])
        assert len(table_nodes) == 4
        assert len({row["node_id"] for row in table_nodes}) == 4
        assert all(row["node_id"].startswith("chunk:table:") for row in table_nodes)
        assert sum(
            row["attrs"]["table_grid_status"] == "parsed" for row in table_nodes
        ) == 3
        failed = [
            row
            for row in table_nodes
            if row["attrs"]["table_grid_status"] != "parsed"
        ]
        assert len(failed) == 1
        assert failed[0]["attrs"]["table_grid_error_code"] == (
            "non_rectangular_table"
        )
        assert "table_grid" not in failed[0]["attrs"]

        memberships = generation.query(
            "SELECT src, dst, attrs FROM okg.graph_edges "
            "WHERE source = :source AND edge_type = 'member_of'",
            {"source": source_name},
        )
        table_ids = {row["node_id"] for row in table_nodes}
        table_memberships = [row for row in memberships if row["src"] in table_ids]
        assert len(table_memberships) == 4
        latex_parsed_ids = {
            row["node_id"]
            for row in table_nodes
            if row["attrs"]["table_extraction_lane"] == "latex"
            and row["attrs"]["table_grid_status"] == "parsed"
        }
        assert len(latex_parsed_ids) == 2
        assert len(
            [row for row in table_memberships if row["src"] in latex_parsed_ids]
        ) == 2

        stale_legacy_ids = set(legacy.current_to_legacy.values())
        assert not stale_legacy_ids & table_ids
        assert not stale_legacy_ids & {row["src"] for row in memberships}
        assert generation.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = :node_id AND subtype = 'document_chunk'",
            {"node_id": _chunk_node_id(_LEGACY_TEXT)},
        ) == [{"node_id": _chunk_node_id(_LEGACY_TEXT)}]

        cell_sql = (
            "SELECT cell_key, table_occurrence_id, doc_id, path, repo, "
            "table_extraction_lane, row_index, row_label, column_index, "
            "column_header, cell_text, row_span, column_span, "
            "source_char_offset, source_char_end, source_page_index, "
            "source_bbox, source_content_sha256, cache_signature "
            "FROM okg.v_table_cells WHERE repo = 'table-proof' "
            "ORDER BY doc_id, table_occurrence_id, row_index, column_index"
        )
        with psycopg.connect(clean_runtime) as conn:
            cell_query = _rewrite_graph_views(
                _expand_composite_views(cell_sql, conn)
            )
        cells = generation.query(cell_query)
        assert cells == expected_cells
        assert not any(
            row["table_occurrence_id"] == failed[0]["node_id"] for row in cells
        )
        assert generation.query(
            "SELECT count(*)::int AS n FROM okg.graph_nodes "
            "WHERE subtype = 'table_cell'"
        ) == [{"n": 0}]
    finally:
        store.close()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int "
            "FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert projection_queue == 0
    assert dlq_count == 0
    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor

    observed_table_chunks = [
        {
            "node_id": row["node_id"],
            "lane": row["attrs"]["table_extraction_lane"],
            "status": row["attrs"]["table_grid_status"],
            "error_code": row["attrs"].get("table_grid_error_code"),
            "text": row["attrs"]["text"],
            "source_locator": row["attrs"]["source_locator"],
            "source_content_sha256": row["attrs"]["source_content_sha256"],
            "cache_signature": row["attrs"].get("cache_signature"),
        }
        for row in table_nodes
    ]
    expected_table_chunks = _expected_table_chunks(cache_signature)
    assert observed_table_chunks == expected_table_chunks
    oracle = {
        "table_chunks": expected_table_chunks,
        "table_cells": expected_cells,
    }
    oracle_digest = _canonical_digest(oracle)
    delta = {
        "nodes": after["nodes"] - before["nodes"],
        "edges": after["edges"] - before["edges"],
    }
    report_data = {
        "schema": "okg.table-grid-publish-report.v1",
        "generation_id": generation_id,
        "generation_status": described["status"],
        "command": _COMMAND,
        "oracle_digest": oracle_digest,
        "oracle": oracle,
        "graph_counts": {"before": before, "after": after, "delta": delta},
        "table_counts": {
            "occurrences": 4,
            "parsed": 3,
            "failed": 1,
            "cells": len(expected_cells),
            "materialized_table_cell_nodes": 0,
            "duplicate_latex_occurrences": len(latex_parsed_ids),
        },
        "provider_cache_counters": {
            "first_cycle": {
                "provider_invocations": first_stats["table_provider_invocations"],
                "external_network_attempts": first_stats[
                    "table_external_network_attempts"
                ],
                "cache_hits": first_stats["table_cache_hits"],
                "cache_misses": first_stats["table_cache_misses"],
            },
            "migration_cycle": {
                "provider_invocations": migration_stats[
                    "table_provider_invocations"
                ],
                "external_network_attempts": migration_stats[
                    "table_external_network_attempts"
                ],
                "cache_hits": migration_stats["table_cache_hits"],
                "cache_misses": migration_stats["table_cache_misses"],
            },
            "no_change_cycle": {
                "provider_invocations": no_change_stats.get(
                    "table_provider_invocations", 0
                ),
                "external_network_attempts": no_change_stats.get(
                    "table_external_network_attempts", 0
                ),
            },
            "provider_calls_total": provider.calls,
        },
        "migration": {
            "node_retractions": migrated.retractions_node,
            "edge_retractions": migrated.retractions_edge,
            "legacy_ids": sorted(stale_legacy_ids),
            "stale_nodes": 0,
            "stale_memberships": 0,
            "converged_no_change": True,
        },
        "readiness": {
            "projection_queue": projection_queue,
            "dlq_count": dlq_count,
            "doctor_ok": doctor["ok"],
            "doctor": doctor,
        },
    }
    evidence_data = {
        "generation_ref": generation_id,
        "generation_status": described["status"],
        "oracle_digest": oracle_digest,
        "nodes_before": before["nodes"],
        "nodes_after": after["nodes"],
        "nodes_delta": delta["nodes"],
        "edges_before": before["edges"],
        "edges_after": after["edges"],
        "edges_delta": delta["edges"],
        "provider_invocations": first_stats["table_provider_invocations"],
        "external_network_attempts": first_stats[
            "table_external_network_attempts"
        ],
        "no_change_provider_invocations": no_change_stats.get(
            "table_provider_invocations", 0
        ),
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor_ok": doctor["ok"],
    }
    report = {
        "report": report_data,
        "evidence": [
            {
                "kind": "published_generation",
                "status": "pass",
                "refs": [
                    "req.table-grid.publish-proof",
                    "acc.table-grid.publish-proof",
                    "task.table-grid.publish-proof",
                    "publish.table-grid.fixture",
                ],
                "summary": (
                    "Published the exact two-lane fixture with four table "
                    "occurrences, eight anchor cells, v2 retraction, and zero "
                    "queue/DLQ."
                ),
                "command": _COMMAND,
                "generation_ref": generation_id,
                "data": dict(evidence_data),
            },
            {
                "kind": "graph_report",
                "status": "pass",
                "refs": [
                    "req.table-grid.publish-proof",
                    "task.table-grid.publish-proof",
                    "graph.table-grid.fixture",
                ],
                "summary": (
                    "Chronos-pinned table chunks and cells matched the immutable "
                    "oracle; migration, cache, and readiness bounds passed."
                ),
                "command": _COMMAND,
                "generation_ref": generation_id,
                "data": dict(evidence_data),
            },
        ],
    }
    artifact_digest = _canonical_digest(report)
    report["report"]["artifact_digest"] = artifact_digest
    report["report"]["artifact_digest_scope"] = (
        "canonical JSON before artifact_digest and artifact_digest_scope "
        "fields are inserted"
    )
    for row in report["evidence"]:
        row["data"]["artifact_digest"] = artifact_digest

    configured_path = request.config.getoption("--table-grid-report")
    artifact = Path(
        configured_path
        or ".scratch/structured-table-extraction/table-grid-publish-report.json"
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    written = json.loads(artifact.read_text())
    written_artifact_digest = written["report"].pop("artifact_digest")
    assert written_artifact_digest == artifact_digest
    written["report"].pop("artifact_digest_scope")
    for row in written["evidence"]:
        row_artifact_digest = row["data"].pop("artifact_digest")
        assert row_artifact_digest == artifact_digest
    assert _canonical_digest(written) == artifact_digest
