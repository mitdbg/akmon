"""Stride A.1 — W9: a file with an unknown extension AND no
sniffable magic bytes emits a Document with
`parse_warnings` carrying `no_format` (not silently skipped).
A second case: extension maps to a format but no parser is
registered for that format → `parse_warnings: 'no_parser'`.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource


pytestmark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )


def _write_minimal_extractors(extr_dir: Path) -> None:
    extr_dir.mkdir(parents=True, exist_ok=True)
    (extr_dir / "extractors.yaml").write_text(yaml.safe_dump({
        # Markdown only — JSON/text/etc. NOT registered.
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }],
    }))


def test_unknown_extension_emits_no_format_warning(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    corpus = tmp_path / "c"
    corpus.mkdir()
    # `.weird` extension — not in DEFAULT_EXTENSION_TO_FORMAT;
    # bytes that don't trigger any sniff_format magic.
    (corpus / "thing.weird").write_bytes(b"opaque blob")

    extr = tmp_path / "e"
    _write_minimal_extractors(extr)

    SourceRunner(
        clean_runtime, source_name="test.unknown.format",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*",),
        ),
        mode="scope_complete",
    )

    warnings = _graph_scalar(
        clean_runtime,
        """
        SELECT attrs->>'parse_warnings' AS value
          FROM okg.graph_nodes
         WHERE source='test.unknown.format'
           AND subtype='document'
           AND attrs->>'path'='thing.weird'
        """,
    )
    n_chunks = _graph_scalar(
        clean_runtime,
        """
        SELECT count(*)::bigint AS value
          FROM okg.graph_nodes
         WHERE source='test.unknown.format'
           AND subtype='document_chunk'
        """,
    )
    assert "no_format" in (warnings or ""), warnings
    assert n_chunks == 0, n_chunks


def test_known_extension_no_parser_emits_no_parser_warning(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    corpus = tmp_path / "c2"
    corpus.mkdir()
    # `.json` is in DEFAULT_EXTENSION_TO_FORMAT but the
    # extractors.yaml below registers ONLY markdown — so
    # parser_for('json') raises ParserNotFound.
    (corpus / "data.json").write_text('{"k": 1}')

    extr = tmp_path / "e2"
    _write_minimal_extractors(extr)

    SourceRunner(
        clean_runtime, source_name="test.no.parser",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.json",),
        ),
        mode="scope_complete",
    )

    warnings = _graph_scalar(
        clean_runtime,
        """
        SELECT attrs->>'parse_warnings' AS value
          FROM okg.graph_nodes
         WHERE source='test.no.parser'
           AND subtype='document'
           AND attrs->>'path'='data.json'
        """,
    )
    assert "no_parser" in (warnings or ""), warnings


def _graph_scalar(dsn: str, sql: str):
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(sql)
    return rows[0]["value"] if rows else None
