"""Per `add-code-symbol-extraction` (2026-04-26): DocCorpusSource
emits code_symbol / code_import / code_reference graph nodes +
their edges when a Python file is parsed AND the
`PythonAstSymbolExtractor` is registered.

Asserts:
  - symbols, imports, references land in Chronos graph nodes;
  - `document contains code_symbol` edges connect each symbol
    to its parent document;
  - `code_symbol member_of code_symbol` for nested methods;
  - `document declares code_import` for each import.
"""
from __future__ import annotations

import textwrap
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


def _write_extractors(extr_dir: Path) -> None:
    extr_dir.mkdir(parents=True, exist_ok=True)
    (extr_dir / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "code",
            "class": (
                "okg.substrate.library.extraction.parse."
                "code_python.PythonAstParser"
            ),
        }],
    }))


def test_python_doc_emits_code_symbols(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    corpus = tmp_path / "c"
    corpus.mkdir()
    (corpus / "lib.py").write_text(textwrap.dedent('''
        """Test fixture for code-symbol extraction."""
        import os
        from os.path import join as J


        def alpha(x):
            """alpha returns its arg."""
            beta()
            return x


        def beta():
            return 0


        class C:
            """A class."""

            def method_one(self):
                helper()

            def method_two(self):
                self.method_one()
    ''').strip() + "\n")

    extr = tmp_path / "ext"
    _write_extractors(extr)

    SourceRunner(
        clean_runtime, source_name="test.code.symbols",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.py",),
            code_extension_to_language={".py": "python"},
        ),
        mode="scope_complete",
    )

    n_symbols = _graph_count(
        clean_runtime,
        "okg.graph_nodes",
        "source = 'test.code.symbols' AND subtype = 'code_symbol'",
    )
    n_imports = _graph_count(
        clean_runtime,
        "okg.graph_nodes",
        "source = 'test.code.symbols' AND subtype = 'code_import'",
    )
    n_refs = _graph_count(
        clean_runtime,
        "okg.graph_nodes",
        "source = 'test.code.symbols' AND subtype = 'code_reference'",
    )
    n_doc_contains_sym = _graph_count(
        clean_runtime,
        "okg.graph_edges",
        "source = 'test.code.symbols' "
        "AND edge_type = 'contains' "
        "AND src LIKE 'doc:t:%' "
        "AND dst LIKE 'symbol:%'",
    )
    n_symbol_member_of = _graph_count(
        clean_runtime,
        "okg.graph_edges",
        "source = 'test.code.symbols' "
        "AND edge_type = 'member_of' "
        "AND src LIKE 'symbol:%'",
    )
    n_doc_declares_import = _graph_count(
        clean_runtime,
        "okg.graph_edges",
        "source = 'test.code.symbols' "
        "AND edge_type = 'declares' "
        "AND src LIKE 'doc:t:%'",
    )
    n_calls = _graph_count(
        clean_runtime,
        "okg.graph_edges",
        "source = 'test.code.symbols' AND edge_type = 'calls'",
    )

    # 5 symbols: alpha, beta, C, C.method_one, C.method_two.
    assert n_symbols == 5, n_symbols
    # 2 imports: `import os`, `from os.path import join as J`.
    assert n_imports == 2, n_imports
    # 3 references: alpha→beta, C.method_one→helper,
    # C.method_two→self.method_one.
    assert n_refs == 3, n_refs
    # `document contains code_symbol`: 5.
    assert n_doc_contains_sym == 5, n_doc_contains_sym
    # `code_symbol member_of code_symbol`: 2 (the two methods).
    assert n_symbol_member_of == 2, n_symbol_member_of
    # `document declares code_import`: 2.
    assert n_doc_declares_import == 2, n_doc_declares_import
    # `code_symbol calls code_symbol`: 3 (within-file).
    assert n_calls == 3, n_calls


def test_code_symbols_are_in_record_set_footprint(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Regression (Wave B): the code_symbol / code_import /
    code_reference nodes + their contains/declares/calls/
    member_of edges MUST be folded into the source's record_set
    footprint. Otherwise the runner's record_set coverage gate
    flags every code-symbol row as a stale source-owned row and
    refuses to advance the watermark, and a second `scope_complete`
    run cannot reconcile.

    Without the fix, `last_applied_rev.record_set` carries the
    document/chunk/mention footprint only — none of the
    code-symbol nodes or edges — so this assertion fails.
    """
    from okg.substrate.library.sources.base import RecordSet

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    corpus = tmp_path / "c"
    corpus.mkdir()
    (corpus / "lib.py").write_text(textwrap.dedent('''
        """Fixture."""
        import os


        def alpha(x):
            beta()
            return x


        def beta():
            return 0


        class C:
            def method_one(self):
                helper()
    ''').strip() + "\n")

    extr = tmp_path / "ext"
    _write_extractors(extr)

    source = "test.code.footprint"
    SourceRunner(clean_runtime, source_name=source).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.py",),
            code_extension_to_language={".py": "python"},
        ),
        mode="scope_complete",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            " WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
    assert row is not None, "watermark did not advance"
    record_set = RecordSet.from_cursor(row[0]["record_set"])

    footprint_subtypes = {
        node.subtype
        for record in record_set.records.values()
        for node in record.descendants
    }
    footprint_edge_types = {
        edge.edge_type
        for record in record_set.records.values()
        for edge in record.edges
    }
    assert "code_symbol" in footprint_subtypes, footprint_subtypes
    assert "code_import" in footprint_subtypes, footprint_subtypes
    assert "code_reference" in footprint_subtypes, footprint_subtypes
    # `document contains code_symbol`, `document declares
    # code_import`, `code_symbol calls code_symbol`,
    # `code_symbol member_of code_symbol`.
    assert "declares" in footprint_edge_types, footprint_edge_types
    assert "calls" in footprint_edge_types, footprint_edge_types


def test_non_code_format_does_not_emit_code_symbols(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A markdown file (format != 'code') does NOT trigger
    the code-symbol extractor even though Python is
    registered."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    corpus = tmp_path / "c2"
    corpus.mkdir()
    (corpus / "note.md").write_text("# A doc\n\ndef foo(): pass\n")

    extr = tmp_path / "ext2"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": (
                "okg.substrate.library.extraction.parse."
                "markdown.MarkdownParser"
            ),
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.markdown.no.code",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True,
            extractors_dir=extr,
            include_globs=("**/*.md",),
        ),
        mode="scope_complete",
    )

    assert _graph_count(
        clean_runtime,
        "okg.graph_nodes",
        "source = 'test.markdown.no.code' AND subtype = 'code_symbol'",
    ) == 0


def _graph_count(dsn: str, table: str, where: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(f"SELECT count(*)::bigint AS n FROM {table} WHERE {where}")
    return int(rows[0]["n"]) if rows else 0
