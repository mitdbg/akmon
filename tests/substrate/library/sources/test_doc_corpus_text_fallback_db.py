"""DocCorpusSource `text_fallback`, end to end through the SourceRunner:

- an extension-less script with a python shebang becomes code chunks
  (PythonSymbolChunker), with no code symbols (its document is not a source_file);
- an unknown-extension text file (.sh) becomes text chunks;
- a structured parse failure (templated YAML, Python 2) is read and chunked as
  text, and the document records both `parse_failed:*` and `text_fallback:<format>`
  -- the fallback is never silent;
- code chunks carry start_line / end_line / symbol on their member_of edge;
- a binary file keeps the no_format behaviour (no chunks);
- without `text_fallback` the same unknown / extension-less files get no chunks;
- turning `text_fallback` on for an unchanged corpus re-reads those files.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource

pytestmark = pytest.mark.postgres

SCRIPT = (
    "#!/usr/bin/env python3\n"
    "import sys\n\n\n"
    "def set_capacity(rse):\n"
    "    threshold = rse_min_free_space_percentage(rse)\n"
    "    return threshold\n"
)


def _seed_cv(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )


def _write_extractors(extr_dir: Path) -> None:
    extr_dir.mkdir(parents=True, exist_ok=True)
    base = "okg.substrate.library.extraction"
    (extr_dir / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [
            {"format": "text", "class": f"{base}.parse.text.TextParser"},
            {"format": "yaml", "class": f"{base}.parse.yaml_struct.YamlParser"},
            {"format": "code", "class": f"{base}.parse.code_python.PythonAstParser"},
        ],
        "chunker": [
            {"format": "code", "class": f"{base}.chunk.python_symbols.PythonSymbolChunker",
             "config": {"max_chars": 4000, "overlap": 400}},
            {"format": "default", "class": f"{base}.chunk.window.WindowChunker",
             "config": {"max_chars": 1500, "overlap": 200}},
        ],
    }))


def _corpus(root: Path) -> None:
    root.mkdir()
    (root / "setCapacity").write_text(SCRIPT)
    (root / "run.sh").write_text("#!/bin/bash\necho reaper_restart_marker\n")
    (root / "values.yaml").write_text("replicas: {{ .Values.reaperCount }}\nname: tape-reaper\n")
    (root / "logo.bin").write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
    (root / "legacy.py").write_text("print 'python2_marker'\n")


def _rows(dsn: str, sql: str) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        return ChronosGraphStore.from_connection(conn, ensure_schema=False).query(sql)


def _run(dsn: str, tmp_path: Path, source: str, **kw) -> None:
    corpus = tmp_path / f"c-{source}"
    if not corpus.exists():
        _corpus(corpus)
    extr = tmp_path / f"e-{source}"
    _write_extractors(extr)
    SourceRunner(dsn, source_name=source).run(
        DocCorpusSource(repo="t", repo_root=corpus, non_git_walk=True,
                        extractors_dir=extr, include_globs=("**/*",), **kw),
        mode="scope_complete",
    )


def _doc(dsn: str, source: str, path: str) -> dict:
    rows = _rows(dsn, f"""
        SELECT d.attrs->>'parse_warnings' AS warnings,
               (SELECT count(*)::bigint FROM okg.graph_edges e
                 WHERE e.edge_type = 'member_of' AND e.dst = d.node_id) AS chunks,
               (SELECT string_agg(c.attrs->>'text', ' ') FROM okg.graph_edges e
                  JOIN okg.graph_nodes c ON c.node_id = e.src
                 WHERE e.edge_type = 'member_of' AND e.dst = d.node_id) AS text,
               (SELECT string_agg(DISTINCT c.attrs->>'chunk_kind', ',') FROM okg.graph_edges e
                  JOIN okg.graph_nodes c ON c.node_id = e.src
                 WHERE e.edge_type = 'member_of' AND e.dst = d.node_id) AS kinds,
               (SELECT string_agg(DISTINCT e.attrs->>'chunker_name', ',') FROM okg.graph_edges e
                 WHERE e.edge_type = 'member_of' AND e.dst = d.node_id) AS chunkers,
               (SELECT bool_and(e.attrs ? 'start_line' AND e.attrs ? 'end_line'
                                AND e.attrs ? 'symbol')
                  FROM okg.graph_edges e
                 WHERE e.edge_type = 'member_of' AND e.dst = d.node_id) AS edge_lines
          FROM okg.graph_nodes d
         WHERE d.source = '{source}' AND d.subtype = 'document' AND d.attrs->>'path' = '{path}'
    """)
    assert rows, f"no document node for {path}"
    return rows[0]


def test_text_fallback_reads_unknown_extensionless_and_unparseable_files(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    src = "test.text.fallback.on"
    _run(clean_runtime, tmp_path, src, text_fallback=True)

    script = _doc(clean_runtime, src, "setCapacity")
    assert script["chunks"] >= 1 and "rse_min_free_space_percentage" in script["text"]
    assert script["kinds"] == "code_block"
    assert script["chunkers"] == "PythonSymbolChunker"
    assert script["edge_lines"] is True  # per-occurrence lines live on the edge
    symbols = _rows(clean_runtime, f"""
        SELECT count(*)::bigint AS n FROM okg.graph_nodes
         WHERE source = '{src}' AND subtype = 'code_symbol'
    """)
    assert symbols[0]["n"] == 0  # extension-less scripts get chunks, not symbols

    shell = _doc(clean_runtime, src, "run.sh")
    assert shell["chunks"] >= 1 and "reaper_restart_marker" in shell["text"]

    templated = _doc(clean_runtime, src, "values.yaml")
    assert templated["chunks"] >= 1 and ".Values.reaperCount" in templated["text"]
    assert "parse_failed:" in (templated["warnings"] or "")
    assert "text_fallback:yaml" in (templated["warnings"] or "")

    # A .py file that does not parse: chunked as text, not as code.
    legacy = _doc(clean_runtime, src, "legacy.py")
    assert "python2_marker" in legacy["text"]
    assert "text_fallback:code" in (legacy["warnings"] or "")
    assert legacy["chunkers"] == "WindowChunker"

    binary = _doc(clean_runtime, src, "logo.bin")
    assert binary["chunks"] == 0 and "no_format" in (binary["warnings"] or "")


def test_without_text_fallback_those_files_have_no_chunks(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    src = "test.text.fallback.off"
    _run(clean_runtime, tmp_path, src)
    for path in ("setCapacity", "run.sh", "values.yaml"):
        assert _doc(clean_runtime, src, path)["chunks"] == 0, path


def test_turning_text_fallback_on_rereads_an_unchanged_corpus(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    src = "test.text.fallback.toggle"
    _run(clean_runtime, tmp_path, src)
    assert _doc(clean_runtime, src, "run.sh")["chunks"] == 0
    _run(clean_runtime, tmp_path, src, text_fallback=True)  # same bytes, new setting
    for path in ("setCapacity", "run.sh", "values.yaml"):
        assert _doc(clean_runtime, src, path)["chunks"] >= 1, path


# -- An extractors.yaml edit reaches files whose bytes did not change (#2671) --

MODULE = (
    '"""Quota helpers."""\n\n\n'
    "def compute_quota(site):\n"
    '    """Return the quota for a site."""\n'
    "    limit = body_only_quota_marker(site)\n"
    "    return limit\n"
)


def _write_docstring_only_extractors(extr_dir: Path, *, comment: str = "") -> None:
    """The Python parser with no `code` chunker: chunks hold docstrings only."""
    extr_dir.mkdir(parents=True, exist_ok=True)
    base = "okg.substrate.library.extraction"
    body = yaml.safe_dump({
        "parser": [
            {"format": "code", "class": f"{base}.parse.code_python.PythonAstParser"},
        ],
        "chunker": [
            {"format": "default", "class": f"{base}.chunk.window.WindowChunker",
             "config": {"max_chars": 1500, "overlap": 200}},
        ],
    })
    (extr_dir / "extractors.yaml").write_text(comment + body)


def _module_source(corpus: Path, extr: Path, **kw) -> DocCorpusSource:
    return DocCorpusSource(repo="t", repo_root=corpus, non_git_walk=True,
                           extractors_dir=extr,
                           include_globs=("**/*.py",), **kw)


def _run_source(dsn: str, name: str, source: DocCorpusSource) -> dict:
    SourceRunner(dsn, source_name=name).run(source, mode="scope_complete")
    return dict(source.last_run_stats or {})


def _live_chunk_texts(dsn: str, name: str) -> list[str]:
    rows = _rows(dsn, f"""
        SELECT c.attrs->>'text' AS text FROM okg.graph_edges e
          JOIN okg.graph_nodes c ON c.node_id = e.src
          JOIN okg.graph_nodes d ON d.node_id = e.dst
         WHERE e.edge_type = 'member_of' AND d.source = '{name}'
           AND d.subtype = 'document' AND d.attrs->>'path' = 'quota.py'
    """)
    return sorted(str(r["text"]) for r in rows)


def _setup_module_corpus(tmp_path: Path, name: str) -> tuple[Path, Path]:
    corpus = tmp_path / f"c-{name}"
    corpus.mkdir()
    (corpus / "quota.py").write_text(MODULE)
    extr = tmp_path / f"e-{name}"
    _write_docstring_only_extractors(extr)
    return corpus, extr


def test_registering_a_chunker_rechunks_an_unchanged_corpus(
    clean_runtime: str, tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    name = "test.extractors.rechunk"
    corpus, extr = _setup_module_corpus(tmp_path, name)

    _run_source(clean_runtime, name, _module_source(corpus, extr))
    before = _live_chunk_texts(clean_runtime, name)
    assert before and not any("body_only_quota_marker" in t for t in before), before

    # Same file bytes; only the extractor config changes.
    _write_extractors(extr)
    stats = _run_source(clean_runtime, name, _module_source(corpus, extr))
    assert stats.get("files_changed") == 1, stats
    after = _live_chunk_texts(clean_runtime, name)
    assert any("body_only_quota_marker" in t for t in after), after
    # The docstring-only chunks were retracted, not left beside the new ones.
    assert not set(before) & set(after), (before, after)

    # A comment-only edit parses to the same config: every file is reused.
    (extr / "extractors.yaml").write_text(
        "# a comment\n" + (extr / "extractors.yaml").read_text()
    )
    stats = _run_source(clean_runtime, name, _module_source(corpus, extr))
    assert stats.get("files_changed") == 0, stats
    # Nothing changed at all, twice: a no-change run must persist every key a
    # reuse gate reads (the fingerprint, the schema version), or the run
    # after it re-parses everything again.
    for _ in range(2):
        stats = _run_source(clean_runtime, name, _module_source(corpus, extr))
        assert stats.get("files_changed", 0) == 0, stats
    assert _live_chunk_texts(clean_runtime, name) == after


def test_a_cursor_without_an_extractors_fingerprint_rereads(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A cursor written before the fingerprint existed counts as changed."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    name = "test.extractors.legacy"
    corpus, extr = _setup_module_corpus(tmp_path, name)
    _run_source(clean_runtime, name, _module_source(corpus, extr))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        updated = conn.execute(
            "UPDATE okg.source_watermarks"
            "   SET last_applied_rev = last_applied_rev"
            "       - 'doc_corpus_extractors_fingerprint'"
            " WHERE source = %s AND last_applied_rev ? 'doc_corpus_extractors_fingerprint'",
            (name,),
        ).rowcount
    assert updated == 1
    # Nothing else differs from the prior run: the same bytes and the same
    # config, so only the missing fingerprint can force the re-read.
    stats = _run_source(clean_runtime, name, _module_source(corpus, extr))
    assert stats.get("files_changed") == 1, stats


def test_a_scoped_run_keeps_the_prior_fingerprint(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A scoped run re-parses only its slice, so it must not claim the new config.

    Otherwise the next complete run would reuse the files outside the slice
    and keep the chunks the old chunker made.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    name = "test.extractors.scoped"
    corpus, extr = _setup_module_corpus(tmp_path, name)
    (corpus / "other.py").write_text(MODULE.replace("quota", "other"))
    _run_source(clean_runtime, name, _module_source(corpus, extr))

    def fingerprint() -> object:
        return _rows(clean_runtime, f"""
            SELECT last_applied_rev->>'doc_corpus_extractors_fingerprint' AS fp
              FROM okg.source_watermarks WHERE source = '{name}'
        """)[0]["fp"]

    old = fingerprint()
    assert old

    _write_extractors(extr)
    SourceRunner(clean_runtime, source_name=name).run(
        _module_source(corpus, extr),
        mode="scope_complete",
        sync_scope={"scope_keys": ["quota.py"]},
    )
    assert fingerprint() == old
    assert any(
        "body_only_quota_marker" in t for t in _live_chunk_texts(clean_runtime, name)
    )

    # The next complete run re-parses the file the scoped run left alone.
    stats = _run_source(clean_runtime, name, _module_source(corpus, extr))
    assert stats.get("files_changed") == 2, stats
    assert fingerprint() != old
    other = _rows(clean_runtime, f"""
        SELECT c.attrs->>'text' AS text FROM okg.graph_edges e
          JOIN okg.graph_nodes c ON c.node_id = e.src
          JOIN okg.graph_nodes d ON d.node_id = e.dst
         WHERE e.edge_type = 'member_of' AND d.source = '{name}'
           AND d.subtype = 'document' AND d.attrs->>'path' = 'other.py'
    """)
    assert any("body_only_other_marker" in str(r["text"]) for r in other), other


def test_a_git_frontier_corpus_rechunks_on_a_chunker_edit(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The unchanged-git-frontier short-circuit declines on a config edit."""
    import pygit2

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
    name = "test.extractors.frontier"
    corpus, extr = _setup_module_corpus(tmp_path, name)
    repo = pygit2.init_repository(str(corpus))
    repo.index.add("quota.py")
    repo.index.write()
    signature = pygit2.Signature("t", "t@example.com")
    repo.create_commit(
        "HEAD", signature, signature, "init", repo.index.write_tree(), [],
    )

    def source() -> DocCorpusSource:
        return _module_source(corpus, extr, git_frontier_skip=True)

    _run_source(clean_runtime, name, source())
    _write_extractors(extr)
    stats = _run_source(clean_runtime, name, source())
    assert stats.get("files_changed") == 1, stats
    assert any(
        "body_only_quota_marker" in t for t in _live_chunk_texts(clean_runtime, name)
    )
