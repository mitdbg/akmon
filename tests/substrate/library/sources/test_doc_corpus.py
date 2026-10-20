"""Stride A — DocCorpusSource adapter end-to-end against
fixture corpora.

Asserts:

  - markdown / text / json / python files emit Document nodes;
  - chunks land with content_sha256-keyed identity;
  - identical paragraph in two markdown files collapses (one
    DocumentChunk, two `member_of` edges);
  - oversize file emits Document with parse_warnings='oversize'
    and ZERO chunks;
  - file-count budget is enforced loudly via DocCorpusOversize;
  - regex NER hits emit EntityMention nodes plus member_of
    edges to their chunks.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.doc_corpus import (
    DocCorpusOversize,
    DocCorpusSource,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres


@pytest.fixture
def corpus_dir(tmp_path: Path) -> Path:
    """Build a 6-file fixture corpus with predictable content."""
    root = tmp_path / "corpus"
    root.mkdir()

    # Two markdown files sharing one verbatim paragraph.
    # MarkdownParser flushes chunks at heading boundaries, so
    # we put the shared paragraph under its own H2 in BOTH
    # files — the resulting chunk text is byte-identical.
    shared_para = (
        "The substrate's narrowing trigger fires at projection "
        "commit (per defer-narrowing-trigger-until-publish)."
    )
    (root / "a.md").write_text(textwrap.dedent(f"""\
        # Doc A

        ## Intro

        Lead paragraph for A.

        ## Shared

        {shared_para}
    """))
    (root / "b.md").write_text(textwrap.dedent(f"""\
        # Doc B

        ## Intro

        Lead paragraph for B that mentions arXiv:1234.56789.

        ## Shared

        {shared_para}
    """))

    # Plain text.
    (root / "notes.txt").write_text(
        "Plain notes carrying a DOI 10.1234/abc.5678 inline.\n"
    )

    # JSON config.
    (root / "config.json").write_text(
        json.dumps({"version": 1, "name": "okg-workspace"}),
    )

    # Python source (parser registered as `code`).
    (root / "snippet.py").write_text(textwrap.dedent("""\
        def foo() -> int:
            return 1
    """))

    # Oversize sentinel: configure adapter with low byte budget;
    # this file exceeds it.
    (root / "huge.txt").write_text("x" * 4096)

    return root


@pytest.fixture
def extractors_dir(tmp_path: Path) -> Path:
    """Write a minimal extractors.yaml that registers parsers
    + a window chunker + the regex NER backend with rules
    matching the fixture content."""
    extr = tmp_path / "extractors-cfg"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [
            {
                "format": "markdown",
                "class": (
                    "okg.substrate.library.extraction."
                    "parse.markdown.MarkdownParser"
                ),
            },
            {
                "format": "text",
                "class": (
                    "okg.substrate.library.extraction."
                    "parse.text.TextParser"
                ),
            },
            {
                "format": "json",
                "class": (
                    "okg.substrate.library.extraction."
                    "parse.json_struct.JsonParser"
                ),
            },
            {
                "format": "code",
                "class": (
                    "okg.substrate.library.extraction."
                    "parse.code_python.PythonAstParser"
                ),
            },
        ],
        "chunker": [{
            "class": (
                "okg.substrate.library.extraction.chunk."
                "window.WindowChunker"
            ),
            "config": {"max_chars": 200, "overlap": 0},
        }],
        "ner": [{
            "class": (
                "okg.substrate.library.extraction.ner."
                "regex_backend.RegexEntityExtractor"
            ),
            "config": {"rules": [
                ["arxiv_id", r"arXiv:[0-9]{4}\.[0-9]{4,5}"],
                ["doi", r"10\.[0-9]{4,9}/[-._;()/:A-Z0-9a-z]+"],
            ]},
        }],
    }))
    return extr



def _graph_nodes(
    dsn: str,
    *,
    source: str,
    subtype: str | None = None,
    where: str = "",
    params: dict | None = None,
) -> list[dict]:
    clauses = ["source = :source"]
    query_params = {"source": source, **(params or {})}
    if subtype is not None:
        clauses.append("subtype = :subtype")
        query_params["subtype"] = subtype
    if where:
        clauses.append(where)
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return store.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            f"WHERE {' AND '.join(clauses)} ORDER BY node_id",
            query_params,
        )
    finally:
        store.close()


def _graph_edge_count(
    dsn: str,
    *,
    source: str,
    edge_type: str,
    src: str | None = None,
    dst: str | None = None,
) -> int:
    clauses = ["source = :source", "edge_type = :edge_type"]
    params: dict = {"source": source, "edge_type": edge_type}
    if src is not None:
        clauses.append("src = :src")
        params["src"] = src
    if dst is not None:
        clauses.append("dst = :dst")
        params["dst"] = dst
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT count(*) AS n FROM okg.graph_edges "
            f"WHERE {' AND '.join(clauses)}",
            params,
        )
        return int(rows[0]["n"])
    finally:
        store.close()


def _seed_predecessor_watermark(
    conn: psycopg.Connection,
    *,
    source: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.source_watermarks "
            "  (source, last_reconcile_at) VALUES (%s, now())",
            (source,),
        )


def _seed_source_file_node(
    conn: psycopg.Connection,
    *,
    repo: str,
    path: str,
) -> None:
    node_id = f"file:{repo}:{path}"
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="source_file",
                attrs={"path": path},
                source_record_id={"repo": repo, "path": path},
                source_revision={"rev": "test"},
            ),
        ],
        source="test.git-files",
    )


def test_doc_corpus_emits_documents_and_chunks(
    clean_runtime: str, corpus_dir: Path, extractors_dir: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = DocCorpusSource(
        repo="test", repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=2048,  # forces huge.txt to oversize
    )
    SourceRunner(
        clean_runtime, source_name="test.doc_corpus",
    ).run(adapter, mode="scope_complete")

    n_docs = len(_graph_nodes(
        clean_runtime, source="test.doc_corpus", subtype="document",
    ))
    n_chunks = len(_graph_nodes(
        clean_runtime, source="test.doc_corpus", subtype="document_chunk",
    ))
    n_mentions = len(_graph_nodes(
        clean_runtime, source="test.doc_corpus", subtype="entity_mention",
    ))
    # 6 files → 6 documents: huge.txt exceeds max_bytes_per_file and is
    # recorded as an `oversize` Document without being read (#2245).
    assert n_docs == 6, n_docs
    assert adapter.last_run_stats["files_recorded_oversize"] == 1
    assert adapter.last_run_stats["files_recorded_oversize_sample"] == ["huge.txt"]
    # Chunks: at least one per non-skipped doc; expect ~5+.
    assert n_chunks >= 4, n_chunks
    # arxiv_id in b.md + doi in notes.txt.
    assert n_mentions >= 2, n_mentions


def test_doc_corpus_skips_derived_from_when_source_file_missing(
    clean_runtime: str,
    tmp_path: Path,
    extractors_dir: Path,
) -> None:
    corpus = tmp_path / "corpus-missing-source-file"
    corpus.mkdir()
    (corpus / "note.md").write_text("# Note\n\nBody.\n")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _seed_predecessor_watermark(conn, source="test.git-files")

    SourceRunner(
        clean_runtime,
        source_name="test.doc_corpus.missing_source_file",
    ).run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors_dir,
            include_globs=("**/*.md",),
            predecessor_source="test.git-files",
            dsn=clean_runtime,
        ),
        mode="scope_complete",
    )

    assert _graph_edge_count(
        clean_runtime,
        source="test.doc_corpus.missing_source_file",
        edge_type="derived_from",
    ) == 0


def test_doc_corpus_standalone_ingest_asserts_no_derived_from(
    clean_runtime: str,
    tmp_path: Path,
    extractors_dir: Path,
) -> None:
    """A standalone run (no predecessor_source) must not assert a
    `document derived_from source_file` edge: with no predecessor there is
    no source_file node to point at, and the previous None ("no filter")
    sentinel emitted a dangling edge that tripped the transform gate. The
    no-predecessor case now resolves to set() (assert none allowed)."""
    corpus = tmp_path / "corpus-standalone"
    corpus.mkdir()
    (corpus / "note.md").write_text("# Note\n\nBody.\n")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(
        clean_runtime,
        source_name="test.doc_corpus.standalone",
    ).run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors_dir,
            include_globs=("**/*.md",),
            dsn=clean_runtime,
        ),
        mode="scope_complete",
    )

    assert _graph_edge_count(
        clean_runtime,
        source="test.doc_corpus.standalone",
        edge_type="derived_from",
    ) == 0


def test_doc_corpus_emits_derived_from_when_source_file_exists(
    clean_runtime: str,
    tmp_path: Path,
    extractors_dir: Path,
) -> None:
    corpus = tmp_path / "corpus-present-source-file"
    corpus.mkdir()
    (corpus / "note.md").write_text("# Note\n\nBody.\n")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _seed_predecessor_watermark(conn, source="test.git-files")
        _seed_source_file_node(conn, repo="test", path="note.md")

    SourceRunner(
        clean_runtime,
        source_name="test.doc_corpus.present_source_file",
    ).run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors_dir,
            include_globs=("**/*.md",),
            predecessor_source="test.git-files",
            dsn=clean_runtime,
        ),
        mode="scope_complete",
    )

    assert _graph_edge_count(
        clean_runtime,
        source="test.doc_corpus.present_source_file",
        edge_type="derived_from",
        src="doc:test:note.md",
        dst="file:test:note.md",
    ) == 1


def test_doc_corpus_scoped_run_emits_only_requested_document(
    corpus_dir: Path,
    extractors_dir: Path,
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside.md"
    outside.write_text("# Outside\n\nThis must not be parsed.\n")
    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=2048,
    )

    run = adapter.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["b.md", "../outside.md"]},
    )
    facts = list(run.facts)
    doc_paths = {
        fact.attrs["path"]
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }

    assert doc_paths == {"b.md"}
    assert run.completed_scope is False
    assert run.record_set is None
    # Per `add-delta-proportional-source-runs`: the cursor's
    # per-file hash map is now keyed under `files` (uniform across
    # AST sources via the shared FileWalk helper) instead of the
    # source-specific `last_content_sha256`.
    assert set(run.next_cursor["files"]) == {"b.md"}


def test_doc_corpus_scoped_scope_complete_carries_prior_record_set(
    corpus_dir: Path,
    extractors_dir: Path,
) -> None:
    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=4096 * 16,
    )

    first = adapter.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    first_records = first.record_set.records
    old_b_nodes = set(first_records["b.md"].nodes_by_id())

    (corpus_dir / "b.md").write_text(textwrap.dedent("""\
        # Doc B

        Replacement paragraph for B only.
    """))

    second = adapter.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == set(first_records)
    assert (
        second.record_set.records["a.md"].nodes_by_id()
        == first_records["a.md"].nodes_by_id()
    )
    assert old_b_nodes - set(second.record_set.records["b.md"].nodes_by_id())
    facts = list(second.facts)
    doc_paths = {
        fact.attrs["path"]
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }
    assert doc_paths == {"b.md"}
    assert set((second.next_cursor or {})["files"]) == set(
        (first.next_cursor or {})["files"],
    )


def test_doc_corpus_scope_complete_external_record_set_ref_no_change_skips_parse(
    corpus_dir: Path,
    extractors_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import doc_corpus as doc_corpus_module

    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=4096 * 16,
    )

    first = adapter.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.next_cursor is not None

    def fail_load_extractors(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged full-scope run loaded extractors")

    monkeypatch.setattr(
        doc_corpus_module,
        "load_extractors",
        fail_load_extractors,
    )
    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 6},
    }

    second = adapter.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is None
    assert second.next_cursor is not None
    assert second.next_cursor["record_set_ref"] == cursor["record_set_ref"]
    assert adapter.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert adapter.last_run_stats["record_set_authority"] == "preserved"
    assert adapter.last_run_stats["emitted_nodes"] == 0
    assert adapter.last_run_stats["emitted_edges"] == 0


def test_doc_corpus_git_frontier_no_change_skips_file_walk(
    corpus_dir: Path,
    extractors_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import doc_corpus as doc_corpus_module
    from okg.substrate.library.sources._git_helpers import run_git

    run_git(corpus_dir, ["init"])
    run_git(corpus_dir, ["config", "user.email", "fixture@example.test"])
    run_git(corpus_dir, ["config", "user.name", "OKG Fixture"])
    run_git(corpus_dir, ["add", "."])
    run_git(corpus_dir, ["commit", "-m", "initial"])
    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=4096 * 16,
        git_frontier_skip=True,
    )

    first = adapter.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    assert first.next_cursor is not None
    assert first.next_cursor.get("git_frontier") is not None

    def fail_walk(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged git frontier should skip file walk")

    def fail_load_extractors(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged git frontier should skip extractor loading")

    monkeypatch.setattr(adapter, "_walk_files", fail_walk)
    monkeypatch.setattr(
        doc_corpus_module,
        "load_extractors",
        fail_load_extractors,
    )
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()

    second = adapter.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is None
    assert second.next_cursor is not None
    assert second.next_cursor["files"] == first.next_cursor["files"]
    assert second.next_cursor["git_frontier"] == first.next_cursor["git_frontier"]
    assert second.next_cursor["record_set"] == cursor["record_set"]
    assert adapter.last_run_stats["scope_status"] == "git_frontier_no_change"
    assert adapter.last_run_stats["files_seen"] == 0


def test_doc_corpus_scope_complete_external_record_set_ref_replaces_changed_slice(
    corpus_dir: Path,
    extractors_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=4096 * 16,
    )

    first = adapter.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.next_cursor is not None

    calls: list[str] = []
    original_format_for = adapter._format_for

    def spy_format_for(path: Path, first_bytes: bytes) -> str | None:
        calls.append(path.relative_to(corpus_dir).as_posix())
        return original_format_for(path, first_bytes)

    monkeypatch.setattr(adapter, "_format_for", spy_format_for)
    (corpus_dir / "b.md").write_text(textwrap.dedent("""\
        # Doc B

        Replacement paragraph for B only.
    """))
    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 6},
    }

    second = adapter.run("run-2", mode="scope_complete", cursor=cursor)
    facts = list(second.facts)

    assert calls == ["b.md"]
    assert {
        fact.attrs["path"]
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    } == {"b.md"}
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"b.md"}
    assert set(second.record_set_replacement_keys or ()) == {"b.md"}
    assert second.next_cursor is not None
    assert "record_set_ref" not in second.next_cursor
    assert adapter.last_run_stats["record_set_authority"] == "replacement_slice"
    assert adapter.last_run_stats["record_set_replacement_keys"] == 1


def test_oversize_file_emits_warnings_no_chunks(
    clean_runtime: str, corpus_dir: Path, extractors_dir: Path,
) -> None:
    """Issues #2245/#1692: a file over `max_bytes_per_file` does not fail
    the run. It is published as a Document with an `oversize` warning and
    no chunks, and counted in `last_run_stats`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    limited = DocCorpusSource(
        repo="test", repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_bytes_per_file=2048,
    )
    SourceRunner(
        clean_runtime, source_name="test.oversize",
    ).run(limited, mode="scope_complete")
    huge_nodes = _graph_nodes(
        clean_runtime,
        source="test.oversize",
        subtype="document",
        where="attrs->>'path' = :path",
        params={"path": "huge.txt"},
    )
    warnings = huge_nodes[0]["attrs"].get("parse_warnings")
    n_chunk_edges = _graph_edge_count(
        clean_runtime,
        source="test.oversize",
        edge_type="contains",
        src="doc:test:huge.txt",
    )
    assert "oversize" in (warnings or ""), warnings
    assert n_chunk_edges == 0, n_chunk_edges
    assert limited.last_run_stats["files_recorded_oversize"] == 1
    assert limited.last_run_stats["files_recorded_oversize_sample"] == ["huge.txt"]
    assert _graph_nodes(
        clean_runtime,
        source="test.oversize",
        subtype="document",
        where="attrs->>'path' = :path",
        params={"path": "a.md"},
    ), "the files under the limit must still be published"


def test_ignored_file_document_is_retracted_on_next_run(
    clean_runtime: str, tmp_path: Path, extractors_dir: Path,
) -> None:
    """Issue #2357: a document published from a file that git now ignores
    is retracted by the next complete run, not left in the graph.

    Run 1 reads the untracked, not-yet-ignored notes.md (include_untracked).
    The file is then added to .gitignore; run 2 no longer reads it, and the
    complete-scope record set retracts its document."""
    import subprocess

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    root = tmp_path / "repo"
    root.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-c", "user.email=f@example.test", "-c", "user.name=F",
             "-c", "commit.gpgsign=false", *args],
            cwd=root, check=True, capture_output=True,
        )

    (root / ".gitignore").write_text("")
    (root / "README.md").write_text("# Readme\n\nTracked prose.\n")
    git("init", "-q")
    git("add", ".")
    git("commit", "-q", "-m", "fixture")
    (root / "notes.md").write_text("# Notes\n\nLocal notes.\n")

    def docs(path: str) -> list:
        return _graph_nodes(
            clean_runtime, source="test.ignored", subtype="document",
            where="attrs->>'path' = :path", params={"path": path},
        )

    def source() -> DocCorpusSource:
        return DocCorpusSource(
            repo="test", repo_root=root, extractors_dir=extractors_dir,
            include_globs=("**/*.md",), include_untracked=True,
        )

    runner = SourceRunner(clean_runtime, source_name="test.ignored")
    runner.run(source(), mode="scope_complete")
    assert len(docs("notes.md")) == 1

    (root / ".gitignore").write_text("notes.md\n")
    runner.run(source(), mode="scope_complete")
    assert docs("notes.md") == [], "the now-ignored file's document must be retracted"
    assert len(docs("README.md")) == 1


def test_identical_chunk_collapses_via_runner_dedup(
    clean_runtime: str, corpus_dir: Path, extractors_dir: Path,
) -> None:
    """The shared paragraph appears verbatim in a.md and b.md.
    The runner-level dedup pass collapses on `node_id`, so
    exactly ONE DocumentChunk NodeFact for that paragraph
    survives — but BOTH documents have a `member_of` edge
    pointing to it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(
        clean_runtime, source_name="test.dedup",
    ).run(
        DocCorpusSource(
            repo="test", repo_root=corpus_dir, non_git_walk=True,
            extractors_dir=extractors_dir,
            max_bytes_per_file=4096 * 16,
        ),
        mode="scope_complete",
    )
    rows = _graph_nodes(
        clean_runtime,
        source="test.dedup",
        subtype="document_chunk",
        where="attrs->>'text' LIKE :needle",
        params={"needle": "%narrowing trigger%"},
    )
    # Exactly one chunk row carries the shared text after
    # dedup.
    assert len(rows) == 1, rows
    shared_chunk_id = rows[0]["node_id"]
    n_parents = _graph_edge_count(
        clean_runtime,
        source="test.dedup",
        edge_type="contains",
        dst=shared_chunk_id,
    )
    assert n_parents == 2, n_parents


def test_doc_corpus_record_set_reconciles_edits(
    clean_runtime: str, corpus_dir: Path, extractors_dir: Path,
) -> None:
    """DocCorpusSource delegates disappeared-chunk retractions to
    SourceRunner's record-set diff. A content-addressed chunk that
    is still owned by another document must not be retracted."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    source_name = "test.doc-corpus-record-set"
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus_dir, non_git_walk=True,
            extractors_dir=extractors_dir,
            max_bytes_per_file=4096 * 16,
        ),
        mode="scope_complete",
    )
    assert first.next_cursor is not None
    assert "record_set" in first.next_cursor
    assert "last_emissions" not in first.next_cursor

    shared_chunk_id = _graph_nodes(
        clean_runtime,
        source=source_name,
        subtype="document_chunk",
        where="attrs->>'text' LIKE :needle",
        params={"needle": "%narrowing trigger%"},
    )[0]["node_id"]

    (corpus_dir / "a.md").write_text(textwrap.dedent("""\
        # Doc A

        Replacement text that drops the old shared section.
    """))

    second = runner.run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus_dir, non_git_walk=True,
            extractors_dir=extractors_dir,
            max_bytes_per_file=4096 * 16,
        ),
        mode="scope_complete",
    )
    assert second.retractions_node > 0
    assert second.retractions_edge > 0

    assert _graph_nodes(
        clean_runtime,
        source=source_name,
        subtype="document_chunk",
        where="node_id = :node_id",
        params={"node_id": shared_chunk_id},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            " WHERE source = %s",
            (source_name,),
        )
        cursor = cur.fetchone()[0]
    assert "record_set" in cursor
    assert "last_emissions" not in cursor


def test_max_files_refuses_loudly(
    clean_runtime: str, tmp_path: Path, extractors_dir: Path,
) -> None:
    big = tmp_path / "big-corpus"
    big.mkdir()
    for i in range(15):
        (big / f"f{i:03d}.txt").write_text(f"doc {i}")

    adapter = DocCorpusSource(
        repo="t", repo_root=big, non_git_walk=True,
        extractors_dir=extractors_dir,
        max_files=10,
    )
    with pytest.raises(DocCorpusOversize) as exc_info:
        adapter.run(run_id="x", mode="scope_complete")
    err = exc_info.value
    assert err.data["max_files"] == 10
