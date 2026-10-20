"""Issue #2688: `doc:<repo>:<path> derived_from file:<repo>:<path>` exists
whenever both nodes exist.

The link must not depend on `predecessor_source`, on which source ran first,
or on whether the corpus bytes changed since doc-corpus last ran. A file node
that appears later adds the link on the next reconcile; one that disappears
retracts it, so the transform-consistency gate never sees a dangling edge.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import psycopg
import pytest

from okg.substrate.consistency.transform_gate import TransformConsistencyGate
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer, seed_catalog


pytestmark = pytest.mark.postgres

_SOURCE = "test.doc_corpus.link"
_FILES_SOURCE = "test.git-files"
# The catalog the strict gate checks every published node and edge against.
_NARROWINGS = [
    ("document", "derived_from", "source_file"),
    ("document", "contains", "document_chunk"),
    ("document_chunk", "member_of", "document"),
]


def _extractors(tmp_path: Path) -> Path:
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n"
    )
    return extractors


def _corpus(
    tmp_path: Path, *, git: bool, paths: tuple[str, ...] = ("note.md",),
) -> Path:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    for path in paths:
        (corpus / path).write_text(f"# {path}\n\nBody of {path}.\n")
    if git:
        for args in (
            ("init", "-q"),
            ("add", *paths),
            ("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "c"),
        ):
            subprocess.run(["git", "-C", str(corpus), *args], check=True)
    return corpus


def _file_fact(op: str, path: str) -> NodeFact:
    return NodeFact(
        node_id=f"file:test:{path}",
        subtype="source_file",
        attrs={"path": path},
        source_record_id={"repo": "test", "path": path},
        source_revision={"rev": "test"},
        op=op,
    )


def _apply_file_node(dsn: str, op: str, path: str = "note.md") -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_file_fact(op, path)], source=_FILES_SOURCE,
        )


def _forget_link_record(dsn: str) -> None:
    """Rewrite the stored cursor as code before #2688 left it: no link record."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.execute(
            "UPDATE okg.source_watermarks "
            "SET last_applied_rev = last_applied_rev - %s WHERE source = %s",
            ("doc_corpus_source_file_links", _SOURCE),
        )
        assert cur.rowcount == 1


def _seed(dsn: str, *, predecessor: bool) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, narrowings=_NARROWINGS)
        if predecessor:
            conn.execute(
                "INSERT INTO okg.source_watermarks (source, last_reconcile_at) "
                "VALUES (%s, now())",
                (_FILES_SOURCE,),
            )


def _source(
    dsn: str,
    corpus: Path,
    extractors: Path,
    *,
    predecessor: bool,
    git_frontier: bool,
) -> DocCorpusSource:
    return DocCorpusSource(
        repo="test",
        repo_root=corpus,
        extractors_dir=extractors,
        include_globs=("**/*.md",),
        predecessor_source=_FILES_SOURCE if predecessor else None,
        git_frontier_skip=git_frontier,
        dsn=dsn,
        non_git_walk=True,
    )


def _run(
    dsn: str,
    source: DocCorpusSource,
    mode: str = "reconcile",
    scope_keys: tuple[str, ...] = (),
) -> str:
    SourceRunner(dsn, source_name=_SOURCE).run(
        source,
        mode=mode,
        sync_scope={"scope_keys": list(scope_keys)} if scope_keys else None,
    )
    return str(source.last_run_stats["scope_status"])


def _link_count(dsn: str, path: str = "note.md") -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT count(*) AS n FROM okg.graph_edges "
            "WHERE source = :source AND edge_type = 'derived_from' "
            "AND src = :src AND dst = :dst",
            {
                "source": _SOURCE,
                "src": f"doc:test:{path}",
                "dst": f"file:test:{path}",
            },
        )
        return int(rows[0]["n"])
    finally:
        store.close()


def _dangling_count(dsn: str) -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT count(*) AS n FROM okg.graph_edges e "
            "WHERE NOT EXISTS (SELECT 1 FROM okg.graph_nodes n "
            "                   WHERE n.node_id = e.dst)"
        )
        return int(rows[0]["n"])
    finally:
        store.close()


def _publish_with_strict_gate(dsn: str) -> dict:
    """Publish with the transform-consistency gate in its blocking mode."""
    return Publisher(
        dsn,
        "default",
        consistency_gates=[TransformConsistencyGate(dangling_policy="block")],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def _why(published: dict) -> str:
    return json.dumps(
        {
            key: published.get(key)
            for key in ("status", "blocked_reason", "consistency_gates")
        },
        default=str,
    )[:4000]


def test_link_written_without_predecessor_source(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A git-files source publishes the file node; doc-corpus names no
    predecessor. The link is still written."""
    _seed(clean_runtime, predecessor=False)
    _apply_file_node(clean_runtime, "I")
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=False),
        _extractors(tmp_path),
        predecessor=False,
        git_frontier=False,
    )

    _run(clean_runtime, source)

    assert _link_count(clean_runtime) == 1
    published = _publish_with_strict_gate(clean_runtime)
    assert published["status"] == "published", _why(published)
    assert _dangling_count(clean_runtime) == 0


def _use_storage(monkeypatch: pytest.MonkeyPatch, storage: str) -> None:
    """`ref` stores the record set outside the cursor, as large corpora do."""
    if storage == "ref":
        monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")


def _assert_only_link_rebuilt(source: DocCorpusSource, storage: str) -> None:
    """No file's bytes changed; only the one document whose link changed was
    rebuilt, and a by-reference record set replaced just that record."""
    stats = source.last_run_stats
    assert stats["files_changed"] == 0, stats
    assert stats["files_processed_unchanged"] == 1, stats
    if storage == "ref":
        assert stats["record_set_authority"] == "replacement_slice", stats
        assert stats["record_set_replacement_keys"] == 1, stats
    else:
        assert stats["record_set_authority"] == "rebuilt", stats


@pytest.mark.parametrize("storage", ["inline", "ref"])
@pytest.mark.parametrize("git_frontier", [False, True], ids=["walk", "git"])
@pytest.mark.parametrize("predecessor", [False, True], ids=["alone", "pred"])
def test_file_node_appearing_after_unchanged_corpus_adds_link(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    git_frontier: bool,
    predecessor: bool,
    storage: str,
) -> None:
    _use_storage(monkeypatch, storage)
    _seed(clean_runtime, predecessor=predecessor)
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=git_frontier),
        _extractors(tmp_path),
        predecessor=predecessor,
        git_frontier=git_frontier,
    )
    _run(clean_runtime, source)
    assert _link_count(clean_runtime) == 0
    # Nothing changed: the fast path still answers a repeat run.
    fast = "git_frontier_no_change" if git_frontier else "full_unchanged_noop"
    assert _run(clean_runtime, source) == fast

    _apply_file_node(clean_runtime, "I")
    # Same corpus bytes; only the file node appeared.
    assert _run(clean_runtime, source) == "full"
    _assert_only_link_rebuilt(source, storage)

    assert _link_count(clean_runtime) == 1
    # The next run finds nothing to do again.
    assert _run(clean_runtime, source) == fast
    assert _link_count(clean_runtime) == 1


@pytest.mark.parametrize("storage", ["inline", "ref"])
@pytest.mark.parametrize("git_frontier", [False, True], ids=["walk", "git"])
@pytest.mark.parametrize("predecessor", [False, True], ids=["alone", "pred"])
def test_file_node_disappearing_retracts_link_without_dangling_edge(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    git_frontier: bool,
    predecessor: bool,
    storage: str,
) -> None:
    _use_storage(monkeypatch, storage)
    _seed(clean_runtime, predecessor=predecessor)
    _apply_file_node(clean_runtime, "I")
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=git_frontier),
        _extractors(tmp_path),
        predecessor=predecessor,
        git_frontier=git_frontier,
    )
    _run(clean_runtime, source)
    assert _link_count(clean_runtime) == 1
    first = _publish_with_strict_gate(clean_runtime)
    assert first["status"] == "published", _why(first)

    _apply_file_node(clean_runtime, "R")
    assert _dangling_count(clean_runtime) == 1  # until doc-corpus runs
    status = _run(clean_runtime, source)

    # The strict gate is the contract: no dangling edge reaches a publish.
    second = _publish_with_strict_gate(clean_runtime)
    assert second["status"] == "published", _why(second)
    assert _dangling_count(clean_runtime) == 0
    assert _link_count(clean_runtime) == 0
    # Retracted without any file's bytes changing.
    assert status == "full"
    _assert_only_link_rebuilt(source, storage)


def test_no_dsn_never_writes_the_link(tmp_path: Path) -> None:
    """Without a database the source cannot see file nodes, so it asserts
    no link rather than guessing one that could dangle."""
    source = DocCorpusSource(
        repo="test",
        repo_root=_corpus(tmp_path, git=False),
        extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
        non_git_walk=True,
    )
    facts = list(source.run("r1", mode="reconcile").facts)
    assert not [
        fact for fact in facts if getattr(fact, "edge_type", None) == "derived_from"
    ]


# --------------------------------------------------------------------------- #
# Two documents: only the one whose file node changed is rebuilt, and a cursor
# written before #2688 (no link record) never hides a stale link.
# --------------------------------------------------------------------------- #

_TWO = ("d1.md", "d2.md")


def _assert_rebuilt(source: DocCorpusSource, storage: str, *, rebuilt: int) -> None:
    stats = source.last_run_stats
    assert stats["files_changed"] == 0, stats
    assert stats["files_processed_unchanged"] == rebuilt, stats
    assert stats["files_reused_prior_record_set"] == len(_TWO) - rebuilt, stats
    if storage == "ref":
        assert stats["record_set_authority"] == "replacement_slice", stats
        assert stats["record_set_replacement_keys"] == rebuilt, stats


@pytest.mark.parametrize("storage", ["inline", "ref"])
@pytest.mark.parametrize("predecessor", [False, True], ids=["alone", "pred"])
def test_only_the_document_whose_file_node_changed_is_rebuilt(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    predecessor: bool,
    storage: str,
) -> None:
    _use_storage(monkeypatch, storage)
    _seed(clean_runtime, predecessor=predecessor)
    for path in _TWO:
        _apply_file_node(clean_runtime, "I", path)
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=False, paths=_TWO),
        _extractors(tmp_path),
        predecessor=predecessor,
        git_frontier=False,
    )
    _run(clean_runtime, source)
    assert [_link_count(clean_runtime, p) for p in _TWO] == [1, 1]

    _apply_file_node(clean_runtime, "R", "d2.md")
    assert _run(clean_runtime, source) == "full"

    _assert_rebuilt(source, storage, rebuilt=1)
    assert [_link_count(clean_runtime, p) for p in _TWO] == [1, 0]
    published = _publish_with_strict_gate(clean_runtime)
    assert published["status"] == "published", _why(published)


@pytest.mark.parametrize("partial", ["scoped", "incomplete"])
def test_partial_run_on_an_old_cursor_does_not_vouch_for_other_links(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    partial: str,
) -> None:
    """With a predecessor, code before #2688 may have linked any document, so
    a scoped or incomplete run cannot record a complete link set. The next
    full run must still rebuild d2 after its file node disappears; reusing
    its stored record would keep an edge to a missing node."""
    _use_storage(monkeypatch, "ref")
    _seed(clean_runtime, predecessor=True)
    for path in _TWO:
        _apply_file_node(clean_runtime, "I", path)
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=False, paths=_TWO),
        _extractors(tmp_path),
        predecessor=True,
        git_frontier=False,
    )
    _run(clean_runtime, source)
    _forget_link_record(clean_runtime)
    if partial == "scoped":
        assert _run(clean_runtime, source, "scope_complete", ("d1.md",)) == "scoped"
    else:
        assert _run(clean_runtime, source, "cursor") == "cursor"

    _apply_file_node(clean_runtime, "R", "d2.md")
    _run(clean_runtime, source)

    published = _publish_with_strict_gate(clean_runtime)
    assert published["status"] == "published", _why(published)
    assert _dangling_count(clean_runtime) == 0
    assert [_link_count(clean_runtime, p) for p in _TWO] == [1, 0]


@pytest.mark.parametrize("storage", ["inline", "ref"])
def test_old_cursor_without_predecessor_rebuilds_only_documents_gaining_a_link(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    storage: str,
) -> None:
    """Without a predecessor, code before #2688 never wrote the link, so an
    absent link record means "none linked": the first run after deploy
    rebuilds only documents whose file node exists, not the whole corpus."""
    _use_storage(monkeypatch, storage)
    _seed(clean_runtime, predecessor=False)
    source = _source(
        clean_runtime,
        _corpus(tmp_path, git=False, paths=_TWO),
        _extractors(tmp_path),
        predecessor=False,
        git_frontier=False,
    )
    _run(clean_runtime, source)
    _forget_link_record(clean_runtime)
    _apply_file_node(clean_runtime, "I", "d1.md")

    assert _run(clean_runtime, source) == "full"

    _assert_rebuilt(source, storage, rebuilt=1)
    assert [_link_count(clean_runtime, p) for p in _TWO] == [1, 0]
    # With nothing left to change, the short-circuit answers again.
    assert _run(clean_runtime, source) == "full_unchanged_noop"
