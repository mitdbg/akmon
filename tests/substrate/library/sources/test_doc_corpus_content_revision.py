"""`DocCorpusSource.source_revision` is a CONTENT revision, not a run token.

The source registry declares doc-corpus with
`source_revision_kind: content_hash`. The Chronos writer's semantic no-op
filter compares `source_revision` alongside the payload, so a revision
carrying `run_id` made every rerun rewrite every doc-corpus row even when
the corpus was byte-identical.

These are no-DB tests: `DocCorpusSource` runs without a `dsn` (the alias
resolver and the source_file endpoint filter both degrade to "no
predecessor" without one), so they belong to the fast lane.
"""
from __future__ import annotations

import hashlib
import json
import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.library.sources.doc_corpus import (
    DocCorpusSource,
    _chunk_identity_sha,
    _chunk_node_id,
)


@pytest.fixture
def extractors_dir(tmp_path: Path) -> Path:
    """Minimal markdown parser + window chunker + regex NER."""
    extr = tmp_path / "extractors-cfg"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": (
                "okg.substrate.library.extraction."
                "parse.markdown.MarkdownParser"
            ),
        }],
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
            ]},
        }],
    }))
    return extr


@pytest.fixture
def corpus_dir(tmp_path: Path) -> Path:
    """Three markdown docs; `b.md` carries a mention so the deferred
    alias pass (which emits after the walk) is exercised too."""
    root = tmp_path / "corpus"
    root.mkdir()
    (root / "a.md").write_text(textwrap.dedent("""\
        # Doc A

        ## Intro

        Lead paragraph for A.
    """))
    (root / "b.md").write_text(textwrap.dedent("""\
        # Doc B

        ## Intro

        Lead paragraph for B that mentions arXiv:1234.56789.
    """))
    (root / "c.md").write_text(textwrap.dedent("""\
        # Doc C

        ## Intro

        Lead paragraph for C, which links to [A](a.md).
    """))
    return root


def _fact_key(fact: Any) -> tuple:
    """Identity of one emitted fact, independent of its revision."""
    return (
        type(fact).__name__,
        getattr(fact, "node_id", None),
        getattr(fact, "src", None),
        getattr(fact, "dst", None),
        getattr(fact, "edge_type", None),
        json.dumps(fact.source_record_id, sort_keys=True, default=str),
    )


def _revisions(corpus_dir: Path, extractors_dir: Path, run_id: str) -> dict:
    """Run the source once; map each emitted fact to its serialized
    `source_revision`."""
    adapter = DocCorpusSource(
        repo="testcorpus",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        include_globs=("**/*.md",),
    )
    out = adapter.run(run_id, mode="reconcile")
    revisions = {
        _fact_key(fact): json.dumps(
            fact.source_revision, sort_keys=True, default=str,
        )
        for fact in out.facts
    }
    assert revisions, "fixture corpus emitted no facts"
    return revisions


def test_rerun_over_identical_bytes_reuses_every_source_revision(
    corpus_dir: Path,
    extractors_dir: Path,
) -> None:
    """Two runs over the same bytes emit byte-identical revisions.

    This is the property the Chronos no-op filter needs: without it the
    filter can never drop a doc-corpus row and every rerun rewrites the
    whole footprint.
    """
    first = _revisions(corpus_dir, extractors_dir, "run-1")
    second = _revisions(corpus_dir, extractors_dir, "run-2")

    assert first.keys() == second.keys()
    differing = {
        key for key in first if first[key] != second[key]
    }
    assert not differing, (
        f"{len(differing)} facts changed source_revision across a rerun "
        f"over identical bytes; sample={sorted(differing)[:3]}"
    )


def test_source_revision_carries_no_run_token(
    corpus_dir: Path,
    extractors_dir: Path,
) -> None:
    """No emitted revision may name the run, and each must name content."""
    for serialized in _revisions(corpus_dir, extractors_dir, "run-1").values():
        revision = json.loads(serialized)
        assert "run_id" not in revision, revision
        assert revision.get("content_sha256"), revision


def test_editing_one_file_changes_only_that_files_revisions(
    corpus_dir: Path,
    extractors_dir: Path,
) -> None:
    """A one-file edit is a one-file revision change.

    A whole-emission digest would satisfy the rerun test above while
    still rewriting every unrelated row on any edit, so pin the
    per-record property explicitly.
    """
    before = _revisions(corpus_dir, extractors_dir, "run-1")

    (corpus_dir / "b.md").write_text(textwrap.dedent("""\
        # Doc B

        ## Intro

        Rewritten lead paragraph for B that mentions arXiv:1234.56789.
    """))
    after = _revisions(corpus_dir, extractors_dir, "run-2")

    def owning_path(key: tuple) -> str:
        return json.loads(key[-1])["path"]

    unchanged_files_changed = {
        key
        for key in before.keys() & after.keys()
        if owning_path(key) != "b.md" and before[key] != after[key]
    }
    assert not unchanged_files_changed, (
        "editing b.md changed the revision of facts owned by other files: "
        f"{sorted(unchanged_files_changed)[:3]}"
    )

    b_changed = {
        key
        for key in before.keys() & after.keys()
        if owning_path(key) == "b.md" and before[key] != after[key]
    }
    assert b_changed, "editing b.md changed no b.md revision"


def test_record_set_revisions_match_the_emitted_facts(
    corpus_dir: Path,
    extractors_dir: Path,
) -> None:
    """The record_set mirrors facts for retraction, so its revisions must
    be the same per-file content revisions, not a walk-final one."""
    adapter = DocCorpusSource(
        repo="testcorpus",
        repo_root=corpus_dir, non_git_walk=True,
        extractors_dir=extractors_dir,
        include_globs=("**/*.md",),
    )
    out = adapter.run("run-1", mode="reconcile")
    record_set = out.record_set
    assert record_set is not None, "reconcile run declared no record_set"

    by_path = {
        fact.source_record_id["path"]: fact.source_revision
        for fact in out.facts
        if getattr(fact, "subtype", None) == "document"
    }
    for rel_str, record in record_set.records.items():
        expected = by_path[rel_str]
        primary = record.primary
        assert primary is not None
        assert primary.source_revision == expected, rel_str
        assert "run_id" not in primary.source_revision


# ---------------------------------------------------------------- shared


@pytest.fixture
def paragraph_extractors_dir(tmp_path: Path) -> Path:
    """Markdown parser + PARAGRAPH-bounded chunker.

    `MarkdownStructuralChunker` emits one chunk per block, so a paragraph
    repeated in two files becomes one content-addressed chunk node --
    which is the case under test. The window chunker cannot express it:
    its windows are cut at fixed character offsets, so two files agree
    only when their whole prefixes do.
    """
    extr = tmp_path / "paragraph-extractors-cfg"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": (
                "okg.substrate.library.extraction."
                "parse.markdown.MarkdownParser"
            ),
        }],
        "chunker": [{
            "class": (
                "okg.substrate.library.extraction.chunk."
                "structural_markdown.MarkdownStructuralChunker"
            ),
        }],
    }))
    return extr


SHARED_PARAGRAPH = (
    "This boilerplate paragraph is copied verbatim into both documents."
)


@pytest.fixture
def shared_paragraph_corpus(tmp_path: Path) -> Path:
    """Two documents whose only common block is `SHARED_PARAGRAPH`."""
    root = tmp_path / "shared-corpus"
    root.mkdir()
    (root / "a.md").write_text(
        f"# Doc A\n\nUnique opening for A.\n\n{SHARED_PARAGRAPH}\n"
    )
    (root / "b.md").write_text(
        f"# Doc B\n\nA quite different opening for B.\n\n"
        f"{SHARED_PARAGRAPH}\n"
    )
    return root


def test_paragraph_shared_by_two_files_stages_one_fact_one_revision(
    shared_paragraph_corpus: Path,
    paragraph_extractors_dir: Path,
) -> None:
    """A content-addressed node gets a CONTENT revision, not a file one.

    `document_chunk` ids are content-addressed, so one paragraph in two
    files is ONE node. `SourceRunner`'s in-run dedupe keys on
    `(node_id, source_revision, op)`, so keying the revision on the
    OWNING FILE's sha would stage that single node twice -- two
    `okg.node_facts` rows per run for a node the graph writes once.
    """
    adapter = DocCorpusSource(
        repo="testcorpus",
        repo_root=shared_paragraph_corpus, non_git_walk=True,
        extractors_dir=paragraph_extractors_dir,
        include_globs=("**/*.md",),
    )
    out = adapter.run("run-1", mode="reconcile")

    shared_id = adapter._canonicalize(_chunk_node_id(SHARED_PARAGRAPH))
    shared_facts = [
        fact for fact in out.facts
        if getattr(fact, "subtype", None) == "document_chunk"
        and fact.node_id == shared_id
    ]
    assert len(shared_facts) == 2, (
        "fixture did not produce the shared paragraph in both files; "
        f"found {len(shared_facts)} chunk facts for {shared_id}"
    )
    owning_paths = {
        fact.source_record_id["path"] for fact in shared_facts
    }
    assert owning_paths == {"a.md", "b.md"}, owning_paths

    revisions = {
        json.dumps(fact.source_revision, sort_keys=True, default=str)
        for fact in shared_facts
    }
    assert len(revisions) == 1, (
        "the same chunk node carried two source_revisions, so the runner's "
        f"in-run dedupe stages it twice: {sorted(revisions)}"
    )

    # And the one revision names the chunk's OWN content, not either file.
    revision = json.loads(next(iter(revisions)))
    assert revision["content_sha256"] == _chunk_identity_sha(
        SHARED_PARAGRAPH,
    ), revision
    file_shas = {
        hashlib.sha256(
            (shared_paragraph_corpus / name).read_bytes(),
        ).hexdigest()
        for name in ("a.md", "b.md")
    }
    assert revision["content_sha256"] not in file_shas, revision

    # The runner's key collapses the pair to one staged row.
    run_keys = {
        (
            fact.node_id,
            json.dumps(fact.source_revision, sort_keys=True, default=str),
            fact.op,
        )
        for fact in shared_facts
    }
    assert len(run_keys) == 1, run_keys


def test_document_nodes_keep_their_owning_file_revision(
    shared_paragraph_corpus: Path,
    paragraph_extractors_dir: Path,
) -> None:
    """The content rule is per-kind: file-identified facts are unchanged.

    Without this, "derive the revision from the content" could be read as
    a blanket rule and would stop a `document` node's revision from moving
    when its file is edited.
    """
    adapter = DocCorpusSource(
        repo="testcorpus",
        repo_root=shared_paragraph_corpus, non_git_walk=True,
        extractors_dir=paragraph_extractors_dir,
        include_globs=("**/*.md",),
    )
    # `out.facts` is a one-shot iterator; this test reads it twice.
    facts = list(adapter.run("run-1", mode="reconcile").facts)

    seen = 0
    for fact in facts:
        if getattr(fact, "subtype", None) != "document":
            continue
        seen += 1
        rel_str = fact.source_record_id["path"]
        expected = hashlib.sha256(
            (shared_paragraph_corpus / rel_str).read_bytes(),
        ).hexdigest()
        assert fact.source_revision["content_sha256"] == expected, rel_str
    assert seen == 2, seen

    # Edges stay file-keyed too: `member_of` carries this document's
    # occurrence payload (`doc_path`, `chunk_ordinal`, offsets), so its
    # revision must move when the file does.
    shared_id = adapter._canonicalize(_chunk_node_id(SHARED_PARAGRAPH))
    member_edges = [
        fact for fact in facts
        if getattr(fact, "edge_type", None) == "member_of"
        and getattr(fact, "src", None) == shared_id
    ]
    assert len(member_edges) == 2, len(member_edges)
    assert len({
        edge.source_revision["content_sha256"] for edge in member_edges
    }) == 2, "the two occurrence edges collapsed to one revision"
