"""Property test: AST-extracting sources are delta-proportional.

Anchors the graph-ingestion spec requirement "Sources of Profile
discovery_crawl Are Delta-Proportional" (per
`add-delta-proportional-source-runs`).

Operationally: each source is constructed against a small fixture
working tree, run once (warm-up), then run AGAIN with the cursor
from the first run and the same working tree. The second run MUST
emit zero NodeFacts and zero EdgeFacts.

The test is intentionally parametrized over the source classes
themselves (not the deployment registry), so it runs in any
environment without a DB. A future source that walks files and
extracts facts should be added to the parametrize list — this is
how the bug class becomes structurally impossible to repeat.
"""
from __future__ import annotations

import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from okg.substrate.library.sources.code_graph import (
    CodeStructureSource,
    InterfaceSource,
)
from okg.substrate.library.sources.doc_corpus import DocCorpusSource


def _build_python_repo(root: Path) -> Path:
    """Two .py files with classes, functions, decorators."""
    repo = root / "repo"
    repo.mkdir()
    (repo / "module_a.py").write_text(textwrap.dedent('''\
        """Module A docstring."""

        def alpha(x):
            return x + 1

        class Foo:
            def beta(self, y):
                return y * 2
    '''))
    (repo / "module_b.py").write_text(textwrap.dedent('''\
        from flask import Flask
        app = Flask(__name__)

        @app.route("/hello")
        def hello():
            return "world"
    '''))
    return repo


def _build_doc_repo(root: Path) -> tuple[Path, Path]:
    """Two .md files + a minimal extractors.yaml configuring a
    markdown parser + a window chunker. Returns (repo_root,
    extractors_dir)."""
    repo = root / "repo"
    repo.mkdir()
    (repo / "intro.md").write_text(textwrap.dedent("""\
        # Intro

        ## Section 1

        Hello world from intro.
    """))
    (repo / "guide.md").write_text(textwrap.dedent("""\
        # Guide

        ## Setup

        Some setup steps.

        ## Usage

        Example usage.
    """))
    extr = root / "extractors"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parsers": [{
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
        "ner": [],
    }))
    return repo, extr


@dataclass(frozen=True)
class _Case:
    name: str
    build_source: Callable[[Path], Any]


def _make_code_structure(tmp_path: Path) -> CodeStructureSource:
    repo = _build_python_repo(tmp_path)
    return CodeStructureSource(repo="test", repo_root=repo)


def _make_interface(tmp_path: Path) -> InterfaceSource:
    repo = _build_python_repo(tmp_path)
    return InterfaceSource(repo="test", repo_root=repo)


def _make_doc_corpus(tmp_path: Path) -> DocCorpusSource:
    repo, extr = _build_doc_repo(tmp_path)
    # predecessor_source=None so _assert_predecessor_ran is a
    # no-op and we don't need a live DB for this property test.
    return DocCorpusSource(
        repo="test",
        repo_root=repo,
        non_git_walk=True,
        extractors_dir=extr,
        predecessor_source=None,
    )


CASES = [
    _Case(name="CodeStructureSource", build_source=_make_code_structure),
    _Case(name="InterfaceSource",     build_source=_make_interface),
    _Case(name="DocCorpusSource",     build_source=_make_doc_corpus),
]


@pytest.mark.parametrize(
    "case",
    CASES,
    ids=[c.name for c in CASES],
)
def test_rerun_on_unchanged_tree_emits_zero_facts(
    tmp_path: Path,
    case: _Case,
) -> None:
    """Per graph-ingestion spec: discovery_crawl sources MUST be
    delta-proportional. Same input twice → second run emits 0
    facts.

    This is the operational anchor for the source-side content-
    hash short-circuit. A failure here means the source is doing
    O(state) work regardless of delta — exactly the bug pattern
    that landed us in this proposal.
    """
    adapter = case.build_source(tmp_path)

    # Warm-up run: cursor=None, so the source does a full scan.
    # We expect SOMETHING to be emitted on this run (otherwise the
    # fixture is degenerate).
    run1 = adapter.run("warm-up", mode="scope_incomplete", cursor=None)
    facts1 = list(run1.facts)
    assert facts1, (
        f"{case.name}: warm-up run emitted ZERO facts. The fixture "
        f"may not match this source's include filter, or the "
        f"adapter is misconfigured."
    )

    cursor1 = run1.next_cursor
    assert cursor1 is not None, (
        f"{case.name}: warm-up next_cursor is None — cannot test "
        f"delta-proportionality without a persisted cursor."
    )
    assert "files" in cursor1, (
        f"{case.name}: warm-up next_cursor lacks the `files` key. "
        f"Per the spec, AST-extracting sources MUST persist a "
        f"`files: {{<rel_path>: <sha256>}}` map. Got cursor keys: "
        f"{sorted(cursor1.keys())}"
    )

    # Second run against the same working tree, with the cursor
    # from run 1. By the spec, this MUST emit zero facts.
    run2 = adapter.run(
        "rerun", mode="scope_incomplete", cursor=cursor1,
    )
    facts2 = list(run2.facts)

    n_node_facts = sum(
        1 for f in facts2 if type(f).__name__ == "NodeFact"
    )
    n_edge_facts = sum(
        1 for f in facts2 if type(f).__name__ == "EdgeFact"
    )

    assert facts2 == [], (
        f"{case.name}: re-run on unchanged input emitted "
        f"{n_node_facts} NodeFacts + {n_edge_facts} EdgeFacts. "
        f"The source is not delta-proportional (graph-ingestion "
        f"spec violation). Likely cause: the source's per-file "
        f"loop is not consulting the prior cursor, OR the cursor "
        f"format is mismatched between writer and reader."
    )


@pytest.mark.parametrize(
    "case",
    CASES,
    ids=[c.name for c in CASES],
)
def test_mtime_fast_path_skips_reads(
    tmp_path: Path,
    case: _Case,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per `shrink-source-walk-cost`: when mtime matches the prior
    cursor's mtime, the source MUST NOT call `path.read_*()`.

    We verify by patching `pathlib.Path.read_text` and
    `pathlib.Path.read_bytes` with counting spies, running the
    source twice, and asserting the second run's read count is 0.
    """
    adapter = case.build_source(tmp_path)

    # Warm-up. Reads happen freely here.
    run1 = adapter.run("warm-up", mode="scope_incomplete", cursor=None)
    list(run1.facts)
    cursor1 = run1.next_cursor

    # Cursor must include mtime entries (the proposal's contract).
    files_entry = cursor1.get("files", {})
    assert files_entry, f"{case.name}: warm-up cursor has empty `files`"
    sample_entry = next(iter(files_entry.values()))
    assert isinstance(sample_entry, dict), (
        f"{case.name}: cursor entries must be dicts "
        f"(`{{'sha256': ..., 'mtime_ns': ...}}`); got "
        f"{type(sample_entry).__name__}"
    )
    assert "mtime_ns" in sample_entry, (
        f"{case.name}: cursor entries must include `mtime_ns`; "
        f"got keys {sorted(sample_entry.keys())}"
    )

    # Second run: spy on file reads. Should be ZERO.
    read_calls: dict[str, int] = {"text": 0, "bytes": 0}

    real_read_text = Path.read_text
    real_read_bytes = Path.read_bytes

    def counting_read_text(self, *args, **kwargs):
        read_calls["text"] += 1
        return real_read_text(self, *args, **kwargs)

    def counting_read_bytes(self, *args, **kwargs):
        read_calls["bytes"] += 1
        return real_read_bytes(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read_text)
    monkeypatch.setattr(Path, "read_bytes", counting_read_bytes)

    run2 = adapter.run("rerun", mode="scope_incomplete", cursor=cursor1)
    list(run2.facts)

    assert read_calls["text"] == 0 and read_calls["bytes"] == 0, (
        f"{case.name}: mtime fast-path failed — re-run read "
        f"{read_calls['text']} text files + {read_calls['bytes']} "
        f"binary files even though every mtime was unchanged."
    )
