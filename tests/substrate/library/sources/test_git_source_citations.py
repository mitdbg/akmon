"""PACT `source-supplied-search-citations` — Git adapters adopt the
citation contract (req.search-citations.git-first).

Real temporary git repositories prove: repository-and-file labels on
every citable emission; immutable (commit-pinned) file URLs only when
truthful; label-only fallback for unsupported hosts, unknown remotes,
and dirty files; and citation refresh on a configuration-only change
with unchanged content (the url joins each source's change-probe
config, so a url change flips the probe verdict and re-emits).
"""
from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest
import yaml

from okg.substrate.library.sources.base import NodeFact


def _init_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(root), "init", "-b", "main"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "config", "user.email", "t@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(root), "config", "user.name", "T"], check=True,
    )


def _commit_all(root: Path) -> None:
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-m", "c", "--no-gpg-sign"],
        check=True,
    )


def _head_sha(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True,
    ).strip()


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "docs").mkdir()
    (root / "docs" / "guide.md").write_text(textwrap.dedent("""\
        # Guide

        ## Setup

        Setup body paragraph with enough text to stand alone as a chunk.
    """))
    (root / "app.py").write_text("def main() -> int:\n    return 1\n")
    _commit_all(root)
    return root


def _nodes(facts, subtype):
    return [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == subtype
    ]


# ---------------------------------------------------------------
# GitFilesSource
# ---------------------------------------------------------------

def test_git_files_citation_label_and_immutable_url(git_repo: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    sha = _head_sha(git_repo)
    src = GitFilesSource(
        repo="r", repo_root=git_repo, url="https://gitlab.cern.ch/g/r.git",
    )
    files = {
        f.attrs["path"]: f
        for f in _nodes(src.run("run-1", mode="cursor").facts, "source_file")
    }
    assert files
    guide = files["docs/guide.md"]
    assert guide.attrs["citation_label"] == "r/docs/guide.md"
    assert guide.attrs["citation_url"] == (
        f"https://gitlab.cern.ch/g/r/-/blob/{sha}/docs/guide.md"
    )


def test_git_files_dirty_file_is_label_only(git_repo: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    (git_repo / "app.py").write_text("def main() -> int:\n    return 2\n")
    src = GitFilesSource(
        repo="r", repo_root=git_repo, url="https://gitlab.cern.ch/g/r.git",
    )
    files = {
        f.attrs["path"]: f
        for f in _nodes(src.run("run-1", mode="cursor").facts, "source_file")
    }
    assert files["app.py"].attrs["citation_label"] == "r/app.py"
    # Dirty bytes are not the bytes at any commit — no truthful link.
    assert "citation_url" not in files["app.py"].attrs
    # The clean sibling still gets its immutable link.
    assert "citation_url" in files["docs/guide.md"].attrs


def test_git_files_no_remote_is_label_only(git_repo: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(repo="r", repo_root=git_repo)
    files = _nodes(src.run("run-1", mode="cursor").facts, "source_file")
    assert files  # guard against vacuous pass
    for f in files:
        assert f.attrs["citation_label"].startswith("r/")
        assert "citation_url" not in f.attrs


def test_git_files_unsupported_host_is_label_only(git_repo: Path) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    src = GitFilesSource(
        repo="r", repo_root=git_repo, url="https://forge.example.org/g/r.git",
    )
    files = _nodes(src.run("run-1", mode="cursor").facts, "source_file")
    assert files
    for f in files:
        assert f.attrs["citation_label"]
        assert "citation_url" not in f.attrs


def test_git_files_url_config_change_flips_probe(git_repo: Path) -> None:
    """A citation-config change must refresh existing records even when
    content is unchanged: the url is part of the change-probe config, so
    a config-only change produces a different probe fingerprint."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    without = GitFilesSource(repo="r", repo_root=git_repo)
    with_url = GitFilesSource(
        repo="r", repo_root=git_repo, url="https://gitlab.cern.ch/g/r.git",
    )
    assert (
        without.change_probe._config_hash
        != with_url.change_probe._config_hash
    )


# ---------------------------------------------------------------
# CodeStructureSource
# ---------------------------------------------------------------

def test_code_structure_symbol_citation(git_repo: Path) -> None:
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    sha = _head_sha(git_repo)
    src = CodeStructureSource(
        repo="r", repo_root=git_repo, url="https://github.com/o/r.git",
    )
    symbols = _nodes(src.run("run-1", mode="cursor").facts, "code_symbol")
    assert symbols
    main = next(s for s in symbols if s.attrs["name"] == "main")
    assert main.attrs["citation_label"] == "r/app.py"
    assert main.attrs["citation_url"] == (
        f"https://github.com/o/r/blob/{sha}/app.py"
    )


def test_code_structure_without_url_is_label_only(git_repo: Path) -> None:
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    src = CodeStructureSource(repo="r", repo_root=git_repo)
    symbols = _nodes(src.run("run-1", mode="cursor").facts, "code_symbol")
    assert symbols  # guard against vacuous pass
    for s in symbols:
        assert s.attrs["citation_label"] == "r/app.py"
        assert "citation_url" not in s.attrs


# ---------------------------------------------------------------
# DocCorpusSource
# ---------------------------------------------------------------

def test_doc_corpus_document_and_chunk_citations(
    git_repo: Path, tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    sha = _head_sha(git_repo)
    extr = tmp_path / "extractors"
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
    src = DocCorpusSource(
        repo="r",
        repo_root=git_repo,
        extractors_dir=extr,
        include_globs=("**/*.md",),
        url="https://gitlab.cern.ch/g/r.git",
    )
    facts = list(src.run("run-1", mode="cursor").facts)
    docs = _nodes(facts, "document")
    chunks = _nodes(facts, "document_chunk")
    assert docs and chunks
    expected_url = (
        f"https://gitlab.cern.ch/g/r/-/blob/{sha}/docs/guide.md"
    )
    guide = next(d for d in docs if d.attrs["path"] == "docs/guide.md")
    assert guide.attrs["citation_label"] == "r/docs/guide.md"
    assert guide.attrs["citation_url"] == expected_url
    # Chunks inherit the parent document's citation verbatim — no
    # anchors, no per-chunk link derivation.
    for chunk in chunks:
        assert chunk.attrs["citation_label"] == "r/docs/guide.md"
        assert chunk.attrs["citation_url"] == expected_url


def test_doc_corpus_without_url_is_label_only(
    git_repo: Path, tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    extr = tmp_path / "extractors"
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
    src = DocCorpusSource(
        repo="r",
        repo_root=git_repo,
        extractors_dir=extr,
        include_globs=("**/*.md",),
    )
    facts = list(src.run("run-1", mode="cursor").facts)
    chunks = _nodes(facts, "document_chunk")
    assert chunks  # guard against vacuous pass
    for chunk in chunks:
        assert chunk.attrs["citation_label"] == "r/docs/guide.md"
        assert "citation_url" not in chunk.attrs


# ---------------------------------------------------------------
# Registry expansion still threads the url config
# ---------------------------------------------------------------

def test_expand_code_repos_threads_url_to_dependent_blocks(
    tmp_path: Path,
) -> None:
    from okg.substrate.sources.registry import _expand_code_repos

    expanded = _expand_code_repos(
        {
            "base": str(tmp_path),
            "expand": ["git-files", "code-structure", "doc-corpus"],
            "repos": [
                {
                    "slug": "unified",
                    "url": (
                        "https://gitlab.cern.ch/"
                        "CMSProductionReprocessing/unified.git"
                    ),
                    "dir": "unified",
                },
            ],
        },
        tmp_path / "source_registry.yaml",
    )
    url = "https://gitlab.cern.ch/CMSProductionReprocessing/unified.git"
    for block in ("git-files", "code-structure", "doc-corpus"):
        assert expanded[f"{block}-unified"]["params"]["url"] == url
    assert expanded["git-files-unified"]["params"]["manage_checkout"] is True
    for block in ("code-structure", "doc-corpus"):
        assert "manage_checkout" not in expanded[f"{block}-unified"]["params"]
