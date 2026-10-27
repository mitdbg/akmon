"""`DocCorpusSource.max_total_bytes`: a size guard for the whole corpus.

A source that stores file text costs about 20 times the bytes it reads, and
the reader had only a file-count limit and a per-file limit. A repository of a
few thousand ordinary files could pass both and still add gigabytes to the
database. `max_total_bytes` is the missing guard.

The rule these tests hold: over the cap, the run REFUSES, with the total and
the folders that hold the bytes. It never reads part of the corpus, because
under `missing_from_completed_scope` a partial read retracts every file it
skipped. The default (None) leaves every existing deployment unchanged.

The tests run the adapter on throwaway git repositories; no database is needed.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.doc_corpus import (
    DocCorpusOversize,
    DocCorpusSource,
)


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c", "user.email=fixture@example.test",
            "-c", "user.name=OKG Fixture",
            "-c", "commit.gpgsign=false",
            *args,
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )


def _repo(tmp_path: Path) -> Path:
    """A committed repo: 600 bytes under src/, 3,000 under vendored_data/,
    10 at the root."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "README.md").write_text("# Title\n\nroot\n")
    (root / "src").mkdir()
    (root / "src" / "a.md").write_text("# A\n\n" + "a" * 300 + "\n")
    (root / "src" / "b.md").write_text("# B\n\n" + "b" * 300 + "\n")
    (root / "vendored_data").mkdir()
    (root / "vendored_data" / "big.md").write_text("# Big\n\n" + "z" * 3_000 + "\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    return root


def _source(root: Path, tmp_path: Path, **kwargs) -> DocCorpusSource:
    extractors = tmp_path / "extractors"
    extractors.mkdir(exist_ok=True)
    (extractors / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n"
        "chunker:\n"
        "  - format: default\n"
        "    class: okg.substrate.library.extraction.chunk.window.WindowChunker\n"
        "    config: {max_chars: 1500, overlap: 200}\n"
    )
    return DocCorpusSource(
        repo="fixture",
        repo_root=root,
        extractors_dir=extractors,
        include_globs=("**/*.md",),
        **kwargs,
    )


def _document_paths(source: DocCorpusSource) -> set[str]:
    run = source.run("run-1", mode="scope_complete")
    return {
        fact.attrs["path"]
        for fact in run.facts
        if getattr(fact, "subtype", None) == "document"
    }


def _emission(source: DocCorpusSource) -> list[tuple[str, str, str]]:
    run = source.run("run-1", mode="scope_complete")
    # `parsed_at` is the wall clock of the run; every other attribute is
    # content.
    return sorted(
        (
            str(fact.node_id),
            str(fact.subtype),
            repr(sorted(
                (key, value) for key, value in fact.attrs.items()
                if key != "parsed_at"
            )),
        )
        for fact in run.facts
        if getattr(fact, "node_id", None) is not None
    )


def test_a_corpus_under_the_cap_is_read(tmp_path: Path) -> None:
    root = _repo(tmp_path)

    paths = _document_paths(_source(root, tmp_path, max_total_bytes=1_000_000))

    assert paths == {"README.md", "src/a.md", "src/b.md", "vendored_data/big.md"}


def test_a_corpus_over_the_cap_is_refused_and_names_its_largest_folders(
    tmp_path: Path,
) -> None:
    """The failure case: the run stops, and says where the bytes are."""
    root = _repo(tmp_path)
    source = _source(root, tmp_path, max_total_bytes=2_000)

    with pytest.raises(DocCorpusOversize) as raised:
        source.run("run-1", mode="scope_complete")

    data = raised.value.data
    assert data["max_total_bytes"] == 2_000
    total = sum(
        (root / rel).stat().st_size
        for rel in ("README.md", "src/a.md", "src/b.md", "vendored_data/big.md")
    )
    assert data["total_bytes"] == total
    assert data["files_counted"] == 4
    folders = [row["path"] for row in data["largest_directories"]]
    assert folders == ["vendored_data", "src", "(repository root files)"]
    assert data["largest_directories"][0]["bytes"] == (
        root / "vendored_data" / "big.md"
    ).stat().st_size
    # The message an operator reads carries the same facts.
    message = str(raised.value)
    assert f"{total:,}" in message
    assert "vendored_data" in message
    assert "Nothing was read" in message


def test_the_refusal_is_all_or_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No file of an over-cap corpus is read: not the small ones either."""
    import okg.substrate.library.sources._file_walk as file_walk

    class _NoRead:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("a file was read from an over-cap corpus")

    monkeypatch.setattr(file_walk, "FileWalk", _NoRead)
    root = _repo(tmp_path)
    source = _source(root, tmp_path, max_total_bytes=2_000)

    with pytest.raises(DocCorpusOversize):
        source.run("run-1", mode="scope_complete")


def test_the_change_probe_refuses_an_over_cap_corpus_too(tmp_path: Path) -> None:
    """The worker's poll reads the same candidate set, so it fails the same way."""
    root = _repo(tmp_path)
    source = _source(root, tmp_path, max_total_bytes=2_000)

    with pytest.raises(DocCorpusOversize):
        source.change_probe.build_token()


def test_a_file_over_the_per_file_cap_does_not_count(tmp_path: Path) -> None:
    """big.md is recorded as an oversize document, so its bytes are not read."""
    root = _repo(tmp_path)
    source = _source(
        root, tmp_path, max_bytes_per_file=1_000, max_total_bytes=1_500,
    )

    paths = _document_paths(source)

    assert paths == {"README.md", "src/a.md", "src/b.md", "vendored_data/big.md"}
    assert set(source.last_run_stats["files_recorded_oversize_sample"]) == {
        "vendored_data/big.md",
    }


def test_no_cap_reads_the_same_corpus_as_a_cap_that_is_not_reached(
    tmp_path: Path,
) -> None:
    """None is the default, and a cap above the corpus changes no emission."""
    root = _repo(tmp_path)

    assert _source(root, tmp_path).max_total_bytes is None
    uncapped = _emission(_source(root, tmp_path))
    capped = _emission(_source(root, tmp_path, max_total_bytes=10_000_000))

    assert uncapped
    assert uncapped == capped


def test_the_cap_is_not_part_of_the_change_probe(tmp_path: Path) -> None:
    """Changing the cap re-reads nothing: the probe token does not move."""
    root = _repo(tmp_path)

    before = _source(root, tmp_path).change_probe.build_token()
    after = _source(root, tmp_path, max_total_bytes=10_000_000).change_probe.build_token()

    from okg.substrate.library.sources.change_probe import tokens_equal

    assert tokens_equal(before, after)


def test_a_scoped_event_run_does_not_recheck_the_cap(tmp_path: Path) -> None:
    """An event names a few files; only a whole-corpus run applies the cap."""
    root = _repo(tmp_path)
    source = _source(root, tmp_path, max_total_bytes=2_000)

    run = source.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["src/a.md"]},
    )

    assert {
        fact.attrs["path"]
        for fact in run.facts
        if getattr(fact, "subtype", None) == "document"
    } == {"src/a.md"}


@pytest.mark.parametrize("bad", [0, -1, True, "64"])
def test_a_cap_that_is_not_a_positive_integer_is_refused(
    tmp_path: Path, bad: object,
) -> None:
    root = _repo(tmp_path)

    with pytest.raises(SubstrateError) as raised:
        _source(root, tmp_path, max_total_bytes=bad)

    assert "max_total_bytes" in str(raised.value)


def test_a_symlink_inside_the_repository_counts_the_bytes_the_read_loads(
    tmp_path: Path,
) -> None:
    """The cap sizes a file through the reader that loads it.

    A tracked link to another file in the repository is read as the text of
    that file, so the cap counts those bytes too.
    """
    root = _repo(tmp_path)
    (root / "src" / "link.md").symlink_to("../vendored_data/big.md")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "link")
    read_bytes = sum(
        len((root / rel).read_bytes())
        for rel in (
            "README.md", "src/a.md", "src/b.md", "src/link.md",
            "vendored_data/big.md",
        )
    )
    assert _document_paths(_source(root, tmp_path)) >= {"src/link.md"}

    with pytest.raises(DocCorpusOversize) as raised:
        _source(root, tmp_path, max_total_bytes=1_000).run(
            "run-1", mode="scope_complete",
        )

    assert raised.value.data["total_bytes"] == read_bytes
    assert raised.value.data["files_counted"] == 5
