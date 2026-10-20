from __future__ import annotations

import os
from types import SimpleNamespace
from pathlib import Path

import pytest
import yaml

import okg.substrate.library.sources.doc_corpus as doc_corpus_module

from okg.substrate.library.sources._file_walk import (
    ContainedFileReader,
    FileContainmentError,
    FileReadLimitError,
    FileWalk,
)

DocCorpusSource = doc_corpus_module.DocCorpusSource


def _extractors(tmp_path: Path) -> Path:
    root = tmp_path / "extractors"
    root.mkdir()
    (root / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": "okg.substrate.library.extraction.parse.markdown.MarkdownParser",
        }, {
            "format": "latex",
            "class": "okg.substrate.library.extraction.parse.latex.LatexParser",
        }],
        "chunker": [{
            "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
            "config": {"max_chars": 200, "overlap": 0},
        }],
        "ner": [],
    }))
    return root


def _facts(source: DocCorpusSource) -> list[object]:
    return list(source.run("containment-test", mode="cursor").facts)


def test_full_run_reads_ordinary_file_with_lexical_identity(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "ordinary.md").write_text("# Ordinary\n\nORDINARY_SENTINEL")
    facts = _facts(DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    ))
    assert any(getattr(fact, "node_id", "") == "doc:repo:ordinary.md" for fact in facts)
    assert any("ORDINARY_SENTINEL" in str(getattr(fact, "attrs", {})) for fact in facts)


def test_full_run_preserves_unscoped_inroot_symlink_lexical_identity(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "target.md").write_text("# Target\n\nINROOT_SENTINEL")
    (corpus / "alias.md").symlink_to("target.md")
    facts = _facts(DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    ))
    assert any(getattr(fact, "node_id", "") == "doc:repo:alias.md" for fact in facts)


def test_full_run_preserves_scoped_inroot_symlink_lexical_identity(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "target.md").write_text("# Target\n\nINROOT_SENTINEL")
    (corpus / "alias.md").symlink_to("target.md")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    run = source.run(
        "scoped", mode="scope_incomplete",
        sync_scope={"scope_keys": ["alias.md"]},
    )
    facts = list(run.facts)
    assert any(getattr(fact, "node_id", "") == "doc:repo:alias.md" for fact in facts)


def test_manifest_selected_inroot_symlink_keeps_lexical_identity(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    paper = corpus / "paper"
    paper.mkdir(parents=True)
    (paper / "target.tex").write_text(
        "\\documentclass{article}\\begin{document}Safe\\end{document}"
    )
    (paper / "alias.tex").symlink_to("target.tex")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        project_manifest={
            "repository_authority": "repo",
            "projects": [{"project_dir": "paper", "entrypoints": ["alias.tex"]}],
        },
    )
    facts = _facts(source)
    assert any(getattr(fact, "node_id", "") == "doc:repo:paper/alias.tex" for fact in facts)


def test_full_run_refuses_outside_symlink_before_parser_chunker_or_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    (corpus / "escape.md").symlink_to(outside)
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    bundle = doc_corpus_module.load_extractors(source.extractors_dir)
    parser_or_chunker_calls: list[str] = []
    for parser in bundle.parsers.values():
        monkeypatch.setattr(
            parser, "parse",
            lambda *_args, **_kwargs: parser_or_chunker_calls.append("parser"),
        )
    if bundle.chunker is not None:
        monkeypatch.setattr(
            bundle.chunker, "chunk",
            lambda *_args, **_kwargs: parser_or_chunker_calls.append("chunker"),
        )
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        source.run("outside", mode="cursor")
    assert parser_or_chunker_calls == []
    assert source._contained_reader is None


def test_secure_read_refuses_final_symlink_retarget_at_open_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    inside = root / "inside"
    inside.write_text("inside")
    outside = tmp_path / "outside"
    outside.write_text("outside")
    candidate = root / "candidate"
    candidate.symlink_to(inside)
    reader = ContainedFileReader(root)
    original = os.readlink

    def retarget(name: str, *, dir_fd: int | None = None) -> str:
        candidate.unlink()
        candidate.symlink_to(outside)
        return original(name, dir_fd=dir_fd)

    monkeypatch.setattr(os, "readlink", retarget)
    with reader:
        with pytest.raises(FileContainmentError, match="escapes corpus root"):
            reader.read_bytes(candidate)


def test_secure_read_refuses_ancestor_directory_replaced_by_outside_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (nested / "doc").write_text("inside")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "doc").write_text("outside")
    original = os.open
    reader = ContainedFileReader(root)
    swapped = False

    def swap_then_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        if path == "nested" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            nested.rename(root / "old")
            nested.symlink_to(outside, target_is_directory=True)
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", swap_then_open)
    with reader:
        with pytest.raises(OSError):
            reader.read_bytes(root / "nested/doc")


def test_secure_read_refuses_opened_ancestor_relocated_outside_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (nested / "doc").write_text("inside")
    reader = ContainedFileReader(root)
    original = os.open
    moved = tmp_path / "moved"
    swapped = False

    def relocate_then_open(
        path: object, flags: int, *args: object, **kwargs: object,
    ) -> int:
        nonlocal swapped
        if path == "doc" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            nested.rename(moved)
            (moved / "doc").unlink()
            (moved / "doc").write_text("OUTSIDE_SENTINEL")
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", relocate_then_open)
    with reader:
        with pytest.raises(FileContainmentError, match="ancestor moved outside"):
            reader.read_bytes(root / "nested/doc")


def test_secure_read_refuses_when_required_dirfd_primitives_are_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "supports_dir_fd", set())
    with pytest.raises(FileContainmentError, match="dir_fd support"):
        ContainedFileReader(tmp_path)


def test_contained_filewalk_enforces_fstat_size_bound(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    candidate = root / "large"
    candidate.write_bytes(b"12345")
    walk = FileWalk(
        prior_cursor=None, repo_root=root, read_as="bytes", contained_io=True,
        max_bytes=4,
    )
    with pytest.raises(FileReadLimitError, match="max_bytes=4"):
        list(walk.iter([candidate]))


def test_contained_filewalk_records_oversize_file_without_loading_it(
    tmp_path: Path,
) -> None:
    """``oversize="record"`` yields an event for a file over ``max_bytes``
    instead of stopping the walk: no content, its first bytes, and the
    SHA-256 of the whole file. The walk continues to the next file, and the
    cursor keeps the oversize file so an unchanged rerun sees it as
    unchanged (issue #2245; operator decision 2026-09-22)."""
    import hashlib

    root = tmp_path / "root"
    root.mkdir()
    big = root / "big.txt"
    big.write_bytes(b"0123456789" * 20)
    small = root / "small.txt"
    small.write_bytes(b"ok")

    walk = FileWalk(
        prior_cursor=None, repo_root=root, read_as="bytes", contained_io=True,
        strict_io=True, max_bytes=16, oversize="record",
    )
    events = {event.rel_path: event for event in walk.iter([big, small])}

    assert set(events) == {"big.txt", "small.txt"}
    oversize = events["big.txt"]
    assert oversize.content is None
    assert oversize.oversize_bytes == 200
    assert oversize.head == (b"0123456789" * 20)[:64]
    assert oversize.content_hash == hashlib.sha256(big.read_bytes()).hexdigest()
    assert oversize.is_changed is True
    assert events["small.txt"].content == b"ok"
    assert events["small.txt"].oversize_bytes is None
    cursor = walk.next_cursor()
    assert set(cursor["files"]) == {"big.txt", "small.txt"}

    rerun = FileWalk(
        prior_cursor=cursor, repo_root=root, read_as="bytes", contained_io=True,
        strict_io=True, max_bytes=16, oversize="record",
    )
    again = {event.rel_path: event for event in rerun.iter([big, small])}
    assert again["big.txt"].is_changed is False
    assert again["big.txt"].oversize_bytes == 200


def test_contained_filewalk_rejects_unknown_oversize_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="oversize"):
        FileWalk(prior_cursor=None, repo_root=tmp_path, oversize="skip")


def test_full_run_records_oversize_file_and_continues(tmp_path: Path) -> None:
    """A DocCorpus run with one file over ``max_bytes_per_file`` records it
    as a Document with an ``oversize`` warning and no chunks, and still
    parses every other file."""
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "ordinary.md").write_text("# Ordinary\n\nORDINARY_SENTINEL")
    (corpus / "huge.md").write_text("# Huge\n\n" + "HUGE_SENTINEL " * 400)
    facts = _facts(DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",), max_bytes_per_file=1024,
    ))

    documents = {
        fact.attrs["path"]: fact.attrs
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }
    assert set(documents) == {"ordinary.md", "huge.md"}
    assert documents["huge.md"]["parse_warnings"] == ["oversize"]
    assert documents["huge.md"]["byte_count"] == (corpus / "huge.md").stat().st_size
    assert "HUGE_SENTINEL" not in str([getattr(f, "attrs", {}) for f in facts])
    assert any("ORDINARY_SENTINEL" in str(getattr(f, "attrs", {})) for f in facts)


def _oversize_corpus(tmp_path: Path) -> Path:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "ordinary.md").write_text("# Ordinary\n\nORDINARY_SENTINEL")
    (corpus / "huge.md").write_text("# Huge\n\n" + "HUGE_SENTINEL " * 400)
    (corpus / "blob.xyz").write_bytes(b"\x00\x01" * 1000)
    return corpus


def _documents(run: object) -> tuple[dict[str, dict], str]:
    facts = list(run.facts)
    documents = {
        fact.attrs["path"]: fact.attrs
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }
    return documents, str([getattr(fact, "attrs", {}) for fact in facts])


def test_every_oversize_file_carries_the_oversize_warning(tmp_path: Path) -> None:
    """An oversize file of unknown format still says ``oversize``; the
    format warning does not replace it."""
    corpus = _oversize_corpus(tmp_path)
    run = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md", "**/*.xyz"), max_bytes_per_file=1024,
    ).run("oversize-formats", mode="scope_complete")
    documents, _text = _documents(run)
    assert "oversize" in documents["blob.xyz"]["parse_warnings"]
    assert documents["blob.xyz"]["byte_count"] == 2000
    assert documents["huge.md"]["parse_warnings"] == ["oversize"]
    assert "oversize" not in documents["ordinary.md"]["parse_warnings"]


@pytest.mark.parametrize("mode", ["scope_complete", "reconcile", "cursor"])
def test_raising_the_byte_limit_reparses_a_recorded_oversize_file(
    tmp_path: Path, mode: str,
) -> None:
    """Unchanged bytes but a raised limit: the file recorded as oversize
    is parsed on the next run instead of the old record being reused."""
    corpus = _oversize_corpus(tmp_path)
    extractors = _extractors(tmp_path)

    def source(limit: int) -> DocCorpusSource:
        return DocCorpusSource(
            repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=extractors,
            include_globs=("**/*.md",), max_bytes_per_file=limit,
        )

    first = source(1024).run("limit-low", mode=mode)
    documents, text = _documents(first)
    assert documents["huge.md"]["parse_warnings"] == ["oversize"]
    assert "HUGE_SENTINEL" not in text
    # What SourceRunner stores: the source cursor plus the record set, which
    # is what lets an unchanged rerun reuse prior records.
    cursor = dict(first.next_cursor)
    if first.record_set is not None:
        cursor["record_set"] = first.record_set.to_cursor()

    unchanged = source(1024).run("limit-low-again", mode=mode, cursor=cursor)
    assert "HUGE_SENTINEL" not in _documents(unchanged)[1]

    raised = source(10**6).run("limit-raised", mode=mode, cursor=cursor)
    documents, text = _documents(raised)
    assert "HUGE_SENTINEL" in text
    assert "oversize" not in documents["huge.md"]["parse_warnings"]


def test_scoped_run_after_a_limit_change_keeps_the_change_pending(
    tmp_path: Path,
) -> None:
    """A scoped run re-parses only its slice, so it must not record the new
    limit as settled; the next full run still re-parses the oversize file."""
    corpus = _oversize_corpus(tmp_path)
    extractors = _extractors(tmp_path)

    def source(limit: int) -> DocCorpusSource:
        return DocCorpusSource(
            repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=extractors,
            include_globs=("**/*.md",), max_bytes_per_file=limit,
        )

    first = source(1024).run("limit-low", mode="scope_complete")
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()

    scoped = source(10**6).run(
        "limit-raised-scoped", mode="scope_incomplete", cursor=cursor,
        sync_scope={"scope_keys": ["ordinary.md"]},
    )
    assert scoped.next_cursor["doc_corpus_max_bytes_per_file"] == -1
    after_scoped = dict(scoped.next_cursor)
    after_scoped["record_set"] = cursor["record_set"]

    full = source(10**6).run(
        "limit-raised-full", mode="scope_complete", cursor=after_scoped,
    )
    assert "HUGE_SENTINEL" in _documents(full)[1]
    assert full.next_cursor["doc_corpus_max_bytes_per_file"] == 10**6

    # Reverting to the old limit after the scoped run also re-parses, so
    # huge.md goes back to an oversize record instead of keeping chunks.
    reverted = source(1024).run(
        "limit-reverted-full", mode="scope_complete", cursor=after_scoped,
    )
    documents, text = _documents(reverted)
    assert documents["huge.md"]["parse_warnings"] == ["oversize"]
    assert "HUGE_SENTINEL" not in text


@pytest.mark.parametrize("next_limit", [1024, 10**6])
def test_unfinished_run_after_a_limit_change_keeps_the_change_pending(
    tmp_path: Path, next_limit: int,
) -> None:
    """Issue #2872: a run that does not complete its scope emits no record
    set, so the chunks of a file it now records as oversize are not
    retracted. It must not record the new limit as settled; the next complete
    run, under either limit, must re-parse the file instead of reusing the
    prior record that still carries its chunks."""
    corpus = _oversize_corpus(tmp_path)
    extractors = _extractors(tmp_path)

    def source(limit: int) -> DocCorpusSource:
        return DocCorpusSource(
            repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=extractors,
            include_globs=("**/*.md",), max_bytes_per_file=limit,
        )

    first = source(10**6).run("limit-high", mode="scope_complete")
    assert "HUGE_SENTINEL" in _documents(first)[1]
    assert first.record_set.records["huge.md"].descendants, "huge.md has chunks"
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()

    # Unscoped and unfinished: cursor mode builds no record set.
    lowered = source(1024).run("limit-lowered-cursor", mode="cursor", cursor=cursor)
    assert lowered.record_set is None
    assert _documents(lowered)[0]["huge.md"]["parse_warnings"] == ["oversize"]
    # What SourceRunner stores after a run with no record set: the prior one.
    after_lowered = dict(lowered.next_cursor)
    after_lowered["record_set"] = cursor["record_set"]

    complete = source(next_limit).run(
        "limit-complete", mode="scope_complete", cursor=after_lowered,
    )
    # Before the fix the lowered run recorded 1024, so a complete run at 1024
    # took the no-change shortcut and carried the chunked record forward.
    assert not complete.preserves_prior_record_set
    assert complete.record_set is not None
    documents, text = _documents(complete)
    assert "huge.md" in documents, "huge.md was reused, not re-parsed"
    assert complete.next_cursor["doc_corpus_max_bytes_per_file"] == next_limit
    huge_record = complete.record_set.records["huge.md"]
    if next_limit == 1024:
        assert documents["huge.md"]["parse_warnings"] == ["oversize"]
        assert "HUGE_SENTINEL" not in text
        assert huge_record.descendants == []
    else:
        assert "HUGE_SENTINEL" in text
        assert huge_record.descendants
    # The cause: the unfinished run recorded a value no real limit equals.
    assert lowered.next_cursor["doc_corpus_max_bytes_per_file"] == -1


def test_unfinished_run_under_an_unchanged_limit_records_the_limit(
    tmp_path: Path,
) -> None:
    """Only a changed limit is held pending; an unfinished run under the
    same limit records it, so the next complete run can still skip work."""
    corpus = _oversize_corpus(tmp_path)
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",), max_bytes_per_file=1024,
    )
    first = source.run("limit", mode="scope_complete")
    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()
    again = source.run("limit-cursor", mode="cursor", cursor=cursor)
    assert again.record_set is None
    assert again.next_cursor["doc_corpus_max_bytes_per_file"] == 1024


def test_change_probe_tokenises_an_oversize_file_without_reading_it(
    tmp_path: Path,
) -> None:
    import hashlib

    corpus = _oversize_corpus(tmp_path)
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",), max_bytes_per_file=1024,
    )
    huge = corpus / "huge.md"
    with ContainedFileReader(corpus) as reader:
        source._contained_reader = reader
        try:
            token = source._contained_probe_bytes(huge)
            ordinary = source._contained_probe_bytes(corpus / "ordinary.md")
        finally:
            source._contained_reader = None
    digest = hashlib.sha256(huge.read_bytes()).hexdigest()
    assert token == f"okg-doc-corpus-oversize:{huge.stat().st_size}:{digest}".encode()
    assert ordinary == (corpus / "ordinary.md").read_bytes()


def test_contained_filewalk_refusal_is_fail_loud_for_strict_io_integration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("OUTSIDE_SENTINEL")
    candidate = root / "escape"
    candidate.symlink_to(outside)
    walk = FileWalk(
        prior_cursor=None, repo_root=root, read_as="bytes", contained_io=True,
    )
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        list(walk.iter([candidate]))
    assert walk.next_cursor()["files"] == {}


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires POSIX")
def test_secure_read_refuses_fifo_before_content_read(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    fifo = root / "fifo"
    os.mkfifo(fifo)
    with ContainedFileReader(root) as reader:
        with pytest.raises(FileContainmentError, match="not a regular file"):
            reader.read_bytes(fifo)


def test_unchanged_probe_refuses_retargeted_candidate(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    document = corpus / "doc.md"
    document.write_text("# Safe\n\nSAFE_SENTINEL")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    first = source.run("first", mode="cursor")
    list(first.facts)
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    document.unlink()
    document.symlink_to(outside)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        source.run("second", mode="cursor", cursor=first.next_cursor)


def test_run_closes_root_descriptor_after_success_and_refusal(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.md").write_text("# Safe\n\nSAFE_SENTINEL")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    _facts(source)
    assert source._contained_reader is None
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    (corpus / "doc.md").unlink()
    (corpus / "doc.md").symlink_to(outside)
    with pytest.raises(FileContainmentError):
        source.run("refusal", mode="cursor")
    assert source._contained_reader is None


def test_scoped_selection_swap_cannot_false_omit_or_read_outside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    candidate = corpus / "doc.md"
    candidate.write_text("SAFE_SENTINEL")
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    original = source._contained_is_file

    def select_then_swap(path: Path) -> bool:
        selected = original(path)
        candidate.unlink()
        candidate.symlink_to(outside)
        return selected

    monkeypatch.setattr(source, "_contained_is_file", select_then_swap)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        source.run(
            "scoped-swap", mode="scope_incomplete",
            sync_scope={"scope_keys": ["doc.md"]},
        )


def test_caption_planning_refuses_retargeted_candidate_before_caption_or_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    paper = corpus / "paper"
    paper.mkdir(parents=True)
    (paper / "main.tex").write_text(
        "\\documentclass{article}\\includegraphics{figure.pdf}"
    )
    figure = paper / "figure.pdf"
    figure.write_bytes(b"%PDF-1.4 SAFE_SENTINEL")
    extractors = _extractors(tmp_path)
    config_path = extractors / "extractors.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["captioning"] = {"enabled": False}
    config_path.write_text(yaml.safe_dump(config))

    class CaptionMustNotRun:
        def caption(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("outside bytes reached caption service")

    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=extractors,
        project_manifest={
            "repository_authority": "repo",
            "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        },
        caption_service=CaptionMustNotRun(),
    )
    first = source.run("caption-first", mode="cursor")
    list(first.facts)
    config["captioning"] = {"enabled": True, "model": "guarded-test-model"}
    config_path.write_text(yaml.safe_dump(config))
    outside = tmp_path / "outside.pdf"
    outside.write_bytes(b"%PDF-1.4 OUTSIDE_SENTINEL")
    original = source._contained_read_bytes
    figure_reads = 0

    def retarget_during_planning(path: Path) -> bytes:
        nonlocal figure_reads
        if Path(path).name == "figure.pdf":
            figure_reads += 1
            if figure_reads == 2:
                figure.unlink()
                figure.symlink_to(outside)
        return original(path)

    monkeypatch.setattr(source, "_contained_read_bytes", retarget_during_planning)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        source.run("caption-refusal", mode="cursor", cursor=first.next_cursor)
    assert figure_reads == 2


@pytest.mark.parametrize("force_full_reprocess", [False, True])
def test_unchanged_and_forced_rereads_use_secure_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, force_full_reprocess: bool,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    candidate = corpus / "doc.md"
    candidate.write_text("# Safe\n\nSAFE_SENTINEL")
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    first = source.run("first", mode="cursor")
    list(first.facts)
    if force_full_reprocess:
        source.snapshot_date = "2026-09-05"
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    original = source._contained_read_bytes
    calls = 0

    def retarget_on_reread(path: Path) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 1:
            candidate.unlink()
            candidate.symlink_to(outside)
        return original(path)

    monkeypatch.setattr(source, "_contained_read_bytes", retarget_on_reread)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        source.run("reread", mode="reconcile", cursor=first.next_cursor)
    assert calls == 1


def test_containment_refusal_precedes_runner_durable_effect_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.ingest import SourceRunner, compensation

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE_SENTINEL")
    (corpus / "escape.md").symlink_to(outside)
    source = DocCorpusSource(
        repo="repo", repo_root=corpus, non_git_walk=True, extractors_dir=_extractors(tmp_path),
        include_globs=("**/*.md",),
    )
    effects: list[str] = []
    monkeypatch.setattr(
        compensation, "snapshot_source_state",
        lambda _dsn, **kw: compensation.SourceStateSnapshot(
            deployment=kw["deployment"], source=kw["source"],
            watermark=None, progress_rows=(),
        ),
    )

    class MemoryRunner(SourceRunner):
        def _acquire_conn(self) -> object:
            return object()

        def _load_volatile_fields(self) -> dict[str, frozenset[str]]:
            return {}

        def _active_nomos_policy_fingerprint(self) -> None:
            return None

        def _load_cursor_progress_boundary(self, **_kwargs: object) -> object:
            return SimpleNamespace(
                prior_cursor={}, prior_progress={}, no_prior_facts=True,
                replay_required=None, persisted_cursor={},
            )

        def _prior_record_set(self, *_args: object, **_kwargs: object) -> object:
            effects.append("reconcile")
            raise AssertionError("reconciliation reached")

        def _advance_watermark(self, **_kwargs: object) -> None:
            effects.append("watermark")
            raise AssertionError("watermark reached")

        def _flush_chunk(self, *_args: object, **_kwargs: object) -> None:
            effects.append("flush")
            raise AssertionError("flush reached")

    runner = MemoryRunner(
        "", source_name="repo", deployment_name="repo",
        write_chronos_graph=False, write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        runner.run(source, mode="reconcile")
    assert effects == []
