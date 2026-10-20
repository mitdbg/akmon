"""Issue #2357: doc-corpus reads what git tracks, never what git ignores.

Before this change DocCorpusSource walked the whole directory tree, so any
git-ignored file matching the include globs -- local state holding database
passwords, a `.env`, a scratch dump -- was read, chunked, and published. A
single tracked file over `max_bytes_per_file` also failed the whole source.

These tests run the adapter directly on throwaway git repositories; no
database is needed.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources import _git_helpers
from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.doc_corpus import (
    DocCorpusNotGitWorkTree,
    DocCorpusSource,
)

SECRET = "postgres-password-do-not-publish"


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


def _extractors(tmp_path: Path) -> Path:
    ex = tmp_path / "extractors"
    ex.mkdir(parents=True)
    (ex / "extractors.yaml").write_text("parser: []\n")
    return ex


def _repo(tmp_path: Path) -> Path:
    """A committed repo with two tracked docs and two ignored secret files."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / ".gitignore").write_text("config/secrets.yaml\n.okg/\n")
    (root / "README.md").write_text("# Readme\n\nTracked prose.\n")
    (root / "docs").mkdir()
    (root / "docs" / "guide.md").write_text("# Guide\n\nTracked guide.\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    (root / "config").mkdir()
    (root / "config" / "secrets.yaml").write_text(f"password: {SECRET}\n")
    (root / ".okg").mkdir()
    (root / ".okg" / "local.env").write_text(f"OKG_DSN=postgresql://u:{SECRET}@h/db\n")
    return root


def _source(root: Path, tmp_path: Path, **kwargs) -> DocCorpusSource:
    return DocCorpusSource(
        repo="fixture",
        repo_root=root,
        extractors_dir=_extractors(tmp_path),
        include_globs=("**/*",),
        **kwargs,
    )


def _run(source: DocCorpusSource) -> tuple[set[str], list]:
    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)
    paths = {
        fact.attrs["path"]
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }
    return paths, facts


def test_git_ignored_secret_files_are_never_read(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    source = _source(root, tmp_path)

    paths, facts = _run(source)

    assert paths == {".gitignore", "README.md", "docs/guide.md"}
    assert SECRET not in repr(facts)
    assert source.last_run_stats["file_enumeration"] == "git"


def test_probe_scope_equals_read_scope(tmp_path: Path) -> None:
    """The change probe hashes the same candidate set the run reads.

    Editing an ignored secret must not move the probe token (the probe did
    not read it); editing a tracked doc must.
    """
    root = _repo(tmp_path)
    source = _source(root, tmp_path)
    before = source.change_probe.build_token()

    (root / "config" / "secrets.yaml").write_text("password: rotated\n")
    assert tokens_equal(before, source.change_probe.build_token())

    (root / "README.md").write_text("# Readme\n\nEdited.\n")
    assert not tokens_equal(before, source.change_probe.build_token())


def test_untracked_file_is_read_only_with_include_untracked(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    (root / "draft.md").write_text("# Draft\n\nNot yet committed.\n")

    default_paths, _ = _run(_source(root, tmp_path / "a"))
    opted_paths, facts = _run(_source(root, tmp_path / "b", include_untracked=True))

    assert "draft.md" not in default_paths
    assert "draft.md" in opted_paths
    # Untracked-but-ignored stays out even with include_untracked.
    assert "config/secrets.yaml" not in opted_paths
    assert ".okg/local.env" not in opted_paths
    assert SECRET not in repr(facts)


def test_include_ignored_paths_opens_only_the_named_ignored_path(
    tmp_path: Path,
) -> None:
    root = _repo(tmp_path)

    paths, _ = _run(
        _source(root, tmp_path, include_ignored_paths=[".okg/local.env"]),
    )

    assert ".okg/local.env" in paths
    assert "config/secrets.yaml" not in paths


def test_oversized_tracked_file_is_recorded_not_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tracked file over the limit is still a candidate: it is recorded as
    an `oversize` document (#2245) with no chunks, counted, and the run
    continues. Its bytes are never handed to the parser: every walk event
    for it carries no content, and the whole-file reader is never called on
    it."""
    from okg.substrate.library.sources import _file_walk

    root = _repo(tmp_path)
    body = "[" + "7," * 4096 + "7]"
    (root / "big.json").write_text(body)
    _git(root, "add", "big.json")
    _git(root, "commit", "-q", "-m", "big")
    source = _source(root, tmp_path, max_bytes_per_file=4096)

    whole_reads: list[str] = []
    real_read_bytes = _file_walk.ContainedFileReader.read_bytes

    def spy_read_bytes(self, path, *args, **kwargs):
        whole_reads.append(Path(path).name)
        return real_read_bytes(self, path, *args, **kwargs)

    big_events: list = []
    real_iter = _file_walk.FileWalk.iter

    def spy_iter(self, paths):
        for event in real_iter(self, paths):
            if event.rel_path == "big.json":
                big_events.append(event)
            yield event

    monkeypatch.setattr(_file_walk.ContainedFileReader, "read_bytes", spy_read_bytes)
    monkeypatch.setattr(_file_walk.FileWalk, "iter", spy_iter)

    paths, facts = _run(source)

    assert "big.json" in paths
    assert "README.md" in paths
    big_doc = next(
        fact for fact in facts
        if getattr(fact, "subtype", None) == "document"
        and fact.attrs["path"] == "big.json"
    )
    assert "oversize" in big_doc.attrs["parse_warnings"]
    assert big_doc.attrs["byte_count"] == len(body)
    # No text from the file reaches any fact: no chunk, no document text.
    assert "7,7,7" not in repr(facts)
    assert not [
        fact for fact in facts
        if getattr(fact, "subtype", None) == "document_chunk"
        and "big.json" in repr(fact.attrs)
    ]
    assert big_events and all(
        event.content is None and event.oversize_bytes == len(body)
        for event in big_events
    )
    assert "big.json" not in whole_reads
    stats = source.last_run_stats
    assert stats["files_recorded_oversize"] == 1
    assert stats["files_recorded_oversize_sample"] == ["big.json"]


def test_non_git_root_without_opt_in_fails_naming_the_param(tmp_path: Path) -> None:
    root = tmp_path / "plain"
    root.mkdir()
    (root / "a.md").write_text("# A\n")

    with pytest.raises(DocCorpusNotGitWorkTree, match="non_git_walk"):
        _run(_source(root, tmp_path / "x"))

    paths, _ = _run(_source(root, tmp_path / "y", non_git_walk=True))
    assert paths == {"a.md"}


def test_ignored_root_inside_a_work_tree_is_refused(tmp_path: Path) -> None:
    """A root git ignores would list nothing and retract the whole corpus."""
    root = _repo(tmp_path)

    with pytest.raises(DocCorpusNotGitWorkTree, match="ignored"):
        _run(_source(root / ".okg", tmp_path / "x"))


def test_scope_key_cannot_pull_in_an_ignored_file(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    source = _source(root, tmp_path)

    run = source.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["config/secrets.yaml", "README.md"]},
    )
    facts = list(run.facts)
    paths = {
        fact.attrs["path"]
        for fact in facts
        if getattr(fact, "subtype", None) == "document"
    }

    assert paths == {"README.md"}
    assert SECRET not in repr(facts)


def test_git_paths_that_are_not_utf8_are_decoded_like_os_walk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`-z` bytes decode with os.fsdecode, so the name still opens.

    macOS refuses to create such a file, so git's output is supplied here;
    the decoding is the part under test. Decoding with errors="replace"
    turned the name into one no file has, and the file was dropped silently.
    """
    root = _repo(tmp_path)
    raw_name = b"caf\xe9.md"
    real_run = subprocess.run

    def fake_git(cmd, **kwargs):
        if "ls-files" in cmd:
            return subprocess.CompletedProcess(
                cmd, 0, raw_name + b"\0README.md\0", b"",
            )
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(_git_helpers.subprocess, "run", fake_git)

    candidates = _source(root, tmp_path)._git_candidate_paths()

    assert candidates == {os.fsdecode(raw_name), "README.md"}


def test_a_non_utf8_tracked_file_is_read(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    raw_name = b"caf\xe9.md"
    try:
        fd = os.open(
            os.path.join(os.fsencode(root), raw_name), os.O_WRONLY | os.O_CREAT,
        )
    except OSError:
        pytest.skip("this filesystem refuses names that are not UTF-8")
    os.write(fd, b"# Cafe\n\nLatin-1 name.\n")
    os.close(fd)
    _git(root, "add", "--all")
    _git(root, "commit", "-q", "-m", "latin-1 name")

    listed = _source(root, tmp_path)._walk_files()

    assert root / os.fsdecode(raw_name) in listed


def _scoped_paths(source: DocCorpusSource, keys: list[str]) -> set[str]:
    run = source.run(
        "run-scoped", mode="scope_incomplete", sync_scope={"scope_keys": keys},
    )
    return {
        fact.attrs["path"]
        for fact in run.facts
        if getattr(fact, "subtype", None) == "document"
    }


def test_dot_slash_scope_key_finds_the_tracked_file(tmp_path: Path) -> None:
    """A scope key spelled `./a` or `a//b` names the file git lists as `a`.

    Before, the raw key was looked up in git's list, missed, and the scoped
    run dropped the file without a word.
    """
    root = _repo(tmp_path)

    paths = _scoped_paths(
        _source(root, tmp_path), ["./README.md", "docs//guide.md"],
    )

    assert paths == {"README.md", "docs/guide.md"}


def test_scope_key_that_leaves_the_root_is_out_of_scope(tmp_path: Path) -> None:
    root = _repo(tmp_path)

    paths = _scoped_paths(
        _source(root, tmp_path), ["../repo/README.md", "docs/../../x.md", "."],
    )

    assert paths == set()


def test_scope_keys_outside_include_globs_are_not_read(tmp_path: Path) -> None:
    """Issue #3259 guard: a scope key narrows the configured globs, never
    widens them, and the exclude globs still win."""
    root = tmp_path / "repo"
    (root / "docs" / "drafts").mkdir(parents=True)
    (root / "README.md").write_text("# Readme\n\nOutside the globs.\n")
    (root / "docs" / "guide.md").write_text("# Guide\n\nInside the globs.\n")
    (root / "docs" / "drafts" / "wip.md").write_text("# WIP\n\nExcluded.\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    source = DocCorpusSource(
        repo="fixture",
        repo_root=root,
        extractors_dir=_extractors(tmp_path),
        include_globs=("docs/**",),
        exclude_globs=("docs/drafts/**",),
    )

    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    full_keys = set(first.record_set.records)
    assert full_keys == {"docs/guide.md"}
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    (root / "docs" / "guide.md").write_text("# Guide\n\nEdited.\n")
    keys = ["docs/guide.md", "README.md", "docs/drafts/wip.md"]
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": keys},
    )
    paths = {
        fact.attrs["path"]
        for fact in scoped.facts
        if getattr(fact, "subtype", None) == "document"
    }

    assert paths == {"docs/guide.md"}
    assert scoped.record_set is not None
    assert set(scoped.record_set.records) == full_keys
    assert set((scoped.next_cursor or {}).get("files") or {}) == full_keys
