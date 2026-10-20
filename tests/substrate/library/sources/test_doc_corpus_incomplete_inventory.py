"""Filesystem faults must not become completed document deletion authority."""

import copy
import errno
import os
from pathlib import Path

import pytest

from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources._file_walk import ContainedFileReader


@pytest.fixture
def source(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    for name in ("a.txt", "b.txt"):
        (root / name).write_text("Document " + name)
    config = tmp_path / "extractors"
    config.mkdir()
    (config / "extractors.yaml").write_text(
        "parser:\n- format: text\n  class: okg.substrate.library.extraction.parse.text.TextParser\n"
        "chunker: []\nner: []\ncaptioning: false\n"
    )
    return DocCorpusSource(
        repo="fixture", repo_root=root, extractors_dir=config, non_git_walk=True,
    )


def prior(source):
    result = source.run("baseline", mode="reconcile")
    list(result.facts)
    cursor = copy.deepcopy(result.next_cursor)
    cursor["record_set"] = result.record_set.to_cursor()
    return cursor


@pytest.mark.parametrize("operation", ["stat", "read_bytes"])
def test_enumerated_failure_refuses_completed_run(source, monkeypatch, operation):
    cursor = prior(source)
    # Force main processing after the probe sees an earlier changed file.
    (source.repo_root / "a.txt").write_text("Changed earlier file")
    target = source.repo_root / "b.txt"
    original_method = ContainedFileReader.open

    def failed(reader, path, *args, **kwargs):
        if path == target:
            raise OSError(errno.EIO, "fixture transient I/O")
        return original_method(reader, path, *args, **kwargs)

    # Remove cached mtimes so the content read cannot be skipped.
    for entry in cursor["files"].values():
        entry.pop("mtime_ns", None)
    original = copy.deepcopy(cursor)
    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError):
        source.run("failed", mode="reconcile", cursor=cursor)
    assert cursor == original


def test_inventory_error_refuses_completed_run(source, monkeypatch):
    cursor = prior(source)

    def walk(*args, **kwargs):
        failure = PermissionError(errno.EACCES, "unreadable directory")
        if kwargs.get("onerror"):
            kwargs["onerror"](failure)
        return iter(())

    monkeypatch.setattr(os, "walk", walk)
    with pytest.raises(OSError):
        source.run("failed", mode="reconcile", cursor=cursor)


def test_missing_root_is_not_complete_empty_inventory(source):
    for p in source.repo_root.iterdir():
        p.unlink()
    source.repo_root.rmdir()
    with pytest.raises(OSError):
        source._walk_files()


@pytest.mark.parametrize("mode", ["reconcile", "scope_complete", "scope_incomplete"])
@pytest.mark.parametrize("external", [False, True])
def test_scoped_read_failure_preserves_authority(source, monkeypatch, mode, external):
    cursor = prior(source)
    if external:
        cursor.pop("record_set")
        cursor["record_set_ref"] = {"fixture": "external-authority"}
    for entry in cursor["files"].values():
        entry.pop("mtime_ns", None)
    before = copy.deepcopy(cursor)
    read = ContainedFileReader.open

    def failed(reader, path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            raise PermissionError(errno.EACCES, "temporarily unreadable")
        return read(reader, path, *args, **kwargs)

    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError):
        source.run(
            "fault", mode=mode, cursor=cursor, sync_scope={"scope_keys": ["b.txt"]}
        )
    assert cursor == before


@pytest.mark.parametrize("project", [False, True])
def test_selected_inventory_does_not_swallow_stat_error(source, monkeypatch, project):
    from types import SimpleNamespace

    if project:
        source.project_manifest = SimpleNamespace(
            source_inventory=[SimpleNamespace(path="b.txt")], projects=[]
        )
    stat = Path.stat

    def failed(path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            raise PermissionError(errno.EACCES, "inventory denied")
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", failed)
    with pytest.raises(OSError):
        source._walk_files(None if project else ["b.txt"])


@pytest.mark.parametrize("force", [False, True])
def test_unchanged_reread_failure_refuses(source, monkeypatch, force):
    cursor = prior(source)
    # No inline authority forces unchanged content to be materialized again.
    cursor.pop("record_set")
    if force:
        cursor["doc_corpus_emission_schema_version"] = -1
    before = copy.deepcopy(cursor)
    read = ContainedFileReader.open
    calls = []

    def failed(reader, path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            calls.append(str(path))
            raise OSError(errno.EIO, "re-read failed")
        return read(reader, path, *args, **kwargs)

    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError):
        source.run("reread", mode="reconcile", cursor=cursor)
    assert calls
    assert cursor == before


def test_genuine_deletion_and_scoped_retention(source):
    cursor = prior(source)
    before = copy.deepcopy(cursor)
    (source.repo_root / "b.txt").unlink()
    result = source.run("deleted", mode="reconcile", cursor=cursor)
    assert result.completed_scope
    assert "b.txt" not in result.next_cursor["files"]
    assert "b.txt" not in result.record_set.records
    assert "a.txt" in result.record_set.records
    scoped = source.run(
        "scoped",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["b.txt"]},
    )
    assert scoped.completed_scope
    assert "a.txt" in scoped.next_cursor["files"]
    assert cursor == before


def test_runner_failure_never_reaches_deletion_or_watermark(source, monkeypatch):
    from types import SimpleNamespace
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.ingest import compensation

    cursor = prior(source)
    calls = []
    snapshots = []

    def snapshot(dsn, **kw):
        snapshots.append((dsn, kw))
        return compensation.SourceStateSnapshot(
            deployment=kw["deployment"],
            source=kw["source"],
            watermark=None,
            progress_rows=(),
        )

    monkeypatch.setattr(compensation, "snapshot_source_state", snapshot)

    class MemoryRunner(SourceRunner):
        def _acquire_conn(self):
            return object()

        def _load_volatile_fields(self):
            return {}

        def _active_nomos_policy_fingerprint(self):
            return None

        def _load_cursor_progress_boundary(self, **kwargs):
            return SimpleNamespace(
                prior_cursor=copy.deepcopy(cursor),
                prior_progress={},
                no_prior_facts=False,
                replay_required=None,
                persisted_cursor=copy.deepcopy(cursor),
            )

        def _prior_record_set(self, *args, **kwargs):
            calls.append("reconcile")
            raise AssertionError("reconciliation reached")

        def _advance_watermark(self, **kwargs):
            calls.append("watermark")
            raise AssertionError("watermark reached")

        def _flush_chunk(self, *args, **kwargs):
            calls.append("flush")
            raise AssertionError("flush reached")

    runner = MemoryRunner(
        "",
        source_name="fixture",
        deployment_name="fixture",
        write_chronos_graph=False,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    # Real adapter and runner control flow, no probe short-circuit authority.
    (source.repo_root / "a.txt").write_text("changed")
    read = ContainedFileReader.open

    def failed(reader, path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            raise OSError(errno.EIO, "runner fixture")
        return read(reader, path, *args, **kwargs)

    for entry in cursor["files"].values():
        entry.pop("mtime_ns", None)
    before = copy.deepcopy(cursor)
    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError, match="runner fixture"):
        runner.run(source, mode="reconcile")
    assert calls == []
    assert cursor == before
    assert runner.compensate_on_failure is True
    assert snapshots == [("", {"deployment": "fixture", "source": "fixture"})]
    assert source.probe_short_circuit_safe is False  # Existing DocCorpus contract.


@pytest.mark.parametrize("external", [False, True])
def test_caption_planning_read_failure_refuses(source, monkeypatch, external):
    cursor = prior(source)
    if external:
        cursor.pop("record_set")
        cursor["record_set_ref"] = {"fixture": "external"}
    # Enable only the planning path: any provider call is a test failure.
    config = source.extractors_dir / "extractors.yaml"
    config.write_text(
        config.read_text().replace(
            "captioning: false",
            "captioning:\n  enabled: true\n  model: fixture\n  tracking_mode: disabled",
        )
    )

    class NoModel:
        def caption(self, *args, **kwargs):
            pytest.fail("model invoked")

    source.caption_service = NoModel()
    for item in cursor["files"].values():
        item.pop("mtime_ns", None)
    before = copy.deepcopy(cursor)
    read = ContainedFileReader.open
    seen = []

    def failed(reader, path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            seen.append(path)
            raise OSError(errno.EIO, "caption inventory")
        return read(reader, path, *args, **kwargs)

    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError, match="caption inventory"):
        source.run(
            "caption-fault",
            mode="scope_complete",
            cursor=cursor,
            sync_scope={"scope_keys": ["b.txt"]},
        )
    assert seen
    assert cursor == before


def test_equal_count_replacement_is_not_full_noop(source):
    cursor = prior(source)
    (source.repo_root / "b.txt").rename(source.repo_root / "c.txt")
    result = source.run("renamed", mode="reconcile", cursor=cursor)
    assert result.record_set is not None
    assert set(result.next_cursor["files"]) == {"a.txt", "c.txt"}
    assert "b.txt" not in result.record_set.records


def test_caption_unchanged_selected_figure_reread_refuses(source, monkeypatch):
    from types import SimpleNamespace

    cursor = prior(source)
    config = source.extractors_dir / "extractors.yaml"
    config.write_text(
        config.read_text().replace(
            "captioning: false",
            "captioning:\n  enabled: true\n  model: fixture\n  tracking_mode: disabled",
        )
    )
    source.caption_service = object()  # No provider can be called.
    item = SimpleNamespace(path="b.txt", roles=("figure",), content_sha256="a" * 64)
    source.project_manifest = SimpleNamespace(
        digest="fixture",
        source_inventory=[item],
        projects=[SimpleNamespace(files=[item], dependencies=[])],
    )
    before = copy.deepcopy(cursor)
    read = ContainedFileReader.open
    calls = []

    def failed(reader, path, *args, **kwargs):
        if path == source.repo_root / "b.txt":
            calls.append(path)
            raise OSError(errno.EIO, "planning re-read")
        return read(reader, path, *args, **kwargs)

    monkeypatch.setattr(ContainedFileReader, "open", failed)
    with pytest.raises(OSError, match="planning re-read"):
        source.run(
            "planning-reread",
            mode="scope_complete",
            cursor=cursor,
            sync_scope={"scope_keys": ["b.txt"]},
        )
    assert calls
    assert cursor == before
