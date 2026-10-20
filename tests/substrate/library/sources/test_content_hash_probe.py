"""Tests for ContentHashProbe (#323) — the general, delete-detecting floor probe.

Proves the membership-sensitive token detects edit / add / DELETE, is immune to
mtime churn, is general across non-file (record) scopes, and that run_probe
honors a sound unchanged verdict.
"""
from __future__ import annotations

import builtins
import hashlib
import json
import os

import pytest

from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ChangeProbeError,
    run_probe,
    tokens_equal,
)
from okg.substrate.library.sources.content_hash_probe import (
    ContentHashProbe,
    file_content_items,
    file_prefix_sha256,
)


def _emit(x):
    return x


def _file_probe(root):
    return ContentHashProbe(
        content_items=lambda: file_content_items(root, include_globs=("*.json",)),
        config={"root": str(root)},
        emit_targets=_emit,
    )


def _cursor(tok):
    return {CURSOR_TOKEN_KEY: tok}


def _write(p, text):
    p.write_text(text)


def test_emit_version_is_cached_read_only_and_side_effect_free(monkeypatch):
    content_reads = 0

    def _content_items():
        nonlocal content_reads
        content_reads += 1
        raise AssertionError("emit_version must not enumerate source content")

    probe = ContentHashProbe(
        content_items=_content_items,
        config={"scope": "test"},
        emit_targets=_emit,
    )
    cached_emit_version = probe._emit_version

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("emit_version must use its construction-time value")

    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.emit_version_for",
        _forbidden,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.hashlib.sha256",
        _forbidden,
    )
    monkeypatch.setattr(builtins, "open", _forbidden)

    assert probe.emit_version is cached_emit_version
    assert probe.emit_version is cached_emit_version
    assert content_reads == 0
    with pytest.raises(AttributeError):
        probe.emit_version = "0" * 64


# ---- file scope: edit / add / delete / churn ----

def test_unchanged_short_circuits(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.probe(cursor=None, sync_scope=None).token
    v = p.probe(cursor=_cursor(t0), sync_scope=None)
    assert v.changed is False


def test_exact_consumed_item_hashes_reproduce_probe_token(tmp_path):
    path = tmp_path / "a.json"
    raw_bytes = b'{"x":1}'
    path.write_bytes(raw_bytes)
    probe = _file_probe(tmp_path)

    applied = probe.token_from_item_hashes({
        "a.json": hashlib.sha256(raw_bytes).hexdigest(),
    })

    assert tokens_equal(applied, probe.build_token())


def test_edit_detected(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    (tmp_path / "a.json").write_text('{"x":2}')
    assert not tokens_equal(t0, p.build_token())


def test_same_size_middle_rewrite_detected(tmp_path):
    path = tmp_path / "a.json"
    path.write_bytes(b"A" * 32 + b"B" * 32 + b"C" * 32)
    p = ContentHashProbe(
        content_items=lambda: [("a.json", path)],
        config={"root": str(tmp_path)},
        emit_targets=_emit,
        read_chunk_bytes=16,
    )
    t0 = p.build_token()
    path.write_bytes(b"A" * 32 + b"X" * 32 + b"C" * 32)
    assert path.stat().st_size == 96
    assert not tokens_equal(t0, p.build_token())
    assert p.last_item_hashes() == {
        "a.json": hashlib.sha256(path.read_bytes()).hexdigest()
    }
    manifest = p.last_item_manifest()
    assert manifest is not None
    assert manifest["schema"] == "okg.source.content_hash.item_manifest.v1"
    assert manifest["items"] == p.last_item_hashes()
    assert manifest["sizes"] == {"a.json": 96}
    assert len(manifest["config_sha256"]) == 64
    assert len(manifest["emit_version_sha256"]) == 64


def test_add_detected(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    (tmp_path / "b.json").write_text('{"y":1}')
    assert not tokens_equal(t0, p.build_token())


def test_delete_detected(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    (tmp_path / "b.json").write_text('{"y":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    os.remove(tmp_path / "b.json")  # DELETE
    assert not tokens_equal(t0, p.build_token()), "a deleted file must bust the token"


def test_mtime_churn_does_not_bust_token(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    os.utime(tmp_path / "a.json", (2_000_000_000, 2_000_000_000))  # mtime churn
    assert tokens_equal(t0, p.build_token()), "content-hash probe is mtime-immune"


def test_old_stable_path_keeps_single_pass_hashing(tmp_path, monkeypatch):
    path = tmp_path / "stable.json"
    path.write_bytes(b"A" * 64)
    probe = ContentHashProbe(
        content_items=lambda: [("stable.json", path)],
        config={},
        emit_targets=_emit,
        read_chunk_bytes=8,
    )
    stable_stat = os.stat(path)
    real_open = builtins.open
    open_count = 0

    def _open(candidate, *args, **kwargs):
        nonlocal open_count
        if candidate == path:
            open_count += 1
        return real_open(candidate, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _open)
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.time.time_ns",
        lambda: stable_stat.st_ctime_ns + 3_000_000_000,
    )

    probe.build_token()

    assert open_count == 1


def test_rename_detected_as_membership_change(tmp_path):
    # Same content, different key (path) — the key is part of the digest.
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    os.rename(tmp_path / "a.json", tmp_path / "renamed.json")
    assert not tokens_equal(t0, p.build_token())


# ---- general (non-file) record scope ----

def _record_probe(records):
    # records: dict[str, bytes] captured by reference so mutations are seen.
    return ContentHashProbe(
        content_items=lambda: [(k, v) for k, v in records.items()],
        config={"kind": "records"},
        emit_targets=_emit,
    )


def test_record_scope_delete_detected():
    records = {"r1": b"one", "r2": b"two"}
    p = _record_probe(records)
    t0 = p.build_token()
    del records["r2"]  # DELETE a record
    assert not tokens_equal(t0, p.build_token())


def test_record_scope_edit_detected():
    records = {"r1": b"one"}
    p = _record_probe(records)
    t0 = p.build_token()
    records["r1"] = b"one-edited"
    assert not tokens_equal(t0, p.build_token())


# ---- soundness boundary ----

def test_run_probe_honors_sound_unchanged(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    v = run_probe(p, cursor=_cursor(t0), sync_scope=None)
    assert v.changed is False


def test_unchanged_content_with_outstanding_backlog_forces_reader(tmp_path):
    (tmp_path / "a.json").write_text('{"x":1}')
    p = _file_probe(tmp_path)
    t0 = p.build_token()
    cursor = {
        CURSOR_TOKEN_KEY: t0,
        "outstanding_backlog": [{"path": "a.json"}],
    }
    verdict = run_probe(p, cursor=cursor, sync_scope=None)
    assert verdict.changed is True
    assert verdict.reason == "outstanding_backlog"


def test_item_read_error_forces_full_read_without_leaking_payload():
    p = ContentHashProbe(
        content_items=lambda: [("gone", _raise)],
        config={}, emit_targets=_emit,
    )
    verdict = run_probe(p, cursor=None, sync_scope=None)
    assert verdict.changed is True
    assert verdict.token == {}
    assert verdict.reason == "probe_error:ChangeProbeError"
    assert "super-secret-payload" not in verdict.reason
    assert p.last_item_hashes() is None


def test_failed_rebuild_clears_prior_successful_manifest():
    records = {"r1": b"one"}
    p = _record_probe(records)
    p.build_token()
    assert p.last_item_manifest() is not None
    records["r1"] = _raise

    with pytest.raises(ChangeProbeError):
        p.build_token()

    assert p.last_item_manifest() is None


def test_duplicate_item_keys_force_full_read():
    p = ContentHashProbe(
        content_items=lambda: [("same", b"a"), ("same", b"b")],
        config={},
        emit_targets=_emit,
    )
    verdict = run_probe(p, cursor=None, sync_scope=None)
    assert verdict.changed is True
    assert verdict.token == {}
    assert p.last_item_hashes() is None


def test_file_prefix_sha256_reads_exact_bounded_prefix(tmp_path):
    path = tmp_path / "prefix.jsonl"
    path.write_bytes(b"abcdef")
    assert file_prefix_sha256(path, 3, read_chunk_bytes=2) == hashlib.sha256(
        b"abc"
    ).hexdigest()
    assert file_prefix_sha256(path, 7, read_chunk_bytes=2) is None


def _raise():
    raise OSError("super-secret-payload")


def test_path_items_are_streamed_with_bounded_reads(tmp_path, monkeypatch):
    path = tmp_path / "large.json"
    path.write_bytes(b"x" * 100)
    requested_sizes = []
    real_open = builtins.open

    class _ReadSpy:
        def __init__(self, handle):
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def read(self, size=-1):
            requested_sizes.append(size)
            return self._handle.read(size)

        def fileno(self):
            return self._handle.fileno()

    def _open(candidate, *args, **kwargs):
        handle = real_open(candidate, *args, **kwargs)
        return _ReadSpy(handle) if candidate == path else handle

    monkeypatch.setattr(builtins, "open", _open)
    p = ContentHashProbe(
        content_items=lambda: [("large.json", path)],
        config={},
        emit_targets=_emit,
        read_chunk_bytes=7,
    )
    p.build_token()
    assert len(requested_sizes) > 1
    assert requested_sizes
    assert all(0 < size <= 7 for size in requested_sizes)


def test_in_place_write_during_streaming_invalidates_identity(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "concurrent.json"
    path.write_bytes(b"A" * 64)
    real_open = builtins.open
    stable_stat = os.stat(path)
    mutated = False

    class _MutatingRead:
        def __init__(self, handle):
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def fileno(self):
            return self._handle.fileno()

        def read(self, size=-1):
            nonlocal mutated
            chunk = self._handle.read(size)
            if chunk and not mutated:
                mutated = True
                with real_open(path, "r+b") as writer:
                    writer.write(b"Z")
                    writer.flush()
                    os.fsync(writer.fileno())
            return chunk

    def _open(candidate, *args, **kwargs):
        handle = real_open(candidate, *args, **kwargs)
        return _MutatingRead(handle) if candidate == path else handle

    monkeypatch.setattr(builtins, "open", _open)
    # Model a filesystem timestamp collision explicitly: the content changes
    # but both descriptor and path metadata remain in the original tick.
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.os.fstat",
        lambda _fd: stable_stat,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.os.stat",
        lambda _path: stable_stat,
    )
    probe = ContentHashProbe(
        content_items=lambda: [("concurrent.json", path)],
        config={},
        emit_targets=_emit,
        read_chunk_bytes=8,
    )

    with pytest.raises(ChangeProbeError):
        probe.build_token()

    assert probe.last_item_manifest() is None
    assert path.read_bytes().startswith(b"Z")


def test_in_place_write_during_prefix_proof_declines_resume(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "prefix-concurrent.jsonl"
    path.write_bytes(b"A" * 64)
    real_open = builtins.open
    stable_stat = os.stat(path)
    mutated = False

    class _MutatingPrefixRead:
        def __init__(self, handle):
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def fileno(self):
            return self._handle.fileno()

        def read(self, size=-1):
            nonlocal mutated
            chunk = self._handle.read(size)
            if chunk and not mutated:
                mutated = True
                with real_open(path, "r+b") as writer:
                    writer.write(b"Z")
                    writer.flush()
                    os.fsync(writer.fileno())
            return chunk

    def _open(candidate, *args, **kwargs):
        handle = real_open(candidate, *args, **kwargs)
        return _MutatingPrefixRead(handle) if candidate == path else handle

    monkeypatch.setattr(builtins, "open", _open)
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.os.fstat",
        lambda _fd: stable_stat,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.os.stat",
        lambda _path: stable_stat,
    )

    assert file_prefix_sha256(path, 32, read_chunk_bytes=8) is None


def test_token_contains_only_digests_not_keys_config_or_raw_content():
    secret = "super-secret-content"
    secret_key = "super-secret-key"
    p = ContentHashProbe(
        content_items=lambda: [(secret_key, secret.encode())],
        config={"credential": "super-secret-config"},
        emit_targets=_emit,
    )
    encoded_token = json.dumps(p.build_token(), sort_keys=True)
    assert secret not in encoded_token
    assert secret_key not in encoded_token
    assert "super-secret-config" not in encoded_token


# ---- artifact sources resolve to a delete-detecting ContentHashProbe ----

def test_sarif_sources_use_content_hash_probe_and_detect_delete(tmp_path):
    from okg.substrate.library.sources.code_graph_sarif import (
        CodeQLDataFlowSource,
        SemgrepDataFlowSource,
    )

    (tmp_path / "a.sarif").write_text('{"runs": []}')
    (tmp_path / "b.sarif").write_text('{"runs": [1]}')
    for cls in (SemgrepDataFlowSource, CodeQLDataFlowSource):
        assert cls.change_probe_kind == "content_hash"
        src = cls(repo="demo", repo_root=str(tmp_path), artifact_globs=("*.sarif",))
        assert isinstance(src.change_probe, ContentHashProbe)
        t0 = src.change_probe.build_token()
        (tmp_path / "b.sarif").unlink()
        assert not tokens_equal(t0, src.change_probe.build_token())
        (tmp_path / "b.sarif").write_text('{"runs": [1]}')  # restore for next cls
