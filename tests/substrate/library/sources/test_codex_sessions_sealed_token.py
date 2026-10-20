"""codex_sessions seals its applied token over the bytes it read (#2469).

A live Codex session keeps appending while the adapter reads it. The adapter
reads files in place (no copy), so the applied token hashes, from the parse's
own handle, exactly the bytes each decision rested on. These tests pin:
the sealed token when input grows mid-run, the unchanged quiet run, the
cases that must stay unverified, and that the next poll reads the append.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.ingest.runner import (
    APPLIED_IDENTITY_SEALED_INPUT_ADVANCED,
    applied_identity_acknowledgeable,
)
from okg.substrate.library.sources import codex_sessions as codex_module
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    tokens_equal,
)
from okg.substrate.library.sources.codex_sessions import CodexSessionsSource


def _meta(session_id: str, cwd: Path) -> dict:
    return {
        "type": "session_meta",
        "timestamp": "2026-09-26T00:00:00Z",
        "payload": {
            "id": session_id,
            "cwd": str(cwd),
            "model": "gpt-5",
            "timestamp": "2026-09-26T00:00:00Z",
        },
    }


def _message(text: str, second: int = 1) -> dict:
    return {
        "type": "response_item",
        "timestamp": f"2026-09-26T00:00:{second:02d}Z",
        "payload": {"type": "message", "role": "assistant", "content": text},
    }


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _append(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def _source(root: Path, **kwargs) -> CodexSessionsSource:
    return CodexSessionsSource(
        repo="okg",
        codex_sessions_root=root,
        archived_sessions_root=None,
        since="all",
        **kwargs,
    )


def _key(source: CodexSessionsSource, path: Path) -> str:
    root = Path(source.codex_sessions_root)
    return f"{root}::{path.relative_to(root).as_posix()}"


def _git_repo(repo: Path) -> Path:
    repo.mkdir()
    for argv in (
        ["init", "-q"],
        ["config", "user.email", "t@t"],
        ["config", "user.name", "t"],
    ):
        subprocess.run(["git", "-C", str(repo), *argv], check=True)
    (repo / "a.py").write_text("X = 1\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "i"], check=True,
    )
    return repo


def _verdict(source: CodexSessionsSource, prior_token):
    return source.change_probe.probe(
        cursor={CURSOR_TOKEN_KEY: prior_token} if prior_token else None,
        sync_scope=None,
    )


def _append_after_parse(monkeypatch, target: Path, row: dict) -> list[int]:
    """Append to ``target`` right after its parse returns, once."""
    original = codex_module._parse_transcript
    fired: list[int] = []

    def _parse_then_append(path, **kwargs):
        parsed = original(path, **kwargs)
        if Path(path) == target.resolve() and not fired:
            _append(target, row)
            fired.append(1)
        return parsed

    monkeypatch.setattr(codex_module, "_parse_transcript", _parse_then_append)
    return fired


def test_append_after_parse_keeps_sealed_token_of_pre_append_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "2026" / "live.jsonl"
    _write(transcript, [_meta("live", tmp_path), _message("BEFORE_APPEND")])
    source = _source(root)
    pre_append_sha = hashlib.sha256(transcript.read_bytes()).hexdigest()
    verdict = _verdict(source, None)
    fired = _append_after_parse(
        monkeypatch, transcript, _message("AFTER_APPEND", 2),
    )

    run = source.run("live", mode="cursor", change_probe_verdict=verdict)
    facts = repr(list(run.facts))
    after = source.change_probe.build_token()

    assert fired, "the hook never appended"
    assert run.applied_token_binding == "sealed_snapshot"
    assert run.applied_token_authority == "exact"
    assert run.applied_token_scope == "global"
    assert tokens_equal(
        run.applied_raw_token,
        source._transcript_probe.token_from_item_hashes(
            {_key(source, transcript): pre_append_sha}
        ),
    )
    assert tokens_equal(run.applied_raw_token, verdict.token)
    assert not tokens_equal(run.applied_raw_token, after)
    assert "BEFORE_APPEND" in facts
    assert "AFTER_APPEND" not in facts
    manifest = run.next_cursor["transcript_content_manifest"]
    assert manifest["items"] == {_key(source, transcript): pre_append_sha}
    # The runner's gate accepts the moved input instead of refusing it.
    accepted, reason = applied_identity_acknowledgeable(
        applied_raw_token=run.applied_raw_token,
        applied_token_authority=run.applied_token_authority,
        applied_token_scope=run.applied_token_scope,
        applied_token_binding=run.applied_token_binding,
        pre_token=verdict.token,
        post_token=after,
        pipeline_stable=True,
        replay_proof=True,
        normalizes_replay_probe=False,
    )
    assert (accepted, reason) == (True, APPLIED_IDENTITY_SEALED_INPUT_ADVANCED)

    # The next poll sees the append as a change and reads it.
    monkeypatch.undo()
    next_verdict = _verdict(source, run.applied_raw_token)
    assert next_verdict.changed is True
    second = source.run(
        "next",
        mode="cursor",
        cursor=run.next_cursor,
        change_probe_verdict=next_verdict,
    )
    second_facts = repr(list(second.facts))
    assert "AFTER_APPEND" in second_facts
    assert source.last_run_stats["append_delta_files"] == 1
    assert second.applied_token_binding == "sealed_snapshot"
    assert tokens_equal(second.applied_raw_token, next_verdict.token)
    assert tokens_equal(
        second.applied_raw_token, source.change_probe.build_token(),
    )


def test_append_mid_run_with_git_repo_composes_union_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _git_repo(tmp_path / "repo")
    root = tmp_path / "sessions"
    transcript = root / "live.jsonl"
    _write(transcript, [_meta("live", repo), _message("BEFORE_APPEND")])
    source = _source(root, repo_root=repo, cwd_filter=[repo])
    verdict = _verdict(source, None)
    _append_after_parse(monkeypatch, transcript, _message("AFTER_APPEND", 2))

    run = source.run("live", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert run.applied_token_binding == "sealed_snapshot"
    assert tokens_equal(run.applied_raw_token, verdict.token)
    assert not tokens_equal(
        run.applied_raw_token, source.change_probe.build_token(),
    )


def test_quiet_run_applied_token_equals_probe_token(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    project = tmp_path / "project"
    elsewhere = tmp_path / "elsewhere"
    project.mkdir()
    elsewhere.mkdir()
    transcript = root / "quiet.jsonl"
    other = root / "foreign.jsonl"
    _write(transcript, [_meta("quiet", project), _message("hello")])
    _write(other, [_meta("foreign", elsewhere), _message("FOREIGN_TEXT")])
    (root / "empty.jsonl").write_bytes(b"")
    source = _source(root, cwd_filter=[project])
    first_verdict = _verdict(source, None)
    first = source.run("first", mode="cursor", change_probe_verdict=first_verdict)
    first_facts = repr(list(first.facts))
    # The foreign transcript really took the out-of-cwd branch: nothing
    # emitted, and its cursor entry is the bare fingerprint.
    assert "hello" in first_facts
    assert "FOREIGN_TEXT" not in first_facts
    foreign_entry = first.next_cursor["transcripts"][str(other.resolve())]
    assert "append_cursor" not in foreign_entry
    assert foreign_entry["bytes"] == other.stat().st_size
    assert first.applied_token_binding == "sealed_snapshot"
    assert tokens_equal(first.applied_raw_token, first_verdict.token)

    verdict = _verdict(source, first.applied_raw_token)
    assert verdict.changed is False
    second = source.run(
        "second",
        mode="cursor",
        cursor=first.next_cursor,
        change_probe_verdict=verdict,
    )
    assert list(second.facts) == []
    assert source.last_run_stats["files_skipped_unchanged"] == 2
    assert second.applied_token_binding == "sealed_snapshot"
    assert second.applied_raw_token == verdict.token


def test_partial_trailing_line_is_part_of_the_observed_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "partial.jsonl"
    _write(transcript, [_meta("partial", tmp_path), _message("done")])
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write('{"type": "response_item", "payl')
    source = _source(root)
    verdict = _verdict(source, None)

    run = source.run("partial", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    # Nothing moved, so the token must equal the probe even though the
    # parse consumed only the complete lines.
    assert run.applied_token_binding == "sealed_snapshot"
    assert run.applied_raw_token == verdict.token

    # The writer finishes the line; the resumed parse verifies the recorded
    # prefix (which ended inside that line) and seals again.
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write(
            'oad": {"type": "message", "role": "assistant", '
            + '"content": "FINISHED"}}\n'
        )
    next_verdict = _verdict(source, run.applied_raw_token)
    assert next_verdict.changed is True
    second = source.run(
        "finished",
        mode="cursor",
        cursor=run.next_cursor,
        change_probe_verdict=next_verdict,
    )
    assert "FINISHED" in repr(list(second.facts))
    assert source.last_run_stats["append_delta_files"] == 1
    assert second.applied_token_binding == "sealed_snapshot"
    assert second.applied_raw_token == next_verdict.token


def test_skipped_file_growing_after_size_check_differs_from_live_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "idle.jsonl"
    _write(transcript, [_meta("idle", tmp_path), _message("STABLE")])
    source = _source(root)
    first_verdict = _verdict(source, None)
    first = source.run("first", mode="cursor", change_probe_verdict=first_verdict)
    list(first.facts)
    verdict = _verdict(source, first.applied_raw_token)
    assert verdict.changed is False

    original = CodexSessionsSource._ordered_candidates

    def _size_check_then_grow(paths, prior, **kwargs):
        result = original(paths, prior, **kwargs)
        _append(transcript, _message("GREW_AFTER_SIZE_CHECK", 3))
        return result

    monkeypatch.setattr(
        CodexSessionsSource,
        "_ordered_candidates",
        staticmethod(_size_check_then_grow),
    )
    second = source.run(
        "second",
        mode="cursor",
        cursor=first.next_cursor,
        change_probe_verdict=verdict,
    )
    assert list(second.facts) == []
    assert source.last_run_stats["files_skipped_unchanged"] == 1
    assert second.applied_token_binding == "sealed_snapshot"
    assert tokens_equal(second.applied_raw_token, verdict.token)
    live = source.change_probe.build_token()
    assert not tokens_equal(second.applied_raw_token, live)

    monkeypatch.undo()
    third_verdict = _verdict(source, second.applied_raw_token)
    assert third_verdict.changed is True
    third = source.run(
        "third",
        mode="cursor",
        cursor=second.next_cursor,
        change_probe_verdict=third_verdict,
    )
    assert "GREW_AFTER_SIZE_CHECK" in repr(list(third.facts))


def test_prefix_rewritten_under_cursor_is_not_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "rewrite.jsonl"
    _write(transcript, [_meta("rewrite", tmp_path), _message("AAAA")])
    source = _source(root)
    first_verdict = _verdict(source, None)
    first = source.run("first", mode="cursor", change_probe_verdict=first_verdict)
    list(first.facts)
    _append(transcript, _message("appended", 2))
    verdict = _verdict(source, first.applied_raw_token)
    original = codex_module._parse_transcript

    def _rewrite_prefix_then_parse(path, **kwargs):
        data = transcript.read_bytes()
        transcript.write_bytes(data.replace(b"AAAA", b"BBBB", 1))
        return original(path, **kwargs)

    monkeypatch.setattr(
        codex_module, "_parse_transcript", _rewrite_prefix_then_parse,
    )
    second = source.run(
        "second",
        mode="cursor",
        cursor=first.next_cursor,
        change_probe_verdict=verdict,
    )
    list(second.facts)

    assert source.last_run_stats["append_delta_files"] == 1
    assert second.applied_token_binding == "unverified"
    assert second.applied_raw_token == verdict.token


def test_outstanding_backlog_keeps_bootstrap_claim_and_no_applied_token(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "big.jsonl"
    _write(
        transcript,
        [_meta("big", tmp_path)]
        + [_message(f"line {i}", i % 60) for i in range(5)],
    )
    source = _source(root, max_transcript_bytes=1)
    verdict = _verdict(source, None)

    run = source.run("big", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert run.next_cursor["outstanding_backlog"]
    assert run.applied_raw_token is None
    assert run.applied_token_binding == "unverified"
    assert run.bootstrap_identity is not None


def test_scoped_run_is_not_sealed(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    transcript = root / "scoped.jsonl"
    _write(transcript, [_meta("scoped", tmp_path), _message("hi")])
    source = _source(root)
    verdict = _verdict(source, None)

    run = source.run(
        "scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [transcript.name]},
        change_probe_verdict=verdict,
    )
    list(run.facts)

    assert run.applied_token_binding == "unverified"


def test_file_deleted_mid_run_is_not_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    keep = root / "keep.jsonl"
    gone = root / "gone.jsonl"
    _write(keep, [_meta("keep", tmp_path), _message("kept")])
    _write(gone, [_meta("gone", tmp_path), _message("gone")])
    source = _source(root)
    verdict = _verdict(source, None)
    original = CodexSessionsSource._ordered_candidates

    def _delete_then_size_check(paths, prior, **kwargs):
        gone.unlink()
        return original(paths, prior, **kwargs)

    monkeypatch.setattr(
        CodexSessionsSource,
        "_ordered_candidates",
        staticmethod(_delete_then_size_check),
    )
    run = source.run("gone", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert run.applied_token_binding == "unverified"
    assert run.health is not None and run.health.status == "acquire_failed"


def test_membership_unavailable_is_not_sealed(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    transcript = root / "m.jsonl"
    _write(transcript, [_meta("m", tmp_path), _message("hi")])
    source = _source(root)
    verdict = _verdict(source, None)

    def _broken_supplier():
        raise OSError("enumeration failed")

    source._transcript_item_supplier = _broken_supplier
    run = source.run("m", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token == verdict.token


def _aged_source(root: Path) -> CodexSessionsSource:
    return CodexSessionsSource(
        repo="okg",
        codex_sessions_root=root,
        archived_sessions_root=None,
        since="30d",
    )


def _write_fresh_and_old(root: Path, cwd: Path) -> Path:
    _write(root / "fresh.jsonl", [_meta("fresh", cwd), _message("FRESH")])
    old = root / "old.jsonl"
    _write(old, [_meta("old", cwd), _message("OLD_TEXT")])
    os.utime(old, (1_000_000_000, 1_000_000_000))
    return old


def test_age_filtered_transcript_uses_probe_identity(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _write_fresh_and_old(root, tmp_path)
    source = _aged_source(root)
    verdict = _verdict(source, None)

    run = source.run("aged", mode="cursor", change_probe_verdict=verdict)
    facts = repr(list(run.facts))

    assert "FRESH" in facts
    assert "OLD_TEXT" not in facts
    assert source.last_run_stats["files_seen"] == 1
    assert run.applied_token_binding == "sealed_snapshot"
    assert run.applied_raw_token == verdict.token


def test_age_filtered_transcript_whose_size_moved_is_not_sealed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sessions"
    old = _write_fresh_and_old(root, tmp_path)
    source = _aged_source(root)
    verdict = _verdict(source, None)
    # Grows after the probe with its mtime kept old, so the age filter
    # still skips it, at a size the probe never hashed.
    _append(old, _message("LATE", 5))
    os.utime(old, (1_000_000_000, 1_000_000_000))

    run = source.run("aged", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert source.last_run_stats["files_seen"] == 1
    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token == verdict.token


def test_transcript_deleted_after_probe_is_not_sealed(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _write(root / "keep.jsonl", [_meta("keep", tmp_path), _message("kept")])
    gone = root / "gone.jsonl"
    _write(gone, [_meta("gone", tmp_path), _message("gone")])
    source = _source(root)
    verdict = _verdict(source, None)
    gone.unlink()

    run = source.run("gone", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert source.last_run_stats["enumeration_failure_count"] == 0
    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token == verdict.token


def test_transcript_added_after_probe_is_not_sealed(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _write(root / "keep.jsonl", [_meta("keep", tmp_path), _message("kept")])
    source = _source(root)
    verdict = _verdict(source, None)
    _write(root / "new.jsonl", [_meta("new", tmp_path), _message("NEW_TEXT")])

    run = source.run("added", mode="cursor", change_probe_verdict=verdict)

    assert "NEW_TEXT" in repr(list(run.facts))
    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token == verdict.token


def test_direct_call_without_probe_verdict_is_not_sealed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "direct.jsonl"
    _write(transcript, [_meta("direct", tmp_path), _message("hi")])
    source = _source(root)
    first = source.run("first", mode="cursor")
    list(first.facts)
    _append(transcript, _message("UNPARSED", 2))

    # The caller says nothing changed, so the run skips the transcript; a
    # manifest built after that decision would cover bytes never parsed.
    run = source.run(
        "direct",
        mode="cursor",
        cursor=first.next_cursor,
        changed_transcript_keys=[],
    )
    facts = repr(list(run.facts))

    assert "UNPARSED" not in facts
    assert first.applied_token_binding == "unverified"
    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token is None


def test_repo_probe_without_repo_snapshot_is_not_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _git_repo(tmp_path / "repo")
    root = tmp_path / "sessions"
    _write(root / "live.jsonl", [_meta("live", repo), _message("hi")])
    source = _source(root, repo_root=repo, cwd_filter=[repo])
    verdict = _verdict(source, None)

    def _no_snapshot():
        raise RuntimeError("repository snapshot unavailable")

    monkeypatch.setattr(source._repo_probe, "capture_snapshot", _no_snapshot)
    run = source.run("live", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)

    assert verdict.token
    assert run.applied_token_binding == "unverified"
    assert run.applied_raw_token == verdict.token


def _append_during_first_prefix_pass(monkeypatch, target: Path, row: dict):
    """Append to ``target`` right after a probe's first read pass of it."""
    original = codex_module._read_transcript_prefix_pass
    fired: list[int] = []

    def _pass_then_append(path, **kwargs):
        scan = original(path, **kwargs)
        if Path(path).resolve() == target.resolve() and not fired:
            _append(target, row)
            fired.append(1)
        return scan

    monkeypatch.setattr(
        codex_module, "_read_transcript_prefix_pass", _pass_then_append,
    )
    return fired


def test_probe_returns_token_while_transcript_grows_during_its_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "live.jsonl"
    _write(transcript, [_meta("live", tmp_path), _message("BEFORE")])
    source = _source(root)
    pre_append_sha = hashlib.sha256(transcript.read_bytes()).hexdigest()
    fired = _append_during_first_prefix_pass(
        monkeypatch, transcript, _message("DURING_PROBE", 2),
    )

    verdict = _verdict(source, None)

    assert fired
    assert verdict.token, verdict.reason
    manifest = source._transcript_probe.last_item_manifest()
    assert manifest["items"] == {_key(source, transcript): pre_append_sha}
    monkeypatch.undo()
    # The bytes appended during the probe make the next probe differ.
    assert not tokens_equal(verdict.token, source.change_probe.build_token())


def test_probe_refuses_prefix_rewritten_during_its_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "rewrite.jsonl"
    _write(transcript, [_meta("rewrite", tmp_path), _message("AAAA")])
    data_before = transcript.read_bytes()
    source = _source(root)
    # No reliance on the recently-touched window: only a stat that moved
    # inside the pass can trigger the second read here.
    monkeypatch.setattr(
        codex_module, "_TRANSCRIPT_TIMESTAMP_COLLISION_WINDOW_NS", 0,
    )
    original = codex_module._read_transcript_prefix_pass
    fired: list[int] = []

    def _rewrite_inside_first_pass(path, **kwargs):
        scan = original(path, **kwargs)
        if fired:
            return scan
        # Same-size rewrite of the prefix that lands before the pass's
        # closing stat: the closing stat therefore shows it.
        data = transcript.read_bytes()
        transcript.write_bytes(data.replace(b"AAAA", b"BBBB", 1))
        moved_ns = int(scan.before.st_mtime_ns) + 1_000_000_000
        os.utime(transcript, ns=(moved_ns, moved_ns))
        fired.append(1)
        return dataclasses.replace(scan, current=os.stat(transcript))

    monkeypatch.setattr(
        codex_module,
        "_read_transcript_prefix_pass",
        _rewrite_inside_first_pass,
    )
    verdict = _verdict(source, None)

    assert fired
    assert transcript.stat().st_size == len(data_before)
    assert verdict.changed is True
    assert verdict.token == {}
    assert verdict.reason.startswith("probe_error")


def test_sealed_run_is_acknowledged_when_post_probe_races_a_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The e2e shape: the writer appends during the run AND the post probe."""
    root = tmp_path / "sessions"
    transcript = root / "live.jsonl"
    _write(transcript, [_meta("live", tmp_path), _message("BEFORE")])
    source = _source(root)
    verdict = _verdict(source, None)
    _append_after_parse(monkeypatch, transcript, _message("AFTER_PARSE", 2))
    run = source.run("live", mode="cursor", change_probe_verdict=verdict)
    list(run.facts)
    monkeypatch.undo()
    _append_during_first_prefix_pass(
        monkeypatch, transcript, _message("DURING_POST_PROBE", 3),
    )

    post = _verdict(source, None)

    assert run.applied_token_binding == "sealed_snapshot"
    assert post.token, post.reason
    accepted, reason = applied_identity_acknowledgeable(
        applied_raw_token=run.applied_raw_token,
        applied_token_authority=run.applied_token_authority,
        applied_token_scope=run.applied_token_scope,
        applied_token_binding=run.applied_token_binding,
        pre_token=verdict.token,
        post_token=post.token,
        pipeline_stable=True,
        replay_proof=True,
        normalizes_replay_probe=False,
    )
    assert (accepted, reason) == (True, APPLIED_IDENTITY_SEALED_INPUT_ADVANCED)


def test_probe_keeps_returning_tokens_under_a_continuous_writer(
    tmp_path: Path,
) -> None:
    import threading

    root = tmp_path / "sessions"
    transcript = root / "live.jsonl"
    _write(
        transcript,
        [_meta("live", tmp_path)]
        + [_message("x" * 2000, i % 60) for i in range(200)],
    )
    source = _source(root)
    stop = threading.Event()
    appended = 0

    def _writer() -> None:
        nonlocal appended
        while not stop.wait(0.001):
            _append(transcript, _message(f"step {appended}", 1))
            appended += 1

    writer = threading.Thread(target=_writer, daemon=True)
    writer.start()
    try:
        verdicts = [_verdict(source, None) for _ in range(25)]
    finally:
        stop.set()
        writer.join()

    assert appended > 0
    assert all(v.token for v in verdicts), [v.reason for v in verdicts]


def test_verified_append_survives_a_writer_during_the_prefix_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "sessions"
    transcript = root / "live.jsonl"
    _write(transcript, [_meta("live", tmp_path), _message("FIRST")])
    source = _source(root)
    first_verdict = _verdict(source, None)
    first = source.run("first", mode="cursor", change_probe_verdict=first_verdict)
    list(first.facts)
    _append(transcript, _message("SECOND", 2))
    verdict = _verdict(source, first.applied_raw_token)
    _append_during_first_prefix_pass(
        monkeypatch, transcript, _message("THIRD", 3),
    )

    second = source.run(
        "second",
        mode="cursor",
        cursor=first.next_cursor,
        change_probe_verdict=verdict,
    )
    facts = repr(list(second.facts))

    # Resumed from the cursor, not rebuilt from offset zero.
    assert "SECOND" in facts
    assert source.last_run_stats["append_delta_files"] == 1
    assert source.last_run_stats["full_parse_files"] == 0



def test_append_tolerant_probe_refuses_options_it_would_ignore(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import (
        _AppendTolerantTranscriptProbe,
    )

    for option in ({"max_bytes_per_item": 10}, {"read_chunk_bytes": 4096}):
        with pytest.raises(TypeError, match=next(iter(option))):
            _AppendTolerantTranscriptProbe(
                content_items=lambda: [],
                config={},
                emit_targets=codex_module,
                **option,
            )
    probe = _AppendTolerantTranscriptProbe(
        content_items=lambda: [("raw", b"bytes are not a path")],
        config={},
        emit_targets=codex_module,
    )
    with pytest.raises(TypeError, match="file paths only"):
        probe.build_token()
