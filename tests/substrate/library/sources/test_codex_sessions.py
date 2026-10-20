from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.codex_sessions import CodexSessionsSource


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _cursor_with_record_set(run) -> dict:
    assert run.next_cursor is not None
    assert run.record_set is not None
    cursor = dict(run.next_cursor)
    cursor["record_set"] = run.record_set.to_cursor()
    return cursor


def _codex_rows(repo: Path, session_id: str, message: str) -> list[dict]:
    return [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": session_id,
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:01Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": message,
            },
        },
    ]


def test_codex_sessions_unbounded_valid_empty_scope_declares_record_set(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    other_repo = tmp_path / "other"
    other_repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "other.jsonl"
    _write_jsonl(
        transcript,
        _codex_rows(other_repo, "session-other", "not this repo"),
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )

    run = source.run("run-empty", mode="scope_complete")

    assert list(run.facts) == []
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.record_set.records == {}
    assert run.health is None


def test_codex_semantic_revisions_use_content_not_run_or_mtime(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "stable.jsonl"
    _write_jsonl(
        transcript,
        _codex_rows(repo, "session-stable", "stable content"),
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )

    before = list(source.run("acquisition-a", mode="scope_complete").facts)
    stat = transcript.stat()
    os.utime(transcript, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    after = list(source.run("acquisition-b", mode="scope_complete").facts)

    assert after == before
    assert all("run_id" not in fact.source_revision for fact in after)
    assert all("transcript_mtime_ns" not in fact.source_revision for fact in after)


def test_codex_sessions_malformed_transcript_fails_closed(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "malformed.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text("{not-json}\n", encoding="utf-8")
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )

    run = source.run("run-malformed", mode="scope_complete")

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.health is not None
    assert run.health.status == "acquire_failed"
    # The cursor records the prefix actually read -- here the whole (tiny)
    # file, since the budget never bound. Two failure modes bracket this and
    # both are worse: claiming consumption that did not happen hides the
    # unread remainder as `unchanged` forever, while recording nothing makes
    # the file look new on every run and re-reads it from zero indefinitely.
    entry = (run.next_cursor.get("transcripts") or {})[str(transcript)]
    assert int(entry["bytes"]) <= transcript.stat().st_size
    # Nothing outstanding: the file was read, it is just not usable.
    assert run.next_cursor.get("outstanding_backlog") == []


def test_codex_sessions_maps_token_count_to_measured_cost_attrs(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "usage.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-usage",
                    "cwd": str(repo),
                    "model": "gpt-5.2-codex",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "event_msg",
                "timestamp": "2026-06-05T00:00:02Z",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {
                            "input_tokens": 1200,
                            "cached_input_tokens": 300,
                            "output_tokens": 40,
                            "reasoning_output_tokens": 15,
                            "total_tokens": 1240,
                            "cost_usd": 0.0,
                        },
                        "cost_usd": 999.0,
                        "model_context_window": 258400,
                    },
                    "cost_usd": 999.0,
                },
            },
        ],
    )

    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
    )

    facts = list(source.run("run-usage", mode="scope_complete").facts)
    session = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    )

    assert session.attrs["input_tokens"] == 1200
    assert session.attrs["cache_read_tokens"] == 300
    assert session.attrs["output_tokens"] == 40
    assert session.attrs["reasoning_output_tokens"] == 15
    assert session.attrs["total_tokens"] == 1240
    assert session.attrs["model_context_window"] == 258400
    assert session.attrs["cost_confidence"] == "measured"
    assert session.attrs["cost_basis"] == "codex_token_count_event"
    assert (
        session.attrs["cost_measurement_source"]
        == "codex.event_msg.token_count"
    )
    assert session.attrs["usage_recorded_at"] == "2026-06-05T00:00:02Z"
    assert session.attrs["cost_usd_estimate"] == 0.0


def test_codex_sessions_without_token_count_marks_cost_modeled(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "modeled.jsonl"
    _write_jsonl(transcript, _codex_rows(repo, "session-modeled", "hello"))

    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
    )

    facts = list(source.run("run-modeled", mode="scope_complete").facts)
    session = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    )

    assert session.attrs["cost_confidence"] == "modeled"
    assert session.attrs["cost_basis"] == "missing_codex_token_count"
    assert "input_tokens" not in session.attrs
    assert "total_tokens" not in session.attrs


def test_codex_sessions_file_edges_follow_source_file_authority(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    (repo / "reports").mkdir()
    (repo / "reports" / "source-sync-performance.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    pact_dir = repo / "pact" / "changes" / "demo"
    pact_dir.mkdir(parents=True)
    (pact_dir / "pact.md").write_text("# generated\n", encoding="utf-8")

    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-1",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": "checking files",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:02Z",
                "payload": {
                    "type": "function_call",
                    "name": "exec_command",
                    "arguments": json.dumps({
                        "cmd": (
                            "python src/app.py && "
                            "cat reports/source-sync-performance.json && "
                            "cat pact/changes/demo/pact.md"
                        ),
                    }),
                },
            },
        ],
    )

    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
        repo_file_exclude_globs=(
            "reports/**",
            "pact/**/pact.md",
        ),
    )

    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)
    edge_dsts = {
        fact.dst
        for fact in facts
        if isinstance(fact, EdgeFact)
        and fact.edge_type == "affects"
    }

    assert "file:okg:src/app.py" in edge_dsts
    assert "file:okg:reports/source-sync-performance.json" not in edge_dsts
    assert "file:okg:pact/changes/demo/pact.md" not in edge_dsts

    assert run.record_set is not None
    record_edges = {
        edge.dst
        for record in run.record_set.records.values()
        for edge in record.edges
        if edge.edge_type == "affects"
    }
    assert "file:okg:src/app.py" in record_edges
    assert "file:okg:reports/source-sync-performance.json" not in record_edges
    assert "file:okg:pact/changes/demo/pact.md" not in record_edges


def test_codex_sessions_repo_file_scan_skips_local_claude_worktrees(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    hidden = repo / ".claude" / "worktrees" / "wf" / "openspec"
    hidden.mkdir(parents=True)
    (hidden / "ignored.py").write_text("print('ignored')\n", encoding="utf-8")

    excluded_root = (repo / ".claude").resolve()
    real_scandir = os.scandir

    def guarded_scandir(path: str | os.PathLike[str]):
        path_obj = Path(path).resolve()
        if path_obj == excluded_root or excluded_root in path_obj.parents:
            raise AssertionError(f"repo scan entered local Claude cache: {path_obj}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    src = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=tmp_path / "sessions",
        archived_sessions_root=None,
        since="all",
    )
    assert src._known_repo_file_paths() == {"src/app.py"}


def test_codex_sessions_git_repo_file_scan_uses_tracked_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _git_helpers

    repo = tmp_path / "repo"
    repo.mkdir()
    seen_args: list[str] = []

    def fake_run_git(root: Path, args: list[str]) -> str:
        seen_args.extend(args)
        return ".claude/worktrees/wf/tracked-but-excluded.py\nsrc/app.py\n"

    monkeypatch.setattr(_git_helpers, "run_git", fake_run_git)

    src = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        repo_file_exclude_globs=(".claude/**", "**/.claude/worktrees/**"),
    )

    assert src._known_repo_file_paths_from_git(repo) == {"src/app.py"}
    assert seen_args == ["ls-files", "--cached"]


def test_codex_sessions_cwd_filter_excludes_repo_local_claude_worktrees() -> None:
    src = CodexSessionsSource(repo="okg", cwd_filter=[Path("/repo")])

    assert src._cwd_matches("/repo/.claude/worktrees/demo") is False
    assert src._cwd_matches("/repo/.claude/worktrees/demo/subdir") is False
    assert src._cwd_matches("/repo/src") is True


def test_codex_sessions_scoped_append_rebuilds_without_global_identity(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    rows = [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": "session-1",
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:01Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": "first message",
            },
        },
    ]
    _write_jsonl(transcript, rows)

    class NoRepoScanCodexSessionsSource(CodexSessionsSource):
        def _known_repo_file_paths(self) -> set[str] | None:
            raise AssertionError("message-only append scanned repo files")

    source = NoRepoScanCodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    baseline = source.run("run-1", mode="scope_incomplete")
    baseline_facts = list(baseline.facts)
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "message:codex:session-1:0"
        for fact in baseline_facts
    )
    assert baseline.next_cursor is not None

    rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:02Z",
        "payload": {
            "type": "message",
            "role": "assistant",
            "content": "second message",
        },
    })
    _write_jsonl(transcript, rows)

    appended = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={"scope_keys": [transcript.name]},
    )
    appended_facts = list(appended.facts)
    message_ids = {
        fact.node_id
        for fact in appended_facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "agent_message"
    }
    session = next(
        fact for fact in appended_facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "agent_session"
    )

    assert message_ids == {
        "message:codex:session-1:0",
        "message:codex:session-1:1",
    }
    assert session.attrs["turn_count"] == 2
    assert appended.completed_scope is False
    assert source.last_run_stats["full_parse_files"] == 1
    assert source.last_run_stats["append_delta_files"] == 0
    assert (
        appended.next_cursor["transcripts"][str(transcript)]
        ["append_cursor"]["message_count"]
        == 2
    )


def test_codex_sessions_unscoped_scope_incomplete_skips_unchanged_transcripts(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-1",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": "first message",
                },
            },
        ],
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
    )

    baseline = source.run("run-1", mode="scope_incomplete")
    assert list(baseline.facts)
    unchanged = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
    )

    assert list(unchanged.facts) == []
    assert (
        unchanged.next_cursor["transcripts"][str(transcript)]
        ["append_cursor"]["message_count"]
        == 1
    )


def test_codex_sessions_bounded_since_keeps_prior_cursor_entries(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    sessions = tmp_path / "codex" / "sessions"
    known = sessions / "known.jsonl"
    unknown = sessions / "unknown.jsonl"
    old_epoch = 946684800
    _write_jsonl(known, _codex_rows(repo, "session-1", "first message"))
    os.utime(known, (old_epoch, old_epoch))
    source_all = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="all",
    )
    baseline = source_all.run("run-1", mode="scope_incomplete")
    assert baseline.next_cursor is not None
    assert str(known) in baseline.next_cursor["transcripts"]

    known_rows = _codex_rows(repo, "session-1", "first message")
    known_rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:02Z",
        "payload": {
            "type": "message",
            "role": "assistant",
            "content": "second message",
        },
    })
    _write_jsonl(known, known_rows)
    os.utime(known, (old_epoch + 1, old_epoch + 1))
    _write_jsonl(unknown, _codex_rows(repo, "session-2", "cold start"))
    os.utime(unknown, (old_epoch, old_epoch))
    source_bounded = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="7d",
    )

    bounded = source_bounded.run(
        "run-2",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
    )
    facts = list(bounded.facts)
    message_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "agent_message"
    }

    assert "message:codex:session-1:1" in message_ids
    assert "message:codex:session-2:0" not in message_ids
    assert bounded.next_cursor is not None
    assert str(known) in bounded.next_cursor["transcripts"]
    assert str(unknown) not in bounded.next_cursor["transcripts"]


def test_codex_sessions_bounded_since_does_not_declare_record_set(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(transcript, _codex_rows(repo, "session-1", "first message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="7d",
    )

    run = source.run("run-1", mode="scope_complete")

    assert run.completed_scope is True
    assert run.record_set is None

    replay = source.run(
        "run-authoritative-replay",
        mode="scope_complete",
        authoritative_replay=True,
    )
    assert replay.completed_scope is True
    assert replay.record_set is not None
    assert str(transcript) in replay.record_set.records


def test_codex_sessions_bounded_scope_complete_uses_cursor_for_unchanged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import codex_sessions

    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(transcript, _codex_rows(repo, "session-1", "first message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="7d",
    )
    baseline = source.run("run-1", mode="scope_incomplete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_parse(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("bounded no-change completed scope parsed")

    monkeypatch.setattr(codex_sessions, "_parse_transcript", fail_parse)
    unchanged = source.run(
        "run-2",
        mode="scope_complete",
        cursor={
            **baseline.next_cursor,
            "record_set_ref": {
                "storage": "okg.source_record_set_blobs",
                "sha256": "stale-full-authority",
            },
        },
    )

    assert list(unchanged.facts) == []
    assert unchanged.completed_scope is True
    assert unchanged.record_set is None
    assert unchanged.next_cursor is not None
    assert "record_set_ref" not in unchanged.next_cursor


def test_codex_sessions_scope_complete_carries_unchanged_record_set(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(transcript, _codex_rows(repo, "session-1", "first message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
    )

    baseline = source.run("run-1", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)
    unchanged = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.record_set is not None
    assert unchanged.record_set.to_cursor() == baseline.record_set.to_cursor()


def test_codex_sessions_external_record_set_ref_no_change_skips_parse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import codex_sessions

    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    _write_jsonl(transcript, _codex_rows(repo, "session-1", "first message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
    )
    baseline = source.run("run-1", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_parse(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged external record_set_ref run parsed")

    monkeypatch.setattr(codex_sessions, "_parse_transcript", fail_parse)
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 1},
    }
    unchanged = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.completed_scope is True
    assert unchanged.record_set is None
    assert unchanged.next_cursor is not None
    assert unchanged.next_cursor["record_set_ref"] == cursor["record_set_ref"]


def test_codex_sessions_scope_complete_replaces_only_changed_record(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    sessions = tmp_path / "codex" / "sessions"
    alpha = sessions / "alpha.jsonl"
    beta = sessions / "beta.jsonl"
    _write_jsonl(alpha, _codex_rows(repo, "session-1", "first message"))
    _write_jsonl(beta, _codex_rows(repo, "session-2", "other message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="all",
    )

    baseline = source.run("run-1", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)
    rows = _codex_rows(repo, "session-1", "first message")
    rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:02Z",
        "payload": {
            "type": "message",
            "role": "assistant",
            "content": "second message",
        },
    })
    _write_jsonl(alpha, rows)

    changed = source.run("run-2", mode="scope_complete", cursor=cursor)
    facts = list(changed.facts)
    session_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "agent_session"
    }
    message_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "agent_message"
    }

    assert session_ids == {"session:codex:session-1"}
    # The appended message is emitted. Under chunked ingest the reconcile
    # RESUMES alpha from its consumed offset instead of re-parsing it, so the
    # already-ingested message is no longer re-emitted -- re-emitting it was
    # never the guarantee, it was the side effect of a full re-parse.
    assert "message:codex:session-1:1" in message_ids
    assert changed.record_set is not None
    assert set(changed.record_set.records) == {str(alpha), str(beta)}
    # What must hold is that alpha's footprint still OWNS the earlier message.
    # If resume shrank the footprint to just the new message, the earlier one
    # would be retracted under missing_from_completed_scope.
    alpha_nodes = set(changed.record_set.records[str(alpha)].nodes_by_id())
    assert {
        "message:codex:session-1:0",
        "message:codex:session-1:1",
    } <= alpha_nodes


def test_codex_sessions_scope_complete_drops_deleted_record(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    sessions = tmp_path / "codex" / "sessions"
    alpha = sessions / "alpha.jsonl"
    beta = sessions / "beta.jsonl"
    _write_jsonl(alpha, _codex_rows(repo, "session-1", "first message"))
    _write_jsonl(beta, _codex_rows(repo, "session-2", "other message"))
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="all",
    )

    baseline = source.run("run-1", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)
    beta.unlink()
    deleted = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(deleted.facts) == []
    assert deleted.record_set is not None
    assert str(alpha) in deleted.record_set.records
    assert str(beta) not in deleted.record_set.records


def test_codex_sessions_append_tool_event_updates_prior_message(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    transcript = tmp_path / "codex" / "sessions" / "rollout.jsonl"
    rows = [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": "session-1",
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:01Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": "about to inspect",
            },
        },
    ]
    _write_jsonl(transcript, rows)
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )
    baseline = source.run("run-1", mode="scope_incomplete")

    rows.append({
        "type": "response_item",
        "timestamp": "2026-06-05T00:00:02Z",
        "payload": {
            "type": "function_call",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "python src/app.py"}),
        },
    })
    _write_jsonl(transcript, rows)

    appended = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={"scope_keys": [transcript.name]},
    )
    facts = list(appended.facts)
    prior_message = next(
        fact for fact in facts
        if isinstance(fact, NodeFact)
        and fact.node_id == "message:codex:session-1:0"
    )
    tool_event = next(
        fact for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "tool_use_event"
    )

    assert prior_message.attrs["tool_call_count"] == 1
    assert tool_event.node_id == "tool_use:codex:session-1:2:0"
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:codex:session-1:0"
        and fact.dst == tool_event.node_id
        and fact.edge_type == "contains"
        for fact in facts
    )


def test_codex_sessions_captures_mcp_tool_events_without_allowlist(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "mcp.jsonl"
    secret = "github_pat_1234567890abcdef"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-1",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": "searching the graph",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:02Z",
                "payload": {
                    "type": "function_call",
                    "name": "mcp__okg-kdflex__query",
                    "arguments": json.dumps({
                        "sql": f"select '{secret}' as leaked"
                    }),
                },
            },
        ],
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    facts = list(source.run("mcp", mode="scope_complete").facts)
    tool = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )

    assert tool.attrs["tool_name"] == "mcp__okg-kdflex__query"
    assert tool.attrs["mcp_server"] == "okg-kdflex"
    assert tool.attrs["mcp_deployment"] == "kdflex"
    assert tool.attrs["mcp_operator"] == "query"
    assert secret not in json.dumps(tool.attrs, sort_keys=True)
    assert "***" in tool.attrs["input_excerpt"]
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:codex:session-1:0"
        and fact.dst == tool.node_id
        and fact.edge_type == "contains"
        for fact in facts
    )


def test_codex_sessions_captures_bare_mcp_operator_calls_without_allowlist(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "bare-mcp.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-bare-mcp",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "tool_search_output",
                    "tools": [
                        {
                            "type": "namespace",
                            "name": "mcp__okg_workspace",
                            "tools": [
                                {"type": "function", "name": "inspect"},
                                {"type": "function", "name": "query"},
                            ],
                        },
                    ],
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:02Z",
                "payload": {
                    "type": "function_call",
                    "name": "query",
                    "namespace": "mcp__okg_workspace",
                    "arguments": json.dumps({"sql": "select 1"}),
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:03Z",
                "payload": {
                    "type": "function_call",
                    "name": "inspect",
                    "arguments": json.dumps({"target": {"kind": "graph"}}),
                },
            },
        ],
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    facts = list(source.run("bare-mcp", mode="scope_complete").facts)
    tools = {
        fact.attrs["mcp_operator"]: fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    }

    assert set(tools) == {"inspect", "query"}
    assert tools["query"].attrs["tool_name"] == "query"
    assert tools["query"].attrs["mcp_server"] == "okg-workspace"
    assert tools["query"].attrs["mcp_deployment"] == "okg-workspace"
    assert tools["query"].attrs["mcp_operator"] == "query"
    assert tools["inspect"].attrs["tool_name"] == "inspect"
    assert tools["inspect"].attrs["mcp_server"] == "okg-workspace"
    assert tools["inspect"].attrs["mcp_deployment"] == "okg-workspace"
    assert tools["inspect"].attrs["mcp_operator"] == "inspect"


def test_codex_sessions_carries_mcp_registry_through_append_cursor(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "append-mcp.jsonl"
    initial_rows = [
        {
            "type": "session_meta",
            "timestamp": "2026-06-05T00:00:00Z",
            "payload": {
                "id": "session-append-mcp",
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-06-05T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:01Z",
            "payload": {
                "type": "tool_search_output",
                "tools": [
                    {
                        "type": "namespace",
                        "name": "mcp__okg_workspace",
                        "tools": [{"type": "function", "name": "inspect"}],
                    },
                ],
            },
        },
    ]
    _write_jsonl(transcript, initial_rows)
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    first = source.run("append-1", mode="cursor")
    assert first.next_cursor is not None
    with transcript.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "type": "response_item",
            "timestamp": "2026-06-05T00:00:02Z",
            "payload": {
                "type": "function_call",
                "name": "inspect",
                "arguments": json.dumps({"target": {"kind": "graph"}}),
            },
        }) + "\n")

    facts = list(
        source.run("append-2", mode="cursor", cursor=first.next_cursor).facts
    )
    tool = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )

    assert tool.attrs["tool_name"] == "inspect"
    assert tool.attrs["mcp_server"] == "okg-workspace"
    assert tool.attrs["mcp_deployment"] == "okg-workspace"
    assert tool.attrs["mcp_operator"] == "inspect"


def test_codex_sessions_leaves_non_okg_mcp_deployment_empty(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "playwright-mcp.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-playwright",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "function_call",
                    "name": "click",
                    "namespace": "mcp__playwright",
                    "arguments": json.dumps({"selector": "#save"}),
                },
            },
        ],
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    facts = list(source.run("playwright-mcp", mode="scope_complete").facts)
    tool = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )

    assert tool.attrs["tool_name"] == "click"
    assert tool.attrs["mcp_server"] == "playwright"
    assert tool.attrs["mcp_operator"] == "click"
    assert "mcp_deployment" not in tool.attrs


def test_codex_sessions_scrubs_credentials_before_storage_and_hashing(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    transcript = tmp_path / "codex" / "sessions" / "leaky.jsonl"
    message_content = (
        "email dev@example.com, read https://example.com/docs, "
        "TOKEN=message-secret"
    )
    url_with_password = "postgres://reader:" + "password123@localhost:5432/db"
    dsn_assignment = "OKG_DSN=postgres://postgres:" + "okg@localhost:5433/okg"
    gh_token = "ghp_" + "1234567890abcdef"
    github_pat = "github_pat_" + "1234567890abcdef"
    openai_key = "sk-" + "1234567890abcdef"
    aws_key = "AKIA" + "1234567890ABCDEF"
    slack_token = "xoxb-" + "1234567890-abcdef"
    command = (
        "curl -H 'Authorization: Bearer bearer-secret-value' "
        f"{url_with_password} "
        f"{dsn_assignment} "
        f"{gh_token} {github_pat} "
        f"{openai_key} {aws_key} "
        f"{slack_token}"
    )
    _write_jsonl(
        transcript,
        [
            {
                "type": "session_meta",
                "timestamp": "2026-06-05T00:00:00Z",
                "payload": {
                    "id": "session-1",
                    "cwd": str(repo),
                    "model": "gpt-5",
                    "timestamp": "2026-06-05T00:00:00Z",
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:01Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": message_content,
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-06-05T00:00:02Z",
                "payload": {
                    "type": "function_call",
                    "name": "exec_command",
                    "arguments": json.dumps({"cmd": command}),
                },
            },
        ],
    )
    source = CodexSessionsSource(
        repo="okg",
        repo_root=repo,
        codex_sessions_root=transcript.parent,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["exec_command"],
    )

    facts = list(source.run("scrub", mode="scope_complete").facts)
    message = next(
        fact.attrs
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    )
    tool = next(
        fact.attrs
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )
    stored = json.dumps({"message": message, "tool": tool}, sort_keys=True)

    for raw in (
        "message-secret",
        "password123",
        "okg@localhost",
        "bearer-secret-value",
        gh_token,
        github_pat,
        openai_key,
        aws_key,
        slack_token,
    ):
        assert raw not in stored
    assert "dev@example.com" in message["content_excerpt"]
    assert "https://example.com/docs" in message["content_excerpt"]
    assert "***" in stored
    assert "postgres://reader:***@localhost:5432/db" in tool["command"]
    expected_hash_input = json.dumps(
        scrub_transcript_text(message_content),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    assert message["content_hash"] == hashlib.sha256(
        expected_hash_input.encode("utf-8")
    ).hexdigest()
