"""End-to-end content identity for compute-env remote transcripts (#323)."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
)
from okg.substrate.library.sources.compute_env_remote_hosts import (
    RemoteHostsFromSessionsSource,
)


def _event(hostname: str, *, session_id: str = "session-1") -> str:
    return json.dumps(
        {
            "type": "assistant",
            "sessionId": session_id,
            "timestamp": "2026-07-25T12:00:00Z",
            "cwd": "/repo/okg",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {"command": f"ssh deploy@{hostname} true"},
                    }
                ],
            },
        },
        separators=(",", ":"),
    ) + "\n"


def _source(root: Path) -> RemoteHostsFromSessionsSource:
    return RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=root,
    )


def _run_with_runner_probe(
    source: RemoteHostsFromSessionsSource,
    run_id: str,
    *,
    cursor: dict | None,
):
    verdict = source.change_probe.probe(cursor=cursor, sync_scope=None)
    run = source.run(
        run_id,
        cursor=cursor,
        change_probe_verdict=verdict,
    )
    next_cursor = dict(run.next_cursor or {})
    if verdict.token:
        next_cursor[CURSOR_TOKEN_KEY] = verdict.token
    return run, next_cursor, verdict


def _host_ids(run) -> set[str]:
    return {
        fact.node_id
        for fact in run.facts
        if isinstance(fact, NodeFact) and fact.subtype == "remote_host"
    }


@pytest.mark.parametrize("line_index", [0, 1, 2], ids=["prefix", "middle", "tail"])
def test_same_size_rewrite_replaces_cached_observation(
    tmp_path: Path,
    line_index: int,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    old_hosts = [
        "old-a.example.com",
        "old-b.example.com",
        "old-c.example.com",
    ]
    new_hosts = [
        "new-a.example.com",
        "new-b.example.com",
        "new-c.example.com",
    ]
    lines = [_event(host) for host in old_hosts]
    transcript.write_text("".join(lines))

    _, cursor, _ = _run_with_runner_probe(
        _source(tmp_path),
        "baseline",
        cursor=None,
    )
    original_stat = transcript.stat()
    lines[line_index] = _event(new_hosts[line_index])
    transcript.write_text("".join(lines))
    os.utime(
        transcript,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    assert transcript.stat().st_size == original_stat.st_size

    changed_run, next_cursor, verdict = _run_with_runner_probe(
        _source(tmp_path),
        "rewrite",
        cursor=cursor,
    )

    assert verdict.changed is True
    assert changed_run.applied_raw_token == verdict.token
    assert changed_run.applied_token_scope == "global"
    assert changed_run.applied_token_authority == "exact"
    hosts = _host_ids(changed_run)
    assert f"host:{new_hosts[line_index]}" in hosts
    assert f"host:{old_hosts[line_index]}" not in hosts
    cached_hits = next_cursor["per_session"]["session-1"]["hits"]
    assert new_hosts[line_index] in cached_hits
    assert old_hosts[line_index] not in cached_hits


def test_probe_error_forces_reader_past_same_size_cache(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("old-a.example.com"))
    _, cursor, _ = _run_with_runner_probe(
        _source(tmp_path),
        "baseline",
        cursor=None,
    )
    original_stat = transcript.stat()
    transcript.write_text(_event("new-a.example.com"))
    os.utime(
        transcript,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )

    source = _source(tmp_path)
    run = source.run(
        "probe-error",
        cursor=cursor,
        change_probe_verdict=ProbeVerdict(
            changed=True,
            token={},
            reason="probe_error:OSError",
        ),
    )

    assert _host_ids(run) == {"host:new-a.example.com"}


def test_add_delete_and_truncate_rebuild_current_membership(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    first_path = project / "session-1.jsonl"
    second_path = project / "session-2.jsonl"
    first_path.write_text(
        _event("one-a.example.com") + _event("one-b.example.com")
    )
    _, cursor, _ = _run_with_runner_probe(
        _source(tmp_path),
        "baseline",
        cursor=None,
    )

    second_path.write_text(
        _event("two-a.example.com", session_id="session-2")
    )
    added, cursor, verdict = _run_with_runner_probe(
        _source(tmp_path),
        "add",
        cursor=cursor,
    )
    assert verdict.changed is True
    assert _host_ids(added) == {
        "host:one-a.example.com",
        "host:one-b.example.com",
        "host:two-a.example.com",
    }

    first_path.write_text(_event("one-a.example.com"))
    truncated, cursor, verdict = _run_with_runner_probe(
        _source(tmp_path),
        "truncate",
        cursor=cursor,
    )
    assert verdict.changed is True
    assert _host_ids(truncated) == {
        "host:one-a.example.com",
        "host:two-a.example.com",
    }

    second_path.unlink()
    deleted, cursor, verdict = _run_with_runner_probe(
        _source(tmp_path),
        "delete",
        cursor=cursor,
    )
    assert verdict.changed is True
    assert _host_ids(deleted) == {"host:one-a.example.com"}
    assert set(cursor["per_session"]) == {"session-1"}


def test_mtime_only_churn_leaves_complete_content_token_unchanged(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("one-a.example.com"))
    source = _source(tmp_path)
    token = source.change_probe.build_token()

    os.utime(transcript, (2_000_000_000, 2_000_000_000))

    assert source.change_probe.build_token() == token


def test_path_scoped_release_never_claims_global_identity_or_completion(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("one-a.example.com"))
    run = _source(tmp_path).run(
        "scoped",
        mode="release_new",
        sync_scope={
            "scope_keys": [transcript.relative_to(tmp_path).as_posix()],
        },
        authoritative_replay=True,
    )

    assert _host_ids(run) == {"host:one-a.example.com"}
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"


def test_path_scoped_fact_revisions_are_independent_of_run_id(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("one-a.example.com"))
    sync_scope = {
        "scope_keys": [transcript.relative_to(tmp_path).as_posix()],
    }

    first = list(_source(tmp_path).run(
        "first-run-id",
        mode="scope_incomplete",
        sync_scope=sync_scope,
    ).facts)
    second = list(_source(tmp_path).run(
        "second-run-id",
        mode="scope_incomplete",
        sync_scope=sync_scope,
    ).facts)

    assert first == second
    assert {
        fact.source_revision["content_sha256"]
        for fact in first
    } == {
        _source(tmp_path)
        .change_probe.token_from_item_hashes({
            transcript.relative_to(tmp_path).as_posix(): (
                hashlib.sha256(transcript.read_bytes()).hexdigest()
            )
        })["components"]["content"]["value"]
    }


def test_release_replay_rebuilds_complete_record_set_from_unchanged_cursor(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("one-a.example.com"))
    baseline = _source(tmp_path).run("baseline", mode="cursor")
    list(baseline.facts)

    replay = _source(tmp_path).run(
        "replay",
        mode="release_new",
        cursor=baseline.next_cursor,
        authoritative_replay=True,
    )

    assert _host_ids(replay) == {"host:one-a.example.com"}
    assert replay.completed_scope is True
    assert replay.record_set is not None
    assert set(replay.record_set.records) == {"host:one-a.example.com"}


@pytest.mark.postgres
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="#2322 fact-log-only replay never retracts a vanished record",
)
def test_source_runner_scope_then_release_replay_retracts_absence(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    transcript = project / "session-1.jsonl"
    transcript.write_text(_event("one-a.example.com"))
    source_name = "test.compute-env-remote-exact-replay"
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    baseline = runner.run(_source(tmp_path), mode="cursor")
    assert baseline.mode == "release_new"
    assert baseline.completed_scope is True
    assert baseline.next_cursor is not None
    assert baseline.next_cursor["record_set"]["records"]
    baseline_token = baseline.next_cursor[CURSOR_TOKEN_KEY]

    scoped = runner.run(
        _source(tmp_path),
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [transcript.relative_to(tmp_path).as_posix()],
        },
    )
    assert scoped.completed_scope is False
    assert scoped.next_cursor is not None
    assert scoped.next_cursor[CURSOR_TOKEN_KEY] == baseline_token
    assert scoped.next_cursor["record_set"] == baseline.next_cursor["record_set"]

    transcript.unlink()
    replay = SourceRunner(
        clean_runtime,
        source_name=source_name,
        registry_binding_fingerprint="changed-binding",
        write_chronos_graph=False,
        write_fact_log=True,
    ).run(_source(tmp_path), mode="cursor")

    assert replay.mode == "release_new"
    assert replay.completed_scope is True
    assert replay.retractions_node == 1
    assert replay.retractions_edge == 1
    assert replay.next_cursor is not None
    assert replay.next_cursor["record_set"]["records"] == {}
    assert replay.next_cursor[CURSOR_TOKEN_KEY] != baseline_token
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id
              FROM okg.node_facts
             WHERE source = %s AND op = 'R'
            """,
            (source_name,),
        )
        assert cur.fetchall() == [("host:one-a.example.com",)]
        cur.execute(
            """
            SELECT src, edge_type, dst
              FROM okg.edge_facts
             WHERE source = %s AND op = 'R'
            """,
            (source_name,),
        )
        assert cur.fetchall() == [(
            "session:session-1",
            "references",
            "host:one-a.example.com",
        )]
