"""Session sources change-probe (#323) — hybrid union(git, full transcripts).

The complete content probe reads and hashes transcripts without parsing, and
busts on a repo change OR any transcript content/membership mutation.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    tokens_equal,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.codex_sessions import CodexSessionsSource
from okg.substrate.library.sources.union_probe import UnionProbe


@pytest.fixture
def hybrid(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "a.py").write_text("import os\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "i"], check=True)
    projects = tmp_path / "projects"
    cwd_dir = projects / "-some-cwd"
    cwd_dir.mkdir(parents=True)
    (cwd_dir / "sess.jsonl").write_text('{"type":"user","cwd":"/x"}\n')
    return repo, projects, cwd_dir


def test_agent_sessions_declares_union(hybrid):
    repo, projects, _ = hybrid
    assert AgentSessionsSource.change_probe_kind == "union"
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects), repo_root=str(repo))
    assert isinstance(s.change_probe, UnionProbe)


def test_agent_sessions_transcript_only_without_repo(hybrid):
    _, projects, _ = hybrid
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects))
    assert isinstance(s.change_probe, ContentHashProbe)  # no repo half


def test_agent_sessions_transcript_append_detected(hybrid):
    repo, projects, cwd_dir = hybrid
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects), repo_root=str(repo))
    t0 = s.change_probe.build_token()
    with open(cwd_dir / "sess.jsonl", "a") as f:
        f.write('{"type":"assistant"}\n')
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_agent_sessions_repo_change_detected(hybrid):
    repo, projects, _ = hybrid
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects), repo_root=str(repo))
    t0 = s.change_probe.build_token()
    (repo / "a.py").write_text("import os\nX = os.getenv('A')\n")
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_agent_sessions_no_change_and_churn_unchanged(hybrid):
    repo, projects, cwd_dir = hybrid
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects), repo_root=str(repo))
    t0 = s.change_probe.build_token()
    os.utime(cwd_dir / "sess.jsonl", (2_000_000_000, 2_000_000_000))
    os.utime(repo / "a.py", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token()), "churn must not bust"


def test_agent_sessions_transcript_delete_detected(hybrid):
    repo, projects, cwd_dir = hybrid
    s = AgentSessionsSource(repo="demo", claude_projects_root=str(projects), repo_root=str(repo))
    t0 = s.change_probe.build_token()
    os.remove(cwd_dir / "sess.jsonl")  # DELETE a transcript
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_agent_sessions_cost_table_is_snapshot_bound_fact_input(
    tmp_path,
):
    projects = tmp_path / "projects"
    transcript = projects / "-repo" / "pricing.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text(
        '{"type":"assistant","sessionId":"pricing","timestamp":'
        '"2026-07-25T00:00:00Z","cwd":"/repo","message":{'
        '"role":"assistant","model":"claude-sonnet-4-6","usage":{'
        '"input_tokens":1000000,"output_tokens":0},"content":[]}}\n'
    )
    cost_table = tmp_path / "costs.yaml"
    cost_table.write_text(
        "models:\n"
        "  claude-sonnet-4-6:\n"
        "    input_per_1m: 3\n"
        "    output_per_1m: 15\n"
    )
    source = AgentSessionsSource(
        repo="demo",
        claude_projects_root=projects,
        since="all",
        cost_table_path=str(cost_table),
    )
    first_token = source.change_probe.build_token()
    first = source.run("first", mode="cursor")
    first_session = next(
        fact
        for fact in first.facts
        if getattr(fact, "node_id", None) == "session:pricing"
    )

    assert first.applied_raw_token == first_token
    assert first_session.attrs["cost_usd_estimate"] == 3.0
    first_version = first_session.attrs["cost_table_version"]

    cost_table.write_text(
        "models:\n"
        "  claude-sonnet-4-6:\n"
        "    input_per_1m: 30\n"
        "    output_per_1m: 150\n"
    )
    verdict = source.change_probe.probe(
        cursor={CURSOR_TOKEN_KEY: first_token},
        sync_scope=None,
    )
    second = source.run(
        "second",
        mode="cursor",
        cursor=first.next_cursor,
        change_probe_verdict=verdict,
    )
    second_session = next(
        fact
        for fact in second.facts
        if getattr(fact, "node_id", None) == "session:pricing"
    )

    assert verdict.changed is True
    assert second.applied_raw_token == verdict.token
    assert second_session.attrs["cost_usd_estimate"] == 30.0
    assert second_session.attrs["cost_table_version"] != first_version


def test_agent_sessions_cost_ack_uses_captured_bytes_during_mutation(
    tmp_path,
    monkeypatch,
):
    from okg.substrate.library.sources import _agent_sessions_emit

    projects = tmp_path / "projects"
    transcript = projects / "-repo" / "pricing.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text(
        '{"type":"assistant","sessionId":"pricing-race","timestamp":'
        '"2026-07-25T00:00:00Z","cwd":"/repo","message":{'
        '"role":"assistant","model":"claude-sonnet-4-6","usage":{'
        '"input_tokens":1000000},"content":[]}}\n'
    )
    cost_table = tmp_path / "costs.yaml"
    cost_table.write_text(
        "models:\n"
        "  claude-sonnet-4-6:\n"
        "    input_per_1m: 3\n"
        "    output_per_1m: 15\n"
    )
    source = AgentSessionsSource(
        repo="demo",
        claude_projects_root=projects,
        since="all",
        cost_table_path=str(cost_table),
    )
    captured_token = source.change_probe.build_token()
    original_init = _agent_sessions_emit.AgentSessionSource.__post_init__

    def _mutate_original_then_initialize(domain):
        cost_table.write_text(
            "models:\n"
            "  claude-sonnet-4-6:\n"
            "    input_per_1m: 30\n"
            "    output_per_1m: 150\n"
        )
        original_init(domain)

    monkeypatch.setattr(
        _agent_sessions_emit.AgentSessionSource,
        "__post_init__",
        _mutate_original_then_initialize,
    )
    run = source.run("racing", mode="cursor")
    session = next(
        fact
        for fact in run.facts
        if getattr(fact, "node_id", None) == "session:pricing-race"
    )

    assert run.applied_raw_token == captured_token
    assert session.attrs["cost_usd_estimate"] == 3.0
    assert not tokens_equal(
        captured_token,
        source.change_probe.build_token(),
    )


def test_codex_sessions_declares_union_and_detects_transcript_append(tmp_path):
    sessions = tmp_path / "codex"
    (sessions / "2026").mkdir(parents=True)
    (sessions / "2026" / "s.jsonl").write_text('{"e":1}\n')
    assert CodexSessionsSource.change_probe_kind == "union"
    s = CodexSessionsSource(
        repo="demo", codex_sessions_root=str(sessions), archived_sessions_root=None,
    )
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    with open(sessions / "2026" / "s.jsonl", "a") as f:
        f.write('{"e":2}\n')
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_codex_sessions_probe_detects_truncation(tmp_path):
    sessions = tmp_path / "codex"
    sessions.mkdir()
    transcript = sessions / "s.jsonl"
    transcript.write_text('{"e":1}\n{"e":2}\n')
    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=sessions,
        archived_sessions_root=None,
    )
    token = source.change_probe.build_token()
    transcript.write_text('{"e":1}\n')

    verdict = source.change_probe.probe(
        cursor={CURSOR_TOKEN_KEY: token},
        sync_scope=None,
    )

    assert verdict.changed is True
    assert verdict.reason == "content_changed"


def test_codex_sessions_probe_token_covers_fact_determining_config(tmp_path):
    sessions = tmp_path / "codex"
    sessions.mkdir()
    (sessions / "s.jsonl").write_text('{"e":1}\n')
    baseline = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="7d",
        capture_tool_use_events=None,
    )
    changed = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=sessions,
        archived_sessions_root=None,
        since="all",
        capture_tool_use_events=["shell_command"],
    )

    assert not tokens_equal(
        baseline.change_probe.build_token(),
        changed.change_probe.build_token(),
    )
