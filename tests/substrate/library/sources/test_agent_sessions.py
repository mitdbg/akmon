"""Substrate-shape parity tests for `AgentSessionsSource` (Phase 4
of `migrate-okg-self-to-substrate`).

The fixture corpus ships two session JSONLs (alpha-001, beta-001)
under `sessions/`. These tests assert:

  - SourceAdapter Protocol fields are populated.
  - cursor mode emits NodeFact for both fixture sessions.
  - Both critical session ids appear (`session:alpha-001`,
    `session:beta-001`).
  - Cross-module REFERENCES edges to openspec change ids fire
    when chapter / branch text contains the slug.
  - Substrate node + edge sets match the domain emit set.
  - next_cursor carries per-transcript size + last_ingested_at;
    a second cursor-mode run with unchanged sizes short-circuits
    once the idle threshold is set to 0 (so "any prior run is
    idle").
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import pytest
import yaml


_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def _projects_root(fixture_paths) -> Path:
    """Claude Code expects `<projects_root>/<cwd-dir>/<*.jsonl>`.
    Our fixture stages the JSONLs at `<tmp>/sessions/*.jsonl`, so
    the projects root is `<tmp>` (sessions/ is the cwd dir)."""
    return fixture_paths.sessions_root.parent


def _cursor_key(fixture_paths, name: str) -> str:
    return (fixture_paths.sessions_root / f"{name}.jsonl").relative_to(
        _projects_root(fixture_paths)
    ).as_posix()


def _ensure_trailing_newline(path: Path) -> None:
    path.write_text(path.read_text(encoding="utf-8").rstrip("\n") + "\n")


def _append_event(path: Path, event: dict) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def _write_parent_with_subagent(
    projects_root: Path,
) -> tuple[Path, Path, Path]:
    project_dir = projects_root / "-repo"
    parent = project_dir / "parent.jsonl"
    child = project_dir / "parent" / "subagents" / "agent-demo.jsonl"
    meta = child.with_suffix(".meta.json")
    _write_jsonl(
        parent,
        [{
            "type": "user",
            "sessionId": "parent",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "parent activity"},
        }],
    )
    _write_jsonl(
        child,
        [{
            "type": "user",
            "sessionId": "agent-demo",
            "timestamp": "2026-07-09T00:00:01Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "child activity"},
        }],
    )
    meta.write_text(
        json.dumps({"agentType": "general-purpose", "description": "child one"}),
        encoding="utf-8",
    )
    return parent, child, meta


def _cursor_with_record_set(run) -> dict:
    assert run.next_cursor is not None
    assert run.record_set is not None
    cursor = dict(run.next_cursor)
    cursor["record_set"] = run.record_set.to_cursor()
    return cursor


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        ("PREFIX_A", "PREFIX_Z"),
        ("MIDDLE_A", "MIDDLE_Z"),
        ("TAIL_AAA", "TAIL_ZZZ"),
    ],
)
def test_complete_identity_forces_full_scan_for_same_size_rewrite(
    tmp_path: Path,
    original: str,
    replacement: str,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    project_dir = tmp_path / "projects" / "-repo"
    transcript = project_dir / "rewrite.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "user",
                "sessionId": "rewrite",
                "timestamp": f"2026-07-09T00:00:0{idx}Z",
                "cwd": "/repo",
                "message": {"role": "user", "content": value},
            }
            for idx, value in enumerate(("PREFIX_A", "MIDDLE_A", "TAIL_AAA"))
        ],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    before = transcript.stat()
    payload = transcript.read_bytes()
    assert len(original) == len(replacement)
    transcript.write_bytes(payload.replace(original.encode(), replacement.encode(), 1))
    os.utime(transcript, ns=(before.st_atime_ns, before.st_mtime_ns))

    changed = source.run(
        "same-size-rewrite",
        mode="cursor",
        cursor=baseline.next_cursor,
    )
    list(changed.facts)

    assert source.last_run_stats["files_scanned_full"] == 1
    assert source.last_run_stats["files_scanned_append_delta"] == 0


def test_complete_identity_rejects_rewritten_prefix_followed_by_append(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    project_dir = tmp_path / "projects" / "-repo"
    transcript = project_dir / "rewrite-append.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "user",
                "sessionId": "rewrite-append",
                "timestamp": "2026-07-09T00:00:00Z",
                "cwd": "/repo",
                "message": {"role": "user", "content": "APPROVE"},
            },
            {
                "type": "assistant",
                "sessionId": "rewrite-append",
                "timestamp": "2026-07-09T00:00:01Z",
                "cwd": "/repo",
                "message": {"role": "assistant", "content": "stable tail"},
            },
        ],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    payload = transcript.read_bytes().replace(b"APPROVE", b"REVOKED", 1)
    transcript.write_bytes(payload)
    _append_event(
        transcript,
        {
            "type": "user",
            "sessionId": "rewrite-append",
            "timestamp": "2026-07-09T00:00:02Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "new tail"},
        },
    )

    changed = source.run(
        "rewritten-prefix-plus-append",
        mode="cursor",
        cursor=baseline.next_cursor,
    )
    list(changed.facts)

    assert source.last_run_stats["files_scanned_full"] == 1
    assert source.last_run_stats["files_scanned_append_delta"] == 0


def test_scoped_rewrite_plus_append_invalidates_agent_resume(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    projects_root = tmp_path / "projects"
    transcript = projects_root / "-repo" / "scoped-rewrite.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "scoped-rewrite",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "APPROVE"},
        }],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    transcript.write_bytes(
        transcript.read_bytes().replace(b"APPROVE", b"REVOKED", 1)
    )
    _append_event(
        transcript,
        {
            "type": "assistant",
            "sessionId": "scoped-rewrite",
            "timestamp": "2026-07-09T00:00:01Z",
            "cwd": "/repo",
            "message": {"role": "assistant", "content": "tail"},
        },
    )
    changed = source.run(
        "scoped-rewrite",
        mode="cursor",
        cursor=baseline.next_cursor,
        sync_scope={
            "scope_keys": [
                transcript.relative_to(projects_root).as_posix(),
            ],
        },
    )
    list(changed.facts)

    assert source.last_run_stats["files_scanned_full"] == 1
    assert source.last_run_stats["files_scanned_append_delta"] == 0


def test_composite_probe_error_cannot_reuse_cached_agent_manifest(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.change_probe import ProbeVerdict

    transcript = tmp_path / "projects" / "-repo" / "probe-error.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "probe-error",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "PREFIX_A"},
        }],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    stat = transcript.stat()
    transcript.write_bytes(
        transcript.read_bytes().replace(b"PREFIX_A", b"PREFIX_Z", 1)
    )
    os.utime(transcript, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    failed_union = ProbeVerdict(
        changed=True,
        token={},
        reason="probe_error:ChangeProbeError",
    )
    changed = source.run(
        "probe-error",
        mode="cursor",
        cursor=baseline.next_cursor,
        change_probe_verdict=failed_union,
    )
    list(changed.facts)

    assert source.last_run_stats["files_scanned_full"] == 1
    assert source.last_run_stats["files_scanned_append_delta"] == 0


def test_complete_identity_timestamp_churn_skips_agent_parser(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    transcript = tmp_path / "projects" / "-repo" / "churn.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "churn",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "unchanged"},
        }],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    os.utime(transcript, ns=(2_000_000_000_000_000_000,) * 2)
    unchanged = source.run(
        "timestamp-churn",
        mode="cursor",
        cursor=baseline.next_cursor,
    )
    list(unchanged.facts)

    assert source.last_run_stats["files_scanned"] == 0
    assert source.last_run_stats["files_skipped_unchanged"] == 1


def test_adapter_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=Path("/tmp"),
        cost_estimation=False,
    )
    assert src.name == "agent_sessions"
    assert src.profile == "discovery_crawl"


def test_agent_sessions_repo_file_scan_skips_local_claude_worktrees(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

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

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        repo_root=repo,
        cost_estimation=False,
    )
    assert src._known_repo_file_paths() == {"src/app.py"}


def test_agent_sessions_git_repo_file_scan_uses_tracked_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _git_helpers
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    repo = tmp_path / "repo"
    repo.mkdir()
    seen_args: list[str] = []

    def fake_run_git(root: Path, args: list[str]) -> str:
        seen_args.extend(args)
        return ".claude/worktrees/wf/tracked-but-excluded.py\nsrc/app.py\n"

    monkeypatch.setattr(_git_helpers, "run_git", fake_run_git)

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        repo_root=repo,
        repo_file_exclude_globs=(".claude/**", "**/.claude/worktrees/**"),
        cost_estimation=False,
    )

    assert src._known_repo_file_paths_from_git(repo) == {"src/app.py"}
    assert seen_args == ["ls-files", "--cached"]


def test_agent_sessions_transcript_probe_skips_local_claude_worktree_projects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    projects_root = tmp_path / "projects"
    skipped = (
        projects_root
        / "-home-example-projects-demo-repo--claude-worktrees-demo"
    )
    skipped.mkdir(parents=True)
    _write_jsonl(
        skipped / "skip.jsonl",
        [
            {
                "type": "system",
                "sessionId": "skip",
                "cwd": "/repo/.claude/worktrees/demo",
            }
        ],
    )

    real_scandir = os.scandir

    def guarded_scandir(path: str | os.PathLike[str]):
        path_obj = Path(path).resolve()
        if path_obj == skipped.resolve() or skipped.resolve() in path_obj.parents:
            raise AssertionError(f"transcript probe entered local Claude cache: {path_obj}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    src.change_probe.build_token()


def test_agent_sessions_worktree_opt_in_bounds_probe_and_detects_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    projects_root = tmp_path / "projects"
    allowed = projects_root / "-repo--claude-worktrees-demo"
    unrelated = projects_root / "-other--claude-worktrees-demo"
    transcript = allowed / "allowed.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "allowed",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo/.claude/worktrees/demo",
            "message": {"role": "user", "content": "before"},
        }],
    )
    _write_jsonl(
        unrelated / "unrelated.jsonl",
        [{
            "type": "user",
            "sessionId": "unrelated",
            "cwd": "/other/.claude/worktrees/demo",
        }],
    )

    real_scandir = os.scandir

    def guarded_scandir(path: str | os.PathLike[str]):
        path_obj = Path(path).resolve()
        if path_obj == unrelated.resolve() or unrelated.resolve() in path_obj.parents:
            raise AssertionError(f"probe entered unrelated project: {path_obj}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        claude_worktree_roots=(Path("/repo"),),
        since="all",
        cost_estimation=False,
    )

    before = src.change_probe.build_token()
    _append_event(
        transcript,
        {
            "type": "user",
            "sessionId": "allowed",
            "timestamp": "2026-07-09T00:01:00Z",
            "cwd": "/repo/.claude/worktrees/demo",
            "message": {"role": "user", "content": "after"},
        },
    )

    assert src.change_probe.build_token() != before


def test_agent_sessions_skips_local_claude_worktree_project_before_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import _agent_sessions_emit as emit_module
    from okg.substrate.library.sources._agent_sessions_emit import (
        AgentSessionSource as DomainAgentSessionSource,
    )

    projects_root = tmp_path / "projects"
    skipped = (
        projects_root
        / "-home-example-projects-demo-repo--claude-worktrees-demo"
    )
    skipped.mkdir(parents=True)
    _write_jsonl(
        skipped / "skip.jsonl",
        [
            {
                "type": "system",
                "sessionId": "skip",
                "cwd": "/repo/.claude/worktrees/demo",
            }
        ],
    )

    def fail_scan(*args, **kwargs):
        pytest.fail("local Claude worktree transcript should not be scanned")

    monkeypatch.setattr(emit_module, "scan_transcript", fail_scan)

    src = DomainAgentSessionSource(
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    assert list(src.nodes()) == []
    assert src.last_run_stats["files_seen"] == 0


def _tool_use_event(session_id: str, timestamp: str, cwd: str, tool: str, path: str) -> dict:
    return {
        "type": "assistant",
        "sessionId": session_id,
        "timestamp": timestamp,
        "cwd": cwd,
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "name": tool,
                    "input": {"file_path": path},
                }
            ],
        },
    }


def _worktree_session_source(tmp_path: Path, *, known_paths: bool):
    """Build a source over one Claude worktree session with file edits.

    The parent session edits `.scratch/x.sh` (absolute, under the worktree)
    and `src/app.py` (relative); its subagent edits `docs/sub.md`. The
    domain emitter names placeholder file nodes after the worktree folder
    (`file:demo:*`), which normalization rewrites to `file:okg:*` (#2366).
    """
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    worktree_cwd = "/repo/.claude/worktrees/demo"
    project_dir = tmp_path / "projects" / "-repo--claude-worktrees-demo"
    parent = project_dir / "wt-session.jsonl"
    child = project_dir / "wt-session" / "subagents" / "agent-wt.jsonl"
    _write_jsonl(
        parent,
        [
            {
                "type": "user",
                "sessionId": "wt-session",
                "timestamp": "2026-07-09T00:00:00Z",
                "cwd": worktree_cwd,
                "message": {"role": "user", "content": "edit files"},
            },
            _tool_use_event(
                "wt-session",
                "2026-07-09T00:00:01Z",
                worktree_cwd,
                "Write",
                f"{worktree_cwd}/.scratch/x.sh",
            ),
            _tool_use_event(
                "wt-session",
                "2026-07-09T00:00:02Z",
                worktree_cwd,
                "Edit",
                "src/app.py",
            ),
        ],
    )
    _write_jsonl(
        child,
        [
            {
                "type": "user",
                "sessionId": "agent-wt",
                "timestamp": "2026-07-09T00:00:03Z",
                "cwd": worktree_cwd,
                "message": {"role": "user", "content": "child edits"},
            },
            _tool_use_event(
                "agent-wt",
                "2026-07-09T00:00:04Z",
                worktree_cwd,
                "Edit",
                "docs/sub.md",
            ),
        ],
    )
    child.with_suffix(".meta.json").write_text(
        json.dumps({"agentType": "general-purpose", "description": "wt child"}),
        encoding="utf-8",
    )
    repo_root = None
    if known_paths:
        repo_root = tmp_path / "repo"
        (repo_root / "src").mkdir(parents=True)
        (repo_root / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    return AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        claude_worktree_roots=(Path("/repo"),),
        repo_root=repo_root,
        since="all",
        cost_estimation=False,
    )


@pytest.mark.parametrize("known_paths", [False, True], ids=["no-file-list", "known-file-list"])
def test_agent_sessions_worktree_file_edits_leave_no_orphan_nodes(
    tmp_path: Path,
    known_paths: bool,
) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    src = _worktree_session_source(tmp_path, known_paths=known_paths)
    run = src.run("worktree-files", mode="scope_complete")
    facts = list(run.facts)

    assert run.completed_scope is True
    assert run.record_set is not None
    emitted = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.op != "R"
    }
    recorded: set[str] = set()
    for emission in run.record_set.records.values():
        recorded.update(emission.nodes_by_id())
    affects = {
        fact.dst
        for fact in facts
        if isinstance(fact, EdgeFact)
        and fact.op != "R"
        and fact.edge_type == "affects"
    }
    file_nodes = {node_id for node_id in emitted if node_id.startswith("file:")}

    assert "session:wt-session" in emitted
    assert "subsession:agent-wt" in emitted
    assert emitted == recorded
    assert not [node_id for node_id in emitted if node_id.startswith("file:demo:")]
    assert all(node_id.startswith("file:okg:") for node_id in file_nodes)
    if known_paths:
        assert src.last_run_stats["repo_file_inventory_available"] is True
        # git-files owns canonical file nodes; only known paths keep an edge.
        assert affects == {"file:okg:src/app.py"}
        assert file_nodes == set()
    else:
        assert affects == {
            "file:okg:.scratch/x.sh",
            "file:okg:src/app.py",
            "file:okg:docs/sub.md",
        }
        assert file_nodes == affects


def test_agent_sessions_refuses_emitted_node_outside_record_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    src = _worktree_session_source(tmp_path, known_paths=True)
    monkeypatch.setattr(
        AgentSessionsSource,
        "_drop_placeholder_source_file_nodes",
        staticmethod(lambda nodes: nodes),
    )

    with pytest.raises(SubstrateError) as excinfo:
        src.run("worktree-files", mode="scope_complete")

    assert str(excinfo.value) == "agent_sessions: emitted nodes outside record set"
    assert excinfo.value.data["count"] >= 1
    assert "file:demo:.scratch/x.sh" in excinfo.value.data["sample"]


def test_agent_sessions_repo_root_session_emits_no_own_file_nodes(
    tmp_path: Path,
) -> None:
    """A session run in the repository root itself, not a worktree.

    The working folder is named after the configured repo, so the dropped
    placeholder node had the canonical id `file:okg:<path>`. With a known
    repo file list git-files owns that node: agent-sessions must emit only
    the affects edge to it, never a source_file node of its own.
    """
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    repo_cwd = "/work/okg"
    project_dir = tmp_path / "projects" / "-work-okg"
    _write_jsonl(
        project_dir / "root-session.jsonl",
        [
            {
                "type": "user",
                "sessionId": "root-session",
                "timestamp": "2026-07-09T00:00:00Z",
                "cwd": repo_cwd,
                "message": {"role": "user", "content": "edit files"},
            },
            _tool_use_event(
                "root-session",
                "2026-07-09T00:00:01Z",
                repo_cwd,
                "Edit",
                "src/app.py",
            ),
            _tool_use_event(
                "root-session",
                "2026-07-09T00:00:02Z",
                repo_cwd,
                "Write",
                f"{repo_cwd}/.scratch/x.sh",
            ),
        ],
    )
    repo_root = tmp_path / "okg"
    (repo_root / "src").mkdir(parents=True)
    (repo_root / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path(repo_cwd)],
        repo_root=repo_root,
        since="all",
        cost_estimation=False,
    )

    run = src.run("repo-root-files", mode="scope_complete")
    facts = list(run.facts)

    assert run.completed_scope is True
    assert run.record_set is not None
    assert src.last_run_stats["repo_file_inventory_available"] is True
    live_nodes = [
        fact for fact in facts if isinstance(fact, NodeFact) and fact.op != "R"
    ]
    emitted = {fact.node_id for fact in live_nodes}
    recorded: set[str] = set()
    for emission in run.record_set.records.values():
        recorded.update(emission.nodes_by_id())
    affects = {
        fact.dst
        for fact in facts
        if isinstance(fact, EdgeFact)
        and fact.op != "R"
        and fact.edge_type == "affects"
    }

    assert "session:root-session" in emitted
    assert [
        fact.node_id
        for fact in live_nodes
        if fact.subtype == "source_file"
        or fact.attrs.get("source_provider") == "agent_sessions"
    ] == []
    assert "file:okg:src/app.py" not in emitted
    # The edit outside the known file list keeps no edge.
    assert affects == {"file:okg:src/app.py"}
    assert emitted == recorded


def test_agent_sessions_worktree_opt_in_emits_and_rejects_encoded_collision(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    allowed_root = Path("/repo/a.b")
    projects_root = tmp_path / "projects"
    collision_dir = projects_root / "-repo-a-b--claude-worktrees-demo"
    allowed = collision_dir / "allowed.jsonl"
    collision = collision_dir / "collision.jsonl"
    _write_jsonl(
        allowed,
        [{
            "type": "user",
            "sessionId": "allowed-worktree",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo/a.b/.claude/worktrees/demo",
            "message": {"role": "user", "content": "allowed"},
        }],
    )
    _write_jsonl(
        collision,
        [{
            "type": "user",
            "sessionId": "encoded-collision",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo/a-b/.claude/worktrees/demo",
            "message": {"role": "user", "content": "reject"},
        }],
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[allowed_root],
        claude_worktree_roots=(allowed_root,),
        since="all",
        cost_estimation=False,
    )
    session_ids = {
        fact.node_id
        for fact in src.run("worktree", mode="cursor").facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    }

    assert session_ids == {"session:allowed-worktree"}


def test_agent_sessions_ordinary_project_scope_prunes_unrelated_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    allowed = projects_root / "-repo"
    unrelated = projects_root / "-other"
    _write_jsonl(
        allowed / "allowed.jsonl",
        [{"type": "system", "sessionId": "allowed", "cwd": "/repo"}],
    )
    _write_jsonl(
        unrelated / "unrelated.jsonl",
        [{"type": "system", "sessionId": "unrelated", "cwd": "/other"}],
    )
    real_scandir = os.scandir
    unrelated_root = unrelated.resolve()

    def guarded_scandir(path: str | os.PathLike[str]):
        resolved = Path(path).resolve()
        if resolved == unrelated_root or unrelated_root in resolved.parents:
            raise AssertionError(f"entered unrelated Claude project: {resolved}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )

    src.change_probe.build_token()
    facts = list(src.run("bounded", mode="cursor").facts)

    assert {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    } == {"session:allowed"}


def test_agent_sessions_rejects_project_and_worktree_symlink_escapes(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._agent_sessions_emit import _cwd_matches
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    outside_project = tmp_path / "outside-project"
    _write_jsonl(
        outside_project / "escape.jsonl",
        [{"type": "system", "sessionId": "escape", "cwd": "/repo"}],
    )
    (projects_root / "-repo").symlink_to(outside_project, target_is_directory=True)

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    assert list(src.run("symlink", mode="cursor").facts) == []

    repo = tmp_path / "repo"
    worktrees = repo / ".claude" / "worktrees"
    worktrees.mkdir(parents=True)
    outside_cwd = tmp_path / "outside-cwd"
    outside_cwd.mkdir()
    escaped_cwd = worktrees / "escape"
    escaped_cwd.symlink_to(outside_cwd, target_is_directory=True)

    assert _cwd_matches(str(escaped_cwd), [repo], None, (repo,)) is False


def test_agent_sessions_rejects_nested_session_directory_symlink_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    project_dir = projects_root / "-repo"
    parent = project_dir / "parent.jsonl"
    _write_jsonl(
        parent,
        [{"type": "system", "sessionId": "parent", "cwd": "/repo"}],
    )
    outside_session = tmp_path / "outside-session"
    child = outside_session / "subagents" / "agent-escape.jsonl"
    _write_jsonl(
        child,
        [{"type": "system", "sessionId": "agent-escape", "cwd": "/repo"}],
    )
    child.with_suffix(".meta.json").write_text(
        json.dumps({"agentType": "general-purpose", "description": "escape"}),
        encoding="utf-8",
    )
    (project_dir / "parent").symlink_to(
        outside_session,
        target_is_directory=True,
    )
    real_scandir = os.scandir
    outside_resolved = outside_session.resolve()

    def guarded_scandir(path: str | os.PathLike[str]):
        resolved = Path(path).resolve()
        if resolved == outside_resolved or outside_resolved in resolved.parents:
            raise AssertionError(f"entered escaped nested session: {resolved}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )

    src.change_probe.build_token()
    facts = list(src.run("nested-symlink", mode="cursor").facts)

    assert not any(
        isinstance(fact, NodeFact) and fact.subtype == "sub_agent_session"
        for fact in facts
    )


def test_claude_exact_paths_encode_glob_metacharacters_literally() -> None:
    from okg.substrate.library.sources._claude_project_scope import (
        claude_project_scope_globs,
        claude_worktree_project_globs,
    )

    exact = "/repo/[one]?*"
    assert claude_project_scope_globs(cwd_filter=[exact]) == (
        "-repo--one---",
        "-repo--one----*",
    )
    assert claude_worktree_project_globs([exact]) == (
        "-repo--one-----claude-worktrees",
        "-repo--one-----claude-worktrees-*",
    )
    assert claude_project_scope_globs(cwd_globs=[exact]) == (
        "-repo-[one]?*",
        "-repo-[one]?*-*",
    )


def test_agent_sessions_filter_change_revalidates_lossy_candidate_collision(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    transcript = projects_root / "-repo-a-b" / "session.jsonl"
    _write_jsonl(
        transcript,
        [{"type": "system", "sessionId": "session", "cwd": "/repo/a.b"}],
    )
    allowed = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo/a.b")],
        since="7d",
        cost_estimation=False,
    )
    baseline = allowed.run("allowed", mode="scope_complete")
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "session:session"
        and fact.op == "I"
        for fact in baseline.facts
    )
    assert baseline.next_cursor is not None

    denied = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo/a-b")],
        since="7d",
        cost_estimation=False,
    )
    changed = denied.run(
        "denied",
        mode="scope_complete",
        cursor=baseline.next_cursor,
    )
    facts = list(changed.facts)

    assert denied.last_run_stats["files_scanned_full"] == 1
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "session:session"
        and fact.op == "R"
        for fact in facts
    )


def test_web_tool_results_are_marked_untrusted_at_ingest(tmp_path) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.transcript_trust import (
        UNTRUSTED_CONTENT_ORIGIN_ATTR,
        UNTRUSTED_CONTENT_TOOL_NAMES_ATTR,
        WEB_TOOL_RESULT_ORIGIN,
    )

    session_path = tmp_path / "repo" / "web-tools.jsonl"
    base = {
        "sessionId": "web-tools",
        "cwd": "/repo",
        "version": "test",
        "timestamp": "2026-06-11T00:00:00Z",
    }
    _write_jsonl(
        session_path,
        [
            {
                **base,
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "web-search-1",
                            "name": "WebSearch",
                            "input": {"query": "external page"},
                        }
                    ],
                },
            },
            {
                **base,
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "web-search-1",
                            "content": "external search result",
                        }
                    ],
                },
            },
            {
                **base,
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "bash-1",
                            "name": "Bash",
                            "input": {"command": "echo internal"},
                        }
                    ],
                },
            },
            {
                **base,
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "bash-1",
                            "content": "internal command result",
                        }
                    ],
                },
            },
            {
                **base,
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "web-fetch-1",
                            "name": "WebFetch",
                            "input": {"url": "https://example.test"},
                        }
                    ],
                },
            },
            {
                **base,
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "web-fetch-1",
                            "content": "external fetch result",
                        }
                    ],
                },
            },
            {
                **base,
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "agent-authored text"}],
                },
            },
        ],
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path,
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["Bash", "WebSearch", "WebFetch"],
    )
    facts = list(src.run("run-1", mode="cursor").facts)
    messages = {
        fact.node_id: fact.attrs
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }

    web_search = messages["message:web-tools:1"]
    assert web_search[UNTRUSTED_CONTENT_ORIGIN_ATTR] == WEB_TOOL_RESULT_ORIGIN
    assert web_search[UNTRUSTED_CONTENT_TOOL_NAMES_ATTR] == ["WebSearch"]

    bash_result = messages["message:web-tools:3"]
    assert UNTRUSTED_CONTENT_ORIGIN_ATTR not in bash_result

    web_fetch = messages["message:web-tools:5"]
    assert web_fetch[UNTRUSTED_CONTENT_ORIGIN_ATTR] == WEB_TOOL_RESULT_ORIGIN
    assert web_fetch[UNTRUSTED_CONTENT_TOOL_NAMES_ATTR] == ["WebFetch"]

    assistant_text = messages["message:web-tools:6"]
    assert UNTRUSTED_CONTENT_ORIGIN_ATTR not in assistant_text


def test_web_tool_result_marker_survives_append_cursor(tmp_path) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.transcript_trust import (
        UNTRUSTED_CONTENT_ORIGIN_ATTR,
        UNTRUSTED_CONTENT_TOOL_NAMES_ATTR,
        WEB_TOOL_RESULT_ORIGIN,
    )

    session_path = tmp_path / "repo" / "append-web-tools.jsonl"
    base = {
        "sessionId": "append-web-tools",
        "cwd": "/repo",
        "version": "test",
        "timestamp": "2026-06-11T00:00:00Z",
    }
    _write_jsonl(
        session_path,
        [
            {
                **base,
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "web-fetch-append",
                            "name": "WebFetch",
                            "input": {"url": "https://example.test"},
                        }
                    ],
                },
            },
        ],
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path,
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["WebFetch"],
    )
    first = src.run("run-1", mode="cursor")
    assert first.next_cursor is not None
    state = first.next_cursor["repo/append-web-tools.jsonl"]["append_cursor"]
    assert state["pending_tool_use_names_by_id"] == {
        "web-fetch-append": "WebFetch"
    }

    _append_event(
        session_path,
        {
            **base,
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "web-fetch-append",
                        "content": "external fetch result",
                    }
                ],
            },
        },
    )

    second = src.run("run-2", mode="cursor", cursor=first.next_cursor)
    messages = {
        fact.node_id: fact.attrs
        for fact in second.facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }
    web_result = messages["message:append-web-tools:1"]
    assert web_result[UNTRUSTED_CONTENT_ORIGIN_ATTR] == WEB_TOOL_RESULT_ORIGIN
    assert web_result[UNTRUSTED_CONTENT_TOOL_NAMES_ATTR] == ["WebFetch"]
    assert (
        second.next_cursor["repo/append-web-tools.jsonl"]["append_cursor"][
            "pending_tool_use_names_by_id"
        ]
        == {}
    )


def test_adapter_emits_critical_session_nodes(fixture_paths) -> None:
    from okg_self_micro import CRITICAL_NODE_IDS  # type: ignore
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}

    expected_sessions = {nid for nid in CRITICAL_NODE_IDS if nid.startswith("session:")}
    missing = expected_sessions - emitted_ids
    assert not missing, (
        f"adapter did not emit critical session ids: {sorted(missing)}; "
        f"emitted: {sorted(emitted_ids)[:15]}"
    )


def test_agent_session_semantic_revisions_ignore_ingestion_run_id(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    def _facts(run_id: str):
        source = AgentSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
            since="all",
            cost_estimation=False,
        )
        return list(source.run(run_id, mode="scope_complete").facts)

    first = _facts("acquisition-a")
    second = _facts("acquisition-b")

    assert second == first
    assert all("run_id" not in fact.source_revision for fact in first)


def test_agent_sessions_cwd_globs_capture_worktree_family(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        cwd_globs=["/repo/*_repo"],
        since="all",
        cost_estimation=False,
    )
    emitted_ids = {
        f.node_id
        for f in src.run("run-1", mode="cursor").facts
        if hasattr(f, "node_id")
    }

    assert {"session:alpha-001", "session:beta-001"} <= emitted_ids

    missed = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        cwd_globs=["/other/*"],
        since="all",
        cost_estimation=False,
    )
    missed_ids = {
        f.node_id
        for f in missed.run("run-2", mode="cursor").facts
        if hasattr(f, "node_id")
    }

    assert "session:alpha-001" not in missed_ids


def test_agent_sessions_cwd_filter_worktree_allowlist_is_fail_closed() -> None:
    from okg.substrate.library.sources._agent_sessions_emit import _cwd_matches

    assert _cwd_matches(
        "/repo/.claude/worktrees/demo",
        [Path("/repo")],
        None,
    ) is False
    assert _cwd_matches(
        "/repo/.claude/worktrees/demo/subdir",
        [Path("/repo")],
        None,
    ) is False
    assert _cwd_matches(
        "/repo/.claude/worktrees/demo/subdir",
        [Path("/repo")],
        None,
        (Path("/repo"),),
    ) is True
    assert _cwd_matches(
        "/repo-other/.claude/worktrees/demo",
        [Path("/repo")],
        None,
        (Path("/repo"),),
    ) is False
    assert _cwd_matches("/repo/src", [Path("/repo")], None) is True


@pytest.mark.parametrize(
    "cwd",
    [
        "/repo/.claude/./worktrees/demo",
        "/repo/.claude//worktrees/demo",
        r"\repo\.claude\worktrees\demo",
    ],
)
def test_agent_sessions_worktree_classification_normalizes_before_auth(
    cwd: str,
) -> None:
    from okg.substrate.library.sources._agent_sessions_emit import _cwd_matches

    assert _cwd_matches(cwd, [Path("/repo")], None) is False
    assert _cwd_matches(cwd, [Path("/repo")], None, (Path("/repo"),)) is True


def test_adapter_subtypes_match_schema(fixture_paths) -> None:
    """Every emitted session carries subtype 'agent_session'."""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        idle_threshold_hours=0,
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    sess_nodes = [
        f
        for f in run.facts
        if isinstance(f, NodeFact) and f.node_id.startswith("session:")
    ]
    assert sess_nodes
    assert all(n.subtype == "agent_session" for n in sess_nodes)


def test_adapter_emits_cross_module_change_references(fixture_paths) -> None:
    """The fixture's user prompts mention `add-greeting-feature`
    and `add-formal-greeting`. The domain emitter does not extract
    slugs from prompt text — only from chapter titles + git
    branch. With no chapters and no branch, no change-reference
    edges should fire on this fixture. (This pins the domain
    contract.)"""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    change_ref_edges = [
        f
        for f in run.facts
        if isinstance(f, EdgeFact)
        and f.dst.startswith("change:")
        and f.edge_type == "references"
    ]
    # Pinned to current domain contract: chapter+branch only, no
    # prompt-text scan. Asserting >= 0 makes the test robust to
    # future enrichment; assert == 0 to pin.
    assert change_ref_edges == []


def test_adapter_emits_ssh_host_references(fixture_paths) -> None:
    """The fixture transcripts run `ssh deploy@staging.example.com`
    and `ssh ops@prod.example.com -p 2222`. Each surfaces as a
    cross-module REFERENCES edge to a host node."""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    host_refs = [
        f
        for f in run.facts
        if isinstance(f, EdgeFact)
        and f.dst.startswith("host:")
        and f.edge_type == "references"
    ]
    host_dsts = {e.dst for e in host_refs}
    assert "host:staging.example.com" in host_dsts
    assert "host:prod.example.com" in host_dsts


def test_adapter_omits_ssh_host_references_when_switched_off(fixture_paths) -> None:
    """A deployment that loads no source declaring host nodes turns the
    links off: an edge to an undeclared host makes admission refuse the
    whole emission (okg-workspace, 2026-10-06). Nothing else changes."""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact

    def facts(**kwargs) -> list:
        src = AgentSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
            since="all",
            cost_estimation=False,
            **kwargs,
        )
        return list(src.run("run-1", mode="cursor").facts)

    def is_host_ref(fact) -> bool:
        return isinstance(fact, EdgeFact) and fact.dst.startswith("host:")

    default = facts()
    switched_off = facts(emit_ssh_host_refs=False)
    assert any(is_host_ref(f) for f in default)
    assert not any(is_host_ref(f) for f in switched_off)
    assert [repr(f) for f in switched_off] == [
        repr(f) for f in default if not is_host_ref(f)
    ]


def test_adapter_node_set_matches_domain(fixture_paths) -> None:
    """Graph-shape parity: the substrate adapter's emitted node_id
    set equals the domain domain class's emit set on the same
    fixture, except adapter-level source_file closure nodes created
    after file-edge normalization."""
    from okg.substrate.library.sources._agent_sessions_emit import (
        AgentSessionSource as DomainAgentSessionSource,
    )
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    domain = DomainAgentSessionSource(
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id for f in run.facts if hasattr(f, "node_id") and f.op == "I"
    }
    assert {
        node_id for node_id in substrate_node_ids if not node_id.startswith("file:")
    } == {
        node_id for node_id in domain_node_ids if not node_id.startswith("file:")
    }


def test_adapter_edge_set_matches_domain(fixture_paths) -> None:
    """Same parity check on the edges side."""
    from okg.substrate.library.sources._agent_sessions_emit import (
        AgentSessionSource as DomainAgentSessionSource,
    )
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    domain = DomainAgentSessionSource(
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    list(domain.nodes())
    domain_edge_keys = {(e.src, e.dst, e.edge_type) for e in domain.edges()}

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    substrate_edge_keys = {
        (f.src, f.dst, f.edge_type) for f in run.facts if hasattr(f, "src")
    }
    assert substrate_edge_keys == domain_edge_keys


def test_agent_session_normalized_file_edges_get_source_file_stubs() -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.source_specs import EdgeSpec

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=Path("/tmp"),
        cost_estimation=False,
    )
    edges = [
        EdgeSpec(
            src="session:demo",
            dst="file:okg:src/app.py",
            edge_type="affects",
        )
    ]

    nodes = src._source_file_nodes_for_normalized_edges(
        edges,
        known_repo_paths=None,
    )

    assert [node.node_id for node in nodes] == ["file:okg:src/app.py"]
    assert nodes[0].subtype == "source_file"
    assert src._source_file_nodes_for_normalized_edges(
        edges,
        known_repo_paths={"src/app.py"},
    ) == []


def test_next_cursor_carries_per_session_state(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("run-1", mode="cursor")
    list(run.facts)
    cursor = run.next_cursor or {}
    # Both fixture sessions stamped in the cursor.
    alpha_key = _cursor_key(fixture_paths, "alpha-001")
    beta_key = _cursor_key(fixture_paths, "beta-001")
    assert alpha_key in cursor
    assert beta_key in cursor
    for sid_state in (cursor[alpha_key], cursor[beta_key]):
        assert "session_id" in sid_state
        assert "transcript_bytes" in sid_state
        assert "last_ingested_at" in sid_state
        assert sid_state["append_cursor"]["schema"] == (
            "okg.agent_sessions.append_cursor.v1"
        )


def test_second_cursor_run_with_unchanged_sizes_skips_sessions(
    fixture_paths,
) -> None:
    """Second cursor-mode run with the same per-session sizes and
    `idle_threshold_hours=0` (so "any past run is idle") emits
    no session NodeFacts."""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        idle_threshold_hours=0,
        cost_estimation=False,
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        idle_threshold_hours=0,
        cost_estimation=False,
    )
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    new_sessions = [
        f
        for f in second.facts
        if isinstance(f, NodeFact) and f.node_id.startswith("session:")
    ]
    assert new_sessions == []


def test_unscoped_scope_incomplete_uses_cursor_for_unchanged_sessions(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    first = src.run("run-1", mode="scope_incomplete")
    list(first.facts)

    second = src.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
    )
    new_sessions = [
        fact
        for fact in second.facts
        if isinstance(fact, NodeFact) and fact.node_id.startswith("session:")
    ]

    assert new_sessions == []


def test_unscoped_scope_incomplete_no_change_skips_repo_file_inventory(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    first_src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        repo_root=fixture_paths.repo_root,
        since="all",
        cost_estimation=False,
    )
    first = first_src.run("run-1", mode="scope_incomplete")
    list(first.facts)

    second_src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        repo_root=fixture_paths.repo_root,
        since="all",
        cost_estimation=False,
    )

    def _fail_repo_inventory() -> set[str] | None:
        pytest.fail("unchanged transcript scope should not inspect repo files")

    monkeypatch.setattr(
        second_src,
        "_known_repo_file_paths",
        _fail_repo_inventory,
    )
    second = second_src.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
    )
    new_sessions = [
        fact
        for fact in second.facts
        if isinstance(fact, NodeFact) and fact.node_id.startswith("session:")
    ]

    assert new_sessions == []
    assert second_src.last_run_stats["files_scanned"] == 0
    assert second_src.last_run_stats["files_skipped_unchanged"] >= 1
    assert second_src.last_run_stats["repo_file_inventory_required"] is False
    assert second_src.last_run_stats["emitted_nodes"] == 0
    assert second_src.last_run_stats["emitted_edges"] == 0


def test_filtered_out_transcripts_persist_cursor_and_skip_rescan(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _agent_sessions_emit as emit_module
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    first_src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        # Lossy encoded-name collision: candidate prefilter admits this
        # project directory, while the recorded cwd remains unauthorized.
        cwd_filter=[Path("/repo/okg.self.micro.repo")],
        since="all",
        cost_estimation=False,
    )
    first = first_src.run("run-1", mode="scope_incomplete")
    first_facts = list(first.facts)

    assert first_facts == []
    assert first.next_cursor
    assert any(
        isinstance(value, dict) and value.get("filtered_out") is True
        for value in first.next_cursor.values()
    )

    def _fail_scan(*args, **kwargs):
        pytest.fail("unchanged filtered transcript should not be rescanned")

    monkeypatch.setattr(emit_module, "scan_transcript", _fail_scan)
    second_src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        cwd_filter=[Path("/repo/okg.self.micro.repo")],
        since="all",
        cost_estimation=False,
    )
    second = second_src.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
    )

    assert list(second.facts) == []
    assert second_src.last_run_stats["files_scanned"] == 0
    assert second_src.last_run_stats["files_skipped_unchanged"] >= 1


def test_scope_incomplete_applies_since_to_unknown_old_transcripts(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="7d",
        cost_estimation=False,
    )
    first = src.run("run-1", mode="scope_incomplete")
    list(first.facts)

    old = fixture_paths.sessions_root / "gamma-old.jsonl"
    old.write_text(
        json.dumps({
            "type": "user",
            "sessionId": "gamma-old",
            "uuid": "gamma-old",
            "timestamp": "2025-01-01T00:00:00Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {"role": "user", "content": "old history"},
        }) + "\n",
        encoding="utf-8",
    )
    old_time = time.time() - (20 * 24 * 60 * 60)
    os.utime(old, (old_time, old_time))

    facts = list(
        src.run("run-2", mode="scope_incomplete", cursor=first.next_cursor).facts
    )
    session_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    }

    assert "session:gamma-old" not in session_ids


def test_agent_sessions_append_run_emits_only_appended_message_rows(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    _ensure_trailing_newline(alpha)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)
    first_cursor = first.next_cursor or {}
    first_message_count = first_cursor[_cursor_key(
        fixture_paths,
        "alpha-001",
    )]["append_cursor"]["message_count"]

    _append_event(
        alpha,
        {
            "type": "user",
            "sessionId": "alpha-001",
            "uuid": "alpha-005",
            "timestamp": "2025-02-01T10:02:00Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {"role": "user", "content": "one more small note"},
        },
    )

    facts = list(src.run("run-2", mode="cursor", cursor=first.next_cursor).facts)
    message_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }
    session_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    }

    assert message_ids == {f"message:alpha-001:{first_message_count}"}
    assert session_ids == {"session:alpha-001"}


def test_agent_sessions_append_tool_event_uses_cumulative_turn_index(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    _ensure_trailing_newline(alpha)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["Bash"],
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    _append_event(
        alpha,
        {
            "type": "assistant",
            "sessionId": "alpha-001",
            "uuid": "alpha-005",
            "timestamp": "2025-02-01T10:02:00Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {"command": "echo append"},
                    }
                ],
            },
        },
    )

    facts = list(src.run("run-2", mode="cursor", cursor=first.next_cursor).facts)
    tool_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    }
    message_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }

    assert tool_ids == {"tool_use:alpha-001:2:0"}
    assert "message:alpha-001:4" in message_ids
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:alpha-001:4"
        and fact.dst == "tool_use:alpha-001:2:0"
        and fact.edge_type == "contains"
        for fact in facts
    )


def test_agent_sessions_captures_mcp_tool_events_without_allowlist(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    projects_root = tmp_path / "projects"
    session_path = projects_root / "repo" / "mcp-session.jsonl"
    secret = "ghp_1234567890abcdef"
    _write_jsonl(
        session_path,
        [
            {
                "type": "assistant",
                "sessionId": "mcp-session",
                "timestamp": "2026-06-11T00:00:00Z",
                "cwd": "/repo",
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "mcp-call-1",
                            "name": "mcp__okg-workspace__search",
                            "input": {
                                "query": f"find TOKEN={secret}",
                                "limit": 3,
                            },
                        }
                    ],
                },
            }
        ],
    )

    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["Bash"],
    )

    facts = list(source.run("mcp", mode="cursor").facts)
    tool = next(
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )

    assert tool.attrs["tool_name"] == "mcp__okg-workspace__search"
    assert tool.attrs["mcp_server"] == "okg-workspace"
    assert tool.attrs["mcp_deployment"] == "okg-workspace"
    assert tool.attrs["mcp_operator"] == "search"
    assert secret not in json.dumps(tool.attrs, sort_keys=True)
    assert "TOKEN=***" in tool.attrs["input_excerpt"]
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:mcp-session:0"
        and fact.dst == tool.node_id
        and fact.edge_type == "contains"
        for fact in facts
    )


def test_agent_sessions_append_permission_event_keeps_tool_context(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    _ensure_trailing_newline(alpha)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    _append_event(
        alpha,
        {
            "type": "assistant",
            "sessionId": "alpha-001",
            "uuid": "alpha-005",
            "timestamp": "2025-02-01T10:02:00Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Edit",
                        "input": {"file_path": "src/okg_micro/core.py"},
                    }
                ],
            },
        },
    )
    second = src.run("run-2", mode="cursor", cursor=first.next_cursor)
    list(second.facts)
    _append_event(
        alpha,
        {
            "type": "user",
            "sessionId": "alpha-001",
            "uuid": "alpha-006",
            "timestamp": "2025-02-01T10:02:30Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "is_error": True,
                        "content": "Permission to use Edit has been denied",
                    }
                ],
            },
        },
    )

    facts = list(src.run("run-3", mode="cursor", cursor=second.next_cursor).facts)
    pevent = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "permission_event"
    )

    assert pevent.node_id == "pevent:alpha-001:0"
    assert pevent.attrs["tool_name"] == "Edit"
    assert pevent.attrs["tool_input_summary"] == "Edit: src/okg_micro/core.py"


def test_reconcile_mode_completed_scope_is_true(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("rr", mode="reconcile")
    assert run.completed_scope is True


def test_cursor_mode_does_not_emit_retractions(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    run = src.run("rc", mode="cursor")
    retractions = [f for f in run.facts if isinstance(f, NodeFact) and f.op == "R"]
    assert retractions == []


def test_scope_incomplete_limits_agent_sessions_to_scoped_transcript(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )

    run = src.run(
        "scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                alpha.relative_to(projects_root).as_posix(),
                "../secret.jsonl",
            ],
        },
    )
    session_ids = {
        f.node_id
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert run.completed_scope is False
    assert session_ids == {"session:alpha-001"}


def test_escaped_agent_session_scope_emits_no_facts(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )

    run = src.run(
        "escaped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["../alpha-001.jsonl"]},
    )

    assert list(run.facts) == []
    assert run.completed_scope is False


def test_agent_session_meta_scope_maps_to_parent_transcript(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    meta = (
        fixture_paths.sessions_root / "alpha-001" / "subagents" / "agent-demo.meta.json"
    )
    meta.parent.mkdir(parents=True)
    meta.write_text("{}", encoding="utf-8")
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )

    run = src.run(
        "scoped-meta",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [meta.relative_to(projects_root).as_posix()],
        },
    )
    session_ids = {
        f.node_id
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert session_ids == {"session:alpha-001"}
    assert run.completed_scope is False


def test_agent_session_subagent_scope_maps_to_parent_transcript(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    subagent = (
        fixture_paths.sessions_root
        / "alpha-001"
        / "subagents"
        / "agent-demo.jsonl"
    )
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    _write_jsonl(
        subagent,
        [{
            "type": "user",
            "sessionId": "agent-demo",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "child activity"},
        }],
    )
    subagent.with_suffix(".meta.json").write_text(
        json.dumps({"agentType": "general-purpose", "description": "demo child"}),
        encoding="utf-8",
    )
    run = src.run(
        "scoped-subagent",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={
            "scope_keys": [subagent.relative_to(projects_root).as_posix()],
        },
    )
    facts = list(run.facts)
    session_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    }
    subagent_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "sub_agent_session"
    }

    assert session_ids == {"session:alpha-001"}
    assert subagent_ids == {"subsession:agent-demo"}
    assert run.next_cursor is not None
    assert _cursor_key(fixture_paths, "alpha-001") in run.next_cursor
    assert subagent.relative_to(projects_root).as_posix() not in run.next_cursor
    assert run.completed_scope is False


def test_agent_session_unscoped_child_append_changes_probe_and_rescans_parent(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    _parent, child, _meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    prior_probe = src.change_probe.build_token()

    _append_event(
        child,
        {
            "type": "user",
            "sessionId": "agent-demo",
            "timestamp": "2026-07-09T00:00:02Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "new child activity"},
        },
    )
    assert src.change_probe.build_token() != prior_probe

    changed = src.run("child-append", mode="cursor", cursor=baseline.next_cursor)
    facts = list(changed.facts)

    assert src.last_run_stats["files_scanned_full"] == 1
    assert src.last_run_stats["files_scanned_append_delta"] == 0
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "message:agent-demo:1"
        for fact in facts
    )


def test_agent_session_probe_tracks_recent_child_of_known_old_parent(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    parent, child, meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    old_time = time.time() - (8 * 24 * 60 * 60)
    for path in (parent, child, meta):
        os.utime(path, (old_time, old_time))
    aged = src.run("aged", mode="cursor", cursor=baseline.next_cursor)
    list(aged.facts)
    assert aged.next_cursor is not None
    aged_probe = src.change_probe.build_token()

    _append_event(
        child,
        {
            "type": "user",
            "sessionId": "agent-demo",
            "timestamp": "2026-07-09T00:00:04Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "recent child append"},
        },
    )
    assert src.change_probe.build_token() != aged_probe

    changed = src.run("old-parent-child", mode="cursor", cursor=aged.next_cursor)
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "message:agent-demo:1"
        for fact in changed.facts
    )


def test_agent_session_probe_tracks_delete_of_known_old_nested_file(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    projects_root = tmp_path / "projects"
    parent, child, meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    old_time = time.time() - (8 * 24 * 60 * 60)
    for path in (parent, child, meta):
        os.utime(path, (old_time, old_time))
    prior_probe = src.change_probe.build_token()

    meta.unlink()

    assert src.change_probe.build_token() != prior_probe


def test_agent_session_unscoped_meta_rewrite_changes_probe_and_refreshes_child(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    _parent, _child, meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    prior_probe = src.change_probe.build_token()

    meta.write_text(
        json.dumps({"agentType": "general-purpose", "description": "child two"}),
        encoding="utf-8",
    )
    assert src.change_probe.build_token() != prior_probe

    changed = src.run("meta-rewrite", mode="cursor", cursor=baseline.next_cursor)
    facts = list(changed.facts)

    assert src.last_run_stats["files_scanned_full"] == 1
    assert src.last_run_stats["files_scanned_append_delta"] == 0
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "subsession:agent-demo"
        and fact.attrs["description"] == "child two"
        for fact in facts
    )


def test_agent_session_bounded_meta_delete_retracts_owned_child_nodes(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    _parent, _child, meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    assert baseline.record_set is None
    assert baseline.next_cursor is not None
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "subsession:agent-demo"
        for fact in baseline_facts
    )

    meta.unlink()
    deleted = src.run(
        "delete-meta",
        mode="scope_complete",
        cursor=baseline.next_cursor,
        sync_scope={
            "scope_keys": [meta.relative_to(projects_root).as_posix()],
        },
    )
    retracted = {
        fact.node_id: fact.subtype
        for fact in deleted.facts
        if isinstance(fact, NodeFact) and fact.op == "R"
    }

    assert deleted.record_set is None
    assert retracted["subsession:agent-demo"] == "sub_agent_session"
    assert retracted["message:agent-demo:0"] == "agent_message"
    assert "session:parent" not in retracted
    assert not any(
        subtype in {"agent", "skill", "source_file"}
        for subtype in retracted.values()
    )


def test_agent_session_bounded_refresh_retracts_disappeared_owned_edge(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    projects_root = tmp_path / "projects"
    transcript = projects_root / "-repo" / "ssh-session.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "assistant",
            "sessionId": "ssh-session",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "name": "Bash",
                    "input": {"command": "ssh user@example.com"},
                }],
            },
        }],
    )
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    assert any(
        isinstance(fact, EdgeFact)
        and (fact.src, fact.edge_type, fact.dst)
        == ("session:ssh-session", "references", "host:example.com")
        for fact in baseline.facts
    )
    assert baseline.next_cursor is not None

    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "ssh-session",
            "timestamp": "2026-07-09T00:00:01Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "no remote host"},
        }],
    )
    refreshed = src.run(
        "refresh",
        mode="scope_complete",
        cursor=baseline.next_cursor,
    )
    facts = list(refreshed.facts)

    assert any(
        isinstance(fact, EdgeFact)
        and fact.op == "R"
        and (fact.src, fact.edge_type, fact.dst)
        == ("session:ssh-session", "references", "host:example.com")
        for fact in facts
    )
    assert not any(
        isinstance(fact, NodeFact)
        and fact.node_id == "session:ssh-session"
        and fact.op == "R"
        for fact in facts
    )


def test_agent_session_incomplete_delete_is_retracted_on_next_complete_run(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    _parent, _child, meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_incomplete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    meta.unlink()
    incomplete = src.run(
        "incomplete-delete",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={
            "scope_keys": [meta.relative_to(projects_root).as_posix()],
        },
    )
    assert not any(
        isinstance(fact, NodeFact) and fact.op == "R"
        for fact in incomplete.facts
    )
    assert incomplete.next_cursor is not None

    completed = src.run(
        "completed-delete",
        mode="scope_complete",
        cursor=incomplete.next_cursor,
    )
    retracted = {
        fact.node_id
        for fact in completed.facts
        if isinstance(fact, NodeFact) and fact.op == "R"
    }

    assert {"subsession:agent-demo", "message:agent-demo:0"} <= retracted


def test_agent_session_incomplete_append_preserves_owned_node_retractions(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    parent, _child, _meta = _write_parent_with_subagent(projects_root)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_incomplete")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    _append_event(
        parent,
        {
            "type": "user",
            "sessionId": "parent",
            "timestamp": "2026-07-09T00:00:03Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "second parent message"},
        },
    )
    appended = src.run(
        "append",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={
            "scope_keys": [parent.relative_to(projects_root).as_posix()],
        },
    )
    list(appended.facts)
    assert appended.next_cursor is not None

    parent.unlink()
    deleted = src.run(
        "delete-parent",
        mode="scope_complete",
        cursor=appended.next_cursor,
        sync_scope={
            "scope_keys": [parent.relative_to(projects_root).as_posix()],
        },
    )
    retracted = {
        fact.node_id
        for fact in deleted.facts
        if isinstance(fact, NodeFact) and fact.op == "R"
    }

    assert {"message:parent:0", "message:parent:1"} <= retracted
    assert "session:parent" in retracted


def test_agent_session_deleted_subagent_meta_scope_reprocesses_parent(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    subagent = (
        fixture_paths.sessions_root
        / "alpha-001"
        / "subagents"
        / "agent-demo.jsonl"
    )
    meta = subagent.with_suffix(".meta.json")
    _write_jsonl(
        subagent,
        [{
            "type": "user",
            "sessionId": "agent-demo",
            "timestamp": "2026-07-09T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "child activity"},
        }],
    )
    meta.write_text(
        json.dumps({"agentType": "general-purpose", "description": "demo child"}),
        encoding="utf-8",
    )
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    assert any(
        isinstance(fact, NodeFact)
        and fact.node_id == "subsession:agent-demo"
        and fact.op == "I"
        for fact in baseline_facts
    )
    cursor = _cursor_with_record_set(baseline)

    meta.unlink()
    scoped = src.run(
        "delete-subagent-meta",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": [meta.relative_to(projects_root).as_posix()]},
    )
    list(scoped.facts)
    assert scoped.record_set is not None
    assert "alpha-001" in scoped.record_set.records
    current_nodes = scoped.record_set.records["alpha-001"].nodes_by_id()
    assert "session:alpha-001" in current_nodes
    assert "subsession:agent-demo" not in current_nodes


def test_agent_sessions_scope_complete_declares_transcript_record_set(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )

    run = src.run("complete", mode="scope_complete")

    assert run.completed_scope is True
    assert run.record_set is not None
    assert {"alpha-001", "beta-001"} <= set(run.record_set.records)
    assert run.record_set.records["alpha-001"].primary is not None
    assert (
        run.record_set.records["alpha-001"].primary.node_id
        == "session:alpha-001"
    )
    assert "record_set" not in (run.next_cursor or {})


def test_agent_sessions_bounded_since_does_not_declare_record_set(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="7d",
        cost_estimation=False,
    )

    run = src.run("bounded", mode="scope_complete")

    assert run.completed_scope is True
    assert run.record_set is None

    replay = src.run(
        "bounded-authoritative-replay",
        mode="scope_complete",
        authoritative_replay=True,
    )
    assert replay.completed_scope is True
    assert replay.record_set is not None
    assert {"alpha-001", "beta-001"} <= set(replay.record_set.records)


def test_agent_sessions_bounded_scope_complete_uses_cursor_for_unchanged(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _agent_sessions_emit
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="7d",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_incomplete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_scan(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("bounded no-change completed scope scanned")

    monkeypatch.setattr(_agent_sessions_emit, "scan_transcript", fail_scan)
    unchanged = src.run(
        "unchanged",
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


def test_agent_sessions_scoped_complete_removes_deleted_transcript_record(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    projects_root = _projects_root(fixture_paths)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    assert baseline.record_set is not None
    cursor = dict(baseline.next_cursor or {})
    cursor["record_set"] = baseline.record_set.to_cursor()

    beta = fixture_paths.sessions_root / "beta-001.jsonl"
    beta.unlink()
    scoped = src.run(
        "delete-beta",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": [beta.relative_to(projects_root).as_posix()]},
    )

    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert "alpha-001" in scoped.record_set.records
    assert "beta-001" not in scoped.record_set.records
    assert _cursor_key(fixture_paths, "beta-001") not in (scoped.next_cursor or {})
    assert "record_set" not in (scoped.next_cursor or {})


def test_agent_sessions_scope_complete_carries_unchanged_record_set(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )

    baseline = src.run("baseline", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)
    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.record_set is not None
    assert unchanged.record_set.to_cursor() == baseline.record_set.to_cursor()


def test_agent_sessions_external_record_set_ref_no_change_skips_scan(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import _agent_sessions_emit
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_scan(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged external record_set_ref run scanned")

    monkeypatch.setattr(_agent_sessions_emit, "scan_transcript", fail_scan)
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 2},
    }
    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.completed_scope is True
    assert unchanged.record_set is None
    assert unchanged.next_cursor is not None
    assert unchanged.next_cursor["record_set_ref"] == cursor["record_set_ref"]


def test_agent_sessions_scope_complete_replaces_only_changed_record(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    _ensure_trailing_newline(alpha)
    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )

    baseline = src.run("baseline", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)
    _append_event(
        alpha,
        {
            "type": "user",
            "sessionId": "alpha-001",
            "uuid": "alpha-005",
            "timestamp": "2025-02-01T10:02:00Z",
            "cwd": "/repo/okg_self_micro_repo",
            "message": {"role": "user", "content": "one more small note"},
        },
    )

    changed = src.run("changed", mode="scope_complete", cursor=cursor)
    facts = list(changed.facts)
    session_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_session"
    }
    message_ids = {
        fact.node_id
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }

    assert session_ids == {"session:alpha-001"}
    assert "message:alpha-001:0" in message_ids
    assert changed.record_set is not None
    assert {"alpha-001", "beta-001"} <= set(changed.record_set.records)


def test_agent_sessions_scope_complete_drops_deleted_record(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    baseline = src.run("baseline", mode="scope_complete")
    cursor = _cursor_with_record_set(baseline)

    beta = fixture_paths.sessions_root / "beta-001.jsonl"
    beta.unlink()
    deleted = src.run("delete-beta", mode="scope_complete", cursor=cursor)

    assert list(deleted.facts) == []
    assert deleted.record_set is not None
    assert "alpha-001" in deleted.record_set.records
    assert "beta-001" not in deleted.record_set.records


def test_agent_message_schema_and_narrowings() -> None:
    schema_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "okg"
        / "substrate"
        / "library"
        / "ontologies"
        / "agent_sessions"
        / "schema.yaml"
    )
    doc = yaml.safe_load(schema_path.read_text())
    classes = doc["classes"]
    agent_message = classes["AgentMessage"]
    attrs = agent_message["attributes"]

    assert {
        "message_id",
        "session_id",
        "harness",
        "role",
        "message_index",
        "line_index",
        "timestamp",
        "message_kind",
        "content_hash",
        "content_excerpt",
        "tool_call_count",
        "redaction_policy",
    } <= set(attrs)

    narrowings = classes["EdgeNarrowings"]["attributes"]
    assert {
        "session_contains_agent_message",
        "subagent_contains_agent_message",
        "chapter_contains_agent_message",
        "agent_message_contains_tool_use_event",
        "agent_message_references_source_file",
        "agent_message_references_open_spec_change",
        "agent_message_references_pact_change",
    } <= set(narrowings)


def test_agent_sessions_emits_agent_messages(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
    )
    facts = list(src.run("messages", mode="cursor").facts)
    message_nodes = {
        fact.node_id: fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }

    assert "message:alpha-001:0" in message_nodes
    assert "message:alpha-001:3" in message_nodes
    first = message_nodes["message:alpha-001:0"].attrs
    assert first["role"] == "user"
    assert first["message_kind"] == "user_prompt"
    assert first["content_excerpt"] == "implement hello() per add-greeting-feature"
    assert len(first["content_hash"]) == 64
    assert first["redaction_policy"] == "bounded_excerpt_sha256"
    assert len(first["content_excerpt"]) <= 500

    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "session:alpha-001"
        and fact.dst == "message:alpha-001:0"
        and fact.edge_type == "contains"
        for fact in facts
    )


def test_agent_sessions_sanitize_nul_in_message_excerpt(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    transcript = fixture_paths.sessions_root / "nul-001.jsonl"
    transcript.write_text(
        "\n".join(
            [
                json.dumps(
                    {"type": "summary", "summary": "nul", "leafUuid": "nul-001"}
                ),
                json.dumps(
                    {
                        "type": "user",
                        "sessionId": "nul-001",
                        "uuid": "nul-001",
                        "timestamp": "2026-05-26T12:00:00Z",
                        "cwd": "/repo/okg_self_micro_repo",
                        "message": {
                            "role": "user",
                            "content": "hello\u0000world",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        cwd_globs=["/repo/*_repo"],
        since="all",
        cost_estimation=False,
    )
    message = next(
        fact
        for fact in src.run("messages", mode="cursor").facts
        if (isinstance(fact, NodeFact) and fact.node_id == "message:nul-001:0")
    )

    assert message.attrs["content_excerpt"] == "helloworld"
    assert "\x00" not in message.attrs["content_excerpt"]


def test_agent_sessions_scrubs_credentials_before_storage_and_hashing(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import NodeFact

    projects_root = tmp_path / "projects"
    session_dir = projects_root / "-repo"
    session_dir.mkdir(parents=True)
    transcript = session_dir / "leaky.jsonl"
    user_content = (
        "contact dev@example.com at https://example.com/docs and "
        "set USER_TOKEN=super-secret-token"
    )
    url_with_password = "postgres://reader:" + "password123@localhost:5432/db"
    dsn_assignment = "OKG_DSN=postgres://postgres:" + "okg@localhost:5433/okg"
    gh_token = "ghp_" + "1234567890abcdef"
    github_pat = "github_pat_" + "1234567890abcdef"
    openai_key = "sk-" + "1234567890abcdef"
    aws_key = "AKIA" + "1234567890ABCDEF"
    slack_token = "xoxb-" + "1234567890-abcdef"
    bash_command = (
        "curl -H 'Authorization: Bearer bearer-secret-value' "
        f"{url_with_password} "
        f"{dsn_assignment} "
        f"{gh_token} {github_pat} "
        f"{openai_key} {aws_key} "
        f"{slack_token}"
    )
    transcript.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "type": "user",
                    "sessionId": "leaky",
                    "timestamp": "2026-06-11T00:00:00Z",
                    "cwd": "/repo",
                    "message": {"role": "user", "content": user_content},
                },
                {
                    "type": "assistant",
                    "sessionId": "leaky",
                    "timestamp": "2026-06-11T00:00:01Z",
                    "cwd": "/repo",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "name": "Bash",
                                "input": {"command": bash_command},
                            }
                        ],
                    },
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["Bash"],
    )
    facts = list(src.run("scrub", mode="cursor").facts)
    nodes = {
        fact.node_id: fact
        for fact in facts
        if isinstance(fact, NodeFact)
    }
    message = nodes["message:leaky:0"].attrs
    tool = next(
        fact.attrs
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "tool_use_event"
    )
    stored = json.dumps({"message": message, "tool": tool}, sort_keys=True)

    for raw in (
        "super-secret-token",
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
        scrub_transcript_text(user_content),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    assert message["content_hash"] == hashlib.sha256(
        expected_hash_input.encode("utf-8")
    ).hexdigest()


def test_agent_message_links_to_pact_and_tool_events(fixture_paths) -> None:
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact

    src = AgentSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
        since="all",
        cost_estimation=False,
        capture_tool_use_events=["Bash"],
    )
    facts = list(src.run("message-links", mode="cursor").facts)

    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:alpha-001:3"
        and fact.dst == "tool_use:alpha-001:1:0"
        and fact.edge_type == "contains"
        for fact in facts
    )

    rules_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "okg"
        / "substrate"
        / "library"
        / "ontologies"
        / "agent_sessions"
        / "cross_links"
        / "sessions_to_openspec.yaml"
    )
    rules = yaml.safe_load(rules_path.read_text())
    rule_by_id = {rule["id"]: rule for rule in rules["rules"]}
    assert "agent_message_mentions_openspec_change_dir" in rule_by_id
    assert "agent_message_mentions_pact_change_dir" in rule_by_id
    assert "agent_message_runs_openspec_command_for_change" in rule_by_id
    assert "agent_message_runs_pact_command_for_change" in rule_by_id
    assert "agent_message_runs_openspec_command_for_change_via_tool_use" in rule_by_id
    assert "agent_message_runs_pact_command_for_change_via_tool_use" in rule_by_id
    assert (
        rule_by_id["agent_message_mentions_openspec_change_dir"]["dst"]["subtype"]
        == "open_spec_change"
    )
    assert (
        rule_by_id["agent_message_mentions_pact_change_dir"]["dst"]["subtype"]
        == "pact_change"
    )
    assert (
        rule_by_id["agent_message_runs_pact_command_for_change_via_tool_use"]["dst"][
            "subtype"
        ]
        == "pact_change"
    )
    rules_text = rules_path.read_text()
    assert "pact/changes" in rules_text
    assert "okg" in rules_text
    assert "pact" in rules_text


def test_agent_session_cross_links_cover_pact_commands() -> None:
    rules_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "okg"
        / "substrate"
        / "library"
        / "ontologies"
        / "agent_sessions"
        / "cross_links"
        / "sessions_to_openspec.yaml"
    )
    rules = yaml.safe_load(rules_path.read_text())["rules"]
    pact_regexes = [
        re.compile(rule["dst"]["derive_from_path"]["regex"])
        for rule in rules
        if rule["id"]
        in {
            "tool_use_event_runs_pact_command_for_change",
            "session_runs_pact_command_for_change_via_tool_use",
            "agent_message_runs_pact_command_for_change",
            "agent_message_runs_pact_command_for_change_via_tool_use",
        }
    ]
    commands = [
        "uv run okg pact status graph-native-pact-projection --json",
        "uv run okg --json pact status graph-native-pact-projection",
        "okg pact --root /repo status graph-native-pact-projection --json",
        "okg pact render --format markdown graph-native-pact-projection",
        "okg pact task done --json graph-native-pact-projection task.2 --evidence ev.demo",
        "okg pact evidence ingest --pytest-report report.xml graph-native-pact-projection",
    ]

    assert len(pact_regexes) == 4
    for regex in pact_regexes:
        captures = [regex.match(command).group(1) for command in commands]
        assert captures == ["graph-native-pact-projection"] * len(commands)


def test_session_edit_rule_finds_an_openspec_tree_at_any_depth() -> None:
    """The edited-file rule matches `openspec/changes/<slug>/` at the root or nested.

    OpenSpec puts the tree at `<repo>/openspec/`; okg moved its own to
    `history/openspec/` (PACT layout-history-openspec). Both forms must map a
    session's edited file to the change slug, through the rule loader the
    linker itself uses, and a path that only resembles the tree must not.
    """
    from okg.substrate.library.linkers.rules import load_rules_file

    rules_path = (
        Path(__file__).resolve().parents[4]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
        / "agent_sessions" / "cross_links" / "sessions_to_openspec.yaml"
    )
    rule = next(
        r for r in load_rules_file(rules_path)
        if r.id == "session_edits_file_under_change_dir"
    )
    derive = rule.dst.derive_from_path
    assert derive is not None
    regex = re.compile(derive.regex)

    def slug(path: str) -> str | None:
        match = regex.match(path)
        return match.group(1) if match else None

    assert slug("openspec/changes/add-thing/proposal.md") == "add-thing"
    assert slug("openspec/changes/archive/add-thing/tasks.md") == "add-thing"
    assert slug("history/openspec/changes/add-thing/proposal.md") == "add-thing"
    assert slug("history/openspec/changes/archive/add-thing/design.md") == (
        "add-thing"
    )
    assert slug("a/b/openspec/changes/other_one/specs/x/spec.md") == "other_one"
    for not_the_tree in (
        "myopenspec/changes/add-thing/proposal.md",
        "history/myopenspec/changes/add-thing/proposal.md",
        "openspec/changesets/add-thing/proposal.md",
        "src/openspec_changes/add-thing/x.py",
        "pact/changes/add-thing/pact.yaml",
        "openspec/changes/add-thing",
    ):
        assert slug(not_the_tree) is None, not_the_tree


def test_transcript_append_after_snapshot_keeps_sealed_applied_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live session keeps writing while the adapter reads its copy.

    The applied token must hash the bytes copied into the snapshot, not the
    live file, and the run declares that binding as a sealed snapshot. A
    path-scoped run reads live files and must not declare it.
    """
    from okg.substrate.library.sources import _content_snapshot
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.library.sources.change_probe import tokens_equal

    transcript = tmp_path / "projects" / "-repo" / "live.jsonl"
    _write_jsonl(
        transcript,
        [{
            "type": "user",
            "sessionId": "live",
            "timestamp": "2026-09-23T00:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "BEFORE_SNAPSHOT"},
        }],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    before = source.change_probe.build_token()
    copied_sha = hashlib.sha256(transcript.read_bytes()).hexdigest()
    original_snapshot = _content_snapshot.CapturedContentSnapshot

    class _AppendAfterCapture(original_snapshot):  # type: ignore[misc,valid-type]
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            _append_event(
                transcript,
                {
                    "type": "assistant",
                    "sessionId": "live",
                    "timestamp": "2026-09-23T00:00:01Z",
                    "cwd": "/repo",
                    "message": {
                        "role": "assistant",
                        "content": "AFTER_SNAPSHOT",
                    },
                },
            )

    monkeypatch.setattr(
        _content_snapshot,
        "CapturedContentSnapshot",
        _AppendAfterCapture,
    )

    run = source.run("live", mode="cursor")
    facts = list(run.facts)
    after = source.change_probe.build_token()

    assert run.applied_token_binding == "sealed_snapshot"
    assert run.applied_token_authority == "exact"
    assert run.applied_token_scope == "global"
    assert tokens_equal(
        run.applied_raw_token,
        source._transcript_probe.token_from_item_hashes(
            {"-repo/live.jsonl": copied_sha}
        ),
    )
    assert tokens_equal(run.applied_raw_token, before)
    assert not tokens_equal(run.applied_raw_token, after)
    assert "BEFORE_SNAPSHOT" in repr(facts)
    assert "AFTER_SNAPSHOT" not in repr(facts)

    monkeypatch.setattr(
        _content_snapshot,
        "CapturedContentSnapshot",
        original_snapshot,
    )
    scoped = source.run(
        "scoped",
        mode="cursor",
        cursor=run.next_cursor,
        sync_scope={"scope_keys": ["-repo/live.jsonl"]},
    )
    list(scoped.facts)
    assert scoped.applied_raw_token is None
    assert scoped.applied_token_binding == "unverified"


def test_append_resume_check_reads_snapshot_copy_not_live_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The append-resume decision must use the bytes the reader parses.

    The snapshot copies a transcript whose prefix was rewritten and then
    appended to. Before the reader runs, the live file reverts to the old
    prefix (same length) plus the append, so the LIVE prefix matches the
    saved hash while the COPY does not. The run must not resume from the
    saved offset and must emit the rewritten prefix from the copy.
    """
    from okg.substrate.library.sources import _content_snapshot
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource

    transcript = tmp_path / "projects" / "-repo" / "revert.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "type": "user",
                "sessionId": "revert",
                "timestamp": "2026-09-23T00:00:00Z",
                "cwd": "/repo",
                "message": {"role": "user", "content": "APPROVE"},
            },
            {
                "type": "assistant",
                "sessionId": "revert",
                "timestamp": "2026-09-23T00:00:01Z",
                "cwd": "/repo",
                "message": {"role": "assistant", "content": "stable tail"},
            },
        ],
    )
    source = AgentSessionsSource(
        repo="okg",
        claude_projects_root=tmp_path / "projects",
        cwd_filter=[Path("/repo")],
        since="all",
        cost_estimation=False,
    )
    baseline = source.run("baseline", mode="cursor")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    original_bytes = transcript.read_bytes()
    transcript.write_bytes(original_bytes.replace(b"APPROVE", b"REVOKED", 1))
    _append_event(
        transcript,
        {
            "type": "user",
            "sessionId": "revert",
            "timestamp": "2026-09-23T00:00:02Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "new tail"},
        },
    )
    rewritten = transcript.read_bytes()
    reverted = rewritten.replace(b"REVOKED", b"APPROVE", 1)
    assert reverted.startswith(original_bytes)
    assert len(reverted) == len(rewritten)
    original_snapshot = _content_snapshot.CapturedContentSnapshot

    class _RevertLiveAfterCapture(original_snapshot):  # type: ignore[misc,valid-type]
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            transcript.write_bytes(reverted)

    monkeypatch.setattr(
        _content_snapshot,
        "CapturedContentSnapshot",
        _RevertLiveAfterCapture,
    )

    changed = source.run(
        "rewrite-copied-live-reverted",
        mode="cursor",
        cursor=baseline.next_cursor,
    )
    facts = list(changed.facts)

    assert transcript.read_bytes() == reverted
    assert source.last_run_stats["files_scanned_full"] == 1
    assert source.last_run_stats["files_scanned_append_delta"] == 0
    assert "REVOKED" in repr(facts)
