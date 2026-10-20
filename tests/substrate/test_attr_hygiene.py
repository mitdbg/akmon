"""Regression tests for `debug-substrate-okg-self` Phase 8.

Three pre-2026-04-25 bugs:

- B27 — `open_spec_change.path` was the ingest host's
  absolute filesystem path; should be repo-relative so the
  graph is portable.
- B30 — `permission_event.timestamp` was empty string for
  `mode_changed` events; should be a real ISO timestamp.
- B34 — `git_commit.created_at` and `source_file.created_at`
  were declared in schema but never populated; the domain
  TimeResolver was a Phase B enricher the substrate doesn't
  run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


_FIXTURES = Path(__file__).resolve().parent / "fixtures"
if str(_FIXTURES) not in sys.path:
    sys.path.insert(0, str(_FIXTURES))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


# ---------- B27 ----------


def test_open_spec_change_path_is_repo_relative(fixture_paths) -> None:
    """The domain openspec source emits `path` as the
    repo-relative `openspec/changes/<slug>` form, not as the
    absolute filesystem path on the ingest host."""
    from okg.substrate.library.sources._openspec_emit import OpenSpecSource

    src = OpenSpecSource(
        openspec_root=fixture_paths.openspec_root,
        repo="okg",
        include_archived=True,
    )
    nodes = list(src.nodes())
    change_nodes = [n for n in nodes if n.subtype == "open_spec_change"]
    assert change_nodes, "fixture should produce at least one change"
    for n in change_nodes:
        path = n.attrs.get("path", "")
        assert path, f"change {n.node_id!r} has empty path"
        assert not path.startswith("/Users/"), (
            f"change {n.node_id!r} has absolute path: {path!r}"
        )
        assert not path.startswith("/home/"), (
            f"change {n.node_id!r} has absolute path: {path!r}"
        )
        # Active changes: openspec/changes/<slug>
        # Archived: openspec/changes/archive/<dated-slug>
        assert path.startswith("openspec/changes"), (
            f"change {n.node_id!r} path doesn't start with openspec/changes: {path!r}"
        )


# ---------- B34: git_commit.created_at ----------


def test_git_commit_created_at_populated(fixture_paths) -> None:
    """Every git_commit emits with `created_at` set to its
    `authored_at` (best-effort proxy without the domain
    Phase B TimeResolver)."""
    from okg.substrate.library.sources._git_graph_history_emit import GitHistorySource

    src = GitHistorySource(
        repo_root=fixture_paths.repo_root,
        repo="okg",
    )
    nodes = list(src.nodes())
    commits = [n for n in nodes if n.subtype == "git_commit"]
    assert commits, "fixture should produce at least one commit"
    for n in commits:
        created_at = n.attrs.get("created_at")
        authored_at = n.attrs.get("authored_at")
        assert created_at, f"commit {n.node_id!r} has no created_at"
        assert created_at == authored_at, (
            f"commit {n.node_id!r}: created_at != authored_at "
            f"({created_at!r} vs {authored_at!r})"
        )


# ---------- B34: source_file.created_at ----------


def test_source_file_created_at_populated(fixture_paths) -> None:
    """Every source_file with a known last_modified_at also emits
    `created_at` and `created_commit_sha`: the commit that first added the
    path, no later than the last change (issue #3468; before it, created_at
    copied last_modified_at)."""
    from datetime import datetime

    from okg.substrate.library.sources._git_graph_files_emit import GitFilesSource

    src = GitFilesSource(
        repo_root=fixture_paths.repo_root,
        repo="okg",
    )
    nodes = list(src.nodes())
    files = [n for n in nodes if n.subtype == "source_file"]
    assert files, "fixture should produce at least one source_file"
    with_mod = [n for n in files if n.attrs.get("last_modified_at")]
    assert with_mod, "fixture should produce at least one file with last_modified_at"
    for n in with_mod:
        created_at = n.attrs.get("created_at")
        last_mod = n.attrs.get("last_modified_at")
        assert created_at and n.attrs.get("created_commit_sha"), n.node_id
        assert datetime.fromisoformat(created_at) <= datetime.fromisoformat(
            last_mod
        ), (
            f"source_file {n.node_id!r}: created_at after "
            f"last_modified_at ({created_at!r} vs {last_mod!r})"
        )
    # The fixture holds a file added and later changed: its dates differ.
    assert any(
        n.attrs["created_at"] != n.attrs["last_modified_at"] for n in with_mod
    )


# ---------- B30: permission_event.timestamp ----------


def test_permission_event_mode_changed_carries_timestamp(tmp_path) -> None:
    """When a permission-mode event lacks a top-level
    timestamp, the extractor falls back to the scan's
    running-max ts (`scan.ended_at`) — never empty string."""
    import json

    # Synthesize a minimal session JSONL with a
    # permission-mode event that has no top-level timestamp
    # but follows other events that do.
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    jsonl = sessions_dir / "synthetic-001.jsonl"
    events = [
        {
            "type": "user",
            "sessionId": "synthetic-001",
            "uuid": "u1",
            "timestamp": "2026-04-25T10:00:00Z",
            "cwd": "/repo",
            "message": {"role": "user", "content": "hi"},
        },
        {
            "type": "assistant",
            "sessionId": "synthetic-001",
            "uuid": "u2",
            "timestamp": "2026-04-25T10:00:30Z",
            "cwd": "/repo",
            "message": {"role": "assistant", "content": "hello"},
        },
        # The mode-change event has no top-level timestamp.
        {
            "type": "permission-mode",
            "sessionId": "synthetic-001",
            "permissionMode": "auto",
            # no "timestamp" field
        },
    ]
    jsonl.write_text("\n".join(json.dumps(e) for e in events) + "\n")

    from okg.substrate.library.sources._agent_sessions_extract import (
        scan_transcript,
    )

    scan = scan_transcript(jsonl)
    assert scan.permission_events, "expected mode_changed event"
    pe = scan.permission_events[0]
    assert pe.kind == "mode_changed"
    assert pe.timestamp != "", (
        "mode_changed event has empty timestamp; expected fallback to scan.ended_at"
    )
    # The ts should be the max observed in the scan so far —
    # i.e. the second user/assistant event's timestamp.
    assert pe.timestamp == "2026-04-25T10:00:30Z"
