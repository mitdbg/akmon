"""compute-env sources resolve to sound change-probes (#323).

Both sources' emission is content/state-pure (NO wall-clock input -- the apparent
time-dependence was dead `_is_idle` code, now removed):
  - compute-env-local: a `reference_catalog` digest of the live probe state
    (host identity + runtime versions + settings.json MCP servers).
  - compute-env-remote: a complete `content_hash` over mutable session jsonl.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from okg.substrate.library.sources.change_probe import (
    emit_version_for,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.compute_env_local import (
    LocalEnvSource,
    _LocalEnvStateProbe,
)
from okg.substrate.library.sources.compute_env_remote_hosts import (
    RemoteHostsFromSessionsSource,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe


def test_local_env_state_probe_digest_and_settings_edit(tmp_path):
    from okg.substrate.library.sources import _compute_env_local_emit
    from okg.substrate.library.sources.emission_identity import (
        emission_identity_targets,
    )

    settings = tmp_path / "settings.json"
    settings.write_text(json.dumps({"mcpServers": {"a": {"command": "x"}}}))
    assert LocalEnvSource.change_probe_kind == "reference_catalog"
    s = LocalEnvSource(repo="okg", settings_path=str(settings))
    assert isinstance(s.change_probe, _LocalEnvStateProbe)
    t0 = s.change_probe.build_token()
    validate_token_composition(t0)
    assert t0["components"]["emit_version"]["value"] == emit_version_for(
        *emission_identity_targets(
            s.name,
            LocalEnvSource,
            _compute_env_local_emit,
        )
    )
    assert tokens_equal(t0, s.change_probe.build_token())  # stable host+settings
    os.utime(settings, (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())  # mtime churn-immune
    # An MCP-server edit changes the emitted catalog -> token busts.
    settings.write_text(
        json.dumps({"mcpServers": {"a": {"command": "x"}, "b": {"command": "y"}}})
    )
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_remote_hosts_complete_content_probe(tmp_path):
    cwd = tmp_path / "proj-a"
    cwd.mkdir()
    (cwd / "s1.jsonl").write_text('{"type":"x"}\n')
    (cwd / "s2.jsonl").write_text('{"type":"y"}\n')
    assert RemoteHostsFromSessionsSource.change_probe_kind == "content_hash"
    r = RemoteHostsFromSessionsSource(repo="okg", claude_projects_root=str(tmp_path))
    assert isinstance(r.change_probe, ContentHashProbe)
    t0 = r.change_probe.build_token()
    validate_token_composition(t0)
    os.utime(cwd / "s1.jsonl", (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, r.change_probe.build_token())  # mtime churn-immune
    with open(cwd / "s1.jsonl", "a") as fh:
        fh.write('{"type":"z"}\n')
    assert not tokens_equal(t0, r.change_probe.build_token())


def test_remote_hosts_probe_detects_same_size_rewrite_and_truncation(tmp_path):
    cwd = tmp_path / "proj-a"
    cwd.mkdir()
    session = cwd / "s1.jsonl"
    session.write_text('{"host":"old.example"}\n{"host":"keep.example"}\n')
    r = RemoteHostsFromSessionsSource(repo="okg", claude_projects_root=str(tmp_path))
    t0 = r.change_probe.build_token()
    original_stat = session.stat()

    session.write_text('{"host":"new.example"}\n{"host":"keep.example"}\n')
    os.utime(
        session,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    assert session.stat().st_size == original_stat.st_size
    assert not tokens_equal(t0, r.change_probe.build_token())

    t1 = r.change_probe.build_token()
    session.write_text('{"host":"new.example"}\n')
    assert not tokens_equal(t1, r.change_probe.build_token())


def test_remote_hosts_probe_detects_delete_and_new_session(tmp_path):
    cwd = tmp_path / "proj-a"
    cwd.mkdir()
    (cwd / "s1.jsonl").write_text('{"type":"x"}\n')
    (cwd / "s2.jsonl").write_text('{"type":"y"}\n')
    r = RemoteHostsFromSessionsSource(repo="okg", claude_projects_root=str(tmp_path))
    t0 = r.change_probe.build_token()
    (cwd / "s2.jsonl").unlink()  # delete -> membership drop
    assert not tokens_equal(t0, r.change_probe.build_token())
    t1 = r.change_probe.build_token()
    (cwd / "s3.jsonl").write_text('{"type":"new"}\n')  # new session -> +membership
    assert not tokens_equal(t1, r.change_probe.build_token())


def test_remote_hosts_probe_skips_local_claude_worktree_projects(
    tmp_path,
    monkeypatch,
):
    skipped = (
        tmp_path
        / "-home-example-projects-demo-repo--claude-worktrees-demo"
    )
    skipped.mkdir()
    (skipped / "s1.jsonl").write_text('{"type":"x"}\n')

    real_scandir = os.scandir

    def guarded_scandir(path):
        path_obj = Path(path).resolve()
        if path_obj == skipped.resolve() or skipped.resolve() in path_obj.parents:
            raise AssertionError(f"remote-host probe entered local Claude cache: {path_obj}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    r = RemoteHostsFromSessionsSource(repo="okg", claude_projects_root=str(tmp_path))
    r.change_probe.build_token()
