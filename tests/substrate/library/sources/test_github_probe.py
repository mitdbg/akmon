"""GitHubSource change-probe (#323): fixture->content-hash, repo->mutable-api.

A fixture-backed source reads local bytes (content-hash floor); a live repo is a
mutable upstream API probed by a cheap high-water `updated_at`. The generic
MutableApiProbe busts on a version bump and forces a full read on any error.
"""
from __future__ import annotations

import json
import sys

from okg.substrate.library.sources.change_probe import (
    run_probe,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.github import GitHubSource
from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe


_GH_MODULE = sys.modules["okg.substrate.library.sources.github"]


def test_fixture_mode_uses_content_hash_probe(tmp_path):
    assert GitHubSource.change_probe_kind == "mutable_api"
    fx = tmp_path / "issues.json"
    fx.write_text(json.dumps([{"number": 1, "updated_at": "2026-01-01T00:00:00Z"}]))
    s = GitHubSource(fixture_path=str(fx))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    validate_token_composition(t0)
    fx.write_text(json.dumps([{"number": 1, "updated_at": "2026-02-02T00:00:00Z"}]))
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_repo_mode_uses_mutable_api_probe():
    s = GitHubSource(repo="owner/name")
    assert isinstance(s.change_probe, MutableApiProbe)


def test_mutable_api_probe_busts_on_version_bump():
    state = {"v": "2026-01-01T00:00:00Z:1"}
    probe = MutableApiProbe(
        version_fn=lambda: state["v"],
        config={"repo": "owner/name"},
        emit_targets=[_GH_MODULE],
        content_kind="updated_at",
    )
    t0 = probe.build_token()
    validate_token_composition(t0)
    assert tokens_equal(t0, probe.build_token())  # stable version
    state["v"] = "2026-02-02T00:00:00Z:2"  # an issue was edited/created
    assert not tokens_equal(t0, probe.build_token())


def test_mutable_api_probe_error_forces_full_read():
    def _boom():
        raise RuntimeError("rate limited")

    probe = MutableApiProbe(version_fn=_boom, config={}, emit_targets=[_GH_MODULE])
    verdict = run_probe(probe, cursor={"change_probe_token": {"x": 1}}, sync_scope=None)
    assert verdict.changed is True
    assert verdict.reason.startswith("probe_error")


def test_config_filter_change_busts_token(tmp_path):
    # Two fixture sources with different `since` filters must not share a token
    # (the filter changes which issues are emitted).
    fx = tmp_path / "issues.json"
    fx.write_text(json.dumps([{"number": 1, "updated_at": "2026-01-01T00:00:00Z"}]))
    a = GitHubSource(fixture_path=str(fx), since=None)
    b = GitHubSource(fixture_path=str(fx), since="2026-01-01T00:00:00Z")
    assert not tokens_equal(a.change_probe.build_token(), b.change_probe.build_token())
