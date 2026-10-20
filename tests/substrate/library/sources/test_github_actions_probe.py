"""GitHub Actions library sources resolve to sound mutable-api probes (#323).

GitHubActionsSource (runs feed updated_at high-water) and
GitHubRunnerInventorySource (membership+state digest, delete-sensitive) both
fix okg-ci AND okg-ci-dev (shared library). fixture mode -> content-hash floor.
No live network: the live paths are exercised by monkeypatching the gh helpers.
"""
from __future__ import annotations

import copy
import json
import os

from okg.substrate.library.sources import github_actions as ga
from okg.substrate.library.sources.change_probe import (
    run_probe,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.library.sources.github_actions import (
    GitHubActionsSource,
    GitHubRunnerInventorySource,
)
from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe


def test_actions_fixture_mode_content_hash(tmp_path):
    fx = tmp_path / "runs.json"
    fx.write_text(json.dumps({"workflow_runs": [{"id": 1, "updated_at": "2026-01-01T00:00:00Z"}]}))
    assert GitHubActionsSource.change_probe_kind == "mutable_api"
    s = GitHubActionsSource(repo="o/n", fixture_path=str(fx))
    assert isinstance(s.change_probe, ContentHashProbe)
    assert s.probe_short_circuit_safe is True
    t0 = s.change_probe.build_token()
    validate_token_composition(t0)
    os.utime(fx, (2_000_000_000, 2_000_000_000))
    assert tokens_equal(t0, s.change_probe.build_token())  # churn-immune
    fx.write_text(json.dumps({"workflow_runs": [{"id": 2, "updated_at": "2026-02-02T00:00:00Z"}]}))
    assert not tokens_equal(t0, s.change_probe.build_token())  # edit busts


def test_actions_repo_mode_watermark(monkeypatch):
    s = GitHubActionsSource(repo="o/n")
    assert isinstance(s.change_probe, MutableApiProbe)
    assert s.probe_short_circuit_safe is False
    state = {"doc": {"workflow_runs": [{"id": 7, "updated_at": "2026-01-01T00:00:00Z", "run_attempt": 1}]}}
    monkeypatch.setattr(ga, "_gh_api_json", lambda path: state["doc"])
    t0 = s.change_probe.build_token()
    assert tokens_equal(t0, s.change_probe.build_token())  # stable upstream
    # A status transition bumps updated_at -> token busts.
    state["doc"] = {"workflow_runs": [{"id": 7, "updated_at": "2026-01-01T01:00:00Z", "run_attempt": 1}]}
    assert not tokens_equal(t0, s.change_probe.build_token())
    # A gh failure PROPAGATES -> run_probe forces a full read (never silent skip).
    def _boom(path):
        raise RuntimeError("rate limited")
    monkeypatch.setattr(ga, "_gh_api_json", _boom)
    v = run_probe(s.change_probe, cursor={"change_probe_token": t0}, sync_scope=None)
    assert v.changed is True and v.reason.startswith("probe_error")


def test_actions_live_reader_does_not_claim_high_water_snapshot_authority(
    monkeypatch,
):
    source = GitHubActionsSource(repo="o/n")
    monkeypatch.setattr(
        source,
        "_fetch_live",
        lambda: {
            "workflow_runs": [],
            "jobs_by_run_id": {},
            "artifacts_by_run_id": {},
            "check_runs_by_sha": {},
        },
    )

    run = source.run("live", mode="scope_complete")
    list(run.facts)

    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"


def test_runner_inventory_digest_detects_deregistration(monkeypatch):
    assert GitHubRunnerInventorySource.change_probe_kind == "mutable_api"
    s = GitHubRunnerInventorySource(repo="o/n")
    assert isinstance(s.change_probe, MutableApiProbe)
    state = {"payload": {"runners": [
        {"id": 1, "status": "online", "busy": False, "version": "2.0", "labels": [{"name": "linux"}]},
        {"id": 2, "status": "online", "busy": True, "version": "2.0", "labels": [{"name": "linux"}]},
    ]}}
    monkeypatch.setattr(GitHubRunnerInventorySource, "_fetch_live", lambda self: state["payload"])
    t0 = s.change_probe.build_token()
    assert tokens_equal(t0, s.change_probe.build_token())  # stable
    # A status flip busts.
    state["payload"]["runners"][0]["status"] = "offline"
    assert not tokens_equal(t0, s.change_probe.build_token())
    t1 = s.change_probe.build_token()
    # A DEREGISTRATION (runner 2 vanishes) busts — the case a max-id watermark misses.
    state["payload"]["runners"] = [state["payload"]["runners"][0]]
    assert not tokens_equal(t1, s.change_probe.build_token())


def test_runner_inventory_fixture_mode(tmp_path):
    fx = tmp_path / "runners.json"
    fx.write_text(json.dumps({"runners": [{"id": 1, "status": "online"}]}))
    s = GitHubRunnerInventorySource(repo="o/n", fixture_path=str(fx))
    assert isinstance(s.change_probe, ContentHashProbe)
    t0 = s.change_probe.build_token()
    fx.write_text(json.dumps({"runners": [{"id": 1, "status": "offline"}]}))
    assert not tokens_equal(t0, s.change_probe.build_token())


def test_runner_inventory_digest_covers_every_emitted_upstream_field(
    monkeypatch,
):
    base = {
        "id": 1,
        "name": "runner-a",
        "runner_group_id": 7,
        "runner_group_name": "Default",
        "status": "online",
        "busy": False,
        "version": "2.321.0",
        "os": "Linux",
        "image_id": "image-a",
        "labels": [
            {"name": "self-hosted"},
            {"name": "X64"},
            {"name": "openstack"},
            {"name": "pr-safe"},
        ],
    }
    state = {"payload": {"runners": [copy.deepcopy(base)]}}
    source = GitHubRunnerInventorySource(repo="o/n")
    monkeypatch.setattr(
        GitHubRunnerInventorySource,
        "_fetch_live",
        lambda self: state["payload"],
    )
    baseline = source.change_probe.build_token()
    mutations = (
        ("id", 2),
        ("name", "runner-b"),
        ("runner_group_id", 8),
        ("runner_group_name", "Large"),
        ("status", "offline"),
        ("busy", True),
        ("version", "2.322.0"),
        ("os", "Windows"),
        ("image_id", "image-b"),
        (
            "labels",
            [
                {"name": "self-hosted"},
                {"name": "ARM64"},
                {"name": "kubernetes"},
                {"name": "gpu"},
            ],
        ),
    )
    for field, value in mutations:
        changed = copy.deepcopy(base)
        changed[field] = value
        state["payload"] = {"runners": [changed]}
        assert not tokens_equal(
            baseline,
            source.change_probe.build_token(),
        ), field

    state["payload"] = {"runners": [copy.deepcopy(base)]}
    run = source.run("live", mode="scope_complete")
    list(run.facts)
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert run.applied_raw_token == source.change_probe.build_token()
