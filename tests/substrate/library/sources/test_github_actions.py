from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.github import GhCliMissing
from okg.substrate.library.sources.github_actions import (
    GitHubActionsSource,
    GitHubRunnerInventorySource,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_github_actions_fixture_emits_ci_run_job_check_artifact_and_runner_links(
    tmp_path: Path,
) -> None:
    fixture = _write_json(
        tmp_path / "actions.json",
        {
            "workflow_runs": [
                {
                    "id": 27473006235,
                    "run_attempt": 1,
                    "name": "Tests",
                    "display_title": "Add OKG-CI deployment boundary",
                    "status": "completed",
                    "conclusion": "success",
                    "event": "pull_request",
                    "head_branch": "codex/design-okg-ci-remote-authority",
                    "head_sha": "6daa6c9a",
                    "html_url": "https://github.com/owner/repo/actions/runs/27473006235",
                    "created_at": "2026-06-13T16:52:16Z",
                    "updated_at": "2026-06-13T17:19:59Z",
                    "pull_requests": [{"number": 80}],
                }
            ],
            "jobs_by_run_id": {
                "27473006235": [
                    {
                        "id": 81207354751,
                        "run_id": 27473006235,
                        "run_attempt": 1,
                        "workflow_name": "Tests",
                        "head_sha": "6daa6c9a",
                        "name": "fast-linux",
                        "status": "completed",
                        "conclusion": "success",
                        "started_at": "2026-06-13T16:54:58Z",
                        "completed_at": "2026-06-13T17:00:32Z",
                        "html_url": "https://github.com/owner/repo/actions/runs/27473006235/job/81207354751",
                        "check_run_url": "https://api.github.com/repos/owner/repo/check-runs/81207354751",
                        "runner_id": 112,
                        "runner_name": "okg-gha-linux-03",
                        "runner_group_name": "Default",
                        "labels": [
                            "self-hosted",
                            "okg",
                            "linux",
                            "x64",
                            "openstack",
                            "persistent",
                            "pr-safe",
                        ],
                    }
                ]
            },
            "artifacts_by_run_id": {
                "27473006235": [
                    {
                        "id": 7612714057,
                        "node_id": "MDg6QXJ0aWZhY3Q",
                        "name": "ci-fast-linux",
                        "size_in_bytes": 93640,
                        "url": "https://api.github.com/repos/owner/repo/actions/artifacts/7612714057",
                        "archive_download_url": "https://api.github.com/repos/owner/repo/actions/artifacts/7612714057/zip",
                        "expired": False,
                        "expires_at": "2026-06-27T17:00:28Z",
                        "created_at": "2026-06-13T17:00:29Z",
                        "updated_at": "2026-06-13T17:00:29Z",
                    }
                ]
            },
            "check_runs_by_sha": {
                "6daa6c9a": [
                    {
                        "id": 81207354751,
                        "name": "fast-linux",
                        "status": "completed",
                        "conclusion": "success",
                        "started_at": "2026-06-13T16:54:58Z",
                        "completed_at": "2026-06-13T17:00:32Z",
                        "html_url": "https://github.com/owner/repo/actions/runs/27473006235/job/81207354751",
                        "details_url": "https://github.com/owner/repo/actions/runs/27473006235/job/81207354751",
                        "app": {"slug": "github-actions"},
                    }
                ]
            },
        },
    )

    source = GitHubActionsSource(
        repo="owner/repo",
        commit_repo="repo",
        fixture_path=fixture,
    )
    run = source.run("fixture", mode="scope_complete")
    facts = list(run.facts)
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert run.applied_raw_token == source.change_probe.build_token()
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    assert {
        "repo",
        "ci_workflow",
        "ci_job",
        "ci_check_run",
        "ci_artifact",
    } <= {node.subtype for node in nodes}
    assert any(
        node.subtype == "ci_job"
        and node.attrs["runner_id"] == 112
        and node.attrs["duration_s"] == 334.0
        for node in nodes
    )
    assert any(
        node.subtype == "ci_artifact" and node.attrs["content_kind"] == "ci_artifact"
        for node in nodes
    )
    triples = {(edge.src, edge.edge_type, edge.dst) for edge in edges}
    workflow = "ci-workflow:github:owner/repo:run:27473006235:attempt:1"
    job = "ci-job:github:owner/repo:job:81207354751:attempt:1"
    check = "ci-check-run:github:owner/repo:81207354751"
    artifact = "ci-artifact:github:owner/repo:7612714057"
    runner = "ci-runner:github:repo:owner/repo:112"
    assert ("repo:github:owner/repo", "contains", workflow) in triples
    assert (workflow, "contains", job) in triples
    assert (workflow, "affects", "commit:repo:6daa6c9a") in triples
    assert (workflow, "references", "pr:github:owner/repo:#80") in triples
    assert (workflow, "references", check) in triples
    assert (job, "references", check) in triples
    assert (job, "references", runner) in triples
    assert (job, "produces", artifact) in triples
    assert (artifact, "references", workflow) in triples

    repeated = list(
        GitHubActionsSource(
            repo="owner/repo",
            commit_repo="repo",
            fixture_path=fixture,
        )
        .run("different-acquisition", mode="scope_complete")
        .facts
    )
    assert repeated == facts
    by_subtype = {
        node.subtype: node
        for node in nodes
        if node.subtype in {"ci_workflow", "ci_job", "ci_artifact"}
    }
    assert by_subtype["ci_workflow"].attrs["observed_at"] == "2026-06-13T17:19:59Z"
    assert by_subtype["ci_job"].attrs["observed_at"] == "2026-06-13T17:00:32Z"
    assert by_subtype["ci_artifact"].attrs["observed_at"] == "2026-06-13T17:00:29Z"


def test_runner_inventory_fixture_emits_runner_snapshot(tmp_path: Path) -> None:
    fixture = _write_json(
        tmp_path / "runners.json",
        {
            "runners": [
                {
                    "id": 112,
                    "name": "okg-gha-linux-03",
                    "os": "Linux",
                    "status": "online",
                    "busy": False,
                    "runner_group_id": 1,
                    "runner_group_name": "Default",
                    "labels": [
                        {"name": "self-hosted"},
                        {"name": "Linux"},
                        {"name": "X64"},
                        {"name": "okg"},
                        {"name": "openstack"},
                        {"name": "pr-safe"},
                        {"name": "persistent"},
                    ],
                }
            ]
        },
    )

    source = GitHubRunnerInventorySource(
        repo="owner/repo",
        fixture_path=fixture,
        observed_at="2026-06-13T17:00:00+00:00",
    )
    run = source.run("fixture", mode="scope_complete")
    facts = list(run.facts)
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert run.applied_raw_token == source.change_probe.build_token()
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    runner = next(node for node in nodes if node.subtype == "ci_runner")

    assert runner.node_id == "ci-runner:github:repo:owner/repo:112"
    assert runner.attrs["provider"] == "openstack"
    assert runner.attrs["pool"] == "okg-openstack-linux-pr"
    assert runner.attrs["arch"] == "x64"
    assert runner.attrs["busy"] is False
    assert "observed_at" not in runner.attrs
    assert "observed_at" not in runner.source_revision
    assert any(
        edge.src == "repo:github:owner/repo"
        and edge.edge_type == "contains"
        and edge.dst == runner.node_id
        for edge in edges
    )

    later = list(
        GitHubRunnerInventorySource(
            repo="owner/repo",
            fixture_path=fixture,
            observed_at="2027-01-01T00:00:00+00:00",
        )
        .run("different-acquisition", mode="scope_complete")
        .facts
    )
    assert later == facts


def test_github_actions_live_without_gh_raises_structured_error(monkeypatch):
    source = GitHubActionsSource(repo="owner/repo")
    monkeypatch.setattr(
        "okg.substrate.library.sources.github_actions.shutil.which",
        lambda _: None,
    )

    with pytest.raises(GhCliMissing):
        source.run("live", mode="scope_complete")


def _patch_runs_listing(monkeypatch, doc: dict[str, object]) -> list[str]:
    calls: list[str] = []

    def _gh_api_json(path: str) -> dict[str, object]:
        calls.append(path)
        return doc

    monkeypatch.setattr(
        "okg.substrate.library.sources.github_actions._gh_api_json",
        _gh_api_json,
    )
    return calls


def _listing_doc(run_ids: list[int], *, total_count: object) -> dict[str, object]:
    doc: dict[str, object] = {
        "workflow_runs": [
            {"id": rid, "run_attempt": 1, "name": "Tests", "head_sha": ""}
            for rid in run_ids
        ],
    }
    if total_count is not _MISSING:
        doc["total_count"] = total_count
    return doc


_MISSING = object()


def _source(**kwargs) -> GitHubActionsSource:
    return GitHubActionsSource(
        repo="owner/repo",
        include_jobs=False,
        include_artifacts=False,
        include_check_runs=False,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("total_count", "reason"),
    [
        (5, "listing_truncated"),
        (_MISSING, "total_count_unavailable"),
        ("5", "total_count_unavailable"),
        (True, "total_count_unavailable"),
    ],
)
def test_github_actions_truncated_listing_claims_no_completed_scope(
    monkeypatch, total_count, reason
):
    _patch_runs_listing(monkeypatch, _listing_doc([5, 4], total_count=total_count))

    run = _source(run_limit=2).run("live", mode="reconcile")
    facts = list(run.facts)

    assert run.completed_scope is False
    assert run.record_set is None
    assert run.run_mode == "reconcile"
    assert run.next_cursor["run_listing"]["complete"] is False
    assert run.next_cursor["run_listing"]["reason"] == reason
    assert run.next_cursor["run_count"] == 2
    assert {
        fact.node_id for fact in facts if isinstance(fact, NodeFact)
    } >= {
        "ci-workflow:github:owner/repo:run:5:attempt:1",
        "ci-workflow:github:owner/repo:run:4:attempt:1",
    }


def test_github_actions_exhausted_listing_claims_completed_scope(monkeypatch):
    _patch_runs_listing(monkeypatch, _listing_doc([2, 1], total_count=2))

    run = _source(run_limit=2).run("live", mode="reconcile")

    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.next_cursor["run_listing"] == {
        "kind": "newest_runs",
        "complete": True,
        "read_count": 2,
        "total_count": 2,
        "reason": "listing_exhausted",
    }


def test_github_actions_unreported_listing_claims_no_completed_scope(monkeypatch):
    source = _source(run_limit=2)
    monkeypatch.setattr(source, "_fetch_live", lambda: {"workflow_runs": []})

    run = source.run("live", mode="reconcile")

    assert run.completed_scope is False
    assert run.record_set is None
    assert run.next_cursor["run_listing"] == {
        "complete": False,
        "reason": "listing_unreported",
    }


def test_github_actions_cursor_mode_never_claims_completed_scope(monkeypatch):
    _patch_runs_listing(monkeypatch, _listing_doc([1], total_count=1))

    run = _source(run_limit=2).run("live", mode="cursor")

    assert run.completed_scope is False
    assert run.record_set is None


def test_github_actions_explicit_run_ids_are_the_whole_scope(monkeypatch):
    calls = _patch_runs_listing(
        monkeypatch,
        {"id": 7, "run_attempt": 1, "name": "Tests", "head_sha": ""},
    )

    run = _source(run_ids=[7], run_limit=1).run("live", mode="reconcile")

    assert calls == ["/repos/owner/repo/actions/runs/7"]
    assert run.completed_scope is True
    assert run.next_cursor["run_listing"] == {"kind": "run_ids", "complete": True}


def test_runner_inventory_live_fetches_every_page(monkeypatch):
    pages = [
        {"total_count": 2, "runners": [{"id": 1}]},
        {"total_count": 2, "runners": [{"id": 2}]},
    ]
    monkeypatch.setattr(
        "okg.substrate.library.sources.github_actions.shutil.which",
        lambda _: "/usr/bin/gh",
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.github_actions.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(pages), stderr=""
        ),
    )
    payload = GitHubRunnerInventorySource(repo="owner/repo")._fetch_live()
    assert [runner["id"] for runner in payload["runners"]] == [1, 2]
