"""GitHubSource — fixture path + gh-CLI shelling.

Per `wire-issues-feed-to-okg-workspace` (2026-04-26).
"""
from __future__ import annotations

import json

import pytest

from okg.substrate.library.sources.base import (
    EdgeFact, NodeFact,
)
from okg.substrate.library.sources.github import (
    GhCliMissing, GitHubSource,
)


def _items():
    return [
        {
            "number": 1, "title": "first issue",
            "state": "open", "user": {"login": "alice"},
            "html_url": "https://github.com/owner/repo/issues/1",
            "body": "body of issue 1", "labels": [],
            "created_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
        },
        {
            "number": 7, "title": "wire stuff", "state": "open",
            "user": {"login": "bob"},
            "html_url": "https://github.com/owner/repo/pull/7",
            "body": "PR body", "labels": [],
            "created_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "merged_at": None,
            "pull_request": {"html_url": "..."},
        },
    ]


def _fixture(tmp_path, items=None):
    p = tmp_path / "issues.json"
    p.write_text(json.dumps(_items() if items is None else items))
    return p


def test_fixture_emits_repo_issue_pr_with_contains_edges(tmp_path):
    src = GitHubSource(
        repo="owner/repo", fixture_path=_fixture(tmp_path),
    )
    run = src.run("test-run-id", mode="scope_complete")
    facts = list(run.facts)

    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    subtypes = {n.subtype for n in nodes}
    assert {"repo", "issue", "pull_request"} == subtypes

    issue_node = next(n for n in nodes if n.subtype == "issue")
    assert issue_node.attrs["title"] == "first issue"
    assert issue_node.attrs["state"] == "open"

    pr_node = next(n for n in nodes if n.subtype == "pull_request")
    assert pr_node.attrs["title"] == "wire stuff"
    assert pr_node.attrs["state"] == "open"
    assert pr_node.attrs["merged"] is False

    # contains edges from repo to each child.
    contain_targets = {e.dst for e in edges if e.edge_type == "contains"}
    assert any(t.startswith("issue:") for t in contain_targets)
    assert any(t.startswith("pr:") for t in contain_targets)
    assert run.record_set is not None
    assert set(run.record_set.records) == {
        "repo:github:owner/repo",
        "issue:github:owner/repo:#1",
        "pr:github:owner/repo:#7",
    }


def test_normalization_redacts_issue_and_pr_text(tmp_path):
    items = [
        {
            "number": 1,
            "title": "uses /Users/alice/.codex/worktrees/okg",
            "state": "open",
            "user": {"login": "alice"},
            "html_url": "https://github.com/owner/repo/issues/1",
            "body": "token=ghp_12345678901234567890 path=/Users/alice/private",
            "labels": [],
            "created_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
        },
        {
            "number": 7,
            "title": "PR from /Users/alice/private",
            "state": "open",
            "user": {"login": "bob"},
            "html_url": "https://github.com/owner/repo/pull/7",
            "body": "reviewed in /Users/alice/.codex/worktrees/okg",
            "labels": [],
            "created_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "merged_at": None,
            "pull_request": {"html_url": "..."},
        },
    ]
    src = GitHubSource(repo="owner/repo", fixture_path=_fixture(tmp_path, items))

    facts = list(src.run("test-run-id", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    blob = "\n".join(
        str(value)
        for node in nodes
        for value in node.attrs.values()
    )

    assert "/Users/alice" not in blob
    assert "/.codex/" not in blob
    assert "ghp_1234567890" not in blob
    assert "<path:redacted>" in blob
    assert "<secret:redacted>" in blob


def test_incremental_scope_does_not_claim_full_record_set(tmp_path) -> None:
    source = GitHubSource(
        repo="owner/repo",
        fixture_path=_fixture(tmp_path),
        since="2026-04-26T00:00:00Z",
    )

    run = source.run("test-run-id", mode="scope_complete")

    assert run.record_set is None


def test_no_repo_no_fixture_raises(tmp_path):
    src = GitHubSource()  # neither repo nor fixture_path
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError):
        list(src.run("rid", mode="scope_complete").facts)


def test_gh_cli_missing_when_no_fixture_and_no_gh(monkeypatch):
    """When `gh` isn't on PATH and only `repo=` is set, the
    adapter raises a structured error pointing at the
    install/auth recipe."""
    src = GitHubSource(repo="owner/repo")  # no fixture_path
    monkeypatch.setattr(
        "okg.substrate.library.sources.github.shutil.which",
        lambda _: None,
    )
    with pytest.raises(GhCliMissing) as exc_info:
        src.run("rid", mode="scope_complete")
    assert "gh auth login" in exc_info.value.fix_hint


@pytest.mark.postgres
def test_record_set_retracts_removed_issue(tmp_path, clean_runtime: str):
    import psycopg

    from okg.substrate.ingest import SourceRunner

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    source_name = "test.github-record-set"
    fixture_path = _fixture(tmp_path)
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        GitHubSource(repo="owner/repo", fixture_path=fixture_path),
        mode="reconcile",
    )
    assert first.next_cursor is not None
    assert "record_set" in first.next_cursor

    fixture_path.write_text(json.dumps([_items()[1]]))
    second = runner.run(
        GitHubSource(repo="owner/repo", fixture_path=fixture_path),
        mode="reconcile",
    )
    assert second.retractions_node == 1
    assert second.retractions_edge == 1

    assert _graph_node_exists(
        clean_runtime,
        node_id="issue:github:owner/repo:#1",
    ) is False


@pytest.mark.postgres
def test_scope_complete_replay_is_an_authoritative_noop(
    tmp_path,
    clean_runtime: str,
) -> None:
    import psycopg

    from okg.substrate.ingest import SourceRunner

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    fixture_path = _fixture(tmp_path)
    runner = SourceRunner(
        clean_runtime,
        source_name="test.github-scope-complete-replay",
    )
    source = GitHubSource(repo="owner/repo", fixture_path=fixture_path)

    first = runner.run(source, mode="scope_complete")
    second = runner.run(source, mode="scope_complete")

    assert first.nodes_appended > 0
    assert second.nodes_appended == 0
    assert second.edges_appended == 0
    assert second.completed_scope is True
    assert second.next_cursor is not None
    assert "record_set" in second.next_cursor


def _graph_node_exists(dsn: str, *, node_id: str) -> bool:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT 1 AS present
              FROM okg.graph_nodes
             WHERE node_id = :node_id
             LIMIT 1
            """,
            {"node_id": node_id},
        )
    return bool(rows)
