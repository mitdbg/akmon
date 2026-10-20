"""Built-in source sync capability coverage."""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.ingest.planning import plan_ingest_sources
from okg.substrate.sources.registry import load_source_registry


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_okg_workspace_built_in_sources_declare_sync_capabilities() -> None:
    registry = load_source_registry(
        REPO_ROOT / "deployments/okg-workspace/source_registry.yaml",
    )
    expected = {
        "git-files": {"fs_event", "manual", "reconcile"},
        "code-structure": {"fs_event", "poll", "manual", "reconcile"},
        "package-manifests": {"fs_event", "manual", "reconcile"},
        "interfaces": {"fs_event", "manual", "reconcile"},
        "code-data-flow": {"manual", "reconcile"},
        "semgrep-data-flow": {"fs_event", "manual", "reconcile"},
        "codeql-data-flow": {"fs_event", "manual", "reconcile"},
        "test-reports": {"fs_event", "manual", "reconcile"},
        "security-analysis": {"fs_event", "manual", "reconcile"},
        "agent-runtime-security-monitor": {"fs_event", "manual", "reconcile"},
        "profile-measurements": {"fs_event", "manual", "reconcile"},
        "self-play-reports": {"fs_event", "manual", "reconcile"},
        "agent-memory": {"fs_event", "manual", "reconcile"},
        "git-history": {"git_ref_change", "manual", "reconcile"},
        "openspec": {"fs_event", "manual", "reconcile"},
        "pact": {"fs_event", "poll", "manual", "reconcile"},
        "agent-sessions": {"poll", "manual", "reconcile"},
        "codex-sessions": {"poll", "manual", "reconcile"},
        "compute-env-local": {"poll", "manual", "reconcile"},
        # compute-env-remote is omitted: quarantined out of okg-workspace's
        # source registry (2026-08-24). Its contract is frozen, not deleted,
        # in test_quarantined_compute_env_remote_stays_scoped_and_compacted
        # below. Restore this entry when the source returns.
        "doc-corpus": {"fs_event", "manual", "reconcile"},
        "coordination-config": {"fs_event", "poll", "manual", "reconcile"},
        "coordination-native": {"poll", "manual", "reconcile"},
        "github-issues": {"webhook", "poll", "manual", "reconcile"},
        "junit-xml": {"fs_event", "manual", "reconcile"},
    }

    for source, triggers in expected.items():
        sync = registry[source].sync
        assert sync is not None, source
        assert set(sync.triggers) >= triggers
        assert sync.reconcile_mode in {
            "scope_complete",
            "reconcile",
            "release_new",
        }
        if "poll" in sync.triggers:
            assert sync.poll is not None, source
        if (
            "fs_event" in triggers
            or "git_ref_change" in triggers
            or source in {
                "agent-sessions",
                "codex-sessions",
            }
        ):
            assert sync.watch is not None, source
        else:
            assert sync.watch is None, source

    assert registry["github-issues"].sync is not None
    assert registry["github-issues"].sync.watch is None
    assert registry["github-issues"].sync.webhook is not None
    assert registry["github-issues"].sync.poll is not None
    assert registry["compute-env-local"].sync is not None
    assert registry["compute-env-local"].sync.watch is None
    assert registry["compute-env-local"].sync.scope is None
    for source in (
        "git-files",
        "openspec",
        "pact",
        "agent-sessions",
        "codex-sessions",
        "test-reports",
        "junit-xml",
        "security-analysis",
        "agent-runtime-security-monitor",
        "profile-measurements",
        "self-play-reports",
        "agent-memory",
    ):
        sync = registry[source].sync
        assert sync is not None
        assert sync.scope is not None
        assert "path" in sync.scope.accepts
        assert sync.default_event_mode == "scope_incomplete"
        assert sync.deletes_require_reconcile is True
        assert sync.compaction is not None
        if source in {"git-files", "openspec"}:
            assert sync.poll is not None
            assert "poll" in sync.triggers
    doc_sync = registry["doc-corpus"].sync
    assert doc_sync is not None
    assert doc_sync.scope is not None
    assert "path" in doc_sync.scope.accepts
    assert doc_sync.default_event_mode == "scope_incomplete"
    assert doc_sync.deletes_require_reconcile is True
    assert doc_sync.compaction is not None
    code_sync = registry["code-structure"].sync
    assert code_sync is not None
    assert code_sync.scope is not None
    assert "path" in code_sync.scope.accepts
    # Source-sync performance hardening made code-structure watch events
    # scope-incomplete (only the touched file is re-indexed); full-scope
    # deletion semantics still flow through the reconcile path.
    assert code_sync.default_event_mode == "scope_incomplete"
    assert code_sync.reconcile_mode == "scope_complete"
    assert code_sync.deletes_require_reconcile is True
    assert code_sync.compaction is not None
    assert registry["doc-corpus"].sync.poll is None
    config_sync = registry["coordination-config"].sync
    assert config_sync is not None
    assert config_sync.watch is not None
    assert config_sync.watch.include_globs == ("coordination.yaml",)
    assert config_sync.poll is not None
    assert config_sync.default_event_mode == "scope_complete"
    assert config_sync.reconcile_mode == "scope_complete"
    assert config_sync.dependents.on_event == "closure"
    assert config_sync.dependents.on_reconcile == "closure"
    native_sync = registry["coordination-native"].sync
    assert native_sync is not None
    assert native_sync.watch is None
    assert native_sync.poll is not None
    assert native_sync.dependents.on_event == "none"
    assert native_sync.dependents.on_reconcile == "none"
    doc_mention_targets = set(
        registry["doc-corpus"].raw["params"]["mention_target_subtypes"],
    )
    doc_policy = registry["doc-corpus"].raw["admission_policy"]
    doc_mention_edge_targets = {
        edge["dst_subtype"]
        for edge in doc_policy["output_signature"]["edges"]
        if edge["src_subtype"] == "entity_mention"
        and edge["edge_type"] == "mentions"
    }
    doc_mention_clause_targets = {
        clause["triple"][2]
        for clause in doc_policy["policy_clauses"]
        if clause["record_kind"] == "edge"
        and clause["triple"][:2] == [
            "entity_mention",
            "mentions",
        ]
    }
    assert doc_mention_edge_targets >= doc_mention_targets
    assert doc_mention_clause_targets >= doc_mention_targets
    assert doc_mention_targets >= {
        "blocker_event",
        "pact_change",
    }
    assert {
        (edge["src_subtype"], edge["edge_type"], edge["dst_subtype"])
        for edge in doc_policy["output_signature"]["edges"]
    } >= {
        ("entity_mention", "mentions", "open_spec_change"),
        ("entity_mention", "mentions", "pact_change"),
        ("entity_mention", "mentions", "agent_assignment"),
        ("entity_mention", "mentions", "code_symbol"),
        ("entity_mention", "mentions", "blocker_event"),
    }
    assert {
        tuple(clause["triple"])
        for clause in doc_policy["policy_clauses"]
        if clause["record_kind"] == "edge"
    } >= {
        ("entity_mention", "mentions", "open_spec_change"),
        ("entity_mention", "mentions", "pact_change"),
        ("entity_mention", "mentions", "agent_assignment"),
        ("entity_mention", "mentions", "code_symbol"),
        ("entity_mention", "mentions", "blocker_event"),
    }
    assert registry["git-files"].sync.dependents.on_event == "none"
    assert registry["openspec"].sync.dependents.on_event == "direct"
    pact_policy = registry["pact"].raw["admission_policy"]
    pact_nodes = pact_policy["output_signature"]["nodes"]
    pact_edges = pact_policy["output_signature"]["edges"]
    pact_clauses = pact_policy["policy_clauses"]
    assert {
        (node["subtype"], node["id_prefix"])
        for node in pact_nodes
    } >= {("pact_change", "pact-change:")}
    assert {
        (edge["src_subtype"], edge["edge_type"], edge["dst_subtype"])
        for edge in pact_edges
    } >= {
        ("pact_change", "contains", "spec_delta"),
        ("pact_change", "contains", "task_group"),
        ("acceptance_criterion", "contains", "verification_step"),
        ("task_group", "contains", "pact_task"),
        ("pact_task", "contains", "verification_step"),
        ("pact_task", "blocks", "pact_task"),
        ("pact_task", "requires", "pact_task"),
        ("pact_task", "enables", "pact_task"),
        ("pact_task", "supersedes", "pact_task"),
        ("pact_task", "conflicts_with", "pact_task"),
    }
    assert {
        tuple(clause["triple"])
        for clause in pact_clauses
        if clause["record_kind"] == "edge"
    } >= {
        ("pact_change", "contains", "spec_delta"),
        ("pact_change", "contains", "task_group"),
        ("acceptance_criterion", "contains", "verification_step"),
        ("task_group", "contains", "pact_task"),
        ("pact_task", "contains", "verification_step"),
        ("pact_task", "blocks", "pact_task"),
        ("pact_task", "requires", "pact_task"),
        ("pact_task", "enables", "pact_task"),
        ("pact_task", "supersedes", "pact_task"),
        ("pact_task", "conflicts_with", "pact_task"),
    }
    assert registry["interfaces"].sync.scope is not None
    assert registry["interfaces"].sync.dependents.on_event == "none"
    assert registry["code-data-flow"].sync.watch is None
    assert registry["agent-sessions"].sync.dependents.on_event == "none"
    assert registry["agent-sessions"].raw["params"]["cwd_globs"]
    assert registry["agent-sessions"].raw["params"]["since"] == "7d"
    assert registry["codex-sessions"].sync.dependents.on_event == "none"
    assert registry["codex-sessions"].raw["params"]["cwd_globs"]
    assert registry["codex-sessions"].raw["params"]["since"] == "7d"
    assert registry["codex-sessions"].raw["params"]["max_transcript_bytes"] == 150000000
    assert registry["doc-corpus"].sync.dependents.on_event == "none"
    for source in (
        "test-reports",
        "junit-xml",
        "security-analysis",
        "profile-measurements",
        "self-play-reports",
        "agent-memory",
    ):
        assert registry[source].sync.dependents.on_event == "direct"


def test_transcript_watch_events_are_scoped_and_compacted() -> None:
    registry = load_source_registry(
        REPO_ROOT / "deployments/okg-workspace/source_registry.yaml",
    )
    for source in (
        "agent-sessions",
        "codex-sessions",
    ):
        sync = registry[source].sync
        assert sync is not None
        # Source-sync performance hardening lets high-churn transcript
        # sources subscribe to fs_event, but only when the watch event is
        # scoped to the touched transcript (no dependent fanout) and heavily
        # compacted; the poll path stays the steady-state driver.
        assert "poll" in set(sync.triggers)
        if "fs_event" in set(sync.triggers):
            assert sync.default_event_mode == "scope_incomplete"
        assert sync.dependents.on_event == "none"
        assert sync.compaction is not None
        assert sync.compaction.window_seconds >= 300
        assert sync.poll is not None
        assert sync.poll.interval_seconds >= 300

        plan = plan_ingest_sources(
            registry,
            include=[source],
            include_predecessors=True,
            include_dependents=True,
            dependent_fanout=sync.dependents.on_event,
        )

        assert (
            registry[source].raw["predecessor_source"]
            == "okg-workspace.git-files"
        )
        assert plan.ordered == ("git-files", source)


@pytest.mark.skip(
    reason="frozen: compute-env-remote is quarantined out of okg-workspace's "
    "source registry (2026-08-24) -- admission rejected every run with "
    "unclassified_empty and it contributed zero data. These assertions are "
    "kept, not deleted, so the contract survives for whoever re-enables it. "
    "Unfreeze when the entry is uncommented in "
    "deployments/okg-workspace/source_registry.yaml."
)
def test_quarantined_compute_env_remote_stays_scoped_and_compacted() -> None:
    """Split out of test_transcript_watch_events_are_scoped_and_compacted so
    quarantining compute-env-remote leaves the two live transcript sources
    (agent-sessions, codex-sessions) covered by that test."""
    registry = load_source_registry(
        REPO_ROOT / "deployments/okg-workspace/source_registry.yaml",
    )
    sync = registry["compute-env-remote"].sync
    assert sync is not None
    assert "poll" in set(sync.triggers)
    if "fs_event" in set(sync.triggers):
        assert sync.default_event_mode == "scope_incomplete"
    assert sync.dependents.on_event == "none"
    assert sync.compaction is not None
    assert sync.compaction.window_seconds >= 300
    assert sync.poll is not None
    assert sync.poll.interval_seconds >= 300

    plan = plan_ingest_sources(
        registry,
        include=["compute-env-remote"],
        include_predecessors=True,
        include_dependents=True,
        dependent_fanout=sync.dependents.on_event,
    )
    assert plan.ordered == ("compute-env-remote",)


def test_git_file_watch_events_do_not_duplicate_downstream_watchers() -> None:
    registry = load_source_registry(
        REPO_ROOT / "deployments/okg-workspace/source_registry.yaml",
    )
    sync = registry["git-files"].sync

    assert sync is not None
    assert sync.dependents.on_event == "none"

    plan = plan_ingest_sources(
        registry,
        include=["git-files"],
        include_predecessors=True,
        include_dependents=True,
        dependent_fanout=sync.dependents.on_event,
    )

    assert plan.ordered == ("git-files",)


def test_okg_workspace_template_declares_sync_for_built_in_sources() -> None:
    registry = load_source_registry(
        REPO_ROOT
        / "src/okg/substrate/library/templates/codebase-index/source_registry.yaml",
    )
    expected = {
        "git-files",
        "code-structure",
        "package-manifests",
        "interfaces",
        "code-data-flow",
        "semgrep-data-flow",
        "codeql-data-flow",
        "test-reports",
        "security-analysis",
        "profile-measurements",
        "codex-sessions",
        "openspec",
        "git-history",
        "compute-env-local",
    }

    missing = sorted(
        source for source in expected
        if registry[source].sync is None
    )
    assert not missing

    for source in (
        "codex-sessions",
        "test-reports",
        "security-analysis",
        "profile-measurements",
    ):
        sync = registry[source].sync
        assert sync is not None
        assert sync.scope is not None
        assert "path" in sync.scope.accepts
        assert sync.default_event_mode == "scope_incomplete"
        assert sync.deletes_require_reconcile is True


def test_source_sync_implementation_has_no_deployment_name_policy() -> None:
    generic_files = [
        REPO_ROOT / "src/okg/substrate/source_sync.py",
        REPO_ROOT / "src/okg/substrate/sources/registry.py",
        REPO_ROOT / "src/okg/substrate/watch/watcher.py",
        REPO_ROOT / "src/okg/substrate/ingest/service.py",
        # Runtime execution and source-sync workflow registration are DBOS-only.
        REPO_ROOT / "src/okg/substrate/runtime/worker.py",
        REPO_ROOT / "src/okg/substrate/runtime/source_sync_workflow.py",
        REPO_ROOT / "src/okg/substrate/source_sync_diagnostics.py",
        REPO_ROOT / "src/okg/substrate/source_sync_enqueue.py",
        REPO_ROOT / "src/okg/substrate/source_sync_producers.py",
    ]
    forbidden = ("okg-workspace", "af_pubs", "cms", "memex", "intersystems")

    for path in generic_files:
        text = path.read_text()
        for deployment_name in forbidden:
            assert deployment_name not in text, (
                f"{path} contains deployment-specific source sync policy: "
                f"{deployment_name}"
            )
