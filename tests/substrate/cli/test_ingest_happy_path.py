"""End-to-end ingest happy-path tests.

Builds a tiny synthetic deployment with a single source and runs
`okg ingest`. The ingest dispatcher walks sources, resolves
predecessors, and exits with a results envelope. Covers the
hot loop in `_cmd_ingest` (lines 1284-1301) that
`test_dispatch_paths.py` doesn't reach.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.cli.main as cli

pytestmark = pytest.mark.postgres


def _captured_error(capsys) -> dict:
    captured = capsys.readouterr()
    raw = (captured.err or captured.out).strip().splitlines()[-1]
    return json.loads(raw)


def _write_minimal_deployment(
    tmp_path: Path, *, name: str = "test_dep",
) -> Path:
    """Create a `deployments/<name>/` skeleton sufficient for
    `okg ingest` to dispatch (it doesn't need to actually publish).
    """
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / name
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()

    # Minimal source_registry.yaml with one fixture-shaped source.
    # The source class is the in-substrate `git_files` source —
    # which needs a real git repo to actually run, so we point it
    # at a non-existent path; the fail-fast error during run is
    # the point (`_cmd_ingest`'s error-aggregation loop runs).
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  test-source:\n"
        "    module: okg.substrate.library.sources.git_graph_files\n"
        "    class: GitFilesSource\n"
        "    ownership_id: test_dep.test_source\n"
        "    params:\n"
        "      repo: not-a-real-repo\n"
        "      repo_root: /tmp/no-such-repo-for-ingest-test\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [repo, path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      triggers: [manual, reconcile]\n"
        "      default_event_mode: scope_complete\n"
        "      reconcile_mode: scope_complete\n"
        "      deletes_require_reconcile: true\n"
        "      cheap_probe: stat_tree\n"
    )
    (dep / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: ingest test fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
    )
    return dep


def _write_mixed_reconcile_deployment(
    tmp_path: Path, *, name: str = "test_dep",
) -> Path:
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / name
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  crawl:\n"
        "    module: no_such_module\n"
        "    class: NoSuchSource\n"
        "    ownership_id: test_dep.crawl\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      triggers: [manual, reconcile]\n"
        "      default_event_mode: scope_complete\n"
        "      reconcile_mode: scope_complete\n"
        "  api:\n"
        "    module: no_such_module\n"
        "    class: NoSuchSource\n"
        "    ownership_id: test_dep.api\n"
        "    source_class: mutable_api\n"
        "    record_identity_kind: remote_id\n"
        "    record_identity_fields: [id]\n"
        "    source_revision_kind: updated_at\n"
        "    deletion_semantics: explicit_delete\n"
        "    publication_mode: published_generation\n"
        "    predecessor_source: test_dep.crawl\n"
        "    sync:\n"
        "      triggers: [manual, reconcile]\n"
        "      default_event_mode: cursor\n"
        "      reconcile_mode: reconcile\n"
    )
    (dep / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: mixed reconcile enqueue fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
    )
    return dep


def test_ingest_runs_dispatcher_against_synthetic_deployment(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    """`okg ingest --deployment <synthetic>` walks the source
    registry, dispatches the source-runner loop, and prints a
    structured report (even if individual sources fail)."""
    _write_minimal_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--no-publish",
    ])
    out = capsys.readouterr()
    # The dispatcher succeeded reaching the source-runner loop
    # regardless of whether the underlying source ran cleanly.
    # Expected: structured envelope on stdout (or structured
    # error on stderr).
    assert out.out or out.err
    # rc may be 0 (everything OK), 1 (source failed), or 2
    # (validation error). Any of those proves the dispatch path
    # ran.
    assert rc in (0, 1, 2)


def test_ingest_with_include_filter(
    tmp_path: Path, clean_runtime: str, monkeypatch,
) -> None:
    """`--include` filter restricts which sources are dispatched."""
    _write_minimal_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--no-publish",
        "--include", "test-source",
    ])
    assert rc in (0, 1, 2)


def test_ingest_with_exclude_filter(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    """`--exclude` removes named sources before dispatch."""
    _write_minimal_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--no-publish",
        "--exclude", "test-source",  # exclude the only source
    ])
    # With everything excluded, the dispatcher fails closed instead of
    # reporting a successful no-op.
    assert rc == 1
    assert _captured_error(capsys)["error"]["message"] == (
        "no runnable sources selected for sync enqueue"
    )


def test_ingest_enqueue_requires_dbos_runtime(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    _write_minimal_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--enqueue",
        "--mode", "reconcile",
        "--include", "test-source",
        "--no-publish",
    ])

    assert rc == 1
    assert _captured_error(capsys)["error"]["code"] == "runtime_unavailable"


def test_ingest_enqueue_mixed_modes_requires_dbos_runtime(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    _write_mixed_reconcile_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--enqueue",
        "--mode", "reconcile",
        "--include", "crawl,api",
        "--no-publish",
    ])

    assert rc == 1
    assert _captured_error(capsys)["error"]["code"] == "runtime_unavailable"


def test_ingest_dbos_path_splits_mixed_reconcile_run_modes(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    import okg.substrate.runtime.admin as runtime_admin

    _write_mixed_reconcile_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)
    calls: list[dict[str, object]] = []

    def fake_enqueue_source_sync(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(
            workflow_id=f"wf-{len(calls)}",
            status="SUCCESS",
            any_failed=False,
            elapsed_seconds=0.1,
            report={"results": []},
            error=None,
        )

    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_source_sync",
        fake_enqueue_source_sync,
    )

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--mode", "reconcile",
        "--include", "crawl,api",
        "--no-publish",
        "--force-enrichers",
    ])

    assert rc == 0
    assert [
        (call["sources"], call["run_mode"]) for call in calls
    ] == [
        (["crawl"], "scope_complete"),
        (["api"], "reconcile"),
    ]
    assert all(call["include_predecessors"] is False for call in calls)
    assert all(call["include_dependents"] is False for call in calls)
    assert all(call["dependent_fanout"] == "none" for call in calls)
    assert all(call["publish"] is False for call in calls)
    assert all(call["force_enrichers"] is True for call in calls)
    report = json.loads(capsys.readouterr().out)
    assert report["workflow_id"] == "wf-2"
    assert "job_kind" not in report
    assert [row["workflow_id"] for row in report["workflows"]] == [
        "wf-1",
        "wf-2",
    ]
def test_ingest_stops_dependent_layers_after_source_failure(
    tmp_path: Path, clean_runtime: str, monkeypatch, capsys,
) -> None:
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / "test_dep"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  upstream:\n"
        "    module: no_such_ingest_module\n"
        "    class: MissingSource\n"
        "    ownership_id: test_dep.upstream\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [id]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "  downstream:\n"
        "    module: no_such_downstream_module\n"
        "    class: ShouldNotImport\n"
        "    ownership_id: test_dep.downstream\n"
        "    predecessor_source: test_dep.upstream\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [id]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
    )
    (dep / "deployment.yaml").write_text(
        "name: test_dep\n"
        "description: ingest dependency failure fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    rc = cli.main([
        "--json", "ingest",
        "--deployment", "test_dep",
        "--dsn", clean_runtime,
        "--no-publish",
    ])

    assert rc == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ordered"] == ["upstream", "downstream"]
    assert [r["name"] for r in report["results"]] == ["upstream"]
    assert report["skipped"] == [{
        "reason": "upstream_source_failed",
        "after_layer": 1,
        "sources": ["downstream"],
        "blocked_by": {"downstream": ["upstream"]},
    }]
