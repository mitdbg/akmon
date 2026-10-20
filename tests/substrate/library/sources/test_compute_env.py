"""Substrate-shape parity tests for compute_env adapters (Phase 5
of `migrate-okg-self-to-substrate`).

Two adapters covered:

  - LocalEnvSource — probes the local machine. Tested with a
    custom hostname_override + a fixture settings.json to keep
    the suite hermetic across CI / dev laptops.
  - RemoteHostsFromSessionsSource — mines ssh user@host patterns
    from the okg_self_micro fixture transcripts. The fixture's
    bash tool calls hit `staging.example.com` and
    `prod.example.com:2222`; both should surface as host nodes.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


# ---------------------------------------------------------------------------
# LocalEnvSource
# ---------------------------------------------------------------------------


@pytest.fixture
def settings_path(tmp_path):
    """Settings file with two MCP servers — one stdio, one http."""
    p = tmp_path / "settings.json"
    p.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "alpha": {"command": "node", "args": ["alpha.js"]},
                    "beta": {"url": "http://localhost:9000/mcp"},
                },
            }
        )
    )
    return p


def test_local_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    src = LocalEnvSource(repo="okg")
    assert src.name == "compute_env_local"
    assert src.profile == "reference_catalog"


def test_local_emits_machine_runtime_mcp(settings_path) -> None:
    """Every run emits one machine node + one node per detected
    runtime + one node per MCP server. The substrate hostname
    override pins the machine id deterministically."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    src = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    run = src.run("run-1", mode="cursor")
    facts = list(run.facts)
    nodes_by_subtype: dict[str, list[NodeFact]] = {}
    for f in facts:
        if isinstance(f, NodeFact):
            nodes_by_subtype.setdefault(f.subtype, []).append(f)

    # One Machine node, pinned to the override.
    machines = nodes_by_subtype.get("machine") or []
    assert len(machines) == 1
    assert machines[0].node_id == "machine:testhost"

    # MCP servers from the fixture settings (alpha + beta).
    mcp_ids = {n.node_id for n in nodes_by_subtype.get("mcp_server") or []}
    assert "mcp:testhost:alpha" in mcp_ids
    assert "mcp:testhost:beta" in mcp_ids

    # Runtimes are environment-dependent; we only assert ≥1 (most
    # dev/CI hosts have python3 + git at minimum).
    assert len(nodes_by_subtype.get("runtime") or []) >= 1


def test_local_env_semantic_revisions_ignore_ingestion_run_id(
    settings_path,
) -> None:
    from okg.substrate.library.sources.compute_env_local import LocalEnvSource

    source = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    first_run = source.run("acquisition-a", mode="scope_complete")
    first = list(first_run.facts)
    second_run = source.run("acquisition-b", mode="scope_complete")
    second = list(second_run.facts)

    assert second == first
    assert all("run_id" not in fact.source_revision for fact in first)
    assert first_run.applied_token_scope == "global"
    assert first_run.applied_token_authority == "exact"
    assert first_run.applied_raw_token == source.change_probe.build_token()


def test_local_node_set_matches_domain(settings_path) -> None:
    """Graph-shape parity against the domain domain class on the
    same fixture settings + hostname override."""
    from okg.substrate.library.sources._compute_env_local_emit import (
        LocalEnvSource as DomainLocalEnvSource,
    )
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    domain = DomainLocalEnvSource(
        settings_path=settings_path,
        hostname_override="testhost",
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id for f in run.facts if hasattr(f, "node_id") and f.op == "I"
    }
    assert substrate_node_ids == domain_node_ids


def test_local_next_cursor_carries_probe_hash(settings_path) -> None:
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    src = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    run = src.run("run-1", mode="cursor")
    list(run.facts)
    cursor = run.next_cursor or {}
    assert "probe_hash" in cursor
    assert isinstance(cursor["probe_hash"], str)
    assert len(cursor["probe_hash"]) == 64  # sha256 hex


def test_local_completed_scope_modes_advance_freshness(settings_path) -> None:
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    src = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )

    assert src.run("cursor", mode="cursor").completed_scope is False
    for mode in ("release_new", "scope_complete", "reconcile"):
        run = src.run(mode, mode=mode)
        assert run.completed_scope is True
        assert run.record_set is not None


def test_local_second_run_with_unchanged_probe_short_circuits(
    settings_path,
) -> None:
    """Second cursor-mode run with the same probe hash emits zero
    NodeFacts."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    src = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = LocalEnvSource(
        repo="okg",
        settings_path=settings_path,
        hostname_override="testhost",
    )
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    new_nodes = [f for f in second.facts if isinstance(f, NodeFact)]
    assert new_nodes == []


@pytest.mark.postgres
def test_local_record_set_retracts_removed_mcp_server(
    clean_runtime: str,
    settings_path,
) -> None:
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.compute_env_local import (
        LocalEnvSource,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    source_name = "test.compute-env-local-record-set"
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        LocalEnvSource(
            repo="okg",
            settings_path=settings_path,
            hostname_override="testhost",
        ),
        mode="release_new",
    )
    assert first.next_cursor is not None
    assert "record_set" in first.next_cursor

    settings_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "alpha": {"command": "node", "args": ["alpha.js"]},
                },
            }
        )
    )
    second = runner.run(
        LocalEnvSource(
            repo="okg",
            settings_path=settings_path,
            hostname_override="testhost",
        ),
        mode="release_new",
    )
    assert second.retractions_node == 1
    assert second.retractions_edge == 1

    assert _node_ids(
        clean_runtime,
        source=source_name,
        subtype="mcp_server",
    ) == {"mcp:testhost:alpha"}


@pytest.mark.postgres
def test_local_env_inline_sync_publishes_compute_records_and_watermark(
    clean_runtime: str,
    settings_path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )
    from okg.substrate.cli.main import main

    deployments_root = tmp_path / "deployments"
    dep = deployments_root / "compute-local-fixture"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "compute-local-fixture",
                "description": "compute env local poll fixture",
                # typed-manifest migration (77139c4fb) hard-requires
                # both declared paths; the fixture creates them below.
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "modules": ["git_graph", "openspec", "agent_sessions", "compute_env"],
                # The fixture lives outside any Git repository, so it
                # declares itself standalone; otherwise the publish refuses
                # because release authority cannot be resolved (#2127).
                "release": {"channel": "dev", "kind": "standalone"},
                "nomos": {
                    "deployment_posture": {
                        "deployment_class": "dogfood_workspace",
                        "read_principals": ["human_operator", "agent"],
                        "write_principals": ["human_operator", "source_adapter"],
                        "mcp_write_default": "review",
                        "export_policy": "internal_only",
                        "secret_handling": "secret_ref_required",
                        "live_call_default": "review",
                        "allowed_source_sensitivities": ["local_private"],
                        "allowed_data_classifications": ["private"],
                        "allowed_exportability": ["no_export"],
                        "privacy_obligations": ["audit"],
                        "readiness_profiles": ["dogfood"],
                    }
                },
            }
        )
    )
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "compute-env-local": {
                        "module": "okg.substrate.library.sources.compute_env_local",
                        "class": "LocalEnvSource",
                        "ownership_id": "fixture.compute-env-local",
                        "params": {
                            "repo": "okg",
                            "settings_path": str(settings_path),
                            "hostname_override": "testhost",
                        },
                        "source_class": "reference_catalog",
                        "record_identity_kind": "domain_key",
                        "record_identity_fields": ["repo", "node_id"],
                        "source_revision_kind": "content_hash",
                        "deletion_semantics": "missing_from_completed_scope",
                        "publication_mode": "published_generation",
                        "source_policy": {
                            "sensitivity": "local_private",
                            "data_classification": "private",
                            "credential_ref_policy": "none",
                            "live_call_allowed": False,
                            "exportability": "no_export",
                            "retention": "normal",
                            "provenance": "local_filesystem",
                            "privacy_obligations": ["audit"],
                            "store_raw": True,
                        },
                        "sync": {
                            "triggers": ["poll", "manual", "reconcile"],
                            "default_event_mode": "release_new",
                            "reconcile_mode": "release_new",
                            "deletes_require_reconcile": True,
                            "cheap_probe": "ttl",
                        },
                    }
                }
            }
        )
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    composed = compose_catalog(dep)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=dep,
        command="test compute-env inline sync",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        apply_composed_catalog(conn, composed, identity=identity)

    ingest_rc = main(
        [
            "--json",
            "ingest",
            "--deployment",
            "compute-local-fixture",
            "--dsn",
            clean_runtime,
            "--include",
            "compute-env-local",
            "--no-predecessors",
            "--inline",
            "--mode",
            "release_new",
        ]
    )
    assert ingest_rc == 0
    _ = capsys.readouterr()

    second_ingest_rc = main(
        [
            "--json",
            "ingest",
            "--deployment",
            "compute-local-fixture",
            "--dsn",
            clean_runtime,
            "--include",
            "compute-env-local",
            "--no-predecessors",
            "--inline",
            "--mode",
            "release_new",
        ]
    )
    assert second_ingest_rc == 0
    _ = capsys.readouterr()

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        latest = store.latest_generation(deployment_name="compute-local-fixture")
        assert latest is not None
        assert latest.metadata["status"] == "published"
        graph = store.checkout_generation(latest.checkpoint_id)
        counts = {
            str(row["subtype"]): int(row["count"])
            for row in graph.query(
                """
                SELECT subtype, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype IN ('machine', 'runtime', 'mcp_server')
                 GROUP BY subtype
                """
            )
        }
    finally:
        store.close()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source, last_reconcile_at
              FROM okg.source_watermarks
             ORDER BY source
            """
        )
        watermarks = {
            str(source): last_reconcile_at
            for source, last_reconcile_at in cur.fetchall()
        }
        from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

        dlq_count = int(summarize_runtime_queue_health(cur)["dlq_count"])

    assert counts["machine"] == 1
    assert counts["runtime"] >= 1
    assert counts["mcp_server"] == 2
    assert watermarks["fixture.compute-env-local"] is not None
    assert dlq_count == 0


# ---------------------------------------------------------------------------
# RemoteHostsFromSessionsSource
# ---------------------------------------------------------------------------


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def _projects_root(fixture_paths) -> Path:
    return fixture_paths.sessions_root.parent


def test_remote_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=Path("/tmp"),
    )
    assert src.name == "compute_env_remote_hosts"
    assert src.profile == "reference_catalog"


def test_remote_emits_host_nodes_from_fixture(fixture_paths) -> None:
    """The fixture transcripts run ssh to two distinct hosts."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    run = src.run("run-1", mode="cursor")
    host_ids = {
        f.node_id
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "remote_host"
    }
    assert "host:staging.example.com" in host_ids
    assert "host:prod.example.com" in host_ids


def test_remote_emits_session_to_host_references(fixture_paths) -> None:
    """alpha-001 references staging.example.com; beta-001
    references prod.example.com (with a non-default port)."""
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    run = src.run("run-1", mode="cursor")
    ref_keys = {
        (f.src, f.dst, f.edge_type)
        for f in run.facts
        if isinstance(f, EdgeFact) and f.edge_type == "references"
    }
    assert ("session:alpha-001", "host:staging.example.com", "references") in ref_keys
    assert ("session:beta-001", "host:prod.example.com", "references") in ref_keys


def test_remote_scoped_run_scans_only_target_transcript(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
    )

    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                alpha.relative_to(projects_root).as_posix(),
                "../escape.jsonl",
            ],
        },
    )
    host_ids = {
        fact.node_id
        for fact in run.facts
        if isinstance(fact, NodeFact) and fact.subtype == "remote_host"
    }

    assert run.completed_scope is False
    assert host_ids == {"host:staging.example.com"}


@pytest.mark.postgres
def test_remote_scoped_sync_publishes_target_transcript_hosts(
    clean_runtime: str,
    fixture_paths,
) -> None:
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )
    from okg.substrate.publisher import Publisher

    dep = fixture_paths.sessions_root.parent / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "test-compute-env-remote",
                "description": "compute env remote scoped fixture",
                "modules": ["git_graph", "openspec", "agent_sessions", "compute_env"],
            }
        )
    )
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    projects_root = _projects_root(fixture_paths)

    def _source() -> RemoteHostsFromSessionsSource:
        return RemoteHostsFromSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
        )

    runner = SourceRunner(clean_runtime, source_name="fixture.compute-env-remote")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            ("fixture.compute-env-remote",),
        )
        last_reconcile_at = cur.fetchone()[0]
    assert last_reconcile_at is not None
    first_publish = Publisher(clean_runtime, "test-compute-env-remote", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-compute-env-remote")).cycle()
    assert first_publish["status"] == "published", first_publish

    beta = fixture_paths.sessions_root / "beta-001.jsonl"
    with beta.open("a", encoding="utf-8") as fh:
        fh.write(
            "\n"
            + json.dumps(
                {
                    "type": "assistant",
                    "sessionId": "beta-001",
                    "timestamp": "2025-02-01T10:02:30Z",
                    "cwd": "/repo/okg_self_micro_repo",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "name": "Bash",
                                "input": {
                                    "command": (
                                        "ssh deploy@canary.example.com 'echo hi'"
                                    ),
                                },
                            }
                        ],
                    },
                }
            )
        )
    scoped = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [beta.relative_to(projects_root).as_posix()]},
    )
    assert scoped.completed_scope is False
    second_publish = Publisher(clean_runtime, "test-compute-env-remote", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-compute-env-remote")).cycle()
    assert second_publish["status"] == "published", second_publish

    scoped_node_ids = _node_ids_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="fixture.compute-env-remote",
        subtype="remote_host",
    )
    assert "host:canary.example.com" in scoped_node_ids
    assert "host:staging.example.com" not in scoped_node_ids
    assert (
        _latest_generation_status_for_deployment(
            clean_runtime,
            "test-compute-env-remote",
        )
        == "published"
    )
    assert _live_node_count(clean_runtime, "remote_host") >= 3
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0


def test_remote_node_set_matches_domain(fixture_paths) -> None:
    """Graph-shape parity against the domain emitter on the same fixture."""
    from okg.substrate.library.sources._compute_env_remote_hosts_emit import (
        RemoteHostsFromSessions as DomainRemoteHostsFromSessions,
    )
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    domain = DomainRemoteHostsFromSessions(
        claude_projects_root=_projects_root(fixture_paths),
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id for f in run.facts if hasattr(f, "node_id") and f.op == "I"
    }
    assert substrate_node_ids == domain_node_ids


def test_remote_edge_set_matches_domain(fixture_paths) -> None:
    from okg.substrate.library.sources._compute_env_remote_hosts_emit import (
        RemoteHostsFromSessions as DomainRemoteHostsFromSessions,
    )
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    domain = DomainRemoteHostsFromSessions(
        claude_projects_root=_projects_root(fixture_paths),
    )
    list(domain.nodes())
    domain_edge_keys = {(e.src, e.dst, e.edge_type) for e in domain.edges()}

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    run = src.run("run-1", mode="cursor")
    substrate_edge_keys = {
        (f.src, f.dst, f.edge_type) for f in run.facts if hasattr(f, "src")
    }
    assert substrate_edge_keys == domain_edge_keys


def test_remote_next_cursor_carries_per_session_state(fixture_paths) -> None:
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    run = src.run("run-1", mode="cursor")
    list(run.facts)
    cursor = run.next_cursor or {}
    assert "per_session" in cursor
    per_session = cursor["per_session"]
    assert "alpha-001" in per_session
    assert "beta-001" in per_session


def test_remote_second_run_skips_emission_when_unchanged(fixture_paths) -> None:
    """Once all per-session sizes are stable, the domain emitter
    short-circuits emission entirely. The substrate adapter must
    surface that same behavior."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    new_nodes = [f for f in second.facts if isinstance(f, NodeFact)]
    assert new_nodes == []


def test_remote_scope_incomplete_invalidates_size_only_cursor(
    fixture_paths,
) -> None:
    """An explicit path scope must re-read instead of trusting cached size."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    projects_root = _projects_root(fixture_paths)
    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
    )
    second = src2.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
        sync_scope={"scope_keys": [alpha.relative_to(projects_root).as_posix()]},
    )
    new_nodes = [f for f in second.facts if isinstance(f, NodeFact)]

    assert second.completed_scope is False
    assert {node.node_id for node in new_nodes} == {
        "host:staging.example.com",
    }


def test_remote_completed_scope_modes_return_complete_record_set(
    fixture_paths,
) -> None:
    """A global completed walk owns the full transcript-derived footprint."""
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )

    src = RemoteHostsFromSessionsSource(
        repo="okg",
        claude_projects_root=_projects_root(fixture_paths),
    )
    assert src.run("rc", mode="cursor").completed_scope is False
    assert src.run("si", mode="scope_incomplete").completed_scope is False
    reconcile = src.run("rr", mode="reconcile")
    assert reconcile.completed_scope is True
    assert reconcile.record_set is not None
    assert reconcile.record_set.records
    complete = src.run("sc", mode="scope_complete")
    assert complete.completed_scope is True
    assert complete.record_set is not None
    assert complete.record_set.records
    release = src.run("rn", mode="release_new", authoritative_replay=True)
    assert release.completed_scope is True
    assert release.record_set is not None
    assert release.record_set.records


# ---------------------------------------------------------------------------
# SSH-pattern regex contract (locks behavior so any future weakening fails)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "command,expected",
    [
        ("ssh user@host", ("user", "host")),
        ("ssh user@login.example.org", ("user", "login.example.org")),
        ("ssh user@login.example.org 'cd /eos/cms'", ("user", "login.example.org")),
        ('ssh user@host "long command"', ("user", "host")),
        ("ssh -A user@login.example.org", ("user", "login.example.org")),
        (
            "ssh -i key.pem user@login.example.org 'cmd'",
            ("user", "login.example.org"),
        ),
        ("ssh -p 2222 user@host -- run-thing", ("user", "host")),
        ("ssh -o StrictHostKeyChecking=no user@host", ("user", "host")),
    ],
)
def test_ssh_pattern_matches_realistic_commands(command, expected):
    from okg.substrate.library.sources._agent_sessions_extract import (
        SSH_PATTERN,
    )

    m = SSH_PATTERN.search(command)
    assert m is not None, f"expected match in {command!r}"
    assert m.groups() == expected


@pytest.mark.parametrize(
    "non_match",
    [
        "Use ssh keys with user@example.com to authenticate.",
        "ssh-keygen -t rsa",
        "git clone git@github.com:foo/bar.git",
    ],
)
def test_ssh_pattern_does_not_overmatch(non_match):
    from okg.substrate.library.sources._agent_sessions_extract import (
        SSH_PATTERN,
    )

    assert SSH_PATTERN.search(non_match) is None, f"unexpected match in {non_match!r}"


def _node_ids_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
) -> set[str]:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE run_id = :run_id
               AND source = :source
               AND subtype = :subtype
            """,
            {"run_id": run_id, "source": source, "subtype": subtype},
        )
    return {str(row["node_id"]) for row in rows}


def _node_ids(
    dsn: str,
    *,
    source: str,
    subtype: str,
) -> set[str]:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
               AND subtype = :subtype
            """,
            {"source": source, "subtype": subtype},
        )
    return {str(row["node_id"]) for row in rows}


def _latest_generation_status_for_deployment(
    dsn: str,
    deployment_name: str,
) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.latest_generation(deployment_name=deployment_name)
        assert latest is not None
        return str(latest.metadata["status"])
    finally:
        store.close()


def _live_node_count(dsn: str, subtype: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout()
        return int(
            graph.query(
                """
            SELECT count(*) AS count
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
                {"subtype": subtype},
            )[0]["count"]
        )
    finally:
        store.close()


def _queued_jobs_by_kind(dsn: str) -> dict[str, int]:
    import psycopg
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return {
        str(kind): int(count)
        for kind, count in summary["queue_depth"].items()
        if int(count)
    }


def _dlq_count(dsn: str) -> int:
    import psycopg
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return int(summary["dlq_count"])
