from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.library.sources.agent_memory import AgentMemorySource
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def test_agent_memory_source_emits_claims_and_evidence_edges(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    packet_dir = repo / ".artifacts" / "agent_memory"
    packet_dir.mkdir(parents=True)
    packet = {
        "format": "okg-agent-memory/v1",
        "observed_at": "2026-05-03T12:00:00Z",
        "extraction_method": "fixture",
        "confidence_kind": "deterministic",
        "scope": "demo",
        "observations": [
            {
                "observation_id": "observation:demo:evidence",
                "summary": "The source file exists.",
                "observation_kind": "graph_query_result",
                "status": "supported",
                "reference_node_ids": ["file:demo:src/app.py"],
            },
        ],
        "verification_results": [
            {
                "verification_id": "verification:demo:claim",
                "verdict": "verified",
                "summary": "The claim has deterministic evidence.",
                "status": "supported",
                "target_node_ids": ["claim:demo:source-file"],
            },
        ],
        "claims": [
            {
                "claim_id": "claim:demo:source-file",
                "statement": "src/app.py is represented in the graph.",
                "claim_kind": "implementation_fact",
                "subject_id": "file:demo:src/app.py",
                "status": "supported",
                "evidence_node_ids": [
                    "observation:demo:evidence",
                    "verification:demo:claim",
                ],
            },
        ],
    }
    (packet_dir / "memory.jsonl").write_text(json.dumps(packet), encoding="utf-8")

    source = AgentMemorySource(
        repo="demo",
        repo_root=repo,
        packet_globs=[".artifacts/agent_memory/*.jsonl"],
    )
    run = source.run("source-run-1", mode="scope_complete")
    facts = list(run.facts)

    assert source.validation_warnings == []
    assert run.record_set is not None
    assert set(run.record_set.records) == {
        ".artifacts/agent_memory/memory.jsonl",
    }
    record = run.record_set.records[".artifacts/agent_memory/memory.jsonl"]
    assert {
        node.node_id for node in record.descendants
    } == {
        "claim:demo:source-file",
        "observation:demo:evidence",
        "verification:demo:claim",
    }
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    assert {f.subtype for f in nodes} == {
        "agent_claim",
        "agent_observation",
        "verification_result",
    }
    claim = next(f for f in nodes if f.subtype == "agent_claim")
    assert claim.attrs["source_artifact"] == ".artifacts/agent_memory/memory.jsonl"
    assert len(claim.attrs["source_hash"]) == 64
    assert claim.attrs["confidence_kind"] == "deterministic"
    assert (
        "claim:demo:source-file",
        "references",
        "file:demo:src/app.py",
    ) in {(e.src, e.edge_type, e.dst) for e in edges}
    assert (
        "claim:demo:source-file",
        "supported_by",
        "observation:demo:evidence",
    ) in {(e.src, e.edge_type, e.dst) for e in edges}
    assert (
        "verification:demo:claim",
        "verifies",
        "claim:demo:source-file",
    ) in {(e.src, e.edge_type, e.dst) for e in edges}


def test_agent_memory_source_warns_and_does_not_fabricate_unknown_evidence(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    packet_dir = repo / ".artifacts" / "agent_memory"
    packet_dir.mkdir(parents=True)
    packet = {
        "format": "okg-agent-memory/v1",
        "observed_at": "2026-05-03T12:00:00Z",
        "extraction_method": "fixture",
        "confidence_kind": "model_inferred",
        "scope": "demo",
        "claims": [
            {
                "claim_id": "claim:demo:bad-evidence",
                "statement": "This should not get fabricated evidence.",
                "claim_kind": "implementation_fact",
                "subject_id": "file:demo:src/app.py",
                "status": "supported",
                "evidence_node_ids": ["nonsense:missing"],
            },
        ],
    }
    (packet_dir / "memory.jsonl").write_text(json.dumps(packet), encoding="utf-8")

    source = AgentMemorySource(
        repo="demo",
        repo_root=repo,
        packet_globs=[".artifacts/agent_memory/*.jsonl"],
    )
    facts = list(source.run("source-run-1", mode="scope_complete").facts)

    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    claim = next(f for f in nodes if f.subtype == "agent_claim")
    assert any("unrecognized_node_id:nonsense:missing" in w for w in source.validation_warnings)
    assert "validation_warnings" in claim.attrs
    assert all(e.dst != "nonsense:missing" for e in edges)
    assert not any(e.edge_type == "supported_by" for e in edges)


def test_agent_memory_source_scopes_to_configured_packet(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    packet_a = _write_memory_packet(
        repo / ".artifacts" / "agent_memory" / "a.jsonl",
        claim_id="claim:demo:a",
        statement="Packet A stays untouched.",
    )
    packet_b = _write_memory_packet(
        repo / ".artifacts" / "agent_memory" / "b.jsonl",
        claim_id="claim:demo:b",
        statement="Packet B changed.",
        subject_id="file:demo:src/app.py",
    )
    unconfigured = _write_memory_packet(
        repo / "other" / "memory.jsonl",
        claim_id="claim:demo:other",
        statement="Ignored packet.",
    )
    source = AgentMemorySource(
        repo="demo",
        repo_root=repo,
        packet_globs=[".artifacts/agent_memory/*.jsonl"],
    )

    run = source.run(
        "source-run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                packet_b.relative_to(repo).as_posix(),
                "../escape.jsonl",
            ],
        },
    )
    facts = list(run.facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]

    assert run.completed_scope is False
    assert source.validation_warnings == []
    assert {node.node_id for node in nodes} == {"claim:demo:b"}
    claim = nodes[0]
    assert claim.attrs["source_artifact"] == ".artifacts/agent_memory/b.jsonl"
    assert claim.attrs["text"] == "Packet B changed."
    assert "transcript" not in claim.attrs

    ignored = source.run(
        "source-run-ignored",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [unconfigured.relative_to(repo).as_posix()]},
    )
    assert list(ignored.facts) == []
    assert ignored.completed_scope is False
    assert packet_a.is_file()


@pytest.mark.postgres
def test_agent_memory_source_scoped_sync_publishes_target_packet(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    packet_a = _write_memory_packet(
        repo / ".artifacts" / "agent_memory" / "a.jsonl",
        claim_id="claim:demo:a",
        statement="Packet A baseline.",
    )
    packet_b = _write_memory_packet(
        repo / ".artifacts" / "agent_memory" / "b.jsonl",
        claim_id="claim:demo:b",
        statement="Packet B baseline.",
    )

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-agent-memory",
        "description": "agent-memory sync fixture",
        "modules": ["agent_memory"],
    }))
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    def _source() -> AgentMemorySource:
        return AgentMemorySource(
            repo="demo",
            repo_root=repo,
            packet_globs=[".artifacts/agent_memory/*.jsonl"],
        )

    runner = SourceRunner(clean_runtime, source_name="fixture.agent-memory")
    baseline = runner.run(_source(), mode="scope_complete")
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test-agent-memory", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-agent-memory")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_memory_packet(
        packet_b,
        claim_id="claim:demo:b",
        statement="Packet B scoped update.",
    )
    scoped = runner.run(
        _source(),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [packet_b.relative_to(repo).as_posix()]},
    )
    assert scoped.completed_scope is False
    second_publish = Publisher(clean_runtime, "test-agent-memory", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-agent-memory")).cycle()
    assert second_publish["status"] == "published", second_publish

    assert _node_record_paths_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="fixture.agent-memory",
        subtype="agent_claim",
    ) == [".artifacts/agent_memory/b.jsonl"]
    assert _node_attr_values_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="fixture.agent-memory",
        subtype="agent_claim",
        field="statement",
    ) == ["Packet B scoped update."]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "agent_claim") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0
    assert packet_a.is_file()


def _write_memory_packet(
    path: Path,
    *,
    claim_id: str,
    statement: str,
    subject_id: str | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    packet = {
        "format": "okg-agent-memory/v1",
        "observed_at": "2026-05-03T12:00:00Z",
        "extraction_method": "fixture",
        "confidence_kind": "deterministic",
        "scope": "demo",
        "claims": [
            {
                "claim_id": claim_id,
                "statement": statement,
                "claim_kind": "implementation_fact",
                "status": "hypothesis",
                **({"subject_id": subject_id} if subject_id else {}),
            },
        ],
    }
    path.write_text(json.dumps(packet), encoding="utf-8")
    return path


def _node_record_paths_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT DISTINCT source_record_id->>'path'
              FROM okg.graph_nodes
             WHERE run_id = :run_id
               AND source = :source
               AND subtype = :subtype
             ORDER BY 1
            """,
            {"run_id": run_id, "source": source, "subtype": subtype},
        )
    return [str(next(iter(row.values()))) for row in rows]


def _node_attr_values_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
    subtype: str,
    field: str,
) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT attrs
              FROM okg.graph_nodes
             WHERE run_id = :run_id
               AND source = :source
               AND subtype = :subtype
            """,
            {
                "run_id": run_id,
                "source": source,
                "subtype": subtype,
            },
        )
    return sorted({
        str(dict(row["attrs"]).get(field))
        for row in rows
        if dict(row["attrs"]).get(field) is not None
    })


def _latest_generation_status(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status
              FROM okg.graph_generations
             ORDER BY generation_id DESC
             LIMIT 1
            """,
        )
        return str(cur.fetchone()[0])


def _live_node_count(dsn: str, subtype: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.latest_generation(deployment_name="test-agent-memory")
        assert latest is not None
        generation = store.checkout_generation(latest.checkpoint_id)
        return int(generation.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        )[0]["count"])
    finally:
        store.close()


def _queued_jobs_by_kind(dsn: str) -> dict[str, int]:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return {
        str(kind): int(count)
        for kind, count in summary["queue_depth"].items()
        if int(count)
    }


def _dlq_count(dsn: str) -> int:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return int(summary["dlq_count"])
