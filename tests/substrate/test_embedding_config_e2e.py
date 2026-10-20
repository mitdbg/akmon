"""Disposable publish proof for the fail-closed embedding contract."""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.embeddings import (
    EMBEDDING_DIMENSION,
    EmbeddingConfig,
    EmbeddingConfigDeprecationWarning,
    EmbeddingConfigError,
    build_embedding_execution_plan,
    parse_embedding_config,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.publisher import Publisher
from okg.substrate.runtime.embed_workflow import (
    EMBED_WORKFLOW_NAME,
    _DECORATED,
    _embed_and_write_one,
    _run_one_embed_batch,
    register_embed_workflow,
)
from okg.substrate.runtime_dlq import current_runtime_dlq
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

_DEPLOYMENT = "embedding-config-e2e"
_EXPECTED_NODE_IDS = ["e2e:contract", "e2e:unrelated"]
_QUERY = "canonical embedding configuration contract"


class _FakeEmbedder:
    def __init__(self, *, declared: int, returned: int) -> None:
        self.dimension = declared
        self.returned = returned
        self.model_id = f"fake:{declared}:{returned}"
        self.calls: list[str] = []

    def embed(self, text: str) -> list[float]:
        self.calls.append(text)
        return [0.25] * self.returned


def _graph_counts(dsn: str, *, generation_id: object | None = None) -> dict[str, int]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = (
            store.checkout_generation(generation_id)
            if generation_id is not None
            else store.checkout()
        )
        row = graph.query(
            """
            SELECT count(*)::int AS nodes,
                   count(embedding)::int AS embeddings
              FROM okg.graph_nodes
            """
        )[0]
        return {"nodes": int(row["nodes"]), "embeddings": int(row["embeddings"])}
    finally:
        store.close()


def _write_deployment(root: Path, dsn: str) -> Path:
    deployment = root / _DEPLOYMENT
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": _DEPLOYMENT,
                "description": "Disposable issue #1229 embedding proof",
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "postgres": {"dsn": dsn},
                "embeddings": {"enabled": True, "backend": "hash"},
                "runtime": {"enabled": False},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return deployment


def _doctor_report(dsn: str) -> tuple[int, dict]:
    from okg.substrate.cli.main import main

    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        rc = main(["--json", "doctor", "--dsn", dsn])
    return rc, json.loads(output.getvalue())


def _runtime_health(dsn: str) -> dict:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(sum(active_count), 0)::int,
                   COALESCE(sum(dlq_count), 0)::int
              FROM okg.v_runtime_queue_health
            """
        )
        active, dlq = cur.fetchone()
        current_dlq = current_runtime_dlq(cur)
    return {
        "active_queue_depth": int(active),
        "queue_view_dlq_count": int(dlq),
        "current_dlq_count": int(current_dlq["current_count"]),
        "current_dlq": current_dlq,
    }


def test_disposable_embedding_deployment_publish_proof(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pytestconfig: pytest.Config,
) -> None:
    """Prove admission, preflight, registration, write, publish, and read."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.deployments import resolve_deployment

    _DECORATED.clear()
    before_rejection = _graph_counts(clean_runtime)
    assert before_rejection == {"nodes": 0, "embeddings": 0}

    rejected: dict[str, dict] = {}
    for case, fake in {
        "declared_384": _FakeEmbedder(declared=384, returned=384),
        "declared_2560_returns_384": _FakeEmbedder(
            declared=EMBEDDING_DIMENSION,
            returned=384,
        ),
    }.items():
        with pytest.raises(SubstrateError) as exc:
            build_embedding_execution_plan(
                EmbeddingConfig(enabled=True, backend="hash"),
                builder=lambda _spec, fake=fake: fake,
            )
        assert exc.value.code == "embedder_dim_mismatch"
        assert _DECORATED == {}
        assert _graph_counts(clean_runtime) == before_rejection
        rejected[case] = {
            "code": exc.value.code,
            "probe_calls": len(fake.calls),
            "workflow_registrations": 0,
            "graph_writes": 0,
        }

    with pytest.warns(EmbeddingConfigDeprecationWarning):
        legacy = parse_embedding_config(
            singular={"enabled": True, "backend": "hash"},
        )
    assert legacy == EmbeddingConfig(enabled=True, backend="hash")
    with pytest.raises(EmbeddingConfigError) as conflict:
        parse_embedding_config(
            singular={"enabled": False},
            plural={"enabled": True, "backend": "hash"},
        )

    deployments_root = tmp_path / "deployments"
    deployment_dir = _write_deployment(deployments_root, clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    manifest = resolve_deployment(_DEPLOYMENT)
    assert manifest.deployment_dir == deployment_dir
    assert manifest.embedding_config == EmbeddingConfig(
        enabled=True,
        backend="hash",
    )

    writer_plan = build_embedding_execution_plan(manifest.embedding_config)
    query_plan = build_embedding_execution_plan(manifest.embedding_config)
    assert writer_plan.fingerprint == query_plan.fingerprint
    assert writer_plan.model_id == query_plan.model_id
    assert writer_plan.observed_dimension == EMBEDDING_DIMENSION

    with mock_dbos_context():
        register_embed_workflow(
            deployment=_DEPLOYMENT,
            embedding_plan=writer_plan,
        )
        assert EMBED_WORKFLOW_NAME in _DECORATED

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id="e2e:contract",
                    subtype="document",
                    attrs={"text": _QUERY},
                    source_record_id={"id": "contract"},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id="e2e:unrelated",
                    subtype="document",
                    attrs={"text": "unrelated operational history"},
                    source_record_id={"id": "unrelated"},
                    source_revision={"v": 1},
                ),
            ],
            source="test.embedding_config_e2e",
        )

    embedded = _run_one_embed_batch(
        deployment=_DEPLOYMENT,
        batch_size=10,
        embed_one=lambda node_id, text, text_hash, _fingerprint: _embed_and_write_one(
            dsn=clean_runtime,
            node_id=node_id,
            text=text,
            text_hash=text_hash,
            embedding_plan=writer_plan,
        ),
        embedding_plan=writer_plan,
    )
    assert embedded["processed"] == 2
    assert embedded["processed_node_ids"] == _EXPECTED_NODE_IDS
    assert embedded["errors"] == 0

    published = Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert published["status"] == "published"
    generation_id = published["generation_id"]
    after = _graph_counts(clean_runtime, generation_id=generation_id)
    assert after == {"nodes": 2, "embeddings": 2}
    observed_delta = {key: after[key] - before_rejection[key] for key in after}
    assert observed_delta == {"nodes": 2, "embeddings": 2}

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=_DEPLOYMENT,
            requested_generation=generation_id,
        )
        semantic = search(
            conn,
            session,
            _QUERY,
            method="semantic",
            embedder=query_plan,
        )
    ordered_ids = [row["node_id"] for row in semantic["results"]]
    assert ordered_ids[0] == "e2e:contract"
    assert semantic["results"][0]["distance"] == pytest.approx(0.0, abs=1e-6)

    health = _runtime_health(clean_runtime)
    assert health["active_queue_depth"] == 0
    assert health["queue_view_dlq_count"] == 0
    assert health["current_dlq_count"] == 0
    doctor_rc, doctor = _doctor_report(clean_runtime)
    assert doctor_rc == 0
    assert doctor["ok"] is True

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT current_database(), inet_server_addr()::text, inet_server_port()"
        )
        database, host, port = cur.fetchone()

    report = {
        "schema": "okg.embedding_config_e2e.v1",
        "deployment": _DEPLOYMENT,
        "database": {"name": database, "host": host, "port": port},
        "manifest": {
            "canonical_spelling": "embeddings",
            "enabled": manifest.embedding_config.enabled,
            "backend": manifest.embedding_config.backend,
            "legacy_warning": EmbeddingConfigDeprecationWarning.__name__,
            "conflict_error": str(conflict.value),
        },
        "preflight_rejections": rejected,
        "execution_plan": {
            "fingerprint": writer_plan.fingerprint,
            "writer_query_fingerprints_equal": (
                writer_plan.fingerprint == query_plan.fingerprint
            ),
            "model_id": writer_plan.model_id,
            "declared_dimension": writer_plan.declared_dimension,
            "observed_dimension": writer_plan.observed_dimension,
        },
        "registered_workflows": sorted(_DECORATED),
        "counts": {
            "before": before_rejection,
            "after": after,
            "expected_delta": {"nodes": 2, "embeddings": 2},
            "observed_delta": observed_delta,
        },
        "embedding_batch": embedded,
        "publish": {
            "generation_id": str(generation_id),
            "status": published["status"],
            "nodes_added": published["nodes_added"],
            "edges_added": published["edges_added"],
        },
        "semantic_search": {
            "query": _QUERY,
            "ranking": "exact_cosine_scan",
            "ordered_node_ids": ordered_ids,
            "top_distance": semantic["results"][0]["distance"],
        },
        "metrics": health,
        "doctor": {"return_code": doctor_rc, "report": doctor},
    }
    report_path = pytestconfig.getoption("--embedding-report")
    if report_path:
        output = Path(report_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
