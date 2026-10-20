"""EmbeddingAliasBackend — postgres-backed cosine resolution."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.alias import EmbeddingAliasBackend
from okg.substrate.embeddings import (
    EmbeddingConfig,
    EmbeddingExecutionPlan,
    build_embedding_execution_plan,
)
from tests.substrate._db_helpers import ensure_published_generation


pytestmark = pytest.mark.postgres


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for sub in ("doc", "section"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (sub, cv),
            )
    ensure_published_generation(conn, catalog_version_id=cv)
    return cv


def _seed_node_with_embedding(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
    text: str,
    embedder: EmbeddingExecutionPlan,
    cv: int,
    stored_model_id: str | None = None,
) -> None:
    """Seed a node WITH its embedding into the Chronos graph (okg.graph_nodes), the
    surface the alias backend reads after the cutover. apply_facts stores
    embedding=NULL, so write the embedding via a direct upsert_rows pass
    mirroring DBOS okg_embed_batch output."""
    from okg.substrate.graph_store import (
        DEFAULT_BRANCH,
        NODE_TABLE,
        ChronosGraphStore,
    )

    vec = embedder.embed(text)
    vecstr = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
    session = ChronosGraphStore.from_connection(conn).checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows(
            NODE_TABLE,
            [
                {
                    "node_id": node_id,
                    "subtype": subtype,
                    "attrs": {"text": text, "label": text[:30]},
                    "text_hash": None,
                    "embedding": vecstr,
                    "embedding_model": stored_model_id or embedder.model_id,
                    "source": "fixture",
                    "source_record_id": {"id": node_id},
                    "source_revision": {"v": 1},
                    "run_id": None,
                    "observed_at": None,
                }
            ],
        )


def test_closest_by_cosine_wins(clean_runtime: str) -> None:
    embedder = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_node_with_embedding(
            conn, node_id="doc:alpha", subtype="doc",
            text="alpha-bravo-charlie",
            embedder=embedder, cv=cv,
        )
        _seed_node_with_embedding(
            conn, node_id="doc:zulu", subtype="doc",
            text="completely unrelated text",
            embedder=embedder, cv=cv,
        )
        backend = EmbeddingAliasBackend(
            conn, embedder=embedder, accept_threshold=0.0,
        )
        matches = backend.match("alpha-bravo-charlie")

    assert matches
    top = max(matches, key=lambda m: m.similarity)
    assert top.canonical == "doc:alpha"
    assert top.method == "embedding"
    assert 0.0 <= top.similarity <= 1.0
    # Self-match on the same hash-deterministic embedding
    # should be very close to 1.0.
    assert top.similarity > 0.99


def test_subtype_filter_restricts_results(
    clean_runtime: str,
) -> None:
    embedder = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_node_with_embedding(
            conn, node_id="doc:foo", subtype="doc",
            text="exact phrase here",
            embedder=embedder, cv=cv,
        )
        _seed_node_with_embedding(
            conn, node_id="section:bar", subtype="section",
            text="exact phrase here",
            embedder=embedder, cv=cv,
        )
        backend = EmbeddingAliasBackend(
            conn, embedder=embedder, accept_threshold=0.0,
            subtype="doc",
        )
        matches = backend.match("exact phrase here")
    canonicals = {m.canonical for m in matches}
    assert "doc:foo" in canonicals
    assert "section:bar" not in canonicals


def test_different_embedding_model_is_not_ranked(clean_runtime: str) -> None:
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_node_with_embedding(
            conn,
            node_id="doc:skewed",
            subtype="doc",
            text="exact phrase here",
            embedder=plan,
            cv=cv,
            stored_model_id="different-model:same-width",
        )
        backend = EmbeddingAliasBackend(
            conn,
            embedder=plan,
            accept_threshold=0.0,
        )
        assert backend.match("exact phrase here") == []


def test_empty_nodes_live_returns_empty(
    clean_runtime: str,
) -> None:
    embedder = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        backend = EmbeddingAliasBackend(
            conn, embedder=embedder,
        )
        assert backend.match("anything") == []


def test_empty_needle_short_circuits(
    clean_runtime: str,
) -> None:
    embedder = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        backend = EmbeddingAliasBackend(
            conn, embedder=embedder,
        )
        assert backend.match("") == []
