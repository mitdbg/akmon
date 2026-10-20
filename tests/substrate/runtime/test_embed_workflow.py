"""Tests for the embed batch DBOS workflow (Phase 3 §3.1).

Three layers:
  1. Registration / accessors (mock DBOS).
  2. Embedder-spec resolution (pure logic, no DB).
  3. End-to-end batch behavior against a per-test Postgres
     (`dbos_integration` mark).
"""
from __future__ import annotations

from contextlib import contextmanager


import psycopg
import pytest


@pytest.fixture(autouse=True)
def _permit_fake_scheduled_enqueues(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def permit(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.promotion_fence.promotion_enqueue_permit",
        permit,
    )


# ---------------------------------------------------------------------------
# Registration tests (mock DBOS only — fast)
# ---------------------------------------------------------------------------


def test_register_embed_workflow_caches_handle():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.embed_workflow import (
            EMBED_WORKFLOW_NAME,
            _DECORATED,
            register_embed_workflow,
        )

        from okg.substrate.embeddings import (
            EmbeddingConfig,
            build_embedding_execution_plan,
        )
        register_embed_workflow(
            deployment="test",
            embedding_plan=build_embedding_execution_plan(
                EmbeddingConfig(enabled=True, backend="hash")
            ),
        )
        assert EMBED_WORKFLOW_NAME in _DECORATED


def test_register_embed_schedule_caches_handle():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.embed_workflow import (
            EMBED_SCHEDULE_ID,
            _SCHEDULE_DECORATED,
            register_embed_schedule,
        )

        register_embed_schedule(
            deployment="test", dsn="postgres://stub",
        )
        assert EMBED_SCHEDULE_ID in _SCHEDULE_DECORATED


def test_apply_embed_schedule_requires_register():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.embed_workflow import (
            _SCHEDULE_DECORATED,
            apply_embed_schedule,
        )
        _SCHEDULE_DECORATED.clear()

        with pytest.raises(RuntimeError, match="before"):
            apply_embed_schedule(schedule_overrides={})


def test_scheduled_enqueue_calls_dbos_client():
    """Scheduled enqueue invokes DBOSClient.enqueue with the
    deployment's queue + embed workflow name."""
    from datetime import datetime, timezone

    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context() as mock:
        from okg.substrate.runtime.embed_workflow import (
            _SCHEDULE_DECORATED,
            register_embed_schedule,
        )

        register_embed_schedule(
            deployment="myapp", dsn="postgres://stub",
        )
        fn = _SCHEDULE_DECORATED[next(iter(_SCHEDULE_DECORATED))]

        wf_id = fn(datetime.now(timezone.utc), None)
        assert wf_id.startswith("mock-wf-")
        mock.assert_workflow_enqueued(
            "okg_embed_batch",
            queue_name="okg-myapp",
        )


# ---------------------------------------------------------------------------
# Embedder-spec resolution (pure logic)
# ---------------------------------------------------------------------------


def test_build_embedder_hash():
    from okg.substrate.runtime.embed_workflow import (
        _build_embedder_from_spec,
    )
    e = _build_embedder_from_spec("hash")
    # The HashEmbedder is the test default.
    assert e.__class__.__name__ == "HashEmbedder"


def test_build_embedder_sentence_transformers_requires_model():
    from okg.substrate.runtime.embed_workflow import (
        _build_embedder_from_spec,
    )
    with pytest.raises(Exception, match="model name"):
        _build_embedder_from_spec("sentence-transformers:")


def test_build_embedder_unknown_spec_raises():
    from okg.substrate.runtime.embed_workflow import (
        _build_embedder_from_spec,
    )
    with pytest.raises(Exception, match="unknown embedder spec"):
        _build_embedder_from_spec("openai:text-embedding-3")


# ---------------------------------------------------------------------------
# Live workflow body test (dbos_integration)
# ---------------------------------------------------------------------------


_EMBED_TEXT = "hello world"


def _embed_text_hash() -> str:
    """The text_hash ChronosGraphStore.apply_facts stamps for the seeded
    node (md5 of the node's text). _embed_and_write_one matches the embed
    call's text_hash against okg.graph_nodes.text_hash, so the test must
    use the same value the write path stored."""
    import hashlib

    return hashlib.md5(_EMBED_TEXT.encode("utf-8")).hexdigest()


def _url_dsn(admin_dsn: str, db_name: str) -> str:
    """Build a URL-style (postgres://...) DSN for `db_name`.

    Post chronos-native cutover the embed workflow opens its own
    ChronosGraphStore.connect(dsn); chronos's connect_sql_database routes
    by URL scheme, so it needs a URL-style DSN (a keyword-style DSN parses
    to an empty scheme and is misrouted to SQLite). _swap_dbname returns
    keyword-style, so reassemble a URL here.
    """
    info = psycopg.conninfo.conninfo_to_dict(admin_dsn)
    user = info.get("user", "postgres")
    password = info.get("password", "")
    host = info.get("host", "localhost")
    port = info.get("port", "5432")
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgres://{auth}{host}:{port}/{db_name}"


@pytest.fixture
def test_db_dsn(admin_dsn: str):
    db_name = "okg_embed_workflow_test"
    dsn = _url_dsn(admin_dsn, db_name)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.execute(f'CREATE DATABASE "{db_name}"')

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_textsearch")

    # The chronos-native cutover dropped okg.nodes_live; the embed workflow
    # reads + UPDATEs okg.graph_nodes through a Chronos checkout. Initialize
    # the chronos graph store (creates okg.graph_nodes with the embedding
    # column + the main/default branch infra) and seed one node with text
    # awaiting an embedding via the canonical apply_facts write path. Use
    # the dsn-owning connect() (which manages + commits its own connection)
    # so the branch infra is durably committed before _embed_and_write_one
    # opens its own ChronosGraphStore.connect(dsn).
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True).apply_facts(
            [NodeFact("n-1", "Doc", {"text": _EMBED_TEXT},
                      {"id": "n-1"}, {"v": 1})],
            source="fixture",
            strict_endpoints=True,
        )

    yield dsn

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


@pytest.mark.dbos_integration
def test_embed_and_write_one_writes_vector(test_db_dsn):
    """The per-node embed step writes a vector + model_id to the
    matching okg.graph_nodes row (read back through a Chronos checkout)."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime.embed_workflow import (
        _embed_and_write_one,
    )

    text_hash = _embed_text_hash()
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    outcome = _embed_and_write_one(
        dsn=test_db_dsn,
        node_id="n-1",
        text=_EMBED_TEXT,
        text_hash=text_hash,
        embedding_plan=plan,
    )
    assert outcome["node_id"] == "n-1"
    assert outcome["text_hash"] == text_hash
    assert outcome["cache_hit"] is False

    with psycopg.connect(test_db_dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        rows = graph.query(
            "SELECT embedding IS NOT NULL AS has_emb, embedding_model "
            "FROM okg.graph_nodes WHERE node_id = 'n-1'"
        )
    assert len(rows) == 1
    assert rows[0]["has_emb"] is True
    # The recorded identity names the MODEL, not the class. See
    # `reachable-semantic-search`: a class-derived id collides across every
    # sentence-transformer at the substrate width.
    from okg.substrate.embeddings import HashEmbedder, embedder_model_id
    assert rows[0]["embedding_model"] == embedder_model_id(HashEmbedder())


@pytest.mark.dbos_integration
def test_embed_and_write_one_cache_hit_skips_embed(test_db_dsn):
    """When the row already has an embedding under the same model,
    the function returns cache_hit=True without re-embedding."""
    from okg.substrate.runtime.embed_workflow import (

        _embed_and_write_one,
    )

    text_hash = _embed_text_hash()
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    # First call: cold cache.
    _embed_and_write_one(
        dsn=test_db_dsn,
        node_id="n-1",
        text=_EMBED_TEXT,
        text_hash=text_hash,
        embedding_plan=plan,
    )
    # Second call: should hit cache.
    outcome = _embed_and_write_one(
        dsn=test_db_dsn,
        node_id="n-1",
        text=_EMBED_TEXT,
        text_hash=text_hash,
        embedding_plan=plan,
    )
    assert outcome["cache_hit"] is True
