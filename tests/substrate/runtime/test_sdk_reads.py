"""Tests for SDK read helpers (Phase 2 §4).

Uses :func:`bind_test_context` to call into the helpers; the worker-
write generation allocator is the data source under test plus the
substrate's pinned-read path.
"""
from __future__ import annotations


import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture
def test_db_dsn(admin_dsn: str):
    db_name = "okg_sdk_reads_test"
    dsn = _swap_dbname(admin_dsn, db_name)

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
            cur.execute("CREATE SCHEMA okg")
            cur.execute(
                "CREATE TABLE okg.catalog_versions ("
                "catalog_version_id BIGSERIAL PRIMARY KEY, "
                "status TEXT NOT NULL)"
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) VALUES ('applied')"
            )
            cur.execute(
                "CREATE TABLE okg.graph_generations ("
                "generation_id BIGSERIAL PRIMARY KEY, "
                "deployment_name TEXT, "
                "status TEXT NOT NULL CHECK (status IN "
                "  ('pending','published','blocked','worker_write')), "
                "catalog_version_id BIGINT NOT NULL "
                "  REFERENCES okg.catalog_versions(catalog_version_id), "
                "published_at TIMESTAMPTZ, "
                "correlation_id TEXT NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            cur.execute(
                "CREATE TABLE okg.node_facts ("
                "fact_id BIGSERIAL, node_id TEXT NOT NULL, "
                "subtype TEXT NOT NULL, attrs JSONB NOT NULL DEFAULT '{}', "
                "source TEXT NOT NULL, source_record_id JSONB NOT NULL, "
                "source_revision JSONB NOT NULL, observed_at TIMESTAMPTZ NOT NULL, "
                "dedupe_key TEXT NOT NULL, run_id UUID NOT NULL, "
                "op CHAR(1) NOT NULL CHECK (op IN ('I','R')), "
                "PRIMARY KEY (fact_id, observed_at), "
                "UNIQUE (dedupe_key, observed_at))"
            )
            cur.execute(
                "CREATE TABLE okg.edge_facts ("
                "fact_id BIGSERIAL, edge_id BIGINT NOT NULL, "
                "src TEXT NOT NULL, dst TEXT NOT NULL, "
                "edge_type TEXT NOT NULL, attrs JSONB NOT NULL DEFAULT '{}', "
                "provenance TEXT NOT NULL, confidence REAL NOT NULL, "
                "source TEXT NOT NULL, source_record_id JSONB NOT NULL, "
                "source_revision JSONB NOT NULL, observed_at TIMESTAMPTZ NOT NULL, "
                "dedupe_key TEXT NOT NULL, run_id UUID NOT NULL, "
                "op CHAR(1) NOT NULL, "
                "PRIMARY KEY (fact_id, observed_at), "
                "UNIQUE (dedupe_key, observed_at))"
            )

    # The chronos-native cutover dropped okg.nodes_live / okg.edges_live; the
    # graph now lives in okg.graph_nodes / okg.graph_edges, read through a
    # Chronos checkout. Initialize the chronos graph store + branch infra on
    # this per-test DB so emit_node_fact's apply_facts and read_node's
    # checkout_generation resolve.
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)

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


def _seed_node_and_publish(conn, node_id, attrs, *, deployment="test"):
    """Helper: emit a node fact in a worker-write gen, then mark a
    publisher gen so list_published_generations can return something.

    Post chronos-native cutover the node lands in the Chronos graph
    (okg.graph_nodes) at the default branch head; a published generation
    is a Chronos checkpoint of that head. So after marking the
    graph_generations row published we checkpoint the branch with that
    generation id, which is what read_node(pin=gen) /
    checkout_generation(gen) resolve against.
    """
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context
    from okg.substrate.graph_store import ChronosGraphStore

    with bind_test_context(deployment=deployment, conn=conn):
        emit_node_fact(node_id, "Subtype", attrs, dedupe_key=f"d-{node_id}")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at)
            VALUES (%s, 'published', 1, %s, now())
            RETURNING generation_id
            """,
            (deployment, f"pub:{deployment}:test"),
        )
        gen = cur.fetchone()[0]
    ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkpoint_generation(gen, deployment_name=deployment)
    return gen


# ---------------------------------------------------------------------------
# read_node
# ---------------------------------------------------------------------------


def test_read_node_returns_row_at_pin(test_db_dsn):
    from okg.deployment import read_node
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen = _seed_node_and_publish(conn, "n-1", {"k": "v"})

        with bind_test_context(deployment="test", conn=conn):
            row = read_node("n-1", pin=gen)

    assert row is not None
    assert row.node_id == "n-1"
    assert row.subtype == "Subtype"
    # The write path stamps a nomos_source_lineage provenance block into
    # the stored attrs, so read_node returns it alongside the user attrs.
    # Assert the user attr as a subset, ignoring the provenance block.
    user_attrs = {k: v for k, v in row.attrs.items() if k != "nomos_source_lineage"}
    assert user_attrs == {"k": "v"}


def test_read_node_returns_none_for_unknown(test_db_dsn):
    """An unknown node id at a real published pin reads back as None —
    the checkout resolves but okg.graph_nodes has no such row."""
    from okg.deployment import read_node
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        # Publish a real generation (checkpoints the chronos branch) so the
        # pin resolves to a checkout; the node we look up simply isn't in it.
        gen = _seed_node_and_publish(conn, "n-present", {"k": "v"})

        with bind_test_context(deployment="test", conn=conn):
            row = read_node("does-not-exist", pin=gen)
    assert row is None


# ---------------------------------------------------------------------------
# query_at_generation
# ---------------------------------------------------------------------------


def test_query_at_generation_runs_arbitrary_select(test_db_dsn):
    """query_at_generation runs an arbitrary SELECT on the caller's
    connection with the okg.pin_generation GUC set. The chronos-native
    cutover dropped okg.nodes_live; graph content is now reconstructed
    through a Chronos checkout (see read_node), not a raw SELECT on the
    caller connection. query_at_generation's contract is the raw pinned
    SELECT itself, exercised here over the okg.node_facts audit log the
    SDK wrote to."""
    from okg.deployment import query_at_generation
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen = _seed_node_and_publish(conn, "n-3", {"k": "v"})

        with bind_test_context(deployment="test", conn=conn):
            rows = query_at_generation(
                "SELECT node_id, subtype FROM okg.node_facts "
                "WHERE node_id = %s AND op = 'I'",
                ("n-3",),
                pin=gen,
            )

    assert len(rows) == 1
    assert rows[0] == {"node_id": "n-3", "subtype": "Subtype"}


# ---------------------------------------------------------------------------
# list_published_generations
# ---------------------------------------------------------------------------


def test_list_published_generations(test_db_dsn):
    from okg.deployment import list_published_generations
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen_a = _seed_node_and_publish(conn, "n-a", {})
        gen_b = _seed_node_and_publish(conn, "n-b", {})

        with bind_test_context(deployment="test", conn=conn):
            rows = list_published_generations("test", limit=10)

    ids = [r.generation_id for r in rows]
    assert ids == sorted([gen_a, gen_b], reverse=True)
    assert all(r.status == "published" for r in rows)


def test_list_published_generations_since_filter(test_db_dsn):
    from okg.deployment import list_published_generations
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen_a = _seed_node_and_publish(conn, "n-a", {})
        gen_b = _seed_node_and_publish(conn, "n-b", {})

        with bind_test_context(deployment="test", conn=conn):
            rows = list_published_generations(
                "test", since_generation_id=gen_a,
            )

    ids = [r.generation_id for r in rows]
    assert ids == [gen_b]


# ---------------------------------------------------------------------------
# Generation-context helpers
# ---------------------------------------------------------------------------


def test_publisher_generation_raises_in_deployment_context(test_db_dsn):
    from okg.deployment import publisher_generation
    from okg.deployment._internal.context import (
        MissingContextError, bind_test_context,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(MissingContextError):
                publisher_generation()


def test_latest_published_generation(test_db_dsn):
    from okg.deployment import latest_published_generation
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen_a = _seed_node_and_publish(conn, "n-a", {})
        gen_b = _seed_node_and_publish(conn, "n-b", {})

        with bind_test_context(deployment="test", conn=conn):
            assert latest_published_generation("test") == max(gen_a, gen_b)


def test_latest_published_generation_no_publish_raises(test_db_dsn):
    from okg.deployment import latest_published_generation
    from okg.deployment._internal.context import (
        MissingContextError, bind_test_context,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(MissingContextError, match="no published"):
                latest_published_generation("test")


def test_pinned_generation_falls_back_to_latest_published(test_db_dsn):
    from okg.deployment import pinned_generation
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        gen = _seed_node_and_publish(conn, "n", {})

        with bind_test_context(deployment="test", conn=conn):
            assert pinned_generation() == gen


def test_pinned_generation_raises_on_unparseable_pin(test_db_dsn):
    """A pin that is SET but not interpretable must raise, never fall back.

    open_session pins chronos TEXT checkpoint refs
    (okg:<deployment>:branch:<b>:gen:<ts>:<hash>); until this read path
    consumes them, serving latest-published to a caller who pinned a
    specific generation would be a silent wrong answer."""
    from okg.deployment import pinned_generation
    from okg.deployment._internal.context import bind_test_context
    from okg.substrate.errors import GenerationPinInvalid

    chronos_ref = "okg:test:branch:main:gen:20260811T000000000000Z:deadbeef0000"
    with psycopg.connect(test_db_dsn) as conn:
        # A published generation exists, so a silent fallback WOULD have
        # something plausible to return — the raise must win anyway.
        _seed_node_and_publish(conn, "n-pin", {})

        with bind_test_context(deployment="test", conn=conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('okg.pin_generation', %s, false)",
                    (chronos_ref,),
                )
            try:
                with pytest.raises(GenerationPinInvalid) as excinfo:
                    pinned_generation()
            finally:
                with conn.cursor() as cur:
                    cur.execute("RESET okg.pin_generation")

    assert excinfo.value.data["guc"] == "okg.pin_generation"
    assert excinfo.value.data["value"] == chronos_ref
    assert chronos_ref in str(excinfo.value)


def test_worker_write_generation_allocates_on_demand(test_db_dsn):
    from okg.deployment import worker_write_generation
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            a = worker_write_generation()
            b = worker_write_generation()
        # Same context → same generation (cached).
        assert a == b
