"""task.csp.leak / req.csp.no-graph-leak (issue #2011).

No sample table is part of a published generation. Sampled calls happen
between two publishes; okg.v_nodes and okg.v_edges counts are unchanged.
"""
from __future__ import annotations

from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.admission import graph_branch_for_admission_mode
from okg.substrate.external_backed.call_sample import (
    RETENTION_CLASSES,
    RefetchPointer,
    RelationPolicyRegistry,
    RelationSamplePolicy,
    live_identity,
    record_tool_call_sample,
    table_for_class,
)
from okg.substrate.external_backed.rollup import RollupSource, RollupTarget
from okg.substrate.external_backed.series import record_sample
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.mcp.run_sql import _rewrite_graph_views
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"
_RELATION = "live.cluster_jobs"


def _conn(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


@pytest.fixture(autouse=True)
def _empty_sample_tables(clean_runtime: str):
    """Start from an empty sample store: a row count must describe THIS test."""
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE okg.call_sample_hot, okg.call_sample_warm, "
            "okg.call_sample_cold, okg.call_sample_watermark"
        )
    yield


def _graph_counts(dsn: str, generation_id: str) -> tuple[int, int]:
    """okg.v_nodes / okg.v_edges counts at a pinned generation.

    This is the operator read path: `run_sql` rewrites the two base graph
    views to their physical tables BY NAME and executes through a Chronos
    checkout (the view bodies are never read, and a bare SQL session is
    refused outright). `_rewrite_graph_views` is that rewrite, so counting
    through it is counting okg.v_nodes and okg.v_edges as an agent sees them.
    """
    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        checkout = graph.checkout_generation(generation_id)
        nodes = checkout.query(
            _rewrite_graph_views("SELECT count(*) AS n FROM okg.v_nodes")
        )
        edges = checkout.query(
            _rewrite_graph_views("SELECT count(*) AS n FROM okg.v_edges")
        )
    finally:
        graph.close()
    return int(nodes[0]["n"]), int(edges[0]["n"])


def _publish(dsn: str) -> dict:
    return Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")
    ).cycle()


def _sample_all_classes(dsn: str) -> None:
    identity = live_identity(
        principal="user:operator",
        decision="allow",
        trace_id="trace-leak-1",
        session_id="session-leak-1",
    )
    policies = {
        "hot": "inline",
        "warm": "digest",
        "cold": "metadata",
    }
    with _conn(dsn) as conn:
        for retention_class in RETENTION_CLASSES:
            record_tool_call_sample(
                conn,
                deployment_name=_DEPLOY,
                tool_name="expand",
                relation=_RELATION,
                node_id=_NODE,
                identity=identity,
                status="ok",
                payload={"rows": [{"job": "a"}, {"job": "b"}]},
                refetch=RefetchPointer(
                    query={"edge_type": _RELATION}, source="slurm"
                ),
                observed_at=datetime.now(timezone.utc),
                registry=RelationPolicyRegistry(
                    [
                        RelationSamplePolicy(
                            relation=_RELATION,
                            policy=policies[retention_class],
                            retention_class=retention_class,
                        )
                    ]
                ),
            )


def test_sampled_calls_leave_the_published_generation_unchanged(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    with _conn(dsn) as conn:
        seed_catalog_version(conn)
        for value in (60.0, 64.0, 71.0):
            record_sample(
                conn,
                deployment_name=_DEPLOY,
                node_id=_NODE,
                metric="gpu_util",
                value=value,
            )

    # A first publish, so the comparison is against a non-empty graph.
    source = RollupSource(
        deployment_name=_DEPLOY,
        dsn=dsn,
        trust_class="trusted",
        targets=[RollupTarget(_NODE, "gpu_util", "trusted")],
    )
    SourceRunner(
        dsn,
        source_name="rollup:trusted",
        deployment_name=_DEPLOY,
        graph_branch=graph_branch_for_admission_mode(source.admission_mode),
    ).run(source, mode="scope_complete")
    first = _publish(dsn)
    assert first["status"] == "published"
    before_nodes, before_edges = _graph_counts(dsn, first["generation_id"])
    assert before_nodes > 0

    # Sampled calls, one per retention class, covering inline/digest/metadata.
    _sample_all_classes(dsn)
    with _conn(dsn) as conn, conn.cursor() as cur:
        for retention_class in RETENTION_CLASSES:
            cur.execute(f"SELECT count(*) FROM {table_for_class(retention_class)}")
            assert cur.fetchone()[0] == 1, (
                f"the {retention_class} sample must really exist, "
                "or this test proves nothing"
            )

    # A publish AFTER the sampled calls. The generation counted below is the
    # one THIS publish reports -- no falling back to the first generation,
    # which would compare it to itself and pass whatever the samples did.
    second = _publish(dsn)
    assert second["status"] == "published", second
    generation_id = second["generation_id"]
    assert generation_id, second

    # The publisher answering with the SAME generation id is itself the
    # result: sampled calls produced nothing for it to publish. A DIFFERENT
    # id would mean something did change, and the counts below then have to
    # prove that it was not a node or an edge. Both outcomes are acceptable;
    # a missing or failed second publish is not, which is why there is no
    # fallback above.
    after_nodes, after_edges = _graph_counts(dsn, generation_id)
    assert (after_nodes, after_edges) == (before_nodes, before_edges)

    # The sampled node is not in the graph at either generation.
    for gen in (first["generation_id"], generation_id):
        graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
        try:
            # The rewrite parses real SQL, so the parameter placeholder is
            # appended after it rather than handed to the parser.
            hits = graph.checkout_generation(gen).query(
                _rewrite_graph_views("SELECT node_id FROM okg.v_nodes")
                + " WHERE node_id = %(n)s",
                {"n": _NODE},
            )
        finally:
            graph.close()
        assert hits == [], f"the sampled node became a graph fact at {gen}"


def test_sampled_calls_alone_publish_an_empty_generation(clean_runtime: str) -> None:
    dsn = clean_runtime
    with _conn(dsn) as conn:
        seed_catalog_version(conn)
    _sample_all_classes(dsn)
    result = _publish(dsn)
    generation_id = result.get("generation_id")
    assert generation_id is not None, (
        "sampled calls alone produced no generation to inspect; the publish "
        f"reported {result.get('status')!r}"
    )
    nodes, edges = _graph_counts(dsn, generation_id)
    # Sampled calls were the only activity, so the generation stays empty:
    # a sample is not a graph fact.
    assert (nodes, edges) == (0, 0)
    with _conn(dsn) as conn, conn.cursor() as cur:
        for retention_class in RETENTION_CLASSES:
            cur.execute(f"SELECT count(*) FROM {table_for_class(retention_class)}")
            assert cur.fetchone()[0] == 1
