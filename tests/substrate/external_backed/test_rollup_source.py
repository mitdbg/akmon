"""req.external-backed-nodes-rollups.rollup-source.

Rollups are a scheduled, trust-homogeneous source: a mixed-trust scope is
rejected; a trusted rollup publishes a sparse fact to a default-branch
generation; an untrusted rollup routes to the quarantine branch (never
laundered onto default); raw samples never become facts.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.admission import graph_branch_for_admission_mode
from okg.substrate.external_backed.rollup import (
    SNAPSHOT_SUBTYPE,
    RollupSource,
    RollupTarget,
    RollupTrustError,
)
from okg.substrate.external_backed.series import record_sample
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def _src(trust_class, targets, dsn):
    return RollupSource(
        deployment_name=_DEPLOY, dsn=dsn, trust_class=trust_class, targets=targets,
    )


def test_mixed_trust_scope_rejected() -> None:
    # No DB: homogeneity is enforced at construction.
    with pytest.raises(RollupTrustError):
        RollupSource(
            deployment_name=_DEPLOY, dsn="x", trust_class="trusted",
            targets=[
                RollupTarget("a", "m", "trusted"),
                RollupTarget("b", "m", "untrusted"),
            ],
        )


def test_trusted_rollup_publishes_sparse_fact(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        for v in (60.0, 64.0, 71.0):
            record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE,
                          metric="gpu_util", value=v)

    src = _src("trusted", [RollupTarget(_NODE, "gpu_util", "trusted")], clean_runtime)
    branch = graph_branch_for_admission_mode(src.admission_mode)
    assert branch == "default"
    SourceRunner(
        clean_runtime, source_name="rollup:trusted",
        deployment_name=_DEPLOY, graph_branch=branch,
    ).run(src, mode="scope_complete")

    with _conn(clean_runtime) as conn:
        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT node_id, attrs->>'sample_count' AS n FROM okg.graph_nodes "
            "WHERE subtype=%(s)s", {"s": SNAPSHOT_SUBTYPE},
        )
    # ONE rollup per target, NOT one fact per raw sample.
    assert len(rows) == 1
    assert rows[0]["n"] == "3"
    node_id = rows[0]["node_id"]

    result = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert result["status"] == "published"

    graph = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        pinned = graph.checkout_generation(result["generation_id"]).query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id=%(n)s", {"n": node_id},
        )
    finally:
        graph.close()
    assert len(pinned) == 1


def test_untrusted_rollup_routes_to_quarantine(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        for v in (10.0, 20.0):
            record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE,
                          metric="gpu_util", value=v)

    src = _src("untrusted", [RollupTarget(_NODE, "gpu_util", "untrusted")], clean_runtime)
    branch = graph_branch_for_admission_mode(src.admission_mode)
    assert branch == "quarantine:review"
    SourceRunner(
        clean_runtime, source_name="rollup:untrusted",
        deployment_name=_DEPLOY, graph_branch=branch,
    ).run(src, mode="scope_complete")

    # Default branch must NOT see it (no laundering of untrusted history).
    with _conn(clean_runtime) as conn:
        default_rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype=%(s)s",
            {"s": SNAPSHOT_SUBTYPE},
        )
    assert len(default_rows) == 0

    # Quarantine branch holds it, pending review.
    graph = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        q_rows = graph.checkout("quarantine:review").query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype=%(s)s",
            {"s": SNAPSHOT_SUBTYPE},
        )
    finally:
        graph.close()
    assert len(q_rows) == 1
