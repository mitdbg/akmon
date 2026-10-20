"""task.external-backed-nodes-rollups.proof — the rollups loop end to end.

On the `default` deployment: seeded node_series; a trusted rollup publishes a
sparse fact at a generation; an untrusted rollup routes to quarantine (not
default); an aged-out series range reads `aged_out`, distinct from `no_data`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.admission import graph_branch_for_admission_mode
from okg.substrate.external_backed.rollup import (
    SNAPSHOT_SUBTYPE, RollupSource, RollupTarget,
)
from okg.substrate.external_backed.series import read_series_window, record_sample
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_TRUSTED = "node:cluster:gpu07"
_UNTRUSTED = "node:ext:thirdparty"


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def _run(dsn, src):
    SourceRunner(
        dsn, source_name=f"rollup:{src.trust_class}",
        deployment_name=_DEPLOY,
        graph_branch=graph_branch_for_admission_mode(src.admission_mode),
    ).run(src, mode="scope_complete")


def test_rollups_end_to_end(clean_runtime: str) -> None:
    now = datetime.now(timezone.utc)
    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        for v in (60.0, 64.0, 71.0):
            record_sample(conn, deployment_name=_DEPLOY, node_id=_TRUSTED,
                          metric="gpu_util", value=v)
        record_sample(conn, deployment_name=_DEPLOY, node_id=_TRUSTED,
                      metric="gpu_util", value=40.0, observed_at=now - timedelta(days=40))
        record_sample(conn, deployment_name=_DEPLOY, node_id=_UNTRUSTED,
                      metric="errors", value=5.0)

    # 1. trusted rollup -> default -> published generation with a sparse fact.
    _run(clean_runtime, RollupSource(
        deployment_name=_DEPLOY, dsn=clean_runtime, trust_class="trusted",
        targets=[RollupTarget(_TRUSTED, "gpu_util", "trusted")]))
    result = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert result["status"] == "published"
    graph = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        pinned = graph.checkout_generation(result["generation_id"]).query(
            "SELECT node_id, attrs->>'sample_count' AS n FROM okg.graph_nodes "
            "WHERE subtype=%(s)s", {"s": SNAPSHOT_SUBTYPE})
    finally:
        graph.close()
    assert len(pinned) == 1  # one sparse rollup, not one fact per sample

    # 2. untrusted rollup -> quarantine, NOT default.
    _run(clean_runtime, RollupSource(
        deployment_name=_DEPLOY, dsn=clean_runtime, trust_class="untrusted",
        targets=[RollupTarget(_UNTRUSTED, "errors", "untrusted")]))
    graph = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        q = graph.checkout("quarantine:review").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id=%(n)s",
            {"n": f"series_rollup:{_UNTRUSTED}:errors"})
        d = graph.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id=%(n)s",
            {"n": f"series_rollup:{_UNTRUSTED}:errors"})
    finally:
        graph.close()
    assert len(q) == 1 and len(d) == 0

    # 3. retention: aged_out distinct from no_data.
    with _conn(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT drop_chunks('okg.node_series', %s::timestamptz)",
                        (now - timedelta(days=30),))
        old = read_series_window(conn, deployment_name=_DEPLOY, node_id=_TRUSTED,
                                 metric="gpu_util",
                                 start=now - timedelta(days=41),
                                 end=now - timedelta(days=39))
        gap = read_series_window(conn, deployment_name=_DEPLOY, node_id=_TRUSTED,
                                 metric="gpu_util",
                                 start=now - timedelta(days=60),
                                 end=now - timedelta(days=59))
    assert old["state"] == "aged_out"
    assert gap["state"] == "no_data"
    assert old["state"] != gap["state"]
