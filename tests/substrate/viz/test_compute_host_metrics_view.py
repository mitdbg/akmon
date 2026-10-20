"""The live operator view `okg.v_compute_host_metrics`.

Moved out of test_resource_monitoring_view.py, which also imports this
repository's own okg-workspace view module and so does not ship in the public
copy. These two tests need only the product view, the inspect operator and the
query operator, so they ship. The deployment name is a label on the samples.
"""
from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("fastapi")

from okg.substrate.external_backed.series import record_sample  # noqa: E402
from okg.substrate.mcp.run_sql import run_sql  # noqa: E402
from okg.substrate.mcp.session import Session, _db_fingerprint  # noqa: E402
from okg.substrate.mcp.tools import inspect_view  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOY = "compute-host-demo"

_EXPECTED_VIEW_COLUMNS = {
    "deployment_name", "node_id", "metric", "last_value", "staleness_seconds",
    "samples_total", "avg_1h", "max_1h", "avg_1d", "max_1d", "avg_7d", "max_7d",
}


def _session(conn: psycopg.Connection) -> Session:
    return Session(
        deployment_name=_DEPLOY, generation_id=1, catalog_version_id=1,
        db_fingerprint=_db_fingerprint(conn), branch_name="default",
        branch_id="default", compat_generation_id=1,
    )


def test_live_view_discoverable_via_inspect(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        schema = inspect_view(conn, _session(conn), "v_compute_host_metrics")
    assert schema["view"] == "okg.v_compute_host_metrics"
    names = {col["name"] for col in schema["columns"]}
    assert _EXPECTED_VIEW_COLUMNS <= names, _EXPECTED_VIEW_COLUMNS - names


def test_live_view_queryable_via_query_operator(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE okg.node_series, okg.node_series_watermark")
        for load in (4.0, 6.0):
            record_sample(conn, deployment_name=_DEPLOY, node_id="host:rmtest-up",
                          metric="load1", value=load, source="test")
        payload = run_sql(
            conn, _session(conn),
            "SELECT node_id, metric, last_value, avg_1h, max_1h, samples_total "
            "FROM okg.v_compute_host_metrics "
            f"WHERE deployment_name = '{_DEPLOY}' AND node_id = 'host:rmtest-up' "
            "ORDER BY metric",
        )
    rows = [dict(zip(payload["columns"], r)) for r in payload["rows"]]
    load = {r["metric"]: r for r in rows}["load1"]
    assert load["last_value"] == 6.0
    assert load["samples_total"] == 2
    assert payload["query_plan_summary"] != "Chronos checkout"
