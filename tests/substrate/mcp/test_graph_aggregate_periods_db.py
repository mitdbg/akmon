"""Date-period aggregates on the graph read path count overlaid dates (PostgreSQL).

PACT lane-s-storage-redesign-s2, requirement ``read-multi-key`` (aggregate
part): a period field (``month(date.attrs.<key>)``, or a named date such as
``month(date.created)`` that the catalog maps to an attribute) reads a date
attribute that is not an ``attrs.<key>`` field. On the graph path (since
PACT lane-s-storage-cutover the only read path) its key must join the
overlay keys, so an accepted overlay that moves a date into another month
is counted in that month.
"""
from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import graph_read
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

#: Graph dates: two in January, one in February, one in March. The overlay
#: moves the first January date to March.
_DATES = ("2026-01-15", "2026-01-20", "2026-02-03", "2026-03-10")
_OVERLAID = "2026-03-05"


@pytest.fixture
def published(clean_runtime: str) -> str:
    dsn, s = clean_runtime, uuid.uuid4().hex[:8]
    ids = [f"doc:p{i}:{s}" for i in range(len(_DATES))]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(
            name="doc", metadata={"created_at_from": "filed"},
        )])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(i, "doc", {"filed": d, "path": f"{n}.py"}, {"id": i}, {"v": 1})
             for n, (i, d) in enumerate(zip(ids, _DATES))],
            source="default", strict_endpoints=True,
        )
    out = Publisher(dsn, "default",
                    catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    gen = str(out["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id=ids[0], generation_id=gen,
            attr_key="filed", value=_OVERLAID, producer="test:s2-periods",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def _aggregate(dsn: str, monkeypatch, gen: str, by: str) -> dict[str, Any]:
    pins: list[str] = []
    real_pin = graph_read.require_graph_pin

    def pin(conn, session):
        pins.append(session.generation_id)
        return real_pin(conn, session)

    monkeypatch.setattr(graph_read, "require_graph_pin", pin)
    mcp = server_mod.build_server(dsn=dsn, deployment_name="default")
    try:
        out = mcp._tool_manager._tools["aggregate"].fn(
            input={"kind": "nodes", "subtype": "doc"}, by=[by], metrics=["count"],
            generation_id=gen,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
        monkeypatch.setattr(graph_read, "require_graph_pin", real_pin)
    assert pins, "the graph path answered"
    return out


def _month_counts(out: dict[str, Any]) -> dict[str, int]:
    """``{"2026-01": n, ...}`` from the aggregate's groups, whatever wrapping
    the key carries."""
    counts: dict[str, int] = {}
    for group in out["groups"]:
        text = str(group)
        months = [m for m in ("2026-01", "2026-02", "2026-03") if m in text]
        assert len(months) == 1, group
        counts[months[0]] = int(group["count"])
    return counts


@pytest.mark.parametrize("by", ["month(date.attrs.filed)", "month(date.created)"])
def test_a_period_aggregate_counts_the_overlaid_month(
    clean_runtime: str, published: str, monkeypatch, by: str,
) -> None:
    graph = _aggregate(clean_runtime, monkeypatch, published, by)
    assert _month_counts(graph) == {"2026-01": 1, "2026-02": 1, "2026-03": 2}, graph
