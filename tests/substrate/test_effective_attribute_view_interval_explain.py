"""ivm-effective-attribute-view / task.read-path-explain-proof.

req.read-path-no-regression: the interval-stab read must stay within a bounded
constant factor of the old generation-equality seek -- in particular it must use an
INDEX for a selective input over a large interval table, never a Seq Scan. We seed a
large `bulk` subtype + a tiny `rare` subtype, publish with the flag on (so the
interval table is populated), and EXPLAIN the exact stab the fast-row and aggregate
readers issue. The plan must be an index scan, not a Seq Scan over the interval table.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    resolve_generation_ordinal_row,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"
_BULK = 4000


def _explain(cur, sql: str, params) -> str:
    cur.execute("EXPLAIN " + sql, params)
    return "\n".join(r[0] for r in cur.fetchall())


def test_interval_stab_uses_index_not_seqscan(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["bulk", "rare"])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(f"b:{i}:{suffix}", "bulk", {"k": "v"}, {"id": f"b:{i}:{suffix}"}, {"v": 1})
            for i in range(_BULK)
        ]
        facts += [
            NodeFact(f"r:{i}:{suffix}", "rare", {"k": "ci"}, {"id": f"r:{i}:{suffix}"}, {"v": 1})
            for i in range(3)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    gen = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    with psycopg.connect(dsn, autocommit=True) as conn:
        db = ChronosGraphStore.from_connection(conn, ensure_schema=False).context.db
        branch_id, g_ord = resolve_generation_ordinal_row(db, gen)
        # schema-interval-narrow-split: the readers materialize attrs through
        # the version-store join, so EXPLAIN the exact joined stab they issue.
        join = (
            "JOIN okg.attribute_versions "
            "USING (target_kind, target_id, attrs_hash) "
        )
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
            cur.execute("ANALYZE okg.attribute_versions")
            # (a) fast-row stab for a selective subtype (the filter/map reader).
            fast_plan = _explain(
                cur,
                f"SELECT target_id, subtype, attrs "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} {join}"
                "WHERE target_kind = 'node' AND branch_id = %s AND namespace = 'default' "
                "AND from_ord <= %s AND %s < to_ord AND subtype = 'rare'",
                (branch_id, g_ord, g_ord),
            )
            # (b) aggregate group-by stab for a selective subtype.
            agg_plan = _explain(
                cur,
                f"SELECT (attrs -> 'k'), count(*) "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} {join}"
                "WHERE target_kind = 'node' AND branch_id = %s AND namespace = 'default' "
                "AND from_ord <= %s AND %s < to_ord AND subtype = 'rare' GROUP BY 1",
                (branch_id, g_ord, g_ord),
            )

    for label, plan in (("fast", fast_plan), ("aggregate", agg_plan)):
        assert (
            "Index Scan" in plan or "Index Only Scan" in plan or "Bitmap Index" in plan
        ), (label, plan)
        assert (
            "Seq Scan on attribute_effective_view_intervals" not in plan
        ), (label, plan)
