"""#3235: edge endpoint indexes on the effective interval table.

A filtered expand (and an edges filter or map with a src/dst predicate) reads
``okg.attribute_effective_view_intervals`` for one edge endpoint at one
generation. With only the edge_type-keyed edge index the planner scans the
whole table (lane B's CMS copy: a parallel seq scan of 800k rows for 241
edges, 94 ms). ``attribute_effective_intervals_edge_src_idx`` and
``..._edge_dst_idx`` key the edge rows by endpoint.

These tests build a fixture big enough for the planner to prefer an index, then
check: the expand-shaped stab plans through the new index in each direction
(and does not once the index is gone, the failure case); a deployment created
before the change gets both indexes from the schema ensure that provision and
``okg migrate`` run; and the index-reclaim judge leaves them alone while edges
are under 90% of the table's rows.
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ensure_attribute_schema,
)
from okg.substrate.db.connection import connect as db_connect  # noqa: E402
from okg.substrate.db.index_reclaim import judge_indexes  # noqa: E402

pytestmark = pytest.mark.postgres

_SRC_IDX = "attribute_effective_intervals_edge_src_idx"
_DST_IDX = "attribute_effective_intervals_edge_dst_idx"
#: #3408: one edge type's page in numeric id order.
_NUM_IDX = "attribute_effective_intervals_edge_num_idx"
_BRANCH = "default"
_NAMESPACE = "default"
# 10,000 nodes and 40,000 edges: edges are 80% of the interval rows, under the
# reclaim judge's 90% partial-index share, as on lane B's CMS copy (76%).
_NODES = 10_000
_EDGES = 40_000
# Edge i runs from node (i % 400) to node (i % 9973): 100 edges per source,
# about 4 per destination.
_SOURCES = 400
_DESTS = 9_973


def _seed(cur: Any) -> None:
    cur.execute(
        """
        WITH targets AS (
          SELECT 'node' AS target_kind, 'n:' || g AS target_id,
                 'doc' AS subtype, NULL::text AS src, NULL::text AS dst,
                 NULL::text AS edge_type
            FROM generate_series(0, %(nodes)s - 1) AS g
          UNION ALL
          SELECT 'edge', g::text, NULL, 'n:' || (g %% %(sources)s),
                 'n:' || (g %% %(dests)s), 'links'
            FROM generate_series(0, %(edges)s - 1) AS g
        ), hashed AS (
          SELECT t.*, jsonb_build_object('k', t.target_id) AS attrs,
                 okg.attribute_content_hash(
                   jsonb_build_object('k', t.target_id), NULL::jsonb
                 ) AS attrs_hash
            FROM targets AS t
        ), versions AS (
          INSERT INTO okg.attribute_versions
            (target_kind, target_id, attrs_hash, attrs, raw_attrs)
          SELECT target_kind, target_id, attrs_hash, attrs, NULL FROM hashed
        )
        INSERT INTO okg.attribute_effective_view_intervals
          (target_kind, target_id, namespace, branch_id, from_ord,
           subtype, src, dst, edge_type, attrs_hash)
        SELECT target_kind, target_id, %(ns)s, %(branch)s, 0,
               subtype, src, dst, edge_type, attrs_hash
          FROM hashed
        """,
        {
            "nodes": _NODES,
            "edges": _EDGES,
            "sources": _SOURCES,
            "dests": _DESTS,
            "ns": _NAMESPACE,
            "branch": _BRANCH,
        },
    )
    cur.execute(f"ANALYZE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
    cur.execute("ANALYZE okg.attribute_versions")


@pytest.fixture
def seeded(clean_runtime: str) -> Iterator[str]:
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            _seed(cur)
    yield clean_runtime


def _expand_stab(column: str, endpoint: str, generation_ord: int = 1) -> sql.Composed:
    """The edge base of #3235's ``_expand_one_hop_effective``, one endpoint.

    The interval rows live at ``generation_ord`` joined to their payloads,
    narrowed by one endpoint, the predicate shape the expand compiles to.
    """
    return sql.SQL(
        "SELECT e.target_id, e.src, e.dst, e.edge_type, e.attrs "
        "FROM (okg.attribute_effective_view_intervals "
        "      JOIN okg.attribute_versions USING (target_kind, target_id, attrs_hash)) AS e "
        "WHERE e.target_kind = 'edge' AND e.branch_id = {branch} "
        "AND e.from_ord <= {g} AND {g} < e.to_ord AND e.namespace = {ns} "
        "AND e.{col} = {endpoint} AND e.{col} IS NOT NULL "
        "ORDER BY (e.target_id)::bigint LIMIT 26"
    ).format(
        branch=sql.Literal(_BRANCH),
        g=sql.Literal(generation_ord),
        ns=sql.Literal(_NAMESPACE),
        col=sql.Identifier(column),
        endpoint=sql.Literal(endpoint),
    )


def _plan(cur: Any, query: sql.Composed) -> dict[str, Any]:
    cur.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + query)
    return cur.fetchone()[0][0]["Plan"]


def _interval_scans(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Every plan node that reads the interval table, depth first."""
    found = []
    if node.get("Relation Name") == "attribute_effective_view_intervals" or str(
        node.get("Index Name", "")
    ).startswith("attribute_effective_"):
        found.append(node)
    for child in node.get("Plans", []):
        found.extend(_interval_scans(child))
    return found


def _index_names(cur: Any) -> dict[str, str]:
    cur.execute(
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = 'okg' AND tablename = 'attribute_effective_view_intervals'"
    )
    return {name: definition for name, definition in cur.fetchall()}


@pytest.mark.parametrize(
    ("column", "endpoint", "index"),
    [("src", "n:7", _SRC_IDX), ("dst", "n:7", _DST_IDX)],
)
def test_expand_endpoint_stab_plans_through_the_endpoint_index(
    seeded: str, column: str, endpoint: str, index: str
) -> None:
    with db_connect(seeded, autocommit=True) as conn, conn.cursor() as cur:
        scans = _interval_scans(_plan(cur, _expand_stab(column, endpoint)))
        # An Index (Only) Scan, or a Bitmap Index Scan under its heap scan;
        # never a Seq Scan of the table.
        assert all(s["Node Type"] != "Seq Scan" for s in scans), scans
        keyed = [s for s in scans if s.get("Index Name") == index]
        assert keyed, scans
        cond = keyed[0].get("Index Cond", "")
        assert f"({column} = " in cond and "from_ord <=" in cond, keyed[0]
        # The plan answers the right rows: every edge of that endpoint.
        cur.execute(_expand_stab(column, endpoint))
        rows = cur.fetchall()
        assert rows and all(row[1 if column == "src" else 2] == endpoint for row in rows)


def test_without_the_index_the_stab_reads_no_endpoint_key(seeded: str) -> None:
    """Failure case: drop the src index and the plan loses its src key.

    This is the #3235 regression shape. The test proves the plan assertion
    above can fail: no remaining index carries ``src`` in its condition.
    """
    with db_connect(seeded, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"DROP INDEX okg.{_SRC_IDX}")
        scans = _interval_scans(_plan(cur, _expand_stab("src", "n:7")))
        assert scans, "the plan must read the interval table"
        assert all(s.get("Index Name") != _SRC_IDX for s in scans), scans
        assert all("(src = " not in s.get("Index Cond", "") for s in scans), scans


def test_schema_ensure_adds_both_indexes_to_an_existing_deployment(
    clean_runtime: str,
) -> None:
    """A deployment built before this change has neither index; the ensure that
    provision and ``okg migrate`` run creates both, with the declared shape,
    and running it again changes nothing."""
    with db_connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        ensure_attribute_schema(conn)
        cur.execute(f"DROP INDEX okg.{_SRC_IDX}")
        cur.execute(f"DROP INDEX okg.{_DST_IDX}")
        before = _index_names(cur)
        assert _SRC_IDX not in before and _DST_IDX not in before
        ensure_attribute_schema(conn)
        after = _index_names(cur)
        for name, column in ((_SRC_IDX, "src"), (_DST_IDX, "dst")):
            assert after[name] == (
                f"CREATE INDEX {name} ON okg.attribute_effective_view_intervals "
                f"USING btree (branch_id, namespace, {column}, from_ord, to_ord, "
                "target_id) WHERE (target_kind = 'edge'::text)"
            )
        ensure_attribute_schema(conn)
        assert _index_names(cur) == after


def test_reclaim_judge_does_not_judge_the_endpoint_indexes(seeded: str) -> None:
    """Edges are 80% of the rows here, under the judge's 90% partial share, so
    the judge reports both as ``partial_index``: never rebuilt, never dropped."""
    with db_connect(seeded, autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            judged = {
                item["index"]: item
                for item in judge_indexes(
                    cur,
                    [ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE],
                    ratio=1.0,
                    min_reclaim_bytes=0,
                    analyzed=True,
                )
            }
        for name in (_SRC_IDX, _DST_IDX, _NUM_IDX):
            assert judged[f"okg.{name}"]["verdict"] == "partial_index", judged
        with conn.cursor() as cur:
            names = _index_names(cur)
        assert _SRC_IDX in names and _DST_IDX in names and _NUM_IDX in names



# ---------- #3408: an edge type's page in numeric id order ----------

#: The page shape of `_effective_view_sql_page` for `map`/`filter` over one
#: edge type: a literal target_kind (so even a generic plan proves the partial
#: index), the type, the live-at-G stab, numeric id order, a page limit.
_TYPE_PAGE = (
    "SELECT target_id FROM okg.attribute_effective_view_intervals "
    "WHERE target_kind = 'edge' AND namespace = $1 AND branch_id = $2 "
    "AND edge_type = $3 AND from_ord <= $4 AND $4 < to_ord "
    "ORDER BY (target_id)::bigint LIMIT 51"
)


def _generic_plan(cur: Any, statement: str) -> dict[str, Any]:
    """The plan of `statement` as a GENERIC prepared plan (no parameter values)."""
    cur.execute("SET plan_cache_mode = force_generic_plan")
    cur.execute(f"PREPARE lanerp_page(text, text, text, int) AS {statement}")
    try:
        cur.execute(sql.SQL("EXPLAIN (FORMAT JSON) EXECUTE lanerp_page({}, {}, {}, 0)").format(
            sql.Literal(_NAMESPACE), sql.Literal(_BRANCH), sql.Literal("links"),
        ))
        return cur.fetchone()[0][0]["Plan"]
    finally:
        cur.execute("DEALLOCATE lanerp_page")
        cur.execute("RESET plan_cache_mode")


def _node_types(node: dict[str, Any]) -> list[str]:
    return [node["Node Type"]] + [t for c in node.get("Plans", []) for t in _node_types(c)]


def test_an_edge_type_page_reads_the_numeric_index_in_order(seeded: str) -> None:
    """The page walks the numeric index for its type and stops at the limit:
    no Sort, the index's condition names the edge type, under a generic plan
    (target_kind is a literal), and the rows are the first ids in order."""
    with db_connect(seeded, autocommit=True) as conn, conn.cursor() as cur:
        plan = _generic_plan(cur, _TYPE_PAGE)
        assert "Sort" not in _node_types(plan), plan
        scans = [s for s in _interval_scans(plan) if s.get("Index Name") == _NUM_IDX]
        assert scans and "(edge_type = " in scans[0].get("Index Cond", ""), plan
        cur.execute(_TYPE_PAGE.replace("$1", "%s").replace("$2", "%s")
                    .replace("$3", "%s").replace("$4", "%s"),
                    (_NAMESPACE, _BRANCH, "links", 0, 0))
        assert [int(r[0]) for r in cur.fetchall()] == list(range(51))


def test_without_the_numeric_index_the_page_sorts_every_row_of_the_type(seeded: str) -> None:
    """Failure case (the #3408 cost): without the index the page sorts."""
    with db_connect(seeded, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"DROP INDEX okg.{_NUM_IDX}")
        assert "Sort" in _node_types(_generic_plan(cur, _TYPE_PAGE))


def test_a_parameter_target_kind_cannot_use_the_partial_index(seeded: str) -> None:
    """Why the page writes target_kind as a literal: under a generic plan a
    parameter cannot prove the index's `WHERE target_kind = 'edge'`."""
    statement = _TYPE_PAGE.replace("target_kind = 'edge'", "target_kind = $5")
    with db_connect(seeded, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET plan_cache_mode = force_generic_plan")
        cur.execute(f"PREPARE lanerp_kind(text, text, text, int, text) AS {statement}")
        try:
            cur.execute(sql.SQL(
                "EXPLAIN (FORMAT JSON) EXECUTE lanerp_kind({}, {}, {}, 0, 'edge')"
            ).format(sql.Literal(_NAMESPACE), sql.Literal(_BRANCH), sql.Literal("links")))
            plan = cur.fetchone()[0][0]["Plan"]
        finally:
            cur.execute("DEALLOCATE lanerp_kind")
            cur.execute("RESET plan_cache_mode")
        assert all(s.get("Index Name") != _NUM_IDX for s in _interval_scans(plan)), plan


def test_schema_ensure_adds_the_numeric_index_to_an_existing_deployment(
    clean_runtime: str,
) -> None:
    with db_connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        ensure_attribute_schema(conn)
        cur.execute(f"DROP INDEX okg.{_NUM_IDX}")
        assert _NUM_IDX not in _index_names(cur)
        ensure_attribute_schema(conn)
        assert _index_names(cur)[_NUM_IDX] == (
            f"CREATE INDEX {_NUM_IDX} ON okg.attribute_effective_view_intervals "
            "USING btree (namespace, branch_id, edge_type, ((target_id)::bigint)) "
            "WHERE (target_kind = 'edge'::text)"
        )
