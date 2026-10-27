"""Issue #3955: the rollups must not probe a changed-id array per row.

Each publish resolves the bucket of every changed edge with
``ontology_rollup._EDGE_BUCKETS_SQL``, a join of the edges to their two end
nodes. It used to filter with ``e.edge_id = ANY(:edge_ids)``. On a staging copy
of a 244,471-node, 335,000-edge graph, with 325,448 changed edges and a server
that allows 32 parallel workers per query, the planner scanned the nodes first
and probed the edges once per node, with the whole id array inside that inner
index scan's condition. Every probe re-read the array; the statement passed
1,200 s, with or without fresh statistics, against 1.2 s for the hashed plan
the same server picked with parallel query off.

The statement now joins the ids as a relation, which the planner can hash or
drive an index probe from, and never evaluates the array as a predicate. These
tests pin, on a real Chronos checkout of a synthetic graph:

1. the statement returns exactly the rows the pre-fix statement returns,
   including a repeated id, an id with no edge and a deleted edge;
2. under the server's planner settings, under settings that make parallel
   plans free, and under those settings with nested loops as the only join, no
   plan node evaluates an ``= ANY`` array condition;
3. under the parallel nested-loop settings the pre-fix statement takes the
   staging shape -- the array inside an index condition on the inner side of a
   nested loop -- so (2) checks a shape the planner really picks.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402

from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import ontology_rollup  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

# Synthetic graph size. Below ANALYZE's 30,000-row sample, so the statistics,
# and with them the plans, are the same on every run.
_NODES = 3_000
_EDGES = 4_000
_CHANGED = 3_500
# Length of the id array the plan tests pass: the changed ids padded with ids
# no edge has, as large as a real publish batch for the planner's purposes.
_ARRAY_LEN = 50_000

# The pre-fix statement, verbatim: the oracle for (1) and the positive control
# for (3).
_PRE_3955_EDGE_BUCKETS_SQL = f"""
SELECT
  e.edge_id AS edge_id,
  ns.subtype AS src_subtype,
  e.edge_type AS edge_type,
  nd.subtype AS dst_subtype,
  {ontology_rollup._PRODUCER_EXPR} AS producer
FROM okg.graph_edges e
JOIN okg.graph_nodes ns ON ns.node_id = e.src
JOIN okg.graph_nodes nd ON nd.node_id = e.dst
WHERE e.edge_id = ANY(:edge_ids)
"""

# Parallel plans cost nothing to start, and index scans are cheap: the
# planner's view of a large server such as the staging one.
_PARALLEL = (
    ("max_parallel_workers_per_gather", "32"),
    ("parallel_setup_cost", "0"),
    ("parallel_tuple_cost", "0"),
    ("min_parallel_table_scan_size", "0"),
    ("min_parallel_index_scan_size", "0"),
    ("effective_cache_size", "140GB"),
    ("random_page_cost", "1.1"),
)
# Nested loops as the only join: the choice the staging planner made.
_NESTED_LOOPS_ONLY = (
    ("enable_hashjoin", "off"),
    ("enable_mergejoin", "off"),
)
_PROFILES = {
    "server": (),
    "parallel": _PARALLEL,
    "parallel_nested_loop": _PARALLEL + _NESTED_LOOPS_ONLY,
}
_CONDITION_KEYS = ("Index Cond", "Recheck Cond", "Filter", "Join Filter")


def _array_conditions(plan: dict[str, Any]) -> list[str]:
    """Every ``= ANY`` condition anywhere in the plan, as 'node: key'."""
    found = [
        f"{plan['Node Type']}: {key}"
        for key in _CONDITION_KEYS
        if "= ANY (" in str(plan.get(key, ""))
    ]
    for child in plan.get("Plans", ()):
        found.extend(_array_conditions(child))
    return found


def _shape(plan: dict[str, Any], depth: int = 0) -> str:
    """The plan as indented node lines with conditions cut short, for messages."""
    conditions = {key: str(plan[key])[:120] for key in _CONDITION_KEYS if key in plan}
    name = plan.get("Index Name", "")
    line = "  " * depth + f"{plan['Node Type']} {name} {conditions}"
    return "\n".join(
        [line] + [_shape(child, depth + 1) for child in plan.get("Plans", ())]
    )


def _inner_index_array_conditions(plan: dict[str, Any]) -> list[str]:
    """``= ANY`` index conditions on the inner side of a nested loop: re-run,
    with the whole array, once per outer row."""
    found: list[str] = []
    children = list(plan.get("Plans", ()))
    if plan["Node Type"] == "Nested Loop" and len(children) == 2:
        found.extend(
            entry
            for entry in _array_conditions(children[1])
            if entry.endswith(("Index Cond", "Recheck Cond"))
        )
    for child in children:
        found.extend(_inner_index_array_conditions(child))
    return found


def _head(statement: str) -> str:
    """The select list: the part of a statement the checkout does not rewrite."""
    return " ".join(statement.strip().split("\nFROM", 1)[0].split())[:60]


@contextmanager
def _explained(prefix: str, head: str) -> Iterator[None]:
    """Prefix the statement whose select list is ``head`` with EXPLAIN.

    The checkout rewrites ``okg.graph_edges`` and ``okg.graph_nodes`` into
    pinned subqueries over the interval tables; wrapping the adapter EXPLAINs
    exactly that rewritten statement with the real parameters.
    """
    original = PostgresDatabaseAdapter.execute

    def execute(self, statement, params=()):  # type: ignore[no-untyped-def]
        if head in " ".join(statement.split()):
            statement = prefix + statement
        return original(self, statement, params)

    PostgresDatabaseAdapter.execute = execute  # type: ignore[method-assign]
    try:
        yield
    finally:
        PostgresDatabaseAdapter.execute = original  # type: ignore[method-assign]


def _plan(
    conn: Any,
    checkout: Any,
    statement: str,
    params: dict[str, Any],
    settings: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    # Session settings, reset afterwards: the checkout commits after each
    # query, so a transaction cannot scope them.
    try:
        for name, value in settings:
            conn.execute(
                sql.SQL("SELECT set_config({}, {}, false)").format(
                    sql.Literal(name), sql.Literal(value)
                )
            )
        with _explained("EXPLAIN (FORMAT JSON) ", _head(statement)):
            rows = checkout.query(statement, params)
    finally:
        for name, _ in settings:
            conn.execute(sql.SQL("RESET {}").format(sql.Identifier(name)))
    payload = rows[0]["QUERY PLAN"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload[0]["Plan"]


def _canonical(rows: list[dict[str, Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


@pytest.fixture
def synthetic_graph(clean_runtime: str) -> Iterator[dict[str, Any]]:
    """A Chronos graph with _NODES nodes and _EDGES edges, statistics fresh.

    A few facts go through the store so the interval tables, their indexes
    and a visible branch point exist; the bulk rows are copied into the
    interval tables with that branch point's visibility, then analyzed.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("p:seed", "person", {}, {"id": "p:seed"}, {"v": 1}),
                NodeFact("t:seed", "ticket", {}, {"id": "t:seed"}, {"v": 1}),
                NodeFact("t:gone", "ticket", {}, {"id": "t:gone"}, {"v": 1}),
            ]
            + [
                EdgeFact(
                    src,
                    "p:seed",
                    "reported_by",
                    source_record_id={"id": f"e:{src}"},
                    source_revision={"v": 1},
                )
                for src in ("t:seed", "t:gone")
            ],
            source="default",
        )
        tables = dict(
            conn.execute(
                "SELECT table_name, physical_table FROM _chronos_branch_tables"
                " WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
                " AND backend = 'interval'"
            ).fetchall()
        )
        nodes = sql.Identifier(tables["okg.graph_nodes"])
        edges = sql.Identifier(tables["okg.graph_edges"])
        seed_edges = [
            int(row[0])
            for row in conn.execute(
                sql.SQL(
                    "SELECT edge_id FROM {} WHERE deleted = FALSE ORDER BY src"
                ).format(edges)
            ).fetchall()
        ]
        assert len(seed_edges) == 2, seed_edges
        # The second seed edge becomes a tombstone: deleted at the visible point.
        deleted_edge = seed_edges[1]
        conn.execute(
            sql.SQL("UPDATE {} SET deleted = TRUE WHERE edge_id = %s").format(edges),
            (deleted_edge,),
        )
        conn.execute(
            sql.SQL(
                "INSERT INTO {} (node_id, subtype, attrs, source, live_lo, live_hi,"
                " writer_segment_id, deleted)"
                " SELECT 'n:' || g, (ARRAY['ticket', 'person', 'page', 'topic'])[1 + g %% 4],"
                " '{{}}'::jsonb, 'synthetic', s.live_lo, s.live_hi, s.writer_segment_id, FALSE"
                " FROM generate_series(1, %s) AS g,"
                " (SELECT live_lo, live_hi, writer_segment_id FROM {}"
                "   WHERE node_id = 'p:seed' AND deleted = FALSE) AS s"
            ).format(nodes, nodes),
            (_NODES,),
        )
        conn.execute(
            sql.SQL(
                "INSERT INTO {} (edge_id, src, dst, edge_type, attrs, source, live_lo,"
                " live_hi, writer_segment_id, deleted)"
                " SELECT 1000000000::bigint + g::bigint * 7919,"
                " 'n:' || (1 + (g * 31) %% %s), 'n:' || (1 + (g * 17 + g / 3) %% %s),"
                " (ARRAY['links_to', 'mentions'])[1 + g %% 2], '{{}}'::jsonb,"
                " CASE WHEN g %% 5 = 0 THEN '_publisher_derived' ELSE 'synthetic' END,"
                " s.live_lo, s.live_hi, s.writer_segment_id, FALSE"
                " FROM generate_series(1, %s) AS g,"
                " (SELECT live_lo, live_hi, writer_segment_id FROM {}"
                "   WHERE edge_id = %s AND deleted = FALSE) AS s"
            ).format(edges, edges),
            (_NODES, _NODES, _EDGES, seed_edges[0]),
        )
        conn.execute(sql.SQL("ANALYZE {}").format(nodes))
        conn.execute(sql.SQL("ANALYZE {}").format(edges))
        checkout = store.checkout("default")
        bulk = [1_000_000_000 + g * 7919 for g in range(1, _CHANGED + 1)]
        wide = bulk + list(range(-_ARRAY_LEN + len(bulk), 0))
        assert len(wide) == _ARRAY_LEN
        try:
            yield {
                "conn": conn,
                "checkout": checkout,
                "ids": bulk,
                "wide_ids": wide,
                "seed_edge": seed_edges[0],
                "deleted_edge": deleted_edge,
            }
        finally:
            store.close()


def test_bucket_rows_match_pre_fix_statement(synthetic_graph: dict[str, Any]) -> None:
    checkout = synthetic_graph["checkout"]
    ids = (
        synthetic_graph["ids"]
        + synthetic_graph["ids"][:25]  # repeated ids
        + [synthetic_graph["seed_edge"], synthetic_graph["deleted_edge"], -7]
    )
    new_rows = checkout.query(ontology_rollup._EDGE_BUCKETS_SQL, {"edge_ids": ids})
    old_rows = checkout.query(_PRE_3955_EDGE_BUCKETS_SQL, {"edge_ids": ids})
    assert _canonical(new_rows) == _canonical(old_rows)
    # One row per live changed edge: the bulk ids, the live seed edge; not the
    # repeats, the deleted edge or the id with no edge.
    assert len(new_rows) == _CHANGED + 1
    returned = {int(row["edge_id"]) for row in new_rows}
    assert synthetic_graph["deleted_edge"] not in returned
    assert synthetic_graph["seed_edge"] in returned
    # The function the rollup calls counts the same buckets.
    counts = ontology_rollup._edge_bucket_counts(checkout, ids)
    assert sum(counts.values()) == _CHANGED + 1
    assert ontology_rollup._edge_bucket_counts(checkout, []) == {}


@pytest.mark.parametrize("profile", sorted(_PROFILES))
def test_bucket_statement_never_evaluates_an_id_array(
    synthetic_graph: dict[str, Any], profile: str
) -> None:
    plan = _plan(
        synthetic_graph["conn"],
        synthetic_graph["checkout"],
        ontology_rollup._EDGE_BUCKETS_SQL,
        {"edge_ids": synthetic_graph["wide_ids"]},
        _PROFILES[profile],
    )

    assert _array_conditions(plan) == [], _shape(plan)


@pytest.mark.parametrize("profile", sorted(_PROFILES))
def test_pre_fix_statement_evaluates_the_array(
    synthetic_graph: dict[str, Any], profile: str
) -> None:
    """Positive control: the pre-fix statement carries the array as a plan
    condition under every profile, so the check above can fail."""
    plan = _plan(
        synthetic_graph["conn"],
        synthetic_graph["checkout"],
        _PRE_3955_EDGE_BUCKETS_SQL,
        {"edge_ids": synthetic_graph["wide_ids"]},
        _PROFILES[profile],
    )

    assert _array_conditions(plan), _shape(plan)


def test_pre_fix_statement_takes_the_staging_shape(
    synthetic_graph: dict[str, Any],
) -> None:
    """Positive control: the parallel nested-loop settings reproduce the #3955
    plan for the old statement: the array inside an index condition on the inner
    side of a nested loop, re-read once per outer row."""
    plan = _plan(
        synthetic_graph["conn"],
        synthetic_graph["checkout"],
        _PRE_3955_EDGE_BUCKETS_SQL,
        {"edge_ids": synthetic_graph["wide_ids"]},
        _PROFILES["parallel_nested_loop"],
    )

    assert _inner_index_array_conditions(plan), _shape(plan)
