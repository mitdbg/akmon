"""Issue #2556: the Chronos dirty-scope query must not go quadratic.

The ``substrate-db-linux`` lane stalled for 10 to 12 minutes, on the nightly
and on pull requests #2657 and #2596, inside the dirty-scope query that
``Publisher._query_dirty_from_chronos_current_segment_by_type`` runs after a
checkpoint. The pre-fix query joined the dirty rows to a set-at-a-time
``previous_state`` subquery. When the planner believes the dirty set holds
about one row (stale statistics on ``writer_segment_id`` do this), it picks a
nested loop with that subquery as its inner side and no materialization, so
the whole previous-state scan re-runs once per dirty row. With the 6,302 dirty
rows of ``test_archived_code_names_restore_6302_qualified_aliases`` that took
70 s on a laptop, against 0.6 s for the fixed statement.

The fix looks up each dirty row's previous state with a ``LATERAL ... LIMIT 1``
primary-key probe. Its only join shape is one index probe per dirty row, and
the row estimate changes how many probes the planner expects, not which plan
it picks. These tests pin, on a real Chronos write:

1. the new statements return exactly the rows the pre-fix statements return,
   including planted intervals that each flip if one lookup condition is
   wrong (a closed interval, one starting exactly at the branch point,
   overlapping intervals, a subtype, source or edge-type change);
2. the rows the new statements process stay within a small multiple of the
   table size with whatever statistics the tables carry and with the dirty
   set underestimated at one row, both as planned and with an unmaterialized
   nested loop as the only join;
3. under that join choice the pre-fix node statement exceeds the bound, so
   the bound detects the quadratic shape rather than passing vacuously.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.publisher.publisher import (  # noqa: E402
    _CHRONOS_DIRTY_NODE_FIELDS_SQL,
    _chronos_dirty_edges_since_previous_sql,
    _chronos_dirty_nodes_since_previous_sql,
    _chronos_segment_table_delta_sql,
)
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

# Dirty-set size. Large enough that a per-dirty-row rescan (N * N rows) is far
# above the linear bound, small enough that the pre-fix statement's quadratic
# plan finishes in seconds.
_N = 400

# The pre-fix statements, verbatim apart from comments: the oracle for
# "returns identical rows" and the positive control for the row-count measure.
_PRE_2556_NODE_SQL = """
WITH current_branch AS (
  SELECT current_segment_id
    FROM _chronos_branch_interval_branches
   WHERE branch_id = %s
), previous_point AS (
  SELECT branch_point
    FROM _chronos_branch_interval_segments
   WHERE segment_id = %s
), dirty AS (
  SELECT DISTINCT ON (n.node_id)
         n.node_id, n.subtype, n.attrs, n.source,
         COALESCE(n.deleted, FALSE) AS deleted
    FROM {node_table} n
    JOIN current_branch b
      ON n.writer_segment_id = b.current_segment_id
   ORDER BY n.node_id, n.live_lo DESC
), previous_state AS (
  SELECT DISTINCT ON (p.node_id)
         p.node_id, p.subtype, p.attrs, p.source,
         COALESCE(p.deleted, FALSE) AS deleted
    FROM {node_table} p
    JOIN dirty d
      ON d.node_id = p.node_id
   CROSS JOIN previous_point pp
   WHERE p.live_lo <= pp.branch_point
     AND pp.branch_point < p.live_hi
   ORDER BY p.node_id, p.live_lo DESC
)
SELECT d.node_id, d.subtype, d.deleted, d.attrs, d.source,
       p.subtype AS previous_subtype,
       p.deleted AS previous_deleted,
       p.attrs AS previous_attrs,
       p.source AS previous_source
  FROM dirty d
  LEFT JOIN previous_state p
    ON p.node_id = d.node_id
 WHERE p.node_id IS NULL
    OR d.deleted IS DISTINCT FROM p.deleted
    OR (
         d.deleted = FALSE
     AND (
          d.subtype IS DISTINCT FROM p.subtype
       OR d.attrs IS DISTINCT FROM p.attrs
       OR d.source IS DISTINCT FROM p.source
     )
    )
"""

_PRE_2556_EDGE_SQL = """
WITH current_branch AS (
  SELECT current_segment_id
    FROM _chronos_branch_interval_branches
   WHERE branch_id = %s
), previous_point AS (
  SELECT branch_point
    FROM _chronos_branch_interval_segments
   WHERE segment_id = %s
), dirty AS (
  SELECT DISTINCT ON (e.edge_id)
         e.edge_id, e.edge_type, e.attrs,
         COALESCE(e.deleted, FALSE) AS deleted
    FROM {edge_table} e
    JOIN current_branch b
      ON e.writer_segment_id = b.current_segment_id
   ORDER BY e.edge_id, e.live_lo DESC
), previous_state AS (
  SELECT DISTINCT ON (p.edge_id)
         p.edge_id, p.edge_type, p.attrs,
         COALESCE(p.deleted, FALSE) AS deleted
    FROM {edge_table} p
    JOIN dirty d
      ON d.edge_id = p.edge_id
   CROSS JOIN previous_point pp
   WHERE p.live_lo <= pp.branch_point
     AND pp.branch_point < p.live_hi
   ORDER BY p.edge_id, p.live_lo DESC
)
SELECT d.edge_id, d.edge_type
  FROM dirty d
  LEFT JOIN previous_state p
    ON p.edge_id = d.edge_id
 WHERE p.edge_id IS NULL
    OR d.deleted IS DISTINCT FROM p.deleted
    OR (
         d.deleted = FALSE
     AND (
          d.edge_type IS DISTINCT FROM p.edge_type
       OR d.attrs IS DISTINCT FROM p.attrs
     )
    )
"""

# Planner settings that leave an unmaterialized nested loop as the only join
# the planner will pick. This is the choice an underestimate leads to: when
# the planner believes the dirty set holds about one row, re-running the inner
# side once per dirty row looks cheapest and caching it looks wasteful. With
# the real 6,302 dirty rows that is quadratic. Forcing it makes the measure
# independent of cost-model details that vary between PostgreSQL versions.
_WORST_CASE_JOINS = (
    "SET LOCAL enable_hashjoin = off",
    "SET LOCAL enable_mergejoin = off",
    "SET LOCAL enable_material = off",
)

# Rows-processed ceiling, as a multiple of the node table's row count.
_LINEAR_FACTOR = 10


def _physical_tables(conn: Any) -> dict[str, str]:
    rows = conn.execute(
        "SELECT table_name, physical_table FROM _chronos_branch_tables"
        " WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
        " AND backend = 'interval'"
    ).fetchall()
    return {str(name): str(physical) for name, physical in rows}


def _canonical(rows: list[tuple[Any, ...]]) -> list[str]:
    return sorted(json.dumps(list(row), sort_keys=True, default=str) for row in rows)


def _rows_processed(plan: dict[str, Any]) -> int:
    """Rows every plan node produced or discarded, summed over all its loops.

    Counting discarded rows too means a scan that reads a whole table to keep
    one row is charged for the whole table. A quadratic plan (the inner side
    of a nested loop re-read once per dirty row) shows up as about
    dirty-rows times inner-rows; a linear plan stays a small multiple of the
    table size.
    """
    loops = int(plan.get("Actual Loops", 0))
    per_loop = int(plan.get("Actual Rows", 0))
    for removed in (
        "Rows Removed by Filter",
        "Rows Removed by Join Filter",
        "Rows Removed by Index Recheck",
    ):
        per_loop += int(plan.get(removed, 0))
    total = per_loop * loops
    for child in plan.get("Plans", ()):
        total += _rows_processed(child)
    return total


def _explain_rows_processed(
    conn: Any,
    statement: sql.Composable,
    params: tuple[Any, ...],
    *,
    worst_case_joins: bool,
) -> tuple[int, dict[str, Any]]:
    with conn.transaction():
        if worst_case_joins:
            for setting in _WORST_CASE_JOINS:
                conn.execute(setting)
        (payload,) = conn.execute(
            sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) ") + statement, params
        ).fetchone()
    if isinstance(payload, str):
        payload = json.loads(payload)
    plan = payload[0]["Plan"]
    return _rows_processed(plan), plan


def _plant_interval_edge_cases(
    conn: Any, tables: dict[str, str], *, previous_segment_id: int, sfx: str
) -> dict[str, str]:
    """Insert rows whose classification depends on each part of the lookup.

    Written straight into the interval tables so each key has exactly the
    intervals named, relative to the previous branch point ``bp``. Every
    dirty row is in the current writer segment. Each case is chosen so one
    wrong lookup condition flips its result:

    - ``gap``: an interval that closed before ``bp`` and nothing after it.
      Same content as the dirty row, so it is dirty only because no row was
      visible at ``bp``. Catches dropping ``bp < live_hi``.
    - ``lo_eq``: the previous interval starts exactly at ``bp``, same content.
      Not dirty. Catches ``live_lo <= bp`` weakened to ``<``.
    - ``overlap``: two earlier intervals both cover ``bp``; the later one
      matches the dirty row, the earlier one does not. Not dirty. Catches
      ``ORDER BY live_lo ASC``.
    - ``subtype`` / ``source`` (nodes) and ``edge_type`` (edges): only that
      column differs. Dirty. Catches dropping that comparison.
    """
    (bp,) = conn.execute(
        "SELECT branch_point FROM _chronos_branch_interval_segments"
        " WHERE segment_id = %s",
        (previous_segment_id,),
    ).fetchone()
    (current,) = conn.execute(
        "SELECT current_segment_id FROM _chronos_branch_interval_branches"
        " WHERE branch_id = 'default'"
    ).fetchone()
    node_table = sql.Identifier(tables["okg.graph_nodes"])
    edge_table = sql.Identifier(tables["okg.graph_edges"])
    (top, open_hi) = conn.execute(
        sql.SQL("SELECT max(live_lo), max(live_hi) FROM {}").format(node_table)
    ).fetchone()
    assert bp > 100 and current != previous_segment_id, (bp, current)
    assert top > bp and open_hi > top + 100, (bp, top, open_hi)

    ids = {
        case: f"t:{sfx}:plant:{case}"
        for case in ("gap", "lo_eq", "overlap", "subtype", "source")
    }
    # (key, subtype, attrs, source, live_lo, live_hi, writer_segment_id)
    node_rows = [
        (ids["gap"], "ticket", {"k": "gap"}, "default", bp - 20, bp - 10, None),
        (ids["gap"], "ticket", {"k": "gap"}, "default", top + 1, open_hi, current),
        (ids["lo_eq"], "ticket", {"k": "lo"}, "default", bp, open_hi, None),
        (ids["lo_eq"], "ticket", {"k": "lo"}, "default", top + 2, open_hi, current),
        (ids["overlap"], "ticket", {"k": "a1"}, "default", bp - 20, open_hi, None),
        (ids["overlap"], "ticket", {"k": "a2"}, "default", bp - 10, open_hi, None),
        (ids["overlap"], "ticket", {"k": "a2"}, "default", top + 3, open_hi, current),
        (ids["subtype"], "ticket", {"k": "st"}, "default", bp - 10, open_hi, None),
        (ids["subtype"], "person", {"k": "st"}, "default", top + 4, open_hi, current),
        (ids["source"], "ticket", {"k": "sr"}, "default", bp - 10, open_hi, None),
        (ids["source"], "ticket", {"k": "sr"}, "other", top + 5, open_hi, current),
    ]
    for key, subtype, attrs, source, lo, hi, writer in node_rows:
        conn.execute(
            sql.SQL(
                "INSERT INTO {} (node_id, subtype, attrs, source, live_lo, live_hi,"
                " writer_segment_id, deleted) VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE)"
            ).format(node_table),
            (
                key,
                subtype,
                json.dumps(attrs),
                source,
                lo,
                hi,
                writer if writer is not None else previous_segment_id,
            ),
        )

    # edge_id is a bigint. Negative ids keep clear of the ids the store mints.
    edge_base = -(int(sfx, 16) * 10)
    edge_ids = {
        case: str(edge_base - offset)
        for offset, case in enumerate(("gap", "lo_eq", "overlap", "edge_type"), 1)
    }
    # (key, edge_type, attrs, live_lo, live_hi, writer_segment_id)
    edge_rows = [
        (edge_ids["gap"], "reported_by", {"k": "gap"}, bp - 20, bp - 10, None),
        (edge_ids["gap"], "reported_by", {"k": "gap"}, top + 1, open_hi, current),
        (edge_ids["lo_eq"], "reported_by", {"k": "lo"}, bp, open_hi, None),
        (edge_ids["lo_eq"], "reported_by", {"k": "lo"}, top + 2, open_hi, current),
        (edge_ids["overlap"], "reported_by", {"k": "a1"}, bp - 20, open_hi, None),
        (edge_ids["overlap"], "reported_by", {"k": "a2"}, bp - 10, open_hi, None),
        (edge_ids["overlap"], "reported_by", {"k": "a2"}, top + 3, open_hi, current),
        (edge_ids["edge_type"], "reported_by", {"k": "et"}, bp - 10, open_hi, None),
        (edge_ids["edge_type"], "assigned_to", {"k": "et"}, top + 4, open_hi, current),
    ]
    for key, edge_type, attrs, lo, hi, writer in edge_rows:
        conn.execute(
            sql.SQL(
                "INSERT INTO {} (edge_id, src, dst, edge_type, attrs, source, live_lo,"
                " live_hi, writer_segment_id, deleted)"
                " VALUES (%s, %s, %s, %s, %s, 'default', %s, %s, %s, FALSE)"
            ).format(edge_table),
            (
                int(key),
                ids["gap"],
                ids["lo_eq"],
                edge_type,
                json.dumps(attrs),
                lo,
                hi,
                writer if writer is not None else previous_segment_id,
            ),
        )
    return {
        **{f"node/{case}": key for case, key in ids.items()},
        **{f"edge/{case}": key for case, key in edge_ids.items()},
    }


@pytest.fixture
def dirty_scope_after_checkpoint(
    clean_runtime: str, request: pytest.FixtureRequest
) -> Iterator[dict[str, Any]]:
    """A published graph, then a second write whose dirty set is about _N rows.

    The second write covers every case the dirty predicate distinguishes:
    attribute changes, unchanged restatements, new nodes, deletions and a
    resurrection (deleted before the checkpoint, live again after it), plus
    changed, new and deleted edges.
    """
    sfx = uuid.uuid4().hex[:8]
    person = f"p:{sfx}"
    tickets = [f"t:{sfx}:{i:04d}" for i in range(_N)]
    revived = f"t:{sfx}:revived"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        tables = _physical_tables(conn)
        assert set(tables) == {"okg.graph_nodes", "okg.graph_edges"}, tables
        # Before the first write: autovacuum must not analyze these tables at
        # a moment of its choosing mid-test, which would make the plans this
        # test measures depend on timing. The teardown turns it back on.
        for physical in tables.values():
            conn.execute(
                sql.SQL("ALTER TABLE {} SET (autovacuum_enabled = false)").format(
                    sql.Identifier(physical)
                )
            )
        request.addfinalizer(lambda: _restore_table_settings(clean_runtime, tables))
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(person, "person", {}, {"id": person}, {"v": 1})]
            + [
                NodeFact(
                    revived, "ticket", {"status": "old"}, {"id": revived}, {"v": 1}
                )
            ]
            + [
                NodeFact(t, "ticket", {"status": "old"}, {"id": t}, {"v": 1})
                for t in tickets
            ]
            + [
                EdgeFact(
                    t,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": f"e:{t}"},
                    source_revision={"v": 1},
                )
                for t in tickets
            ],
            source="default",
            strict_endpoints=True,
        )
        store.apply_facts(
            [NodeFact(revived, "ticket", {}, {"id": revived}, {"v": 2}, op="R")],
            source="default",
            strict_endpoints=True,
        )
    publisher = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    assert publisher.cycle()["status"] == "published"

    changed = tickets[: _N // 2]
    restated = tickets[_N // 2 : (3 * _N) // 4]
    deleted = tickets[(3 * _N) // 4 : (3 * _N) // 4 + 10]
    new = [f"t:{sfx}:new:{i:04d}" for i in range(_N // 4)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        previous = store.latest_generation(deployment_name="default")
        assert previous is not None
        store.apply_facts(
            [
                NodeFact(t, "ticket", {"status": "new"}, {"id": t}, {"v": 2})
                for t in changed
            ]
            + [
                NodeFact(t, "ticket", {"status": "old"}, {"id": t}, {"v": 2})
                for t in restated
            ]
            + [NodeFact(t, "ticket", {}, {"id": t}, {"v": 2}, op="R") for t in deleted]
            + [
                NodeFact(t, "ticket", {"status": "new"}, {"id": t}, {"v": 1})
                for t in new
            ]
            + [
                NodeFact(
                    revived, "ticket", {"status": "back"}, {"id": revived}, {"v": 3}
                )
            ]
            + [
                EdgeFact(
                    t,
                    person,
                    "reported_by",
                    attrs={"weight": 2},
                    source_record_id={"id": f"e:{t}"},
                    source_revision={"v": 2},
                )
                for t in changed
            ]
            + [
                EdgeFact(
                    t,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": f"e:{t}"},
                    source_revision={"v": 1},
                )
                for t in new
            ],
            source="default",
            strict_endpoints=True,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        planted = _plant_interval_edge_cases(
            conn, tables, previous_segment_id=int(str(previous.ref)), sfx=sfx
        )
    yield {
        "dsn": clean_runtime,
        "previous": previous,
        "tables": tables,
        "changed": changed,
        "restated": restated,
        "deleted": deleted,
        "new": new,
        "revived": revived,
        "planted": planted,
        "person": person,
    }


def _restore_table_settings(dsn: str, tables: dict[str, str]) -> None:
    """Undo this module's table options and forced statistics.

    Table options and column statistics settings survive the per-test
    TRUNCATE; later tests on this database must plan as usual.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        for physical in tables.values():
            conn.execute(
                sql.SQL("ALTER TABLE {} RESET (autovacuum_enabled)").format(
                    sql.Identifier(physical)
                )
            )
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {} ALTER COLUMN writer_segment_id RESET (n_distinct)"
                ).format(sql.Identifier(physical))
            )
            # Replace any forced statistics with ordinary ones.
            conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(physical)))


def test_lateral_dirty_scope_returns_the_pre_fix_rows(
    dirty_scope_after_checkpoint: dict[str, Any],
) -> None:
    scope = dirty_scope_after_checkpoint
    node_table = scope["tables"]["okg.graph_nodes"]
    edge_table = scope["tables"]["okg.graph_edges"]
    params = ("default", str(scope["previous"].ref))
    with psycopg.connect(scope["dsn"], autocommit=True) as conn:
        new_nodes = conn.execute(
            _chronos_dirty_nodes_since_previous_sql(node_table), params
        ).fetchall()
        old_nodes = conn.execute(
            sql.SQL(_PRE_2556_NODE_SQL).format(node_table=sql.Identifier(node_table)),
            params,
        ).fetchall()
        new_edges = conn.execute(
            _chronos_dirty_edges_since_previous_sql(edge_table), params
        ).fetchall()
        old_edges = conn.execute(
            sql.SQL(_PRE_2556_EDGE_SQL).format(edge_table=sql.Identifier(edge_table)),
            params,
        ).fetchall()

    # The node rows also carry payload_changed (issue #2941); the first nine
    # columns are the pre-fix statement's.
    assert _canonical([row[:9] for row in new_nodes]) == _canonical(old_nodes)
    # The edge rows also carry the delta-count classification (issue #2941):
    # deleted, was_live, payload_changed. The ids and types are the rows the
    # pre-fix statement returned.
    assert _canonical([row[:2] for row in new_edges]) == _canonical(old_edges)
    edge_class = {int(row[0]): row[2:] for row in new_edges}
    from okg.substrate.graph_store import _edge_id

    for t in scope["changed"]:
        # Live before and now, weight 1 -> 2: an update.
        assert edge_class[int(_edge_id(t, "reported_by", scope["person"]))] == (
            False, True, True,
        )
    for t in scope["new"]:
        assert edge_class[int(_edge_id(t, "reported_by", scope["person"]))] == (
            False, False, True,
        )

    # The comparison only means something if every case the predicate
    # distinguishes is present and classified the way the publisher reads it.
    # Columns: node_id, subtype, deleted, attrs, source, previous_subtype,
    # previous_deleted, previous_attrs, previous_source.
    by_id = {str(row[0]): row for row in new_nodes}
    for node_id in scope["changed"]:
        assert by_id[node_id][3]["status"] == "new"
        assert by_id[node_id][7]["status"] == "old"
    for node_id in scope["deleted"]:
        assert by_id[node_id][2] is True and by_id[node_id][6] is False
    for node_id in scope["new"]:
        assert by_id[node_id][5] is None  # no previous row
    revived = by_id[scope["revived"]]
    assert revived[2] is False and revived[6] is True  # resurrection
    assert not set(scope["restated"]) & set(by_id)  # unchanged: not dirty
    assert len(new_edges) >= len(scope["changed"]) + len(scope["new"])

    # Planted interval cases (see _plant_interval_edge_cases): each one flips
    # if one lookup condition is wrong.
    planted = scope["planted"]
    gap = by_id[planted["node/gap"]]
    assert gap[5] is None, gap  # nothing visible at the branch point
    assert planted["node/lo_eq"] not in by_id  # previous starts exactly at bp
    assert planted["node/overlap"] not in by_id  # later covering interval wins
    subtype = by_id[planted["node/subtype"]]
    assert (subtype[1], subtype[5]) == ("person", "ticket"), subtype
    source = by_id[planted["node/source"]]
    assert (source[4], source[8]) == ("other", "default"), source
    edge_ids = {str(row[0]) for row in new_edges}
    assert planted["edge/gap"] in edge_ids
    assert planted["edge/lo_eq"] not in edge_ids
    assert planted["edge/overlap"] not in edge_ids
    assert planted["edge/edge_type"] in edge_ids


def _estimated_dirty_rows(conn: Any, table: str) -> float:
    (payload,) = conn.execute(
        sql.SQL(
            "EXPLAIN (FORMAT JSON) SELECT 1 FROM {} t"
            " JOIN _chronos_branch_interval_branches b"
            " ON t.writer_segment_id = b.current_segment_id"
            " WHERE b.branch_id = 'default'"
        ).format(sql.Identifier(table))
    ).fetchone()
    if isinstance(payload, str):
        payload = json.loads(payload)
    return float(payload[0]["Plan"]["Plan Rows"])


def _underestimate_dirty_set(conn: Any, tables: tuple[str, ...]) -> None:
    """Make the planner believe the current segment wrote about one row.

    Declaring ``writer_segment_id`` unique and analyzing gives the join to the
    branch pointer a selectivity of one row over the table, the estimate that
    stale statistics produce and that led the pre-fix statement into its
    per-dirty-row rescan.
    """
    for physical in tables:
        conn.execute(
            sql.SQL(
                "ALTER TABLE {} ALTER COLUMN writer_segment_id SET (n_distinct = -1)"
            ).format(sql.Identifier(physical))
        )
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(physical)))


def test_dirty_scope_stays_linear_when_the_planner_misestimates(
    dirty_scope_after_checkpoint: dict[str, Any],
) -> None:
    scope = dirty_scope_after_checkpoint
    node_table = scope["tables"]["okg.graph_nodes"]
    edge_table = scope["tables"]["okg.graph_edges"]
    params = ("default", str(scope["previous"].ref))
    measured: dict[str, int] = {}
    with psycopg.connect(scope["dsn"], autocommit=True) as conn:
        (node_rows,) = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(node_table))
        ).fetchone()
        statements = (
            ("nodes", _chronos_dirty_nodes_since_previous_sql(node_table)),
            ("edges", _chronos_dirty_edges_since_previous_sql(edge_table)),
        )

        def measure(label: str, *, worst_case_joins: bool) -> None:
            for kind, statement in statements:
                rows, plan = _explain_rows_processed(
                    conn, statement, params, worst_case_joins=worst_case_joins
                )
                measured[f"{kind}/{label}"] = rows
                assert rows <= _LINEAR_FACTOR * node_rows, (
                    kind,
                    label,
                    rows,
                    node_rows,
                    json.dumps(plan)[:4000],
                )

        # Whatever statistics the tables carry: autovacuum is off for them
        # from before the first write, but pg_statistic rows left by earlier
        # tests on this database survive TRUNCATE, so this state is not
        # assumed to be "never analyzed". The bound must hold in any state.
        measure("as-found", worst_case_joins=False)
        measure("as-found/worst-case-joins", worst_case_joins=True)

        # The state that stalled the lane: the planner thinks the current
        # segment wrote about one row, while it wrote hundreds.
        _underestimate_dirty_set(conn, (node_table, edge_table))
        estimate = _estimated_dirty_rows(conn, node_table)
        assert estimate <= 5, (
            f"dirty-set estimate is {estimate}; the test needs an underestimate"
        )
        measure("underestimated", worst_case_joins=False)
        measure("underestimated/worst-case-joins", worst_case_joins=True)

        # Positive control: with the join choice the underestimate leads to
        # (an unmaterialized nested loop), the pre-fix node statement re-runs
        # the previous-state scan per dirty row and exceeds the bound. The
        # join choice is forced so the control does not depend on cost-model
        # details; the planner's unforced plan is recorded for diagnosis.
        pre_fix = sql.SQL(_PRE_2556_NODE_SQL).format(
            node_table=sql.Identifier(node_table)
        )
        planned_rows, _ = _explain_rows_processed(
            conn, pre_fix, params, worst_case_joins=False
        )
        measured["pre-fix-nodes/underestimated"] = planned_rows
        old_rows, old_plan = _explain_rows_processed(
            conn, pre_fix, params, worst_case_joins=True
        )
    assert old_rows > _LINEAR_FACTOR * node_rows, (
        old_rows,
        node_rows,
        measured,
        json.dumps(old_plan)[:4000],
    )


def _plan_nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", ()):
        yield from _plan_nodes(child)


def test_hint_coverage_check_reads_the_segment_index_only(
    dirty_scope_after_checkpoint: dict[str, Any],
) -> None:
    """The dirty-scope hint check scans only the (writer_segment_id, key) index.

    ``segment_keys_outside_hint`` runs on every publish that brings a hint,
    so it must read the current segment's keys from the index alone, never
    the heap, and answer both ways correctly: a hint listing every key in
    the segment is covered; one missing a single key is not.
    """
    from okg.substrate.publisher.dirty_hint_coverage import (
        keys_outside_hint_query,
        segment_keys_outside_hint,
        segment_signature_query,
        segment_write_signature,
    )

    scope = dirty_scope_after_checkpoint
    with psycopg.connect(scope["dsn"], autocommit=True) as conn:
        # Index-only scans need the visibility map; autovacuum is off here.
        for physical in scope["tables"].values():
            conn.execute(
                sql.SQL("VACUUM (ANALYZE) {}").format(sql.Identifier(physical))
            )
        (segment,) = conn.execute(
            "SELECT current_segment_id FROM _chronos_branch_interval_branches"
            " WHERE branch_id = 'default'"
        ).fetchone()
        keys: dict[str, list[Any]] = {}
        for label, table, key, key_type in (
            ("nodes", "okg.graph_nodes", "node_id", "text"),
            ("edges", "okg.graph_edges", "edge_id", "bigint"),
        ):
            physical = scope["tables"][table]
            keys[label] = [
                row[0]
                for row in conn.execute(
                    sql.SQL(
                        "SELECT DISTINCT {} FROM {} WHERE writer_segment_id = %s"
                    ).format(sql.Identifier(key), sql.Identifier(physical)),
                    (segment,),
                ).fetchall()
            ]
            assert keys[label], label
            (plan_doc,) = conn.execute(
                sql.SQL("EXPLAIN (FORMAT JSON) ")
                + keys_outside_hint_query(
                    physical_table=physical, key_column=key, key_type=key_type,
                ),
                {"segment": segment, "hint": keys[label]},
            ).fetchone()
            on_table = [
                node for node in _plan_nodes(plan_doc[0]["Plan"])
                if node.get("Relation Name") == physical
            ]
            assert on_table, json.dumps(plan_doc)[:4000]
            assert all(
                node["Node Type"] == "Index Only Scan" for node in on_table
            ), json.dumps(plan_doc)[:4000]
            # The write signature must read the heap (see its module), but it
            # can reach the segment's rows through the segment index. On this
            # few-hundred-row fixture the planner rightly prefers a sequential
            # scan, so rule that out to prove an index path exists.
            with conn.transaction():
                conn.execute("SET LOCAL enable_seqscan = off")
                (plan_doc,) = conn.execute(
                    sql.SQL("EXPLAIN (FORMAT JSON) ")
                    + segment_signature_query(physical),
                    {"segment": segment},
                ).fetchone()
            nodes_in_plan = list(_plan_nodes(plan_doc[0]["Plan"]))
            assert not any(
                node["Node Type"] == "Seq Scan" for node in nodes_in_plan
            ), json.dumps(plan_doc)[:4000]
            assert any(
                "writer_segme" in str(node.get("Index Name", ""))
                for node in nodes_in_plan
            ), json.dumps(plan_doc)[:4000]

        covered = segment_keys_outside_hint(
            conn, branch_name="default",
            node_ids=keys["nodes"], edge_ids=keys["edges"],
        )
        missing_node = segment_keys_outside_hint(
            conn, branch_name="default",
            node_ids=keys["nodes"][1:], edge_ids=keys["edges"],
        )
        missing_edge = segment_keys_outside_hint(
            conn, branch_name="default",
            node_ids=keys["nodes"], edge_ids=keys["edges"][1:],
        )
        before = segment_write_signature(conn, branch_name="default")
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(scope["changed"][0], "ticket", {"status": "later"},
                      {"id": scope["changed"][0]}, {"v": 9})],
            source="default",
            strict_endpoints=True,
        )
        after = segment_write_signature(conn, branch_name="default")
    assert covered == {
        "checked": True, "segment_id": segment, "nodes": False, "edges": False,
    }
    assert (missing_node["nodes"], missing_node["edges"]) == (True, False)
    assert (missing_edge["nodes"], missing_edge["edges"]) == (False, True)
    # Chronos rewrites a key already in the segment in place, so the row
    # count stays the same; the version hash still moves.
    assert before["segment_id"] == after["segment_id"] == segment
    assert after["nodes"]["rows"] == before["nodes"]["rows"]
    assert after["nodes"]["versions"] != before["nodes"]["versions"]
    assert after["edges"] == before["edges"]


def _python_changed_fields(row: tuple[Any, ...]) -> list[str]:
    """The changed-field rules the scan applied in Python before issue
    #2941 moved them into the statement (``fields_in_sql``)."""
    (_node_id, subtype, deleted, attrs, source, previous_subtype,
     previous_deleted, previous_attrs, previous_source, _payload) = row
    fields: set[str] = set()
    if previous_subtype is None:
        fields.add("__new__")
        fields.update(dict(attrs or {}).keys())
    elif int(deleted or 0) != int(previous_deleted or 0):
        if int(deleted or 0) != 0:
            fields.add("__deleted__")
            fields.update(dict(previous_attrs or {}).keys())
        else:
            fields.add("__new__")
            fields.update(dict(attrs or {}).keys())
    else:
        if str(previous_subtype) != str(subtype):
            fields.add("__subtype__")
        if str(previous_source or "") != str(source or ""):
            fields.add("__source__")
        current, prior = dict(attrs or {}), dict(previous_attrs or {})
        for key in set(current) | set(prior):
            if current.get(key) != prior.get(key):
                fields.add(str(key))
    return sorted(fields)


def test_changed_fields_in_sql_match_the_python_rules(
    dirty_scope_after_checkpoint: dict[str, Any],
) -> None:
    """Issue #2941: the statement the scan runs (``fields_in_sql``) returns,
    for every dirty node -- changed, deleted, new, revived and the planted
    interval cases -- the same rows and the same changed-field list the scan
    derived in Python from the two attribute documents."""
    scope = dirty_scope_after_checkpoint
    node_table = scope["tables"]["okg.graph_nodes"]
    params = ("default", str(scope["previous"].ref))
    with psycopg.connect(scope["dsn"], autocommit=True) as conn:
        full = conn.execute(
            _chronos_dirty_nodes_since_previous_sql(node_table), params
        ).fetchall()
        lean = conn.execute(
            _chronos_dirty_nodes_since_previous_sql(node_table, fields_in_sql=True),
            params,
        ).fetchall()
    expected = {
        str(row[0]): (row[1], row[2], row[5], row[6], row[9], _python_changed_fields(row))
        for row in full
    }
    got = {
        str(row[0]): (row[1], row[2], row[3], row[4], row[5], sorted(row[6]))
        for row in lean
    }
    assert got == expected
    # Every case the fixture plants is present.
    assert set(scope["changed"]) | set(scope["deleted"]) | set(scope["new"]) <= set(got)
    assert scope["revived"] in got


_NO_PREVIOUS = (None, None, None, None)

# (current (subtype, deleted, attrs, source), previous (same) or _NO_PREVIOUS,
#  expected SQL fields, or None for "same as the Python rules")
_FIELD_CASES: list[tuple[tuple[Any, ...], tuple[Any, ...], list[str] | None]] = [
    (("doc", False, {"k": 1}, "s"), _NO_PREVIOUS, None),
    (("doc", False, None, "s"), _NO_PREVIOUS, None),
    (("doc", True, {"k": 1}, "s"), ("doc", False, {"k": 1, "j": 2}, "s"), None),
    (("doc", False, {"k": 1}, "s"), ("doc", True, {"old": 1}, "s"), None),
    (("doc", False, {"k": 1}, "s"), ("doc", False, {"k": 1, "j": 2}, "s"), None),  # key removed
    (("doc", False, {"k": None}, "s"), ("doc", False, {}, "s"), None),  # null == missing
    (("doc", False, {}, "s"), ("doc", False, {"k": 1}, "s"), None),
    (("doc", False, {"k": {"a": [1, 2]}}, "s"), ("doc", False, {"k": {"a": [2, 1]}}, "s"), None),
    (("doc", False, {"k": {"a": 1, "b": 2}}, "s"), ("doc", False, {"k": {"b": 2, "a": 1}}, "s"), None),
    (("note", False, {"k": 1}, "s"), ("doc", False, {"k": 1}, "s"), None),  # subtype only
    (("doc", False, {"k": 1}, "t"), ("doc", False, {"k": 1}, None), None),  # source only
    (("doc", False, {"k": 1.0}, "s"), ("doc", False, {"k": 1}, "s"), None),
    (("doc", False, {"k": "x"}, "s"), ("doc", False, {"k": "y"}, "s"), None),
    # Documented differences: jsonb tells these apart, Python did not.
    (("doc", False, {"k": True}, "s"), ("doc", False, {"k": 1}, "s"), ["k"]),
    (("doc", False, {"k": False}, "s"), ("doc", False, {"k": 0}, "s"), ["k"]),
    (("doc", False, {"k": [True]}, "s"), ("doc", False, {"k": [1]}, "s"), ["k"]),
    # Attributes that are not an object read as empty (Python raised on some).
    (("doc", False, [1, 2], "s"), ("doc", False, {"k": 1}, "s"), ["k"]),
    (("doc", False, 7, "s"), _NO_PREVIOUS, ["__new__"]),
]


def test_changed_field_rules_match_python_case_by_case(clean_runtime: str) -> None:
    """Issue #2941: the SQL changed-field rules, case by case, against the
    Python rules they replace (null against a missing key, a removed key,
    nested values, key order, subtype- and source-only changes), plus the
    stated differences: jsonb tells true from 1 and false from 0, and an
    attribute value that is not an object reads as empty."""
    from psycopg.types.json import Jsonb

    statement = (
        "SELECT " + _CHRONOS_DIRTY_NODE_FIELDS_SQL + """
          FROM (SELECT %s::text AS subtype, %s::boolean AS deleted,
                       %s::jsonb AS attrs, %s::text AS source) d
         CROSS JOIN (SELECT %s::text AS subtype, %s::boolean AS deleted,
                            %s::jsonb AS attrs, %s::text AS source) p
        """
    )

    def jsonb(value: Any) -> Any:
        return None if value is None else Jsonb(value)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for current, previous, expected in _FIELD_CASES:
            (sub, dele, attrs, src), (psub, pdele, pattrs, psrc) = current, previous
            (fields,) = conn.execute(
                statement,
                (sub, dele, jsonb(attrs), src, psub, pdele, jsonb(pattrs), psrc),
            ).fetchone()
            got = sorted(fields)
            if expected is None:
                expected = _python_changed_fields(
                    (None, sub, dele, attrs, src, psub, pdele, pattrs, psrc, None)
                )
            assert got == sorted(expected), (current, previous, got, expected)


# The node delta-count statement before issues #3593 and #2941: each dirty
# key's previous state came from a set-at-a-time join (``previous_state``),
# the shape #2556 removed from the dirty-scope statements. A cold build hit
# the 900 s statement timeout here.
_PRE_3593_NODE_COUNT_SQL = """
WITH current_seg AS (
  SELECT s.live_lo, s.live_hi
    FROM _chronos_branch_interval_segments s
    JOIN _chronos_branch_interval_branches b
      ON b.current_segment_id = s.segment_id
   WHERE b.branch_id = %s
), previous_point AS (
  SELECT branch_point
    FROM _chronos_branch_interval_segments
   WHERE segment_id = %s
), dirty AS (
  SELECT DISTINCT ON (t.node_id)
         t.node_id AS graph_key, COALESCE(t.deleted, FALSE) AS deleted,
         t.subtype AS subtype, t.attrs AS attrs
    FROM {node_table} t
    JOIN current_seg s ON t.live_lo >= s.live_lo AND t.live_hi <= s.live_hi
   ORDER BY t.node_id, t.live_lo DESC
), previous_state AS (
  SELECT DISTINCT ON (t.node_id)
         t.node_id AS graph_key, COALESCE(t.deleted, FALSE) AS deleted,
         t.subtype AS subtype, t.attrs AS attrs
    FROM {node_table} t
    JOIN dirty d ON d.graph_key = t.node_id
   CROSS JOIN previous_point p
   WHERE t.live_lo <= p.branch_point AND p.branch_point < t.live_hi
   ORDER BY t.node_id, t.live_lo DESC
), classified AS (
  SELECT d.deleted AS now_deleted,
         (p.graph_key IS NOT NULL AND NOT p.deleted) AS was_live,
         (d.subtype IS DISTINCT FROM p.subtype
          OR d.attrs IS DISTINCT FROM p.attrs) AS payload_changed
    FROM dirty d
    LEFT JOIN previous_state p ON p.graph_key = d.graph_key
)
SELECT COUNT(*) FILTER (WHERE NOT now_deleted AND NOT was_live)::bigint,
       COUNT(*) FILTER (WHERE now_deleted AND was_live)::bigint,
       COUNT(*) FILTER (
         WHERE NOT now_deleted AND was_live AND payload_changed
       )::bigint
  FROM classified
"""


def test_delta_count_statement_stays_linear_and_counts_as_before(
    dirty_scope_after_checkpoint: dict[str, Any],
) -> None:
    """Issues #3593 and #2941: the delta-count statement looks up each dirty
    key's previous state with one probe, so it stays linear whatever the
    planner believes, and it counts exactly what the pre-fix statement
    counted, whole-segment and keyed. Under the join choice that misled the
    planner, the pre-fix statement exceeds the bound (positive control)."""
    scope = dirty_scope_after_checkpoint
    node_table = scope["tables"]["okg.graph_nodes"]
    edge_table = scope["tables"]["okg.graph_edges"]
    previous = str(scope["previous"].ref)

    def statement(table: str, kind: str, keys: list[Any] | None = None):
        return _chronos_segment_table_delta_sql(
            physical_table=table,
            key_column=f"{kind}_id",
            compare_columns=(
                ("subtype", "attrs") if kind == "node" else ("edge_type", "attrs")
            ),
            branch_name="default",
            previous_segment_id=previous,
            key_values=keys,
        )

    with psycopg.connect(scope["dsn"], autocommit=True) as conn:
        (node_rows,) = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(node_table))
        ).fetchone()
        new_sql, params = statement(node_table, "node")
        pre_fix = sql.SQL(_PRE_3593_NODE_COUNT_SQL).format(
            node_table=sql.Identifier(node_table)
        )
        assert conn.execute(new_sql, params).fetchone() == conn.execute(
            pre_fix, params,
        ).fetchone()
        # The keyed form (the scoped count) agrees with the whole-segment form
        # over the dirty keys, for nodes and edges.
        for table, kind in ((node_table, "node"), (edge_table, "edge")):
            whole_sql, whole_params = statement(table, kind)
            whole = conn.execute(whole_sql, whole_params).fetchone()
            keys = [
                row[0] for row in conn.execute(
                    sql.SQL(
                        "SELECT DISTINCT {} FROM {} t"
                        " JOIN _chronos_branch_interval_segments s"
                        "   ON s.segment_id = ("
                        "     SELECT current_segment_id"
                        "       FROM _chronos_branch_interval_branches"
                        "      WHERE branch_id = 'default')"
                        " WHERE t.live_lo >= s.live_lo"
                    ).format(sql.Identifier(f"{kind}_id"), sql.Identifier(table))
                ).fetchall()
            ]
            keyed_sql, keyed_params = statement(table, kind, keys)
            assert conn.execute(keyed_sql, keyed_params).fetchone() == whole, kind
        expected_added = len(scope["new"]) + 1  # the revived node
        added, retracted, updated = conn.execute(new_sql, params).fetchone()
        assert added >= expected_added
        assert retracted >= len(scope["deleted"])
        assert updated >= len(scope["changed"])

        _underestimate_dirty_set(conn, (node_table, edge_table))
        for worst_case_joins in (False, True):
            for table, kind in ((node_table, "node"), (edge_table, "edge")):
                stmt, stmt_params = statement(table, kind)
                rows, plan = _explain_rows_processed(
                    conn, stmt, stmt_params, worst_case_joins=worst_case_joins,
                )
                assert rows <= _LINEAR_FACTOR * node_rows, (
                    kind, worst_case_joins, rows, node_rows,
                    json.dumps(plan)[:4000],
                )
        old_rows, old_plan = _explain_rows_processed(
            conn, pre_fix, params, worst_case_joins=True,
        )
    assert old_rows > _LINEAR_FACTOR * node_rows, (
        old_rows, node_rows, json.dumps(old_plan)[:4000],
    )
