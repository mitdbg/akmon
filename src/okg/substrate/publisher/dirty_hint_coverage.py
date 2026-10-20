"""Does a caller's changed-row hint list every row in the branch head?

An ingest run hands the publisher a dirty-scope hint: the node and edge keys
its sources wrote. The publisher may use it instead of scanning the branch
head's current write segment (every row written since the last checkpoint).
The hint lists only the sources' own writes. Any other write in that segment
-- `okg provision`'s views-registry projection before the run, a runtime
admission or a record write during it -- is missing from the hint, and a
publish that trusted it would stamp a node or edge count that is too low
(on okg-workspace, 53,710 recorded against 53,718 visible).

``segment_keys_outside_hint`` answers that question with one anti-join per
graph table over the ``(writer_segment_id, key)`` index. It plans as an
Index Only Scan, but PostgreSQL can skip the heap only for pages the
visibility map marks all-visible, which needs a VACUUM: on a segment the
ingest run just wrote, most rows still take a heap fetch. The cost is one
index pass plus up to one heap fetch per row written since the last
checkpoint -- the same order as the scan the hint replaces, and cheaper
once autovacuum has run.

``segment_write_signature`` fingerprints every row in the segment (a row
count plus a hash of each row version's ``(ctid, xmin)``); the publisher
compares two readings to learn whether anything was written between them.
It always reads the heap, reaching the segment's rows through the segment
index. A table rewrite (VACUUM FULL, CLUSTER, pg_repack) changes every
``ctid`` and so reads as a write: a publish running across one is refused
loudly, never passed silently.

Both raise on any database error: the caller must fail the publish rather
than trust a hint or a reading it could not check.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from psycopg import sql

# The planner reads the segment's keys from the ``(writer_segment_id, key)``
# index (Index Only Scan; heap fetches for pages not yet all-visible, see the
# module docstring) and hashes the hint's keys for the anti-join.
_KEYS_OUTSIDE_HINT_SQL = """
SELECT EXISTS (
  SELECT 1
    FROM {physical} AS written
   WHERE written.writer_segment_id = %(segment)s
     AND NOT EXISTS (
       SELECT 1
         FROM unnest(%(hint)s::{key_type}[]) AS hinted(key)
        WHERE hinted.key = written.{key}
     )
)
"""

# The same check over several segments: every segment the branch wrote since
# its previous generation, when source runs' run-base checkpoints (issue
# #2993) split the head between that generation and now. One probe of the
# ``(writer_segment_id, key)`` index per listed segment.
_KEYS_OUTSIDE_HINT_SEGMENTS_SQL = """
SELECT EXISTS (
  SELECT 1
    FROM {physical} AS written
   WHERE written.writer_segment_id = ANY(%(segments)s::bigint[])
     AND NOT EXISTS (
       SELECT 1
         FROM unnest(%(hint)s::{key_type}[]) AS hinted(key)
        WHERE hinted.key = written.{key}
     )
)
"""


# Chronos rewrites a key already written in the current segment IN PLACE
# (same key and live_lo), so a row count misses updates and retractions. A
# rewritten row is a new tuple version with a new (ctid, xmin), so hashing
# them catches every write. That needs the heap: the segment index finds the
# rows (a bitmap or index scan, never a sequential scan of the table).
_SEGMENT_SIGNATURE_SQL = """
SELECT count(*),
       COALESCE(sum(hashtextextended(ctid::text || ':' || xmin::text, 0)), 0)
  FROM {physical}
 WHERE writer_segment_id = %(segment)s
"""


def segment_signature_query(physical_table: str) -> sql.Composed:
    """One table's segment fingerprint, exposed so a test can EXPLAIN it."""
    return sql.SQL(_SEGMENT_SIGNATURE_SQL).format(
        physical=sql.Identifier(physical_table),
    )


def _graph_tables_and_segment(
    cur: Any, branch_name: str,
) -> tuple[dict[str, str], int] | str:
    cur.execute(
        """
        SELECT table_name, physical_table
          FROM _chronos_branch_tables
         WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
           AND backend = 'interval'
        """
    )
    tables = {str(name): str(physical) for name, physical in cur.fetchall()}
    if set(tables) != {"okg.graph_nodes", "okg.graph_edges"}:
        return "graph_interval_tables_missing"
    cur.execute(
        "SELECT current_segment_id FROM _chronos_branch_interval_branches "
        "WHERE branch_id = %s",
        (branch_name,),
    )
    row = cur.fetchone()
    if row is None:
        return "branch_missing"
    return tables, int(row[0])


def segment_write_signature(
    conn: Any, *, branch_name: str,
) -> dict[str, Any] | None:
    """A fingerprint of every row in the branch head's current segment.

    Equal readings (same segment id, row counts and version hashes) mean
    nothing was written to the branch head in between. ``None`` when the
    branch or its interval graph tables do not exist yet.
    """
    with conn.cursor() as cur:
        found = _graph_tables_and_segment(cur, branch_name)
        if isinstance(found, str):
            return None
        tables, segment_id = found
        signature: dict[str, Any] = {"segment_id": segment_id}
        for label, table in (
            ("nodes", "okg.graph_nodes"), ("edges", "okg.graph_edges"),
        ):
            cur.execute(
                segment_signature_query(tables[table]), {"segment": segment_id},
            )
            rows, versions = cur.fetchone()
            signature[label] = {"rows": int(rows), "versions": str(versions)}
    return signature


def keys_outside_hint_query(
    *, physical_table: str, key_column: str, key_type: str,
    several_segments: bool = False,
) -> sql.Composed:
    """The per-table check, exposed so a test can EXPLAIN it."""
    template = (
        _KEYS_OUTSIDE_HINT_SEGMENTS_SQL if several_segments
        else _KEYS_OUTSIDE_HINT_SQL
    )
    return sql.SQL(template).format(
        physical=sql.Identifier(physical_table),
        key=sql.Identifier(key_column),
        key_type=sql.SQL(key_type),
    )


def segment_keys_outside_hint(
    conn: Any,
    *,
    branch_name: str,
    node_ids: Iterable[str],
    edge_ids: Iterable[int],
    segment_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Which graph tables' current-segment rows the hint does not list.

    Returns ``{"checked": False, "reason": ...}`` only when the branch or its
    interval graph tables do not exist yet (nothing can be written ahead of a
    hint then). Otherwise ``{"checked": True, "segment_id": ..., "nodes":
    bool, "edges": bool}``, where each flag is True when that table has a row
    in the current segment whose key the hint omits.

    ``segment_ids``, when it lists more than one segment, widens the check to
    every row those segments wrote (issue #2993: the segments since the
    previous generation, split by source runs' run-base checkpoints).
    """
    with conn.cursor() as cur:
        found = _graph_tables_and_segment(cur, branch_name)
        if isinstance(found, str):
            return {"checked": False, "reason": found}
        tables, segment_id = found
        outside: dict[str, Any] = {"checked": True, "segment_id": segment_id}
        several = segment_ids is not None and len(segment_ids) > 1
        if several:
            outside["segment_ids"] = [int(item) for item in segment_ids or ()]
        for label, table, key, key_type, hint in (
            ("nodes", "okg.graph_nodes", "node_id", "text",
             sorted({str(node_id) for node_id in node_ids})),
            ("edges", "okg.graph_edges", "edge_id", "bigint",
             sorted({int(edge_id) for edge_id in edge_ids})),
        ):
            cur.execute(
                keys_outside_hint_query(
                    physical_table=tables[table],
                    key_column=key,
                    key_type=key_type,
                    several_segments=several,
                ),
                (
                    {"segments": outside["segment_ids"], "hint": hint}
                    if several else {"segment": segment_id, "hint": hint}
                ),
            )
            outside[label] = bool(cur.fetchone()[0])
    return outside
