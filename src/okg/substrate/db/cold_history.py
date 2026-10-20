"""Online maintenance for Chronos live-front visibility indexes.

Chronos retains every graph version.  The ordinary visibility indexes do not
need to retain every closed version at B-tree cost, though: recent pinned reads
can use a partial index whose predicate is ``live_hi > FLOOR``.  This module
owns the runtime-derived floor, concurrent build/drop protocol, and the durable
manifest consumed by schema reconciliation and doctor.

The open-interval value is intentionally never a constant here.  It is read
from the current ``default`` segment and reported only as an observed store
fact.  The partial-index predicate uses a lineage-derived floor instead.
"""
from __future__ import annotations

import contextvars
from dataclasses import dataclass
from hashlib import sha1
import logging
import re
import shlex
import time
from typing import Any, Callable, Iterable, Mapping

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.errors import SubstrateError
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.support.user_hints import okg_prefix

_log = logging.getLogger(__name__)

HOT_INDEX_MANIFEST_TABLE = "okg.chronos_hot_index_manifest"
DEFAULT_HEADROOM_GENERATIONS = 100
COLD_HISTORY_RECLAIM_ESTIMATE_KIND = (
    "gross_full_index_bytes_before_hot_replacement"
)
COLD_HISTORY_RECLAIM_WARNING = (
    "Activation builds and verifies replacement hot indexes before dropping "
    "the listed full-history B-tree predecessors. Graph history rows remain, "
    "and older-history access relies on retained BRIN and primary-key service. "
    "Replacement indexes remain, so net free space is lower than the gross "
    "full-index bytes and the upgrade may still lack headroom."
)


#: A bare SQL word: an index access method or an operator class name.
_SQL_WORD_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class VisibilityIndexSpec:
    """One OKG-owned interval read index.

    ``rotate`` is false for the all-history containment index used by publisher
    delta scans.  Primary-key, pk_hi, writer-segment, and BM25 indexes are
    Chronos/special-purpose indexes and therefore do not appear here.

    A plain spec is a B-tree over ``columns``; its predicate is
    ``deleted = FALSE`` exactly when it is rotatable. PACT lane-s-redesign-s1
    adds four optional fields for a spec that is not such a B-tree:

    * ``method``: the index access method (``gin``); None means the default.
    * ``opclass``: an operator class applied to every key (``jsonb_path_ops``).
    * ``key_expressions``: ``(column, expression)`` pairs; the key for that
      column is the parenthesised expression instead of the bare column
      (``("edge_id", "(edge_id::text)")``). The column still counts for the
      "does the table have these columns" check.
    * ``predicate``: the spec's own partial-index predicate, independent of
      ``rotate``.

    Such a spec must not be rotatable: the hot-index rotation only knows how
    to rebuild a plain B-tree with ``live_hi > floor`` added. Every field is
    a source constant and is inlined into DDL, so each is validated here.
    """

    suffix: str
    columns: tuple[str, ...]
    rotate: bool = True
    method: str | None = None
    opclass: str | None = None
    key_expressions: tuple[tuple[str, str], ...] = ()
    predicate: str | None = None

    def __post_init__(self) -> None:
        if self.extended and self.rotate:
            raise ValueError(
                f"interval read index spec {self.suffix!r}: a spec with a method, "
                "operator class, key expression or own predicate cannot be "
                "rotatable"
            )
        for word in (self.method, self.opclass):
            if word is not None and not _SQL_WORD_RE.match(word):
                raise ValueError(
                    f"interval read index spec {self.suffix!r}: {word!r} is not "
                    "a bare SQL word"
                )
        for column, expression in self.key_expressions:
            if column not in self.columns:
                raise ValueError(
                    f"interval read index spec {self.suffix!r}: key expression "
                    f"for {column!r}, which is not one of its columns"
                )
            if not (expression.startswith("(") and expression.endswith(")")) or ";" in expression:
                raise ValueError(
                    f"interval read index spec {self.suffix!r}: key expression "
                    f"{expression!r} must be one parenthesised expression"
                )
        if self.predicate is not None and (";" in self.predicate or not self.predicate.strip()):
            raise ValueError(
                f"interval read index spec {self.suffix!r}: bad predicate "
                f"{self.predicate!r}"
            )

    @property
    def extended(self) -> bool:
        """True for a spec that is not a plain B-tree over bare columns."""
        return bool(
            self.method or self.opclass or self.key_expressions
            or self.predicate is not None
        )

    def index_predicate(self) -> str | None:
        """The partial-index predicate: the spec's own, else ``deleted =
        FALSE`` for a rotatable spec, else none."""
        if self.predicate is not None:
            return self.predicate
        return "deleted = FALSE" if self.rotate else None

    def key_sql(self, quote: Callable[[str], str]) -> list[str]:
        """The index keys as SQL text, each column quoted by ``quote``."""
        expressions = dict(self.key_expressions)
        keys = []
        for column in self.columns:
            key = expressions.get(column) or quote(column)
            if self.opclass:
                key = f"{key} {self.opclass}"
            keys.append(key)
        return keys


#: The generic GIN index on node ``attrs`` (PACT lane-s-redesign-s1, decision
#: ``gin`` A of lane-s-storage-redesign). Live rows only (``deleted = false``,
#: not "open interval": a pinned read must find closed versions too), so it
#: is not rotatable. ``okg migrate`` builds it online like every spec here;
#: no read path uses it before stage S2. Rollback: move this suffix to
#: :data:`RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES` and the next migrate
#: drops it with ``DROP INDEX CONCURRENTLY``.
NODE_ATTRS_GIN_SPEC = VisibilityIndexSpec(
    "node_attrs_gin",
    ("attrs",),
    rotate=False,
    method="gin",
    opclass="jsonb_path_ops",
    predicate="deleted = FALSE",
)


#: The supporting B-tree for pages of one node subtype in node-id order
#: (PACT lane-s-redesign-s1, requirement supporting-indexes). The prototype
#: read SQL of the storage redesign pages ``WHERE subtype = ? AND deleted =
#: false AND live_lo <= bp AND live_hi > bp ORDER BY node_id LIMIT n``; this
#: index returns those rows in order, checks visibility on its own
#: ``live_lo``/``live_hi`` keys, and lets the read stop at the page size.
#: Measured on a Krama-scale copy: without it gate row b takes 1.57-1.75 s
#: (limit about 0.11 s), with it 12 ms; 94 MB when built. Same predicate and
#: rotation rule as the GIN index: live rows of every version, so a pinned
#: read finds closed versions too, and only ``okg migrate`` builds it.
NODE_SUBTYPE_NODE_ID_SPEC = VisibilityIndexSpec(
    "node_subtype_node_id",
    ("subtype", "node_id", "live_lo", "live_hi"),
    rotate=False,
    predicate="deleted = FALSE",
)

#: The supporting B-tree for pages of one edge type in edge-id order (PACT
#: lane-s-redesign-s1, requirement supporting-indexes). ``edge_id`` is BIGINT
#: and the storage redesign's Chronos-direct read (stage S2) pages edges of
#: one type by it as a number, for parity with today's effective-view edge
#: page, which orders by ``(target_id)::bigint`` since #3235 (served there by
#: the interval table's own index, not this one). When S1 was designed that
#: order was the text id and this index keyed on ``(edge_id::text)``. No other
#: Chronos edge index returns edges of one type in that order (the plan
#: without it sorts). Measured on a Krama-scale copy with the text key:
#: without it gate row d1 takes 0.43-0.64 s (limit 5.6 ms), with it 0.18 ms;
#: 116 MB. Rollback for either supporting index: move its suffix to
#: :data:`RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES`.
EDGE_TYPE_EDGE_ID_SPEC = VisibilityIndexSpec(
    "edge_type_edge_id",
    ("edge_type", "edge_id", "live_lo", "live_hi"),
    rotate=False,
    predicate="deleted = FALSE",
)

INTERVAL_SOURCE_READ_INDEX_SPECS: dict[str, tuple[VisibilityIndexSpec, ...]] = {
    "okg.graph_nodes": (
        VisibilityIndexSpec(
            "node_delta_segment",
            ("live_lo", "live_hi", "node_id", "deleted"),
            rotate=False,
        ),
        VisibilityIndexSpec("node_id_visible", ("node_id", "live_lo", "live_hi")),
        VisibilityIndexSpec(
            "node_subtype_visible", ("subtype", "live_lo", "live_hi")
        ),
        VisibilityIndexSpec(
            "node_source_node_visible",
            ("source", "node_id", "live_lo", "live_hi"),
        ),
        VisibilityIndexSpec(
            "node_source_run_visible",
            ("source", "run_id", "live_lo", "live_hi", "node_id"),
        ),
        NODE_ATTRS_GIN_SPEC,
        NODE_SUBTYPE_NODE_ID_SPEC,
    ),
    "okg.graph_edges": (
        VisibilityIndexSpec(
            "edge_delta_segment",
            ("live_lo", "live_hi", "edge_id", "deleted"),
            rotate=False,
        ),
        VisibilityIndexSpec("edge_id_visible", ("edge_id", "live_lo", "live_hi")),
        VisibilityIndexSpec(
            "edge_identity_visible",
            ("src", "edge_type", "live_lo", "live_hi", "dst"),
        ),
        VisibilityIndexSpec(
            "edge_dst_visible", ("dst", "live_lo", "live_hi", "edge_id")
        ),
        VisibilityIndexSpec(
            "edge_source_edgeid_visible",
            ("source", "edge_id", "live_lo", "live_hi"),
        ),
        VisibilityIndexSpec(
            "edge_source_dst_visible",
            ("source", "dst", "live_lo", "live_hi", "edge_id"),
        ),
        # Rows of one source run: ``count(*)`` (admission) and the run's
        # ``edge_id`` keys (failed-run compensation), both index-only here.
        # It replaces ``edge_source_run_visible``, which also carried the wide
        # ``src, edge_type, dst`` text keys that no reader needs (okg#3112).
        VisibilityIndexSpec(
            "edge_source_run_key_visible",
            ("source", "run_id", "live_lo", "live_hi", "edge_id"),
        ),
        EDGE_TYPE_EDGE_ID_SPEC,
    ),
}

#: Interval read indexes okg no longer builds (okg#3112, PACT
#: lane-h-3112-step1). :func:`converge_interval_read_indexes` drops them with
#: ``DROP INDEX CONCURRENTLY`` on every ``okg migrate``, together with any
#: cold-history hot replacement of one. Each was checked against its readers
#: on dev before it went:
#:
#: * ``*_live_lo_visible``: its one column leads ``*_delta_segment``, which
#:   covers the same rows and more.
#: * ``*_live_hi_visible``: no reader filters on ``live_hi`` alone among
#:   visible rows; the full-visible counts it sometimes served run on the
#:   source-leading indexes or the delta-segment index instead.
#: * ``edge_src_visible``: ``edge_identity_visible`` leads with the same
#:   ``src`` column and serves ``src = ?`` and ``src = ANY(...)``.
#: * ``edge_source_order_visible``: it served ``WHERE source = ? ORDER BY src,
#:   edge_type, dst`` in the source runner's record-set reads, which now order
#:   by ``edge_id`` (served by ``edge_source_edgeid_visible``).
#: * ``edge_source_run_visible``: replaced by ``edge_source_run_key_visible``.
#: * ``edge_type_text_id``: an early build of PACT lane-s-redesign-s1 keyed the
#:   edge supporting index on ``(edge_id::text)``; replaced by
#:   ``edge_type_edge_id``. Only test and measurement copies migrated with that
#:   build carry it; the drop is a no-op elsewhere.
RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES: dict[str, tuple[str, ...]] = {
    "okg.graph_nodes": ("node_live_lo_visible", "node_live_hi_visible"),
    "okg.graph_edges": (
        "edge_live_lo_visible",
        "edge_live_hi_visible",
        "edge_src_visible",
        "edge_source_order_visible",
        "edge_source_run_visible",
        "edge_type_text_id",
    ),
}


def _full_index_name(physical_table: str, suffix: str) -> str:
    # Kept byte-for-byte compatible with graph_store_internal.common without
    # importing graph-store code into the DB maintenance layer.
    digest = sha1(f"{physical_table}:{suffix}".encode("utf-8")).hexdigest()[:12]
    return f"idx_okg_chronos_{digest}_{suffix}"


def hot_index_name(physical_table: str, suffix: str, floor: int) -> str:
    """Return a short, floor-specific name suitable for side-by-side rebuilds."""
    digest = sha1(
        f"{physical_table}:{suffix}:hot:{floor}".encode("utf-8")
    ).hexdigest()[:12]
    hot_suffix = suffix.removesuffix("_visible") + "_hot"
    return f"idx_okg_chronos_{digest}_{hot_suffix}"


def brin_index_name(physical_table: str, column: str) -> str:
    digest = sha1(
        f"{physical_table}:{column}:cold-history-brin".encode("utf-8")
    ).hexdigest()[:12]
    return f"idx_okg_chronos_{digest}_{column}_brin"


def _regclass_exists(cur: psycopg.Cursor[Any], name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) AS reg", (name,))
    return cur.fetchone()["reg"] is not None


def _registered_interval_tables(cur: psycopg.Cursor[Any]) -> dict[str, str]:
    cur.execute(
        """
        SELECT table_name, physical_table
          FROM _chronos_branch_tables
         WHERE backend = 'interval'
           AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')
        """
    )
    rows = {
        str(row["table_name"]): str(row["physical_table"])
        for row in cur.fetchall()
    }
    missing = set(INTERVAL_SOURCE_READ_INDEX_SPECS) - set(rows)
    if missing:
        raise SubstrateError(
            "Chronos graph tables are not registered for cold-history maintenance",
            data={"missing_tables": sorted(missing)},
        )
    return rows


def resolve_hot_floor(
    cur: psycopg.Cursor[Any],
    *,
    headroom_generations: int = DEFAULT_HEADROOM_GENERATIONS,
) -> dict[str, Any]:
    """Resolve the hot floor and open value from live Chronos lineage state.

    The candidate is the checkpoint ``headroom_generations`` behind the latest
    published checkpoint on the default lineage.  Every non-trunk live branch
    can lower it further.  Missing/ambiguous lineage state raises; there is no
    sentinel or numeric fallback.
    """
    if headroom_generations < 1:
        raise SubstrateError(
            "cold-history headroom must be at least one generation",
            data={"headroom_generations": headroom_generations},
        )
    cur.execute(
        """
        SELECT b.current_segment_id,
               s.live_lo::text,
               s.live_hi::text,
               s.branch_point::text,
               s.segment_kind
          FROM _chronos_branch_interval_branches AS b
          JOIN _chronos_branch_interval_segments AS s
            ON s.segment_id = b.current_segment_id
         WHERE b.branch_id = 'default'
        """
    )
    default_rows = cur.fetchall()
    if len(default_rows) != 1:
        raise SubstrateError(
            "Chronos default branch does not resolve to exactly one segment",
            data={"resolved_rows": len(default_rows)},
        )
    default = default_rows[0]
    if str(default["segment_kind"]) != "mutable":
        raise SubstrateError(
            "Chronos default branch current segment is not mutable",
            data={
                "segment_id": int(default["current_segment_id"]),
                "segment_kind": str(default["segment_kind"]),
            },
        )

    # Each mutable continuation on the default lineage has a checkpoint sibling:
    # same parent, with the checkpoint's live_hi equal to the continuation's
    # live_lo.  This avoids assuming segment-id arithmetic or equating graph
    # generation ids with Chronos visibility coordinates.
    cur.execute(
        """
        WITH RECURSIVE lineage AS (
          SELECT s.segment_id, s.parent_segment_id, s.live_lo, 0 AS depth
            FROM _chronos_branch_interval_branches AS b
            JOIN _chronos_branch_interval_segments AS s
              ON s.segment_id = b.current_segment_id
           WHERE b.branch_id = 'default'
          UNION ALL
          SELECT parent.segment_id,
                 parent.parent_segment_id,
                 parent.live_lo,
                 lineage.depth + 1
            FROM lineage
            JOIN _chronos_branch_interval_segments AS parent
              ON parent.segment_id = lineage.parent_segment_id
           WHERE lineage.depth < 100000
        ), checkpoints AS (
          SELECT checkpoint.segment_id,
                 checkpoint.branch_point,
                 checkpoint.live_lo,
                 checkpoint.live_hi,
                 lineage.depth,
                 row_number() OVER (ORDER BY lineage.depth) AS recency
            FROM lineage
            JOIN _chronos_branch_interval_segments AS checkpoint
              ON checkpoint.parent_segment_id = lineage.parent_segment_id
             AND checkpoint.segment_kind = 'checkpoint'
             AND checkpoint.live_hi = lineage.live_lo
            JOIN _chronos_branch_interval_checkpoints AS recorded
              ON recorded.segment_id = checkpoint.segment_id
             AND recorded.branch_id = 'default'
             AND recorded.metadata::jsonb ->> 'status' = 'published'
        )
        SELECT segment_id,
               branch_point::text,
               live_lo::text,
               live_hi::text,
               recency,
               count(*) OVER () AS checkpoint_count
          FROM checkpoints
         ORDER BY recency
         OFFSET %s
         LIMIT 1
        """,
        (headroom_generations,),
    )
    floor_row = cur.fetchone()
    if floor_row is None:
        raise SubstrateError(
            "default lineage is shorter than the requested cold-history headroom",
            data={"headroom_generations": headroom_generations},
            fix_hint="choose a smaller explicit headroom after measuring pin ages",
        )
    checkpoint_floor = int(floor_row["branch_point"])

    cur.execute(
        """
        SELECT b.branch_id, s.live_lo::text
          FROM _chronos_branch_interval_branches AS b
          JOIN _chronos_branch_interval_segments AS s
            ON s.segment_id = b.current_segment_id
         WHERE b.branch_id NOT IN ('default', 'main')
         ORDER BY b.branch_id
        """
    )
    live_forks = [
        {"branch_id": str(row["branch_id"]), "live_lo": int(row["live_lo"])}
        for row in cur.fetchall()
    ]
    floor = min(
        [checkpoint_floor, *(fork["live_lo"] for fork in live_forks)]
    )
    return {
        "floor": floor,
        "headroom_generations": headroom_generations,
        "floor_checkpoint_segment_id": int(floor_row["segment_id"]),
        "floor_checkpoint_branch_point": checkpoint_floor,
        "checkpoint_count": int(floor_row["checkpoint_count"]),
        "latest_mutable_segment_id": int(default["current_segment_id"]),
        "latest_mutable_live_lo": int(default["live_lo"]),
        "latest_mutable_branch_point": int(default["branch_point"]),
        "open_interval_value": int(default["live_hi"]),
        "live_forks": live_forks,
        "floor_pinned_by": (
            "checkpoint_headroom"
            if floor == checkpoint_floor
            else next(
                fork["branch_id"] for fork in live_forks
                if fork["live_lo"] == floor
            )
        ),
    }


def _index_catalog_row(
    cur: psycopg.Cursor[Any], index_name: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT i.relname AS index_name,
               t.relname AS table_name,
               x.indisvalid,
               x.indisready,
               pg_relation_size(i.oid) AS bytes,
               pg_get_indexdef(i.oid) AS indexdef,
               pg_get_expr(x.indpred, x.indrelid) AS predicate,
               ARRAY(
                 SELECT pg_get_indexdef(i.oid, key_position, TRUE)
                   FROM generate_series(1, x.indnkeyatts) AS key_position
                  ORDER BY key_position
               ) AS indexed_columns
          FROM pg_class AS i
          JOIN pg_index AS x ON x.indexrelid = i.oid
          JOIN pg_class AS t ON t.oid = x.indrelid
         WHERE i.oid = to_regclass(%s)
        """,
        (index_name,),
    )
    row = cur.fetchone()
    return dict(row) if row is not None else None


def _normalized_hot_predicate(predicate: Any) -> str:
    value = str(predicate or "").lower()
    value = re.sub(
        r"'(-?\d+)'::(?:bigint|integer|numeric)",
        r"\1",
        value,
    )
    value = re.sub(r"::(?:bigint|integer|numeric)", "", value)
    value = re.sub(r"'(-?\d+)'", r"\1", value)
    return re.sub(r'[\s()"]+', "", value)


def hot_index_definition_matches(
    observed: Any,
    *,
    physical_table: str,
    spec: VisibilityIndexSpec,
    floor: int,
) -> bool:
    """Whether a catalog row is the exact authoritative hot definition."""
    if not isinstance(observed, dict):
        try:
            observed = dict(observed)
        except (TypeError, ValueError):
            return False
    if not bool(observed.get("indisvalid")) or not bool(
        observed.get("indisready")
    ):
        return False
    if str(observed.get("table_name") or "") != physical_table:
        return False
    columns = tuple(
        str(column).strip().strip('"')
        for column in (observed.get("indexed_columns") or ())
    )
    if columns != spec.columns:
        return False
    predicate = _normalized_hot_predicate(observed.get("predicate"))
    expected = {
        f"deleted=falseandlive_hi>{int(floor)}",
        f"notdeletedandlive_hi>{int(floor)}",
    }
    return predicate in expected


#: The storage-parameter clause of ``pg_get_indexdef`` text (``WITH
#: (fastupdate=off)``); PostgreSQL prints it after the key list and before
#: any predicate, and its values hold no parentheses.
_STORAGE_PARAMETERS_RE = re.compile(r"\)\s+WITH\s*\([^()]*\)", re.IGNORECASE)


def _normalized_index_text(text: str) -> str:
    return re.sub(r'[\s"()]+', "", text.lower())


def spec_index_definition_matches(
    indexdef: Any, spec: VisibilityIndexSpec
) -> bool | None:
    """Whether ``pg_get_indexdef`` text is exactly ``spec``'s definition:
    access method, keys with operator class, and predicate.

    Compared with case, quotes, parentheses and whitespace removed, so
    PostgreSQL's spelling (``USING gin (attrs jsonb_path_ops) WHERE (deleted
    = false)``, ``((edge_id)::text)``) matches the spec's. A storage-parameter
    clause (``WITH (...)``) is ignored. None when there is no definition
    text.
    """
    if not indexdef:
        return None
    # Storage parameters (``WITH (fastupdate=off)``) tune an index without
    # changing what it serves: an operator's tuning is not a wrong
    # definition, and must not make migrate drop and rebuild the index.
    text = _STORAGE_PARAMETERS_RE.sub(")", str(indexdef), count=1)
    predicate = spec.index_predicate()
    expected = (
        f"USING {spec.method or 'btree'} ({', '.join(spec.key_sql(lambda c: c))})"
        + (f" WHERE {predicate}" if predicate else "")
    )
    return _normalized_index_text(text).endswith(_normalized_index_text(expected))


def cold_history_report(
    dsn: str,
    *,
    headroom_generations: int = DEFAULT_HEADROOM_GENERATIONS,
) -> dict[str, Any]:
    """Return a no-mutation baseline/manifest report for interval indexes."""
    with db_connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            tables = _registered_interval_tables(cur)
            floor = resolve_hot_floor(
                cur, headroom_generations=headroom_generations
            )
            manifest_present = _regclass_exists(cur, HOT_INDEX_MANIFEST_TABLE)
            manifest: list[dict[str, Any]] = []
            if manifest_present:
                cur.execute(
                    """
                    SELECT logical_table, physical_table, suffix,
                           full_index_name, hot_index_name, floor::text,
                           headroom_generations, state, created_at, activated_at
                      FROM okg.chronos_hot_index_manifest
                     WHERE state IN ('building', 'activating', 'active')
                     ORDER BY logical_table, suffix, created_at DESC
                    """
                )
                manifest = [dict(row) for row in cur.fetchall()]

            indexes: list[dict[str, Any]] = []
            support_indexes: list[dict[str, Any]] = []
            for logical_table, specs in INTERVAL_SOURCE_READ_INDEX_SPECS.items():
                physical = tables[logical_table]
                for column in ("live_hi", "live_lo"):
                    name = brin_index_name(physical, column)
                    support_indexes.append(
                        {
                            "logical_table": logical_table,
                            "physical_table": physical,
                            "column": column,
                            "index_name": name,
                            "index": _index_catalog_row(cur, name),
                        }
                    )
                for spec in specs:
                    full_name = _full_index_name(physical, spec.suffix)
                    full = _index_catalog_row(cur, full_name)
                    indexes.append(
                        {
                            "logical_table": logical_table,
                            "physical_table": physical,
                            "suffix": spec.suffix,
                            "columns": list(spec.columns),
                            "rotatable": spec.rotate,
                            "full_index": full,
                        }
                    )
    return {
        "ok": True,
        "floor": floor,
        "manifest_present": manifest_present,
        "manifest": manifest,
        "support_indexes": support_indexes,
        "indexes": indexes,
    }


def _cold_history_reclaim_base(
    deployment: str,
    *,
    headroom_generations: int,
) -> dict[str, Any]:
    quoted_deployment = shlex.quote(deployment)
    return {
        "applicable": True,
        "status": "unavailable",
        "read_only": True,
        "rotation_performed": False,
        "headroom_generations": int(headroom_generations),
        "floor": None,
        "rotatable_index_count": None,
        "rotatable_full_index_bytes": None,
        "estimate_kind": COLD_HISTORY_RECLAIM_ESTIMATE_KIND,
        "rotatable_indexes": None,
        "inspect_command": (
            f"{okg_prefix()} db cold-history --deployment "
            f"{quoted_deployment} --json"
        ),
        "activate_command": (
            f"{okg_prefix()} db cold-history --deployment "
            f"{quoted_deployment} --activate --confirm --apply --json"
        ),
        "warning": COLD_HISTORY_RECLAIM_WARNING,
        "error": None,
    }


def summarize_cold_history_reclaim(
    report: Mapping[str, Any],
    *,
    deployment: str,
    headroom_generations: int = DEFAULT_HEADROOM_GENERATIONS,
) -> dict[str, Any]:
    """Reduce a cold-history catalog read to a stable headroom advisory.

    The byte total is deliberately gross: it sums existing full predecessor
    indexes that activation may drop, without subtracting the hot/support
    indexes that remain. Missing predecessors are already active/absent and do
    not count; non-rotatable delta and support indexes never enter the input set.
    """

    floor_report = report.get("floor")
    indexes = report.get("indexes")
    if not isinstance(floor_report, Mapping) or not isinstance(indexes, list):
        raise SubstrateError(
            "cold-history report is missing floor or index catalog data"
        )
    floor = floor_report.get("floor")
    observed_headroom = floor_report.get("headroom_generations")
    if (
        isinstance(floor, bool)
        or not isinstance(floor, int)
        or isinstance(observed_headroom, bool)
        or not isinstance(observed_headroom, int)
        or observed_headroom != headroom_generations
    ):
        raise SubstrateError(
            "cold-history report has invalid or mismatched floor authority",
            data={
                "floor": floor,
                "expected_headroom_generations": headroom_generations,
                "observed_headroom_generations": observed_headroom,
            },
        )

    candidates: list[dict[str, Any]] = []
    for index in indexes:
        if not isinstance(index, Mapping):
            raise SubstrateError("cold-history index catalog row is malformed")
        if index.get("rotatable") is not True:
            continue
        full_index = index.get("full_index")
        if full_index is None:
            continue
        if not isinstance(full_index, Mapping):
            raise SubstrateError(
                "cold-history full-index catalog row is malformed"
            )
        byte_count = full_index.get("bytes")
        valid = full_index.get("indisvalid")
        ready = full_index.get("indisready")
        text_fields = {
            "logical_table": index.get("logical_table"),
            "physical_table": index.get("physical_table"),
            "suffix": index.get("suffix"),
            "full_index_name": full_index.get("index_name"),
        }
        if (
            isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count < 0
            or not all(
                isinstance(value, str) and value
                for value in text_fields.values()
            )
            or not isinstance(valid, bool)
            or not isinstance(ready, bool)
        ):
            raise SubstrateError(
                "cold-history full-index catalog row has invalid fields",
                data={
                    "logical_table": index.get("logical_table"),
                    "suffix": index.get("suffix"),
                },
            )
        candidates.append({
            **text_fields,
            "bytes": byte_count,
            "full_index_valid": valid,
            "full_index_ready": ready,
        })

    candidates.sort(
        key=lambda row: (
            str(row["logical_table"]),
            str(row["suffix"]),
            str(row["full_index_name"]),
        )
    )
    advisory = _cold_history_reclaim_base(
        deployment,
        headroom_generations=headroom_generations,
    )
    advisory.update({
        "status": "candidates" if candidates else "no_candidates",
        "floor": floor,
        "rotatable_index_count": len(candidates),
        "rotatable_full_index_bytes": sum(
            int(row["bytes"]) for row in candidates
        ),
        "rotatable_indexes": candidates,
    })
    return advisory


def cold_history_reclaim_advisory(
    dsn: str,
    *,
    deployment: str,
    headroom_generations: int = DEFAULT_HEADROOM_GENERATIONS,
) -> dict[str, Any]:
    """Return a no-mutation headroom advisory, including explicit failures."""

    try:
        report = cold_history_report(
            dsn,
            headroom_generations=headroom_generations,
        )
        return summarize_cold_history_reclaim(
            report,
            deployment=deployment,
            headroom_generations=headroom_generations,
        )
    except Exception as exc:  # noqa: BLE001 - advisory failure stays explicit without replacing the original headroom refusal.
        advisory = _cold_history_reclaim_base(
            deployment,
            headroom_generations=headroom_generations,
        )
        advisory["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        return advisory


def cold_history_health_report(dsn: str) -> dict[str, Any]:
    """Validate the active manifest, floor and support indexes."""
    findings: list[dict[str, Any]] = []
    active_rows: list[dict[str, Any]] = []
    with db_connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if not _regclass_exists(cur, HOT_INDEX_MANIFEST_TABLE):
                return {
                    "ok": True,
                    "active": False,
                    "findings": [],
                    "reason": "manifest_not_installed",
                }
            cur.execute(
                """
                SELECT logical_table, physical_table, suffix,
                       full_index_name, hot_index_name, floor::text,
                       headroom_generations, open_interval_value::text,
                       state
                  FROM okg.chronos_hot_index_manifest
                 WHERE state IN ('building', 'activating', 'active', 'failed')
                 ORDER BY logical_table, suffix
                """
            )
            manifest_rows = [dict(row) for row in cur.fetchall()]
            for row in manifest_rows:
                if row["state"] in {"building", "failed"}:
                    findings.append(
                        {
                            "severity": "error",
                            "code": "chronos_hot_index_rotation_incomplete",
                            "suffix": row["suffix"],
                            "hot_index_name": row["hot_index_name"],
                            "state": row["state"],
                        }
                    )
            active_rows = [
                row
                for row in manifest_rows
                if row["state"] in {"activating", "active"}
            ]
            if not active_rows:
                return {
                    "ok": not findings,
                    "active": False,
                    "findings": findings,
                    "reason": (
                        "incomplete_rotation" if findings else "no_active_rotation"
                    ),
                }

            floors_by_headroom: dict[int, dict[str, Any]] = {}
            for row in active_rows:
                headroom = int(row["headroom_generations"])
                if headroom not in floors_by_headroom:
                    floors_by_headroom[headroom] = resolve_hot_floor(
                        cur, headroom_generations=headroom
                    )
                current = floors_by_headroom[headroom]
                manifest_floor = int(row["floor"])
                if manifest_floor > int(current["floor"]):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "chronos_hot_index_floor_unsafe",
                            "suffix": row["suffix"],
                            "observed": manifest_floor,
                            "maximum_safe_floor": int(current["floor"]),
                            "floor_pinned_by": current["floor_pinned_by"],
                        }
                    )
                if int(row["open_interval_value"]) != int(
                    current["open_interval_value"]
                ):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "chronos_open_interval_value_drift",
                            "suffix": row["suffix"],
                            "recorded": int(row["open_interval_value"]),
                            "observed": int(current["open_interval_value"]),
                        }
                    )
                hot = _index_catalog_row(cur, str(row["hot_index_name"]))
                spec = next(
                    (
                        candidate
                        for candidate in INTERVAL_SOURCE_READ_INDEX_SPECS.get(
                            str(row["logical_table"]),
                            (),
                        )
                        if candidate.suffix == str(row["suffix"])
                    ),
                    None,
                )
                if (
                    spec is None
                    or not hot_index_definition_matches(
                        hot,
                        physical_table=str(row["physical_table"]),
                        spec=spec,
                        floor=manifest_floor,
                    )
                ):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "chronos_hot_index_manifest_mismatch",
                            "suffix": row["suffix"],
                            "hot_index_name": row["hot_index_name"],
                            "observed": hot,
                        }
                    )
                if (
                    row["state"] == "active"
                    and _index_catalog_row(cur, str(row["full_index_name"]))
                    is not None
                ):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "chronos_full_index_reappeared",
                            "suffix": row["suffix"],
                            "full_index_name": row["full_index_name"],
                        }
                    )

            for logical_table, physical_table in sorted(
                {
                    (str(row["logical_table"]), str(row["physical_table"]))
                    for row in active_rows
                }
            ):
                for column in ("live_hi", "live_lo"):
                    name = brin_index_name(physical_table, column)
                    support = _index_catalog_row(cur, name)
                    if (
                        support is None
                        or not bool(support["indisvalid"])
                        or "USING brin" not in str(support["indexdef"])
                    ):
                        findings.append(
                            {
                                "severity": "error",
                                "code": "chronos_cold_history_brin_missing",
                                "logical_table": logical_table,
                                "index_name": name,
                            }
                        )

            # The read-role plan_cache_mode check moved to the database
            # doctor (db/management.py, PACT lane-s-redesign-s1): it runs on
            # every deployment, not only after a rotation.

    return {
        "ok": not findings,
        "active": True,
        "active_index_count": len(active_rows),
        "findings": findings,
    }


def rotate_hot_indexes(
    dsn: str,
    *,
    headroom_generations: int = DEFAULT_HEADROOM_GENERATIONS,
    logical_tables: Iterable[str] | None = None,
    suffixes: Iterable[str] | None = None,
    activate: bool = False,
) -> dict[str, Any]:
    """Build selected hot indexes concurrently and optionally activate them.

    Dry-run behaviour belongs in the CLI; calling this function always builds.
    With ``activate=False`` the full index remains and the manifest records a
    proof build.  Activation marks the hot definition as the schema authority,
    drops the corresponding full index concurrently, and leaves every row of
    graph history untouched.
    """
    requested_tables = set(logical_tables or INTERVAL_SOURCE_READ_INDEX_SPECS)
    requested_suffixes = set(suffixes or ())
    unknown_tables = requested_tables - set(INTERVAL_SOURCE_READ_INDEX_SPECS)
    if unknown_tables:
        raise SubstrateError(
            "unknown logical table requested for cold-history rotation",
            data={"logical_tables": sorted(unknown_tables)},
        )
    known_suffixes = {
        spec.suffix
        for logical_table in requested_tables
        for spec in INTERVAL_SOURCE_READ_INDEX_SPECS[logical_table]
        if spec.rotate
    }
    unknown_suffixes = requested_suffixes - known_suffixes
    if unknown_suffixes:
        raise SubstrateError(
            "unknown or non-rotatable index suffix requested",
            data={"suffixes": sorted(unknown_suffixes)},
        )

    # Concurrent index DDL must run outside a transaction.
    with db_connect(
        dsn, autocommit=True, row_factory=dict_row
    ) as conn:
        with conn.cursor() as cur:
            if not _regclass_exists(cur, HOT_INDEX_MANIFEST_TABLE):
                raise SubstrateError(
                    "cold-history manifest migration is not applied",
                    data={"required_table": HOT_INDEX_MANIFEST_TABLE},
                    fix_hint="run `okg migrate` through the ordered migration runner",
                )
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s)) AS locked",
                ("okg:cold-history-index-rotation",),
            )
            if not bool(cur.fetchone()["locked"]):
                raise SubstrateError(
                    "another cold-history index rotation holds the database lock"
                )
            try:
                tables = _registered_interval_tables(cur)
                floor = resolve_hot_floor(
                    cur, headroom_generations=headroom_generations
                )
                support_indexes: list[dict[str, Any]] = []
                for logical_table in sorted(requested_tables):
                    support_indexes.extend(
                        _ensure_brin_support(
                            cur,
                            logical_table=logical_table,
                            physical_table=tables[logical_table],
                        )
                    )
                built: list[dict[str, Any]] = []
                for logical_table in sorted(requested_tables):
                    physical = tables[logical_table]
                    for spec in INTERVAL_SOURCE_READ_INDEX_SPECS[logical_table]:
                        if not spec.rotate:
                            continue
                        if requested_suffixes and spec.suffix not in requested_suffixes:
                            continue
                        built.append(
                            _rotate_one(
                                cur,
                                logical_table=logical_table,
                                physical_table=physical,
                                spec=spec,
                                floor=floor,
                                activate=activate,
                            )
                        )
            finally:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s))",
                    ("okg:cold-history-index-rotation",),
                )
    return {
        "ok": True,
        "activated": activate,
        "floor": floor,
        "support_indexes": support_indexes,
        "indexes": built,
    }


def _ensure_brin_support(
    cur: psycopg.Cursor[Any],
    *,
    logical_table: str,
    physical_table: str,
) -> list[dict[str, Any]]:
    """Build the all-history range-pruning fallback used by deep pins."""
    rows: list[dict[str, Any]] = []
    for column in ("live_hi", "live_lo"):
        index_name = brin_index_name(physical_table, column)
        cur.execute(
            sql.SQL(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON public.{} "
                "USING brin ({}) WITH (pages_per_range = 128)"
            ).format(
                sql.Identifier(index_name),
                sql.Identifier(physical_table),
                sql.Identifier(column),
            )
        )
        observed = _index_catalog_row(cur, index_name)
        if (
            observed is None
            or not bool(observed["indisvalid"])
            or not bool(observed["indisready"])
            or "USING brin" not in str(observed["indexdef"])
        ):
            raise SubstrateError(
                "cold-history BRIN support index failed verification",
                data={"index": index_name, "observed": observed},
            )
        rows.append(
            {
                "logical_table": logical_table,
                "physical_table": physical_table,
                "column": column,
                "index_name": index_name,
                "bytes": int(observed["bytes"]),
            }
        )
    return rows


def _rotate_one(
    cur: psycopg.Cursor[Any],
    *,
    logical_table: str,
    physical_table: str,
    spec: VisibilityIndexSpec,
    floor: dict[str, Any],
    activate: bool,
) -> dict[str, Any]:
    full_name = _full_index_name(physical_table, spec.suffix)
    hot_name = hot_index_name(physical_table, spec.suffix, int(floor["floor"]))
    full_before = _index_catalog_row(cur, full_name)
    existing_hot = _index_catalog_row(cur, hot_name)
    if existing_hot is not None and (
        not bool(existing_hot["indisvalid"])
        or not bool(existing_hot["indisready"])
    ):
        cur.execute(
            sql.SQL("DROP INDEX CONCURRENTLY {}").format(
                sql.Identifier(hot_name)
            )
        )
        existing_hot = None
    if existing_hot is not None and not hot_index_definition_matches(
        existing_hot,
        physical_table=physical_table,
        spec=spec,
        floor=int(floor["floor"]),
    ):
        raise SubstrateError(
            "existing hot visibility index has the wrong definition",
            data={
                "index": hot_name,
                "expected_table": physical_table,
                "expected_columns": list(spec.columns),
                "expected_floor": int(floor["floor"]),
                "observed": existing_hot,
            },
            fix_hint=(
                "inspect and remove the conflicting index before retrying; "
                "the full visibility index remains authoritative"
            ),
        )
    cur.execute(
        """
        SELECT state
          FROM okg.chronos_hot_index_manifest
         WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
        """,
        (physical_table, spec.suffix, hot_name),
    )
    manifest_before = cur.fetchone()
    if (
        full_before is None
        and existing_hot is not None
        and manifest_before is not None
        and manifest_before["state"] == "active"
    ):
        # A repeated proof/apply at an unchanged floor is idempotent. In
        # particular, never demote the only authoritative replacement to
        # ``proof`` merely because the operator reran the command.
        return {
            "logical_table": logical_table,
            "physical_table": physical_table,
            "suffix": spec.suffix,
            "full_index_name": full_name,
            "full_index_bytes_before": None,
            "hot_index_name": hot_name,
            "hot_index_bytes": int(existing_hot["bytes"]),
            "shrink_ratio": None,
            "state": "active",
            "already_active": True,
            "retired_hot_indexes": [],
        }
    if full_before is None:
        cur.execute(
            """
            SELECT hot_index_name, floor::text, state
              FROM okg.chronos_hot_index_manifest
             WHERE physical_table = %s AND suffix = %s AND state = 'active'
             ORDER BY activated_at DESC NULLS LAST, created_at DESC
             LIMIT 1
            """,
            (physical_table, spec.suffix),
        )
        active = cur.fetchone()
        if active is None:
            raise SubstrateError(
                "full visibility index is missing without an active hot replacement",
                data={"index": full_name, "suffix": spec.suffix},
            )

    cur.execute(
        """
        INSERT INTO okg.chronos_hot_index_manifest
          (logical_table, physical_table, suffix, full_index_name,
           hot_index_name, floor, headroom_generations,
           floor_checkpoint_segment_id, floor_checkpoint_branch_point,
           open_interval_value, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'building')
        ON CONFLICT (physical_table, suffix, hot_index_name)
        DO UPDATE SET state = CASE
          WHEN okg.chronos_hot_index_manifest.state = 'active' THEN 'active'
          ELSE 'building' END,
          failure = NULL
        """,
        (
            logical_table,
            physical_table,
            spec.suffix,
            full_name,
            hot_name,
            int(floor["floor"]),
            int(floor["headroom_generations"]),
            int(floor["floor_checkpoint_segment_id"]),
            int(floor["floor_checkpoint_branch_point"]),
            int(floor["open_interval_value"]),
        ),
    )
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in spec.columns)
    try:
        cur.execute(
            sql.SQL(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON public.{} ({}) "
                "WHERE deleted = FALSE AND live_hi > {}"
            ).format(
                sql.Identifier(hot_name),
                sql.Identifier(physical_table),
                columns,
                sql.Literal(int(floor["floor"])),
            )
        )
    except Exception as exc:
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'failed', failure = %s
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            (str(exc), physical_table, spec.suffix, hot_name),
        )
        raise
    hot = _index_catalog_row(cur, hot_name)
    if not hot_index_definition_matches(
        hot,
        physical_table=physical_table,
        spec=spec,
        floor=int(floor["floor"]),
    ):
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'failed', failure = %s
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            ("built index failed validity/predicate verification", physical_table,
             spec.suffix, hot_name),
        )
        raise SubstrateError(
            "hot visibility index failed post-build verification",
            data={"index": hot_name, "observed": hot},
        )

    if activate:
        # Once state is activating, schema reconciliation must never recreate
        # the full definition.  The hot index is already valid before that
        # authority change, so the following concurrent drop is fail-safe.
        cur.execute(
            """
            SELECT hot_index_name
              FROM okg.chronos_hot_index_manifest
             WHERE physical_table = %s AND suffix = %s
               AND state IN ('active', 'activating')
               AND hot_index_name <> %s
             ORDER BY created_at
            """,
            (physical_table, spec.suffix, hot_name),
        )
        retired_hot_indexes = [
            str(row["hot_index_name"]) for row in cur.fetchall()
        ]
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'retired'
             WHERE physical_table = %s AND suffix = %s
               AND state IN ('active', 'activating')
               AND hot_index_name <> %s
            """,
            (physical_table, spec.suffix, hot_name),
        )
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'activating'
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            (physical_table, spec.suffix, hot_name),
        )
        if full_before is not None:
            cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier(full_name)
                )
            )
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'active', activated_at = now(),
                   index_definition = %s, index_bytes_at_activation = %s
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            (
                hot["indexdef"],
                int(hot["bytes"]),
                physical_table,
                spec.suffix,
                hot_name,
            ),
        )
        for retired_name in retired_hot_indexes:
            cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier(retired_name)
                )
            )
    else:
        retired_hot_indexes = []
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'proof', index_definition = %s,
                   index_bytes_at_activation = %s
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            (
                hot["indexdef"],
                int(hot["bytes"]),
                physical_table,
                spec.suffix,
                hot_name,
            ),
        )
    return {
        "logical_table": logical_table,
        "physical_table": physical_table,
        "suffix": spec.suffix,
        "full_index_name": full_name,
        "full_index_bytes_before": (
            int(full_before["bytes"]) if full_before is not None else None
        ),
        "hot_index_name": hot_name,
        "hot_index_bytes": int(hot["bytes"]),
        "shrink_ratio": (
            round(int(full_before["bytes"]) / int(hot["bytes"]), 3)
            if full_before is not None and int(hot["bytes"]) > 0
            else None
        ),
        "state": "active" if activate else "proof",
        "already_active": False,
        "retired_hot_indexes": retired_hot_indexes,
    }


#: Advisory lock shared with :func:`rotate_hot_indexes`: one concurrent index
#: change on the graph tables at a time.
_INDEX_DDL_LOCK = "okg:cold-history-index-rotation"

#: Spec index names whose online build was deferred in this context (a
#: migrate whose online step timed out or found another index change
#: running). Schema reconciliation skips building these with its plain,
#: write-blocking CREATE INDEX; the next migrate or the post-publish rebuild
#: builds them concurrently.
DEFERRED_INDEX_BUILDS: contextvars.ContextVar[frozenset[str]] = contextvars.ContextVar(
    "okg_deferred_interval_index_builds", default=frozenset()
)

#: Lock-wait failures that defer the online step instead of failing it.
_DEFERRABLE_SQLSTATES = frozenset({"55P03", "57014"})


def registered_graph_tables(cur: psycopg.Cursor[Any]) -> dict[str, str] | None:
    """``{logical: physical}`` for both graph tables, or None before they exist."""
    if not _regclass_exists(cur, "public._chronos_branch_tables"):
        return None
    cur.execute(
        """
        SELECT table_name, physical_table
          FROM public._chronos_branch_tables
         WHERE backend = 'interval'
           AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')
        """
    )
    tables = {str(row["table_name"]): str(row["physical_table"]) for row in cur.fetchall()}
    if set(tables) != set(INTERVAL_SOURCE_READ_INDEX_SPECS):
        return None
    return tables


def pending_interval_index_changes(
    cur: psycopg.Cursor[Any], tables: Mapping[str, str]
) -> dict[str, list[str]]:
    """Spec indexes still to build (missing or invalid) and retired ones still
    to drop, by index name. Hot replacements the cold-history tool owns are
    not counted."""
    active_hot = _active_hot(cur)
    build: list[str] = []
    for logical_table, specs in INTERVAL_SOURCE_READ_INDEX_SPECS.items():
        physical = tables[logical_table]
        columns = _table_columns(cur, physical)
        for spec in specs:
            if (physical, spec.suffix) in active_hot or not set(spec.columns) <= columns:
                continue
            name = _full_index_name(physical, spec.suffix)
            if not _spec_index_current(_index_catalog_row(cur, name), spec):
                build.append(name)
    drop: list[str] = []
    for logical_table, suffixes in RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES.items():
        physical = tables[logical_table]
        for suffix in suffixes:
            name = _full_index_name(physical, suffix)
            if _index_catalog_row(cur, name) is not None:
                drop.append(name)
    return {"build": build, "drop": drop}


def _spec_index_current(row: Mapping[str, Any] | None, spec: VisibilityIndexSpec) -> bool:
    """True when a spec index needs no build: present, valid and ready, and,
    for an extended spec (PACT lane-s-redesign-s1), of the spec's exact
    definition. A plain spec is judged on validity alone, as before."""
    if row is None or not (bool(row["indisvalid"]) and bool(row["indisready"])):
        return False
    if spec.extended:
        return spec_index_definition_matches(row.get("indexdef"), spec) is True
    return True


def spec_index_statement(
    index_name: str, physical_table: str, spec: VisibilityIndexSpec
) -> sql.Composed:
    """The ``CREATE INDEX CONCURRENTLY`` statement for one spec index.

    A plain spec composes exactly the statement okg#3112 step 1 built (the
    lane-s-redesign-s1 tests compare it byte for byte); an extended spec adds
    ``USING <method>``, its operator class, key expressions and predicate.
    """
    if spec.extended:
        keys: sql.Composable = sql.SQL(", ".join(
            spec.key_sql(lambda column: '"' + column.replace('"', '""') + '"')
        ))
        where = spec.index_predicate()
        predicate: sql.Composable = sql.SQL(f" WHERE {where}" if where else "")
        using: sql.Composable = sql.SQL(f" USING {spec.method}" if spec.method else "")
    else:
        keys = sql.SQL(", ").join(sql.Identifier(column) for column in spec.columns)
        predicate = sql.SQL(" WHERE deleted = FALSE") if spec.rotate else sql.SQL("")
        using = sql.SQL("")
    # One statement shape, so the reviewed dynamic CREATE inventory in
    # scripts/check_migration_drop_recreate.py keeps its count for this file.
    return sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}{} ({}){}").format(
        sql.Identifier(index_name),
        sql.Identifier("public", physical_table),
        using,
        keys,
        predicate,
    )


def graph_read_index_findings(dsn: str) -> list[dict[str, Any]]:
    """Doctor findings for the extended graph read-index specs (PACT
    lane-s-redesign-s1: the GIN index on node attrs, and any later spec that
    is not a plain B-tree): an error for each one that is missing, invalid or
    of another definition (``okg migrate`` builds it online). Nothing before
    the graph tables exist.

    Plain specs are left out on purpose: S1 does not change what doctor says
    about them. Reporting every missing or invalid spec index, and a retired
    one still present, is a possible follow-up
    (``pending_interval_index_changes`` already lists them all)."""
    findings: list[dict[str, Any]] = []
    with db_connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            tables = registered_graph_tables(cur)
            if tables is None:
                return findings
            extended = extended_spec_index_names(tables)
            pending = pending_interval_index_changes(cur, tables)
            for name in pending["build"]:
                if name not in extended:
                    continue
                row = _index_catalog_row(cur, name)
                if row is None:
                    state = "missing"
                elif not (bool(row["indisvalid"]) and bool(row["indisready"])):
                    state = "invalid"
                else:
                    state = "wrong_definition"
                findings.append({
                    "severity": "error",
                    "subsystem": "graph_read_indexes",
                    "code": f"graph_read_index_{state}",
                    "index_name": name,
                    "observed": row.get("indexdef") if row else None,
                    "expected": "the index is built, valid and of its spec's definition",
                    "fix_hint": "run `okg migrate`: it builds the index with CREATE INDEX "
                    + "CONCURRENTLY (an invalid or wrong one is dropped first); the "
                    + "post-publish index rebuild never builds it",
                })
    return findings


def _relation_bytes(cur: psycopg.Cursor[Any], physical_table: str) -> int:
    """Heap bytes of one graph table (a scale hint for an index build)."""
    cur.execute(
        "SELECT pg_relation_size(to_regclass(%s)) AS bytes",
        (sql.Identifier("public", physical_table).as_string(cur.connection),),
    )
    row = cur.fetchone()
    return int(row["bytes"] or 0) if row else 0


def _table_columns(cur: psycopg.Cursor[Any], physical_table: str) -> set[str]:
    cur.execute(
        """
        SELECT a.attname
          FROM pg_attribute a
         WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
        """,
        (f'public."{physical_table}"',),
    )
    return {str(row["attname"]) for row in cur.fetchall()}


def _active_hot(cur: psycopg.Cursor[Any]) -> set[tuple[str, str]]:
    if not _regclass_exists(cur, HOT_INDEX_MANIFEST_TABLE):
        return set()
    cur.execute(
        """
        SELECT physical_table, suffix
          FROM okg.chronos_hot_index_manifest
         WHERE state IN ('activating', 'active')
        """
    )
    return {(str(row["physical_table"]), str(row["suffix"])) for row in cur.fetchall()}


def converge_interval_read_indexes(
    dsn: str,
    *,
    lock_timeout_ms: int,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Build missing interval read indexes and drop retired ones, online.

    ``okg migrate`` runs this before its schema transaction (okg#3112, PACT
    lane-h-3112-step1). Schema reconciliation
    (``_ensure_interval_source_read_indexes``) creates a missing spec index
    with a plain ``CREATE INDEX`` inside the migrate transaction, which blocks
    every graph write for the whole build on a populated deployment. This
    step builds each missing one first with ``CREATE INDEX CONCURRENTLY``
    outside any transaction, so reconciliation then finds it and does
    nothing, and it drops each index named in
    :data:`RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES` (and any cold-history
    hot replacement of one, whose manifest row becomes ``retired``) with
    ``DROP INDEX CONCURRENTLY``. Every new index is built before any is
    dropped.

    It never fails a migrate over a lock. When another index change holds
    the shared DDL lock (a concurrent migrate, a cold-history rotation, the
    post-publish rebuild) it returns ``status: deferred`` at once, and when a
    statement's lock wait exceeds ``lock_timeout_ms`` (a long publish
    transaction, say) it drops the invalid index its own interrupted build
    left and returns ``status: deferred`` with what is still ``pending``.
    The next migrate, or the post-publish rebuild, finishes the job (the
    post-publish rebuild never builds an extended spec index such as the GIN
    index on node attrs; only a migrate does). Any other error raises, except
    a failed extended spec build, which is dropped, reported under
    ``failed`` and leaves the status ``partial`` (see
    :func:`converge_on_cursor`).

    An invalid index left by an interrupted earlier build is dropped and
    rebuilt. A spec index whose active hot replacement is the schema
    authority is left to the cold-history tool.

    Returns ``status: not_registered`` when the graph tables do not exist
    yet: a fresh database gets its indexes from reconciliation on empty
    tables, where a plain build takes no time.
    """
    say = on_progress or (lambda _msg: None)
    with db_connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            tables = registered_graph_tables(cur)
            if tables is None:
                return {"status": "not_registered", "created": [], "dropped": []}
            cur.execute(
                sql.SQL("SET lock_timeout = {}").format(
                    sql.Literal(f"{int(lock_timeout_ms)}ms")
                )
            )
            # The lock wait is what must be bounded: a build stopped by a
            # statement timeout leaves an invalid index behind.
            cur.execute("SET statement_timeout = 0")
            try:
                cur.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s)) AS got", (_INDEX_DDL_LOCK,)
                )
                if not bool(cur.fetchone()["got"]):
                    return {
                        "status": "deferred",
                        "reason": "another_index_change_running",
                        "created": [],
                        "dropped": [],
                        "retired_hot_indexes": [],
                        "pending": pending_interval_index_changes(cur, tables),
                    }
                try:
                    return converge_on_cursor(cur, tables=tables, say=say)
                finally:
                    cur.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))", (_INDEX_DDL_LOCK,)
                    )
            finally:
                cur.execute("RESET lock_timeout")
                cur.execute("RESET statement_timeout")


def extended_spec_index_names(tables: Mapping[str, str]) -> set[str]:
    """Index names of the extended specs (not a plain B-tree; PACT
    lane-s-redesign-s1: the GIN index on node attrs) on these graph tables."""
    return {
        _full_index_name(tables[logical_table], spec.suffix)
        for logical_table, specs in INTERVAL_SOURCE_READ_INDEX_SPECS.items()
        for spec in specs
        if spec.extended
    }


def converge_on_cursor(
    cur: psycopg.Cursor[Any],
    *,
    tables: Mapping[str, str],
    say: Callable[[str], None],
    step: Callable[[Callable[[], Any]], Any] | None = None,
    build_extended: bool = True,
) -> dict[str, Any]:
    """The convergence itself, on an autocommit ``dict_row`` cursor whose
    session already holds the DDL lock and bounded lock settings.

    ``step`` wraps each DDL statement (the post-publish rebuild passes one
    that takes the publisher lock for the statement and yields to a publish);
    whatever it raises propagates after the cleanup below.

    ``build_extended`` False (the post-publish rebuild) skips every missing
    extended spec index (PACT lane-s-redesign-s1: the GIN index on node
    attrs, whose build on a Krama-scale table is one long statement) and
    lists it under ``skipped_extended``: only ``okg migrate`` builds those,
    never a step under the publisher lock; doctor reports them missing until
    then.

    An extended spec whose build fails with an error that is not a lock wait
    (disk full, out of memory, a temporary-file limit) does not stop the
    convergence: its invalid leftover is dropped, the failure is listed
    under ``failed`` and logged as a warning, and the remaining specs are
    converged, so ``okg migrate`` still applies the schema; doctor keeps
    reporting the index. A plain spec's failure still raises, as before.
    Either way the status is ``partial`` and ``pending`` lists what is left.
    """
    run = step or (lambda action: action())
    created: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    retired_hot: list[dict[str, Any]] = []
    building: list[str] = []
    failed: list[dict[str, Any]] = []
    skipped_extended: list[str] = []

    def report(status: str, **extra: Any) -> dict[str, Any]:
        return {
            "status": status,
            "created": created,
            "dropped": dropped,
            "retired_hot_indexes": retired_hot,
            "dropped_bytes": sum(item["bytes"] for item in dropped)
            + sum(item["bytes"] for item in retired_hot),
            **extra,
        }

    try:
        _converge_steps(
            cur, tables=tables, created=created, dropped=dropped,
            retired_hot=retired_hot, building=building, say=say, run=run,
            build_extended=build_extended, failed=failed,
            skipped_extended=skipped_extended,
        )
    except psycopg.Error as exc:
        if getattr(exc, "sqlstate", None) not in _DEFERRABLE_SQLSTATES:
            raise
        # The interrupted build already dropped its own invalid index inside
        # its step (under the publisher lock on the post-publish path);
        # `building` names any it could not drop.
        return report(
            "deferred",
            reason="lock_timeout",
            error=f"{type(exc).__name__}: {exc}",
            invalid_index_left=list(building),
            pending=pending_interval_index_changes(cur, tables),
            failed=failed,
            skipped_extended=skipped_extended,
        )
    if failed or skipped_extended:
        return report(
            "partial",
            reason="extended_index_build_failed" if failed else "extended_builds_need_migrate",
            failed=failed,
            skipped_extended=skipped_extended,
            pending=pending_interval_index_changes(cur, tables),
        )
    return report("converged")


def _drop_own_invalid_build(
    cur: psycopg.Cursor[Any], building: list[str], *, say: Callable[[str], None]
) -> list[str]:
    """Drop the invalid index the interrupted ``CREATE INDEX CONCURRENTLY``
    of this run left (``building`` names it); return any that stayed."""
    left: list[str] = []
    conn = cur.connection
    for name in building:
        if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
            left.append(name)
            continue
        try:
            row = _index_catalog_row(cur, name)
            if row is None or (bool(row["indisvalid"]) and bool(row["indisready"])):
                continue
            say(f"DROP INDEX CONCURRENTLY {name} (left by the interrupted build)")
            cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier("public", name)
                )
            )
        except psycopg.Error:  # noqa: BLE001 -- reported as left; next run drops it
            left.append(name)
    building.clear()
    return left


def _converge_steps(
    cur: psycopg.Cursor[Any],
    *,
    tables: Mapping[str, str],
    created: list[dict[str, Any]],
    dropped: list[dict[str, Any]],
    retired_hot: list[dict[str, Any]],
    building: list[str],
    say: Callable[[str], None],
    run: Callable[[Callable[[], Any]], Any],
    build_extended: bool = True,
    failed: list[dict[str, Any]] | None = None,
    skipped_extended: list[str] | None = None,
) -> None:
    failed = [] if failed is None else failed
    skipped_extended = [] if skipped_extended is None else skipped_extended
    manifest_present = _regclass_exists(cur, HOT_INDEX_MANIFEST_TABLE)
    active_hot = _active_hot(cur)
    for logical_table, specs in INTERVAL_SOURCE_READ_INDEX_SPECS.items():
        physical = tables[logical_table]
        columns = _table_columns(cur, physical)
        for spec in specs:
            # Schema reconciliation skips a spec whose columns the table
            # lacks (an older graph table); so does this step.
            if (physical, spec.suffix) in active_hot or not set(spec.columns) <= columns:
                continue
            if spec.extended and not build_extended:
                name = _full_index_name(physical, spec.suffix)
                if not _spec_index_current(_index_catalog_row(cur, name), spec):
                    say(f"skipping {name}: only `okg migrate` builds it")
                    skipped_extended.append(name)
                continue
            try:
                built = _build_spec_index_concurrently(
                    cur, physical_table=physical, spec=spec, say=say, run=run,
                    building=building,
                )
            except psycopg.Error as exc:
                if not _extended_build_failure_is_reportable(cur, spec, exc):
                    raise
                name = _full_index_name(physical, spec.suffix)
                left = list(building)
                building.clear()
                failure = {
                    "logical_table": logical_table,
                    "suffix": spec.suffix,
                    "index_name": name,
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "error": f"{type(exc).__name__}: {exc}",
                    "invalid_index_left": left,
                }
                failed.append(failure)
                _log.warning(
                    "graph read index %s was not built (%s); the rest of the "
                    "convergence continues, doctor reports the index until a "
                    "later `okg migrate` builds it%s",
                    name,
                    failure["error"],
                    f"; invalid leftover still present: {left}" if left else "",
                )
                say(f"FAILED to build {name}: {failure['error']}")
                continue
            if built is not None:
                created.append({"logical_table": logical_table, **built})
    for logical_table, suffixes in RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES.items():
        physical = tables[logical_table]
        for suffix in suffixes:
            name = _full_index_name(physical, suffix)
            row = _index_catalog_row(cur, name)
            if row is not None:
                say(f"DROP INDEX CONCURRENTLY {name}")
                started = time.monotonic()
                run(lambda n=name: cur.execute(
                    sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                        sql.Identifier("public", n)
                    )
                ))
                dropped.append({
                    "logical_table": logical_table,
                    "suffix": suffix,
                    "index_name": name,
                    "bytes": int(row["bytes"]),
                    "seconds": round(time.monotonic() - started, 3),
                })
            if manifest_present:
                retired_hot.extend(
                    _retire_hot_indexes(
                        cur, physical_table=physical, suffix=suffix, say=say, run=run
                    )
                )


def _extended_build_failure_is_reportable(
    cur: psycopg.Cursor[Any], spec: VisibilityIndexSpec, exc: psycopg.Error
) -> bool:
    """Whether a failed spec build is reported and skipped instead of raised:
    only for an extended spec, only for an error that is not a lock wait
    (those defer the whole step, as before), and only while the session is
    still usable (a server crash ends the migrate anyway)."""
    if not spec.extended:
        return False
    if getattr(exc, "sqlstate", None) in _DEFERRABLE_SQLSTATES:
        return False
    conn = cur.connection
    return not (
        conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
    )


def _build_spec_index_concurrently(
    cur: psycopg.Cursor[Any],
    *,
    physical_table: str,
    spec: VisibilityIndexSpec,
    say: Callable[[str], None],
    run: Callable[[Callable[[], Any]], Any],
    building: list[str],
) -> dict[str, Any] | None:
    """``CREATE INDEX CONCURRENTLY`` one missing spec index; None if present."""
    name = _full_index_name(physical_table, spec.suffix)
    row = _index_catalog_row(cur, name)
    if _spec_index_current(row, spec):
        return None
    # Present but invalid (left by an interrupted concurrent build), or, for
    # an extended spec, valid but of another definition.
    rebuilt_invalid = row is not None
    statement = spec_index_statement(name, physical_table, spec)

    def build() -> None:
        if rebuilt_invalid:
            # Reconciliation's CREATE INDEX IF NOT EXISTS would accept it as
            # present.
            say(f"DROP INDEX CONCURRENTLY {name} (invalid or wrong definition)")
            cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier("public", name)
                )
            )
        if spec.extended:
            # No free-space check: PostgreSQL exposes no free disk space
            # without an extension, so the table size is reported instead.
            # A build that runs out of room fails, drops its invalid index
            # and is reported (see converge_on_cursor).
            say(
                f"building index {name} concurrently (table "
                f"{_relation_bytes(cur, physical_table)} bytes; free disk space "
                "is not visible to PostgreSQL, so it is not checked)"
            )
        else:
            say(f"building index {name} concurrently")
        building.append(name)
        try:
            cur.execute(statement)
        except BaseException:
            # Still inside this step (under the publisher lock on the
            # post-publish path): drop the invalid index the interrupted
            # build left; `building` keeps any that could not be dropped.
            building[:] = _drop_own_invalid_build(cur, building, say=say)
            raise
        building.remove(name)

    started = time.monotonic()
    run(build)
    seconds = round(time.monotonic() - started, 3)
    built = _index_catalog_row(cur, name)
    if not _spec_index_current(built, spec):
        raise SubstrateError(
            "a concurrently built interval read index is not valid",
            data={"index": name, "observed": built},
            fix_hint="rerun `okg migrate`: it drops the invalid index and builds it again",
        )
    return {
        "suffix": spec.suffix,
        "index_name": name,
        "bytes": int(built["bytes"]),
        "seconds": seconds,
        "rebuilt_invalid": rebuilt_invalid,
    }


def _retire_hot_indexes(
    cur: psycopg.Cursor[Any],
    *,
    physical_table: str,
    suffix: str,
    say: Callable[[str], None],
    run: Callable[[Callable[[], Any]], Any],
) -> list[dict[str, Any]]:
    """Drop every hot replacement of a retired suffix and mark its row retired."""
    cur.execute(
        """
        SELECT hot_index_name, state
          FROM okg.chronos_hot_index_manifest
         WHERE physical_table = %s AND suffix = %s AND state <> 'retired'
         ORDER BY created_at
        """,
        (physical_table, suffix),
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        name = str(row["hot_index_name"])
        observed = _index_catalog_row(cur, name)
        if observed is not None:
            say(f"DROP INDEX CONCURRENTLY {name}")
            run(lambda n=name: cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier("public", n)
                )
            ))
        cur.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET state = 'retired'
             WHERE physical_table = %s AND suffix = %s AND hot_index_name = %s
            """,
            (physical_table, suffix, name),
        )
        out.append({
            "suffix": suffix,
            "hot_index_name": name,
            "previous_state": str(row["state"]),
            "bytes": int(observed["bytes"]) if observed is not None else 0,
        })
    return out
