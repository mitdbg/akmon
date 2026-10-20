"""`file_symbols` — a pure non-LLM operator view over the git+code graph.

For each file node, list the ``code_symbol`` nodes it ``contains``
(its functions, classes, methods). No LLM: the planner emits only ``QuoteSpec``
fragments (which carry the per-file lineage citations) and the table rows come
from a deterministic graph read.

Relation-first: the view declares a ``sql_relation`` output slot for the table
and persists the native JSON manifest as the fragment cache. The relation is the
user-facing output whose scopes the scheduler carries forward, while the JSON
manifest lets refresh reuse unchanged quote fragments instead of treating every
scoped run as brand-new.

Chronos-native read: ``build_corpus`` reads the branch HEAD (not
``checkout_generation`` — the checkpoint isn't resolvable at view-materialize
time; not ``okg.v_nodes``/``nodes_live`` — empty under chronos-native).

Compute-incrementality (opt-in via the ``delta_scope`` kwarg): when the publisher
hands a ``ViewDeltaScope``, the view rebuilds ONLY the files whose symbols could
have changed and tells the substrate which scopes to replace. A file's rows
change iff its path/lifecycle attrs changed, one of its ``code_symbol`` nodes
changed, or a ``contains`` edge changed.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from okg.substrate.views.base import ViewDeltaScope
from okg.views.planners.base import (
    FragmentSpec,
    QuoteSpec,
    ViewDefinition,
    ViewOutputTemplate,
    ViewRelationColumn,
)
from okg.views.types import Anchor

VIEW_ID = "bench.file_symbols"
ANCHOR_PREFIX = "file_symbols:"
ROWS_TEMP_TABLE = "file_symbols_rows_v2"
RELATION_ROWS_KEY = "file_symbols"
RELATION_VISIBLE_NODE_ATTRS = {
    "__new__",
    "__deleted__",
    "__subtype__",
    "kind",
    "name",
    "path",
    "qualified_name",
    "start_line",
    "symbol_kind",
}

ROWS_SQL = f"""
SELECT scope_key, row_key, file_path, symbol_name, symbol_kind,
       qualified_name, start_line
  FROM pg_temp.{ROWS_TEMP_TABLE}
 WHERE symbol_kind <> 'module'
 ORDER BY scope_key, row_key
"""

VIEW_DEF_HASH = (
    "bench.file_symbols.v3:"
    f"{hashlib.sha256(ROWS_SQL.encode()).hexdigest()[:16]}"
)


def _file_of_symbol(symbol_id: str) -> str | None:
    """Best-effort legacy symbol-id owner mapping.

    LSP semantic symbol ids are ``symbol:{repo}:semantic:{hash}``; those do not
    encode the file path, so callers must prefer graph edge endpoints.
    """
    parts = symbol_id.split(":")
    if len(parts) < 3 or parts[0] != "symbol" or parts[2] == "semantic":
        return None
    return f"file:{parts[1]}:{parts[2]}"


def _dirty_file_scopes(
    delta_scope: ViewDeltaScope,
    maybe_delta_scope: ViewDeltaScope | None = None,
    *,
    graph: Any | None = None,
) -> set[str] | None:
    """The COMPLETE set of file scope keys whose rows could change this cycle.

    Semantic symbol IDs do not carry the path. Use changed ``contains`` edge
    endpoints first, then current contains edges, then only legacy path-encoded
    ids. Returns None (-> full scan) when a changed symbol cannot be mapped.
    """
    if maybe_delta_scope is not None:
        graph = delta_scope
        delta_scope = maybe_delta_scope

    files: set[str] = set()
    source_file_field_changes = delta_scope.changed_node_attr_fields.get(
        "source_file",
        {},
    )
    for node_id in delta_scope.changed_nodes.get("source_file", ()):
        node_key = str(node_id)
        fields = set(source_file_field_changes.get(node_key, ()))
        if not fields:
            return None
        if RELATION_VISIBLE_NODE_ATTRS.intersection(fields):
            files.add(node_key)

    changed_symbols = {
        str(node_id)
        for node_id in delta_scope.changed_nodes.get("code_symbol", ())
        if str(node_id)
    }
    covered_symbols: set[str] = set()
    for endpoint in delta_scope.changed_edge_endpoints.get("contains", ()):
        src = endpoint.get("src")
        dst = endpoint.get("dst")
        if src:
            files.add(str(src))
        if dst:
            covered_symbols.add(str(dst))

    unresolved_symbols: set[str] = set()
    for node_id in changed_symbols - covered_symbols:
        owner = _file_of_symbol(str(node_id))
        if owner is None:
            unresolved_symbols.add(node_id)
            continue
        files.add(owner)
        covered_symbols.add(node_id)

    if unresolved_symbols:
        if graph is None:
            return None
        rows = graph.query(
            "SELECT src, dst FROM okg.graph_edges "
            "WHERE edge_type = :et AND dst = ANY(:ids)",
            {"et": "contains", "ids": sorted(unresolved_symbols)},
        )
        for row in rows:
            src = row.get("src")
            dst = row.get("dst")
            if src:
                files.add(str(src))
            if dst:
                covered_symbols.add(str(dst))

    for node_id in unresolved_symbols - covered_symbols:
        owner = _file_of_symbol(str(node_id))
        if owner is None:
            return None
        files.add(owner)
    return files


def _interval_file_symbol_rows(
    store: Any,
    *,
    file_filter: set[str],
) -> list[dict[str, Any]] | None:
    """Fast path for scoped interval-backend reads.

    Chronos' generic visible-subquery rewrite is intentionally general, but this
    view's scoped path has a much tighter shape: start from the dirty file's
    ``contains`` edges and do indexed node lookups for the visible destination
    symbols. Return None when backend internals are unavailable.
    """

    if not file_filter:
        return []
    backend = getattr(getattr(store, "context", None), "_backend", None)
    if backend is None:
        return None
    try:
        edge_meta = backend._require_table("okg.graph_edges")
        node_meta = backend._require_table("okg.graph_nodes")
        segment = backend._current_segment("default")
        db = backend.db
    except Exception:  # noqa: BLE001 - fake/non-interval backend; use graph.query
        return None
    edge_table = edge_meta.physical_name
    node_table = node_meta.physical_name
    branch_point = segment.branch_point
    rows = db.execute(
        f"""
        WITH bp(branch_point) AS MATERIALIZED (VALUES (?::numeric)),
        scoped_edges AS MATERIALIZED (
            SELECT e.src, e.dst
              FROM {edge_table} e
              CROSS JOIN bp
             WHERE e.edge_type = ?
               AND e.src = ANY(?)
               AND e.live_lo <= bp.branch_point
               AND bp.branch_point < e.live_hi
               AND e.deleted = FALSE
        )
        SELECT e.src AS file_id,
               s.node_id AS symbol_id,
               COALESCE(s.attrs->>'path', e.src) AS file_path,
               s.attrs->>'name' AS symbol_name,
               s.attrs->>'kind' AS symbol_kind,
               s.attrs->>'qualified_name' AS qualified_name,
               CASE
                 WHEN (s.attrs->>'start_line') ~ '^-?[0-9]+$'
                 THEN (s.attrs->>'start_line')::bigint
                 ELSE NULL
               END AS start_line
          FROM scoped_edges e
          CROSS JOIN bp
          JOIN LATERAL (
              SELECT s.node_id, s.attrs
                FROM {node_table} s
               WHERE s.node_id = e.dst
                 AND s.subtype = ?
                 AND s.live_lo <= bp.branch_point
                 AND bp.branch_point < s.live_hi
                 AND s.deleted = FALSE
               LIMIT 1
          ) s ON TRUE
        """,
        [
            branch_point,
            "contains",
            sorted(file_filter),
            "code_symbol",
        ],
    ).fetchall()
    return [dict(row) for row in rows]


def build_corpus(
    conn: Any,
    *,
    generation_id: int,
    delta_scope: ViewDeltaScope | None = None,
    graph_store: Any | None = None,
) -> dict[str, Anchor]:
    """Read the published graph, stage file->symbol rows, return per-file anchors.

    Full scan when ``delta_scope`` is None; O(delta) when given a complete dirty
    file set (reads only those files' symbols and marks ``delta_scope.scoped``).
    """
    del generation_id  # read the branch head (chronos-native); see module docs
    def _record_timing(name: str, started: float) -> None:
        if delta_scope is None:
            return
        elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
        delta_scope.timings_ms[name] = round(
            delta_scope.timings_ms.get(name, 0.0) + elapsed,
            3,
        )

    if graph_store is None:
        from okg.substrate.graph_store import ChronosGraphStore

        stage_started = time.perf_counter()
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        _record_timing("store_attach", stage_started)
    else:
        store = graph_store
        if delta_scope is not None:
            delta_scope.timings_ms["store_attach.reused"] = 1.0

    graph: Any | None = None

    def _graph() -> Any:
        nonlocal graph
        if graph is None:
            stage_started = time.perf_counter()
            graph = store.checkout(branch="default")
            _record_timing("graph_checkout", stage_started)
        return graph

    file_filter: set[str] | None = None
    if delta_scope is not None:
        stage_started = time.perf_counter()
        file_filter = _dirty_file_scopes(delta_scope)
        if file_filter is None:
            file_filter = _dirty_file_scopes(delta_scope, graph=_graph())
        _record_timing("dirty_file_scopes", stage_started)
        if file_filter is not None:
            delta_scope.dirty_scope_keys.update(file_filter)
            delta_scope.scoped = True
            if delta_scope.changed_edge_endpoints.get("contains"):
                delta_scope.relation_rows_known_changed.add(RELATION_ROWS_KEY)
            elif any(
                RELATION_VISIBLE_NODE_ATTRS.intersection(fields)
                for changes in (
                    delta_scope.changed_node_attr_fields.get("source_file", {}),
                    delta_scope.changed_node_attr_fields.get("code_symbol", {}),
                )
                for fields in changes.values()
            ):
                delta_scope.relation_rows_known_changed.add(RELATION_ROWS_KEY)
        # file_filter None -> couldn't guarantee completeness -> full scan below.

    files: dict[str, str] = {}
    if file_filter is not None:
        # A code-only benchmark may have file -> symbol edges before the
        # git/files source has produced source_file nodes. Treat the edge src as
        # the file scope and use source_file attrs only as optional metadata.
        stage_started = time.perf_counter()
        symbol_rows = _interval_file_symbol_rows(
            store,
            file_filter=file_filter,
        )
        if symbol_rows is not None and delta_scope is not None:
            delta_scope.timings_ms["contains_symbol_join.direct_interval"] = 1.0
        if symbol_rows is None:
            symbol_rows = _graph().query(
                """
                SELECT e.src AS file_id,
                       s.node_id AS symbol_id,
                       COALESCE(f.attrs->>'path', s.attrs->>'path', e.src)
                         AS file_path,
                       s.attrs->>'name' AS symbol_name,
                       s.attrs->>'kind' AS symbol_kind,
                       s.attrs->>'qualified_name' AS qualified_name,
                       CASE
                         WHEN (s.attrs->>'start_line') ~ '^-?[0-9]+$'
                         THEN (s.attrs->>'start_line')::bigint
                         ELSE NULL
                       END AS start_line
                  FROM okg.graph_edges e
                  LEFT JOIN okg.graph_nodes f
                    ON f.node_id = e.src
                   AND f.subtype = :file_st
                 JOIN okg.graph_nodes s
                    ON s.node_id = e.dst
                   AND s.subtype = :symbol_st
                 WHERE e.edge_type = :et
                   AND e.src = ANY(:ids)
                """,
                {
                    "et": "contains",
                    "ids": sorted(file_filter),
                    "file_st": "source_file",
                    "symbol_st": "code_symbol",
                },
            )
        delta_scope.timings_ms["contains_symbol_join.row_count"] = float(
            len(symbol_rows)
        )
        _record_timing("contains_symbol_join", stage_started)
    else:
        stage_started = time.perf_counter()
        symbol_rows = _graph().query(
            """
            SELECT e.src AS file_id,
                   s.node_id AS symbol_id,
                   COALESCE(f.attrs->>'path', s.attrs->>'path', e.src)
                     AS file_path,
                   s.attrs->>'name' AS symbol_name,
                   s.attrs->>'kind' AS symbol_kind,
                   s.attrs->>'qualified_name' AS qualified_name,
                   CASE
                     WHEN (s.attrs->>'start_line') ~ '^-?[0-9]+$'
                     THEN (s.attrs->>'start_line')::bigint
                     ELSE NULL
                   END AS start_line
              FROM okg.graph_edges e
              LEFT JOIN okg.graph_nodes f
                ON f.node_id = e.src
               AND f.subtype = :file_st
              JOIN okg.graph_nodes s
                ON s.node_id = e.dst
               AND s.subtype = :symbol_st
             WHERE e.edge_type = :et
            """,
            {
                "et": "contains",
                "file_st": "source_file",
                "symbol_st": "code_symbol",
            },
        )
        if delta_scope is not None:
            delta_scope.timings_ms["contains_symbol_join.row_count"] = float(
                len(symbol_rows)
            )
        _record_timing("contains_symbol_join", stage_started)

    stage_started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for raw in symbol_rows:
        file_id = str(raw["file_id"])
        symbol_id = str(raw["symbol_id"])
        file_path = raw.get("file_path") or file_id
        if file_id not in files:
            files[file_id] = str(file_path)
        rows.append(
            {
                "scope_key": file_id,
                "row_key": symbol_id,
                "file_path": files[file_id],
                "symbol_name": raw.get("symbol_name"),
                "symbol_kind": raw.get("symbol_kind"),
                "qualified_name": raw.get("qualified_name"),
                "start_line": raw.get("start_line"),
            }
        )
        counts[file_id] = counts.get(file_id, 0) + 1
    _record_timing("row_build", stage_started)

    direct_relation_rows = delta_scope is not None and delta_scope.scoped
    if direct_relation_rows:
        delta_scope.relation_rows[RELATION_ROWS_KEY] = [
            row for row in rows if row.get("symbol_kind") != "module"
        ]
        if delta_scope is not None:
            delta_scope.timings_ms["stage_rows.direct"] = 1.0
    else:
        stage_started = time.perf_counter()
        with conn.cursor() as cur:
            step_started = time.perf_counter()
            cur.execute(
                f"""
                CREATE TEMP TABLE IF NOT EXISTS {ROWS_TEMP_TABLE} (
                  scope_key      text,
                  row_key        text,
                  file_path      text,
                  symbol_name    text,
                  symbol_kind    text,
                  qualified_name text,
                  start_line     bigint
                ) ON COMMIT PRESERVE ROWS
                """
            )
            _record_timing("stage_rows.create_temp", step_started)
            step_started = time.perf_counter()
            cur.execute(f"TRUNCATE {ROWS_TEMP_TABLE}")
            _record_timing("stage_rows.truncate", step_started)
            if rows:
                step_started = time.perf_counter()
                row_tuples = [
                    (
                        row["scope_key"],
                        row["row_key"],
                        row["file_path"],
                        row["symbol_name"],
                        row["symbol_kind"],
                        row["qualified_name"],
                        row["start_line"],
                    )
                    for row in rows
                ]
                copy = getattr(cur, "copy", None)
                if copy is not None:
                    with copy(
                        f"COPY {ROWS_TEMP_TABLE} "
                        "(scope_key,row_key,file_path,symbol_name,symbol_kind,"
                        "qualified_name,start_line) FROM STDIN"
                    ) as copy_ctx:
                        for row in row_tuples:
                            copy_ctx.write_row(row)
                else:
                    cur.executemany(
                        f"INSERT INTO {ROWS_TEMP_TABLE} "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        row_tuples,
                    )
                _record_timing("stage_rows.insert_rows", step_started)
        _record_timing("stage_rows", stage_started)
    if delta_scope is not None:
        delta_scope.timings_ms["stage_rows.row_count"] = float(len(rows))

    stage_started = time.perf_counter()
    corpus: dict[str, Anchor] = {}
    for node_id, path in sorted(files.items()):
        anchor_id = f"{ANCHOR_PREFIX}{node_id}"
        corpus[anchor_id] = Anchor.from_content(
            id=anchor_id,
            content=json.dumps(
                {"file": path, "node_id": node_id, "symbols": counts[node_id]},
                sort_keys=True,
            ),
        )
    _record_timing("build_anchors", stage_started)
    return corpus


def file_symbols_planner(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[FragmentSpec]:
    """Emit one verbatim quote per source-file anchor (carries lineage). No LLM."""
    del view_def
    return [QuoteSpec(id=anchor_id, anchor_id=anchor_id) for anchor_id in sorted(corpus)]


def file_symbols_assembler(view: Any) -> str:
    """Required by ViewDefinition; relation-only view renders no markdown body."""
    del view
    return ""


def anchor_resolver(anchor_id: str) -> str | None:
    """Recover the ``source_file`` node id an anchor cites (or None)."""
    if not anchor_id.startswith(ANCHOR_PREFIX):
        return None
    node_id = anchor_id[len(ANCHOR_PREFIX):]
    return node_id or None


def file_symbols_view_def() -> ViewDefinition:
    return ViewDefinition(
        view_id=VIEW_ID,
        planner=file_symbols_planner,
        assembler=file_symbols_assembler,
        view_def_hash=VIEW_DEF_HASH,
        display_name="File symbols (contains)",
        output_templates=(
            ViewOutputTemplate.native_json(),
            ViewOutputTemplate.sql_relation(
                slot_name="file_symbols",
                logical_relation_id=f"{VIEW_ID}.rows",
                title="Symbols per source file",
                columns=(
                    ViewRelationColumn("scope_key", label="File node"),
                    ViewRelationColumn("row_key", label="Symbol node"),
                    ViewRelationColumn("file_path", label="File"),
                    ViewRelationColumn("symbol_name", label="Symbol"),
                    ViewRelationColumn("symbol_kind", label="Kind"),
                    ViewRelationColumn("qualified_name", label="Qualified name"),
                    ViewRelationColumn("start_line", label="Line", data_type="integer"),
                ),
                scope_key_column="scope_key",
                row_key_column="row_key",
                sql=ROWS_SQL,
                timeout_ms=10000,
                direct_rows_key=RELATION_ROWS_KEY,
            ),
        ),
    )


__all__ = [
    "VIEW_ID",
    "build_corpus",
    "file_symbols_planner",
    "file_symbols_assembler",
    "anchor_resolver",
    "file_symbols_view_def",
]
