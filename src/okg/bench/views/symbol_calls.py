"""`symbol_calls` — a pure non-LLM operator view over the code call graph.

For each `code_symbol` that makes calls, list the symbols it `calls`. This is a
second, deliberately different view from `file_symbols`: its scope axis is the
caller *symbol* (not the file), and it reads `calls` edges (not `contains`).
Having two views with independent scopes lets us measure cross-view
incrementality (a definition edit to one view must not rebuild the other).

Same chronos-native shape as `file_symbols`: `build_corpus` reads the graph via
a branch-head checkout and stages rows into a `pg_temp` table; the `sql_relation`
slot reads that table; the corpus anchors cite the caller `code_symbol` nodes
(which carry `source` + `source_record_id`) so lineage envelopes are non-empty.
No LLM is involved (QuoteSpec planner only).
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
    default_output_templates,
)
from okg.views.types import Anchor

VIEW_ID = "bench.symbol_calls"
ANCHOR_PREFIX = "symbol_calls:"
ROWS_TEMP_TABLE = "symbol_calls_rows_v2"

ROWS_SQL = f"""
SELECT scope_key, row_key, caller, callee, callee_kind, caller_file
  FROM pg_temp.{ROWS_TEMP_TABLE}
 ORDER BY scope_key, row_key
"""

VIEW_DEF_HASH = (
    "bench.symbol_calls.v1:"
    f"{hashlib.sha256(ROWS_SQL.encode()).hexdigest()[:16]}"
)
_NEEDS_GRAPH_FOR_DIRTY_CALLERS = object()


def _dirty_caller_scopes_from_delta_only(
    delta_scope: ViewDeltaScope,
) -> set[str] | object:
    """Return caller scopes when the delta packet is sufficient by itself."""
    changed_symbols = {
        str(node_id)
        for node_id in delta_scope.changed_nodes.get("code_symbol", ())
        if str(node_id)
    }
    changed_edge_ids = {
        int(edge_id)
        for edge_id in delta_scope.changed_edges.get("calls", ())
    }
    attr_changes = delta_scope.changed_node_attr_fields.get("code_symbol", {})
    lifecycle_only_symbols = (
        bool(changed_symbols)
        and changed_symbols <= set(attr_changes)
        and all(
            {"__new__", "__deleted__"} & {
                str(field) for field in attr_changes.get(node_id, ())
            }
            for node_id in changed_symbols
        )
    )
    if changed_symbols and not lifecycle_only_symbols:
        return _NEEDS_GRAPH_FOR_DIRTY_CALLERS
    if changed_edge_ids:
        return _NEEDS_GRAPH_FOR_DIRTY_CALLERS
    return set()


def _dirty_caller_scopes(
    graph: Any,
    delta_scope: ViewDeltaScope,
) -> set[str] | None:
    """Return the complete caller-scope set, or None when it is unknowable."""
    changed_symbols = {
        str(node_id)
        for node_id in delta_scope.changed_nodes.get("code_symbol", ())
        if str(node_id)
    }
    callers: set[str] = set()

    changed_edge_ids = {
        int(edge_id)
        for edge_id in delta_scope.changed_edges.get("calls", ())
    }
    attr_changes = delta_scope.changed_node_attr_fields.get("code_symbol", {})
    lifecycle_only_symbols = (
        bool(changed_symbols)
        and changed_symbols <= set(attr_changes)
        and all(
            {"__new__", "__deleted__"} & {
                str(field) for field in attr_changes.get(node_id, ())
            }
            for node_id in changed_symbols
        )
    )

    if changed_symbols and not lifecycle_only_symbols:
        for edge in graph.query(
            "SELECT src FROM okg.graph_edges "
            "WHERE edge_type = :et AND src = ANY(:ids)",
            {"et": "calls", "ids": sorted(changed_symbols)},
        ):
            callers.add(str(edge["src"]))
        for edge in graph.query(
            "SELECT src FROM okg.graph_edges "
            "WHERE edge_type = :et AND dst = ANY(:ids)",
            {"et": "calls", "ids": sorted(changed_symbols)},
        ):
            callers.add(str(edge["src"]))

    if changed_edge_ids:
        edge_rows = list(
            graph.query(
                "SELECT edge_id, src FROM okg.graph_edges "
                "WHERE edge_type = :et AND edge_id = ANY(:ids)",
                {"et": "calls", "ids": sorted(changed_edge_ids)},
            )
        )
        resolved_edge_ids = {int(row["edge_id"]) for row in edge_rows}
        endpoint_rows = [
            endpoint
            for endpoint in delta_scope.changed_edge_endpoints.get("calls", ())
            if int(endpoint.get("edge_id", -1)) in changed_edge_ids
        ]
        resolved_edge_ids.update(int(row["edge_id"]) for row in endpoint_rows)
        if resolved_edge_ids != changed_edge_ids:
            # Deleted call edges are not visible at branch head, and this dirty
            # scope does not carry every old endpoint. Full-scan rather than
            # risk carrying a stale caller row forward.
            return None
        callers.update(str(row["src"]) for row in edge_rows)
        callers.update(str(row["src"]) for row in endpoint_rows)

    return callers


def _load_prior_quote_anchors(
    conn: Any,
    *,
    before_generation_id: int,
    dirty_anchor_ids: set[str],
) -> dict[str, Anchor] | None:
    """Load clean caller anchors from the previous native view manifest.

    `symbol_calls` keeps markdown/json outputs in addition to the scoped
    relation. A scoped corpus therefore still needs clean anchors so the normal
    view refresh can reuse clean quote fragments instead of dropping them.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.inline_payload
              FROM okg.derived_output_manifests m
              JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
               AND s.slot_name = 'json'
             WHERE m.definition_id = %(definition_id)s
               AND m.target_generation_id < %(before_generation_id)s
               AND m.status = 'completed'
             ORDER BY m.target_generation_id DESC, m.promoted_at DESC
             LIMIT 1
            """,
            {
                "definition_id": f"view:{VIEW_ID}",
                "before_generation_id": before_generation_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        return None

    inline_payload = dict(row[0] or {})
    if inline_payload.get("kind") != "json":
        return None
    value = inline_payload.get("value")
    if not isinstance(value, dict):
        return None
    view_manifest = value.get("view_manifest")
    if not isinstance(view_manifest, dict):
        return None
    fragments = view_manifest.get("fragments")
    if not isinstance(fragments, list):
        return None

    corpus: dict[str, Anchor] = {}
    for fragment in fragments:
        if not isinstance(fragment, dict):
            return None
        if fragment.get("kind") != "quote":
            return None
        sources = fragment.get("sources")
        if not isinstance(sources, list) or len(sources) != 1:
            return None
        anchor_id = str(sources[0])
        if anchor_id in dirty_anchor_ids:
            continue
        source_hashes = fragment.get("source_hashes")
        content = fragment.get("content")
        if not isinstance(source_hashes, dict) or not isinstance(content, str):
            return None
        source_hash = source_hashes.get(anchor_id)
        if not isinstance(source_hash, str):
            return None
        corpus[anchor_id] = Anchor(
            id=anchor_id,
            content=content,
            content_hash=source_hash,
        )
    return corpus


def _has_prior_view_manifest(
    conn: Any,
    *,
    before_generation_id: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM okg.derived_output_manifests
             WHERE definition_id = %(definition_id)s
               AND target_generation_id < %(before_generation_id)s
               AND status = 'completed'
             LIMIT 1
            """,
            {
                "definition_id": f"view:{VIEW_ID}",
                "before_generation_id": before_generation_id,
            },
        )
        return cur.fetchone() is not None


def _query_symbol_attrs(
    graph: Any,
    symbol_ids: set[str] | None,
) -> dict[str, dict[str, Any]]:
    if symbol_ids is not None and not symbol_ids:
        return {}
    if symbol_ids is None:
        rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = :st",
            {"st": "code_symbol"},
        )
    else:
        rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE subtype = :st AND node_id = ANY(:ids)",
            {"st": "code_symbol", "ids": sorted(symbol_ids)},
        )
    return {str(raw["node_id"]): (raw["attrs"] or {}) for raw in rows}


def _query_call_edges(
    graph: Any,
    caller_ids: set[str] | None,
) -> list[tuple[str, str]]:
    if caller_ids is not None and not caller_ids:
        return []
    if caller_ids is None:
        rows = graph.query(
            "SELECT src, dst FROM okg.graph_edges WHERE edge_type = :et",
            {"et": "calls"},
        )
    else:
        rows = graph.query(
            "SELECT src, dst FROM okg.graph_edges "
            "WHERE edge_type = :et AND src = ANY(:ids)",
            {"et": "calls", "ids": sorted(caller_ids)},
        )
    return [(str(edge["src"]), str(edge["dst"])) for edge in rows]


def _stage_rows_and_build_dirty_corpus(
    conn: Any,
    *,
    symbols: dict[str, dict[str, Any]],
    edges: list[tuple[str, str]],
    corpus: dict[str, Anchor] | None = None,
    timing_sink: dict[str, float] | None = None,
) -> dict[str, Anchor]:
    def _record_timing(name: str, started: float) -> None:
        if timing_sink is None:
            return
        elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
        timing_sink[name] = round(timing_sink.get(name, 0.0) + elapsed, 3)

    rows: list[tuple[Any, ...]] = []
    callers: dict[str, int] = {}
    for src, dst in edges:
        if src not in symbols:
            continue
        caller = symbols[src]
        callee = symbols.get(dst) or {}
        rows.append(
            (
                src,                                   # scope_key = caller symbol
                dst,                                   # row_key  = callee symbol
                caller.get("qualified_name") or caller.get("name"),
                callee.get("qualified_name") or callee.get("name") or dst,
                callee.get("kind") or "unresolved",
                caller.get("path"),
            )
        )
        callers[src] = callers.get(src, 0) + 1

    with conn.cursor() as cur:
        step_started = time.perf_counter()
        cur.execute(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS {ROWS_TEMP_TABLE} (
              scope_key   text,
              row_key     text,
              caller      text,
              callee      text,
              callee_kind text,
              caller_file text
            ) ON COMMIT PRESERVE ROWS
            """
        )
        _record_timing("stage_rows.create_temp", step_started)
        step_started = time.perf_counter()
        cur.execute(f"TRUNCATE {ROWS_TEMP_TABLE}")
        _record_timing("stage_rows.truncate", step_started)
        if rows:
            step_started = time.perf_counter()
            copy = getattr(cur, "copy", None)
            if copy is not None:
                with copy(
                    f"COPY {ROWS_TEMP_TABLE} "
                    "(scope_key,row_key,caller,callee,callee_kind,caller_file) "
                    "FROM STDIN"
                ) as copy_ctx:
                    for row in rows:
                        copy_ctx.write_row(row)
            else:
                cur.executemany(
                    f"INSERT INTO {ROWS_TEMP_TABLE} VALUES (%s,%s,%s,%s,%s,%s)",
                    rows,
                )
            _record_timing("stage_rows.insert_rows", step_started)
    if timing_sink is not None:
        timing_sink["stage_rows.row_count"] = float(len(rows))

    out = dict(corpus or {})
    for node_id, call_count in sorted(callers.items()):
        anchor_id = f"{ANCHOR_PREFIX}{node_id}"
        attrs = symbols[node_id]
        out[anchor_id] = Anchor.from_content(
            id=anchor_id,
            content=json.dumps(
                {
                    "symbol": attrs.get("qualified_name") or attrs.get("name"),
                    "node_id": node_id,
                    "calls": call_count,
                },
                sort_keys=True,
            ),
        )
    return out


def build_corpus(
    conn: Any,
    *,
    generation_id: int,
    delta_scope: ViewDeltaScope | None = None,
) -> dict[str, Anchor]:
    """Read code symbols + `calls` edges, stage caller->callee rows, cite callers."""
    graph: Any | None = None

    def _graph() -> Any:
        nonlocal graph
        if graph is None:
            stage_started = time.perf_counter()
            from okg.substrate.graph_store import ChronosGraphStore

            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            graph = store.checkout(branch="default")
            _record_timing("graph_checkout", stage_started)
        return graph

    def _record_timing(name: str, started: float) -> None:
        if delta_scope is None:
            return
        elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
        delta_scope.timings_ms[name] = round(
            delta_scope.timings_ms.get(name, 0.0) + elapsed,
            3,
        )

    caller_filter: set[str] | None = None
    prior_corpus: dict[str, Anchor] | None = None
    if delta_scope is not None:
        stage_started = time.perf_counter()
        caller_filter = _dirty_caller_scopes_from_delta_only(delta_scope)
        if caller_filter is _NEEDS_GRAPH_FOR_DIRTY_CALLERS:
            caller_filter = _dirty_caller_scopes(_graph(), delta_scope)
        _record_timing("dirty_caller_scopes", stage_started)
        if caller_filter is not None:
            if not caller_filter:
                delta_scope.scoped = True
                delta_scope.timings_ms["prior_manifest_probe.skipped_empty_delta"] = 1.0
                return {}
            else:
                dirty_anchor_ids = {
                    f"{ANCHOR_PREFIX}{node_id}" for node_id in caller_filter
                }
                stage_started = time.perf_counter()
                prior_corpus = _load_prior_quote_anchors(
                    conn,
                    before_generation_id=generation_id,
                    dirty_anchor_ids=dirty_anchor_ids,
                )
                _record_timing("load_prior_quote_anchors", stage_started)
                if prior_corpus is None:
                    caller_filter = None
                else:
                    delta_scope.dirty_scope_keys.update(caller_filter)
                    delta_scope.scoped = True

    graph = _graph()
    stage_started = time.perf_counter()
    symbols = _query_symbol_attrs(graph, caller_filter)
    _record_timing("query_caller_symbols", stage_started)
    stage_started = time.perf_counter()
    edges = _query_call_edges(graph, caller_filter)
    _record_timing("query_call_edges", stage_started)
    callee_ids = {dst for _, dst in edges} - set(symbols)
    stage_started = time.perf_counter()
    symbols.update(_query_symbol_attrs(graph, callee_ids))
    _record_timing("query_callee_symbols", stage_started)
    stage_started = time.perf_counter()
    corpus = _stage_rows_and_build_dirty_corpus(
        conn,
        symbols=symbols,
        edges=edges,
        corpus=prior_corpus,
        timing_sink=delta_scope.timings_ms if delta_scope is not None else None,
    )
    _record_timing("stage_rows_and_build_corpus", stage_started)
    return corpus


def symbol_calls_planner(
    corpus: dict[str, Anchor],
    view_def: ViewDefinition,
) -> list[FragmentSpec]:
    del view_def
    return [QuoteSpec(id=anchor_id, anchor_id=anchor_id) for anchor_id in sorted(corpus)]


def symbol_calls_assembler(view: Any) -> str:
    callers: list[dict[str, Any]] = []
    for fragment in view.fragments:
        try:
            callers.append(json.loads(fragment.content))
        except Exception:  # noqa: BLE001
            continue
    total_calls = sum(int(item.get("calls") or 0) for item in callers)
    lines = [
        "# Symbol Calls",
        "",
        f"Calling symbols: **{len(callers)}**",
        f"Call edges: **{total_calls}**",
        "",
        "Per-caller call rows are available in the `symbol_calls` relation.",
    ]
    return "\n".join(lines).rstrip() + "\n"


def anchor_resolver(anchor_id: str) -> str | None:
    if not anchor_id.startswith(ANCHOR_PREFIX):
        return None
    node_id = anchor_id[len(ANCHOR_PREFIX):]
    return node_id or None


def symbol_calls_view_def() -> ViewDefinition:
    return ViewDefinition(
        view_id=VIEW_ID,
        planner=symbol_calls_planner,
        assembler=symbol_calls_assembler,
        view_def_hash=VIEW_DEF_HASH,
        display_name="Symbol calls (call graph)",
        output_templates=(
            *default_output_templates(),
            ViewOutputTemplate.sql_relation(
                slot_name="symbol_calls",
                logical_relation_id=f"{VIEW_ID}.rows",
                title="Calls per symbol",
                columns=(
                    ViewRelationColumn("scope_key", label="Caller node"),
                    ViewRelationColumn("row_key", label="Callee node"),
                    ViewRelationColumn("caller", label="Caller"),
                    ViewRelationColumn("callee", label="Callee"),
                    ViewRelationColumn("callee_kind", label="Callee kind"),
                    ViewRelationColumn("caller_file", label="File"),
                ),
                scope_key_column="scope_key",
                row_key_column="row_key",
                sql=ROWS_SQL,
                timeout_ms=10000,
            ),
        ),
    )


__all__ = [
    "VIEW_ID",
    "build_corpus",
    "symbol_calls_planner",
    "symbol_calls_assembler",
    "anchor_resolver",
    "symbol_calls_view_def",
]
