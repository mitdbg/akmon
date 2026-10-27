#!/usr/bin/env python3
"""Read-speed and row-equality gate for storage redesign S2.

PACT lane-s-storage-redesign-s2, requirements gate-baseline, gate-sql-p50,
gate-sql-p95, gate-tool-call, gate-rows-equal and gate-production-settings
(design section 11). Each step is a subcommand:

``bench``
    Runs every shape of a shapes file through the MCP tools, as the role in
    ``OKG_MCP_RO_DSN`` (``okg_mcp``), pinned to one generation, in whichever
    code tree runs this file: today's path is an older tree's code (its own
    copy of this file), the graph path is this tree, which since PACT
    lane-s-storage-cutover has no other read path. Per shape:
    one capture call, ``--warm`` warm-ups and ``--runs`` timed calls (whole
    tool call and the SQL client wall of the read's statements, including a
    statement that fails, such as a count the count budget cancels: the
    database spent that time; ``sql_ms_returned`` keeps the walls of the
    statements that returned only), then the same warm-ups and runs with
    ``EXPLAIN (ANALYZE, FORMAT JSON)`` of each read statement for its
    execution time. Every call's answer is digested twice: whole, and its
    rows without the total count; each call's total count and its basis are
    kept (the EXPLAIN calls' counts apart, not judged), as are the answer's
    ids and the search supplement's candidate ids,
    for the row checks. Any statement that sets a planner, parallel or
    plan-cache option is recorded.
``recompute``
    The independent answer: the Chronos rows visible at the pin's branch
    point, plus that generation's rows in ``okg.attribute_effective_attrs``
    (reserved keys excluded), evaluated in Python.
``parity``
    Whole-graph overlay parity at one pin (requirement overlay-parity), as
    okg_mcp, read-only, in the S2 code tree: every node and edge through the
    page read under the graph path, in keyset batches, against the
    recompute's rows; a JSON report and a non-zero exit on any mismatch.
``verdict``
    Reads alternating bench blocks (today, graph, today, graph, ...) and the
    recompute, and prints the table and the verdict per requirement.

Thresholds (each adjacent today/graph pair of blocks): SQL client wall p50
of the graph path <= max(1.2 x today, today + 5 ms); p95 <= max(1.5 x today,
today + 10 ms); whole tool-call p50 <= 1.1 x today. Rows: every call of
both paths returns the same rows, and they equal the recompute; a shape
today's path refuses or answers stale is listed by name and compared with
the recompute only. Total counts (rule A, Jason via the orchestrator,
2026-10-19): a count the count budget left "unknown" on one side is not a
row difference; every known count of both paths must agree with each other
and with the recompute; and the graph path answering "unknown" (or a lower
bound) where today's path had an exact count is its own failure, "count
became unknown". Nothing here sets a planner, parallel or plan-cache
option.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
from collections import Counter
from typing import Any, Callable, Iterable, Mapping, Sequence

# --------------------------------------------------------------------------
# Pure parts (unit-tested in tests/bench/test_storage_redesign_gate.py)
# --------------------------------------------------------------------------

#: The tables a read statement touches. The SQL client wall of a call sums
#: the statements that reference any of them, so the graph path's own checks
#: (overlay marker, compaction) count against it. (okg#3112 S4 dropped the
#: snapshot and its payload store, which the materialized path read.)
READ_RELATIONS = (
    "attribute_effective_attrs",
    "attribute_overlay_generations",
    "_chronos_b_interval_okg_graph_",
    "compacted_generation",
)

#: A statement that changes a planner, parallel or plan-cache option.
#: ``statement_timeout`` (the bounded count) is not one (gate-production-settings).
_SETTING = re.compile(
    r"\b(SET\s+(LOCAL\s+|SESSION\s+)?(enable_\w+|plan_cache_mode|max_parallel\w*|"
    r"parallel_\w+|random_page_cost|seq_page_cost|cpu_\w+|work_mem|jit\w*|"
    r"geqo\w*|from_collapse_limit|join_collapse_limit)\b"
    r"|set_config\s*\(\s*'(enable_\w+|plan_cache_mode|max_parallel\w*|parallel_\w+|"
    r"random_page_cost|work_mem|jit\w*)')",
    re.IGNORECASE,
)


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile (q in (0, 100]); None for no values. p50 of
    an even count is the mean of the two middle values (the median)."""
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if q == 50:
        n = len(ordered)
        mid = n // 2
        return ordered[mid] if n % 2 else (ordered[mid - 1] + ordered[mid]) / 2
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def sql_p50_limit(today_p50: float) -> float:
    return max(1.2 * today_p50, today_p50 + 5.0)


def sql_p95_limit(today_p95: float) -> float:
    return max(1.5 * today_p95, today_p95 + 10.0)


def tool_p50_limit(today_p50: float) -> float:
    return 1.1 * today_p50


def gate_pair(today: Mapping[str, Sequence[float]], graph: Mapping[str, Sequence[float]]
              ) -> dict[str, Any]:
    """The three timing checks for one adjacent pair of blocks. ``today`` and
    ``graph`` map ``sql_ms`` and ``tool_ms`` to the timed runs."""
    out: dict[str, Any] = {}
    t50, g50 = percentile(today["sql_ms"], 50), percentile(graph["sql_ms"], 50)
    t95, g95 = percentile(today["sql_ms"], 95), percentile(graph["sql_ms"], 95)
    tt, gt = percentile(today["tool_ms"], 50), percentile(graph["tool_ms"], 50)
    out["sql_p50"] = {"today": t50, "graph": g50, "limit": sql_p50_limit(t50),
                      "pass": g50 <= sql_p50_limit(t50)}
    out["sql_p95"] = {"today": t95, "graph": g95, "limit": sql_p95_limit(t95),
                      "pass": g95 <= sql_p95_limit(t95)}
    out["tool_p50"] = {"today": tt, "graph": gt, "limit": tool_p50_limit(tt),
                       "pass": gt <= tool_p50_limit(tt)}
    return out


def is_read_statement(sql: str) -> bool:
    head = sql.lstrip()[:12].upper()
    if head.startswith(("EXPLAIN", "SET ", "SHOW ", "BEGIN", "COMMIT", "ROLLBACK")):
        return False
    return any(rel in sql for rel in READ_RELATIONS)


def sets_planner_option(sql: str) -> bool:
    return bool(_SETTING.search(sql))


_VOLATILE = {"publish_freshness", "source_freshness", "timing", "timings_ms", "elapsed_ms",
             "request_id", "served_at", "notes", "diagnostics", "budget", "logical_session_id",
             "continuation_token", "result_reference", "expires_at"}


def answer_of(result: Any) -> dict[str, Any]:
    """The comparable answer of a tool result: its rows in order (groups as
    a sorted set) and its paging facts; volatile fields left out."""
    if not isinstance(result, dict):
        return {"value": result}
    if "exception" in result:
        return {"exception": result["exception"], "code": result.get("code")}
    rows: Any
    if "groups" in result:
        rows = sorted(json.dumps(g, sort_keys=True, default=str) for g in result["groups"])
    else:
        rows = [json.dumps(r, sort_keys=True, default=str)
                for r in (result.get("items") or result.get("results")
                          or result.get("neighbors") or result.get("rows") or [])]
    meta = {k: result.get(k) for k in ("total_count", "total_count_basis", "truncated",
                                       "next_offset", "has_more", "error", "code")
            if k in result}
    return {"rows": rows, "meta": meta}


#: The answer facts a budget-bounded count decides (rule A compares them apart).
COUNT_KEYS = ("total_count", "total_count_basis")


def rows_of(answer: Mapping[str, Any]) -> dict[str, Any]:
    """An ``answer_of`` without the count's value and basis: what rule A
    compares as the rows (ids in order, paging, ``has_more``, refusals).
    Whether the answer reports a count at all stays in, so an answer that
    drops its count keys is a row difference, not a count rule A excuses."""
    if "meta" not in answer:
        return dict(answer)
    meta = {k: v for k, v in answer["meta"].items() if k not in COUNT_KEYS}
    meta["count_reported"] = any(k in answer["meta"] for k in COUNT_KEYS)
    return {"rows": answer["rows"], "meta": meta}


def count_of(result: Any) -> dict[str, Any] | None:
    """One call's total count: ``{"n", "basis"}``; ``n`` None is a count the
    budget left unknown. None for an answer without a count (``rows_of``
    keeps that absence in the rows)."""
    if not isinstance(result, dict) or "exception" in result:
        return None
    if not any(k in result for k in COUNT_KEYS):
        return None
    n = result.get("total_count")
    basis = result.get("total_count_basis") or ("unknown" if n is None else "exact")
    return {"n": n, "basis": "unknown" if n is None else basis}


def compare_counts(today: Sequence[Mapping[str, Any] | None],
                   graph: Sequence[Mapping[str, Any] | None]) -> dict[str, Any]:
    """Rule A over the counts of the capture and timed calls of both paths
    (the EXPLAIN calls run each statement twice, slower, so their counts are
    kept apart and not judged). ``agree``: every known count matches every
    other (exact equal; a lower bound at most every exact count and equal to
    every other lower bound). ``lost``: today's path had an exact count in
    some call and the graph path answered unknown or a lower bound in some
    call."""
    t = [c for c in today if c is not None]
    g = [c for c in graph if c is not None]
    exact = {c["n"] for c in t + g if c["basis"] != "unknown" and c["basis"] != "lower_bound"}
    bounds = {c["n"] for c in t + g if c["basis"] == "lower_bound"}
    agree = (len(exact) <= 1 and len(bounds) <= 1
             and all(b <= e for b in bounds for e in exact))
    today_exact = any(c["basis"] not in ("unknown", "lower_bound") for c in t)
    graph_vague = any(c["basis"] in ("unknown", "lower_bound") for c in g)
    return {"agree": agree, "lost": today_exact and graph_vague,
            "today": sorted({f"{c['basis']}:{c['n']}" for c in t}),
            "graph": sorted({f"{c['basis']}:{c['n']}" for c in g})}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:16]


def answer_ids(result: Any) -> list[str] | None:
    """The ids of a page's rows in order (node_id, edge_id or result id)."""
    if not isinstance(result, dict):
        return None
    rows = result.get("items") or result.get("results") or result.get("neighbors")
    if rows is None:
        return None
    out = []
    for row in rows:
        for key in ("node_id", "edge_id", "id"):
            if isinstance(row, dict) and key in row:
                out.append(str(row[key]))
                break
    return out


def answer_projected(result: Any, projection: Sequence[str] | None) -> list[list[Any]] | None:
    """A map page's projected values in order (its rows carry no id): the
    ``attrs.k`` projection is served as the row key ``attrs_k``."""
    if not projection or not isinstance(result, dict) or result.get("items") is None:
        return None
    keys = [str(f).replace(".", "_", 1) for f in projection]
    return [[row.get(k) for k in keys] for row in result["items"] if isinstance(row, dict)]


def answer_groups(result: Any) -> dict[str, int] | None:
    """Aggregate groups as {key: count}; a missing key and JSON null both
    read as null, so their counts are summed."""
    if not isinstance(result, dict) or "groups" not in result:
        return None
    out: Counter = Counter()
    for group in result["groups"]:
        count = group.get("count")
        key = {k: v for k, v in group.items() if k not in ("count", "metrics")}
        out[json.dumps(key.get("key", key), sort_keys=True, default=str)] += int(count or 0)
    return dict(out)


# --- the independent evaluator (recompute) -----------------------------------

_ABSENT = object()


def json_equal(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(json_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(json_equal(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def json_contains(a: Any, b: Any) -> bool:
    """jsonb ``a @> b`` one level below the top (no raw-scalar exception)."""
    if isinstance(b, dict):
        return isinstance(a, dict) and all(k in a and json_contains(a[k], v) for k, v in b.items())
    if isinstance(b, list):
        return isinstance(a, list) and all(any(json_contains(x, y) for x in a) for y in b)
    if isinstance(a, (dict, list)):
        return False
    return json_equal(a, b)


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def matches(row: Mapping[str, Any], predicate: Any) -> bool:
    """The v1 predicate on one row (``attrs`` merged). Covers the gated
    shapes: and, or, not, eq, in, exists, between, lt, gt."""
    if predicate is None:
        return True
    op = predicate["op"]
    if op == "and":
        return all(matches(row, c) for c in predicate["clauses"])
    if op == "or":
        return any(matches(row, c) for c in predicate["clauses"])
    if op == "not":
        return not matches(row, predicate["clause"])
    field = predicate["field"]
    if field.startswith("attrs."):
        value = row["attrs"].get(field[6:], _ABSENT)
    else:
        value = row.get(field, _ABSENT)
        if value is None:
            value = _ABSENT
    if op == "exists":
        return value is not _ABSENT
    if op == "eq":
        if predicate["value"] is None:
            return value is _ABSENT or value is None
        return value is not _ABSENT and json_equal(value, predicate["value"])
    if op == "in":
        return any(matches(row, {"op": "eq", "field": field, "value": v})
                   for v in predicate["values"])
    if op in ("lt", "gt", "between"):
        bounds = predicate["values"] if op == "between" else [predicate["value"]]
        if all(_number(b) for b in bounds):
            if not _number(value):
                return False
        elif all(isinstance(b, str) for b in bounds):
            if not isinstance(value, str):
                return False
        else:
            return False
        if op == "lt":
            return value < bounds[0]
        if op == "gt":
            return value > bounds[0]
        return bounds[0] <= value <= bounds[1]
    raise ValueError(f"recompute has no rule for {op!r}")


def supplement_matches(attrs: Mapping[str, Any], attr_filters: Mapping[str, Any],
                       terms: Iterable[str]) -> bool:
    """The search supplement's row test: text present, object containment of
    the filter, every term a case-insensitive substring of the text."""
    if "text" not in attrs or not json_contains(dict(attrs), dict(attr_filters)):
        return False
    value = attrs["text"]
    lowered = (value if isinstance(value, str) else json.dumps(value)).lower()
    return all(t.lower() in lowered for t in terms)


# --- the whole-graph overlay parity check (parity) ---------------------------

_END = object()
#: The row fields compared besides the attributes, per kind (when both sides carry them).
PARITY_FIELDS = {"nodes": ("subtype",), "edges": ("edge_type", "src", "dst")}


def row_id(row: Mapping[str, Any]) -> str:
    for key in ("node_id", "edge_id", "id"):
        if key in row:
            return str(row[key])
    raise KeyError("row has no node_id, edge_id or id")


def _shown(value: Any, limit: int = 200) -> Any:
    text = json.dumps(value, sort_keys=True, default=str)
    return value if len(text) <= limit else text[:limit] + "..."


def attrs_diff(reference: Mapping[str, Any], other: Mapping[str, Any]) -> dict[str, Any]:
    """Which attribute keys differ: only in the reference, only in the other
    side, and present in both with different values (both values shown)."""
    return {
        "only_reference": sorted(k for k in reference if k not in other),
        "only_side": sorted(k for k in other if k not in reference),
        "different": {k: [_shown(reference[k]), _shown(other[k])]
                      for k in sorted(set(reference) & set(other))
                      if not json_equal(reference[k], other[k])},
    }


def compare_streams(kind: str, streams: Mapping[str, Iterable[Mapping[str, Any]]],
                    reference: str, limit: int | None = None,
                    max_mismatches: int = 50) -> dict[str, Any]:
    """Walk id-ordered row streams in step and compare each with ``reference``:
    the same id at each position, the same type fields and equal attributes.
    A position whose ids differ (a row missing or extra) is a mismatch and
    ends the walk: the ids after it can no longer be paired. The walk also
    ends at ``max_mismatches`` and at ``limit`` positions (a test aid; a
    stream longer than the limit is not read further). ``compared`` counts
    the positions every stream agreed on in id; ``counts`` the rows read from
    each stream. ``complete`` is true only when every stream was read to its
    end with no early stop: a walk cut by ``limit`` (even one that happens to
    land on the last row) is not complete."""
    iters = {name: iter(stream) for name, stream in streams.items()}
    counts = {name: 0 for name in streams}
    mismatches: list[dict[str, Any]] = []
    compared = 0
    stopped: str | None = None
    exhausted = False
    while limit is None or compared < limit:
        heads = {name: next(it, _END) for name, it in iters.items()}
        for name, head in heads.items():
            if head is not _END:
                counts[name] += 1
        if all(h is _END for h in heads.values()):
            exhausted = True
            break
        ref = heads[reference]
        ref_id = None if ref is _END else row_id(ref)
        for name, head in heads.items():
            if name == reference:
                continue
            side_id = None if head is _END else row_id(head)
            if ref is _END or head is _END or ref_id != side_id:
                mismatches.append({"kind": kind, "problem": "id_differs", "side": name,
                                   "reference_id": ref_id, "side_id": side_id,
                                   "position": compared})
                stopped = "ids_differ"
                continue
            fields = [f for f in PARITY_FIELDS[kind]
                      if f in ref and f in head and str(ref[f]) != str(head[f])]
            attrs = None
            if not json_equal(dict(ref.get("attrs") or {}), dict(head.get("attrs") or {})):
                attrs = attrs_diff(ref.get("attrs") or {}, head.get("attrs") or {})
            if fields or attrs is not None:
                mismatches.append({"kind": kind, "problem": "values_differ", "side": name,
                                   "id": ref_id, "fields": fields, "attrs": attrs})
        if stopped:
            break
        compared += 1
        if len(mismatches) >= max_mismatches:
            stopped = "max_mismatches"
            break
    return {"kind": kind, "reference": reference, "compared": compared, "counts": counts,
            "mismatches": mismatches, "stopped": stopped,
            "complete": exhausted and stopped is None}


def build_parity_report(*, pin: str, generation_id: str, branch_point: Any,
                        overlay_targets: Mapping[str, int], kinds: Sequence[Mapping[str, Any]],
                        materialized: Mapping[str, Any], fallback_notes: Mapping[str, Any],
                        role: Sequence[Any], code: Mapping[str, Any], settings: Mapping[str, Any],
                        limit: int | None, batch: int,
                        generations: Mapping[str, str]) -> dict[str, Any]:
    """The JSON report: what was compared, at which pin, as which role, with
    which code and settings, and every mismatch with its ids. ``generations``
    is the generation id each connection resolved the pin to. ``ok`` is false
    on any mismatch, any fallback note (a read the graph path did not serve),
    a comparison that ended early or did not cover the whole pin (``complete``
    false, as under ``--limit``), nothing compared at all, or connections that
    resolved the pin to different generations (named in ``failure_reasons``)."""
    report: dict[str, Any] = {
        "pin": pin, "generation_id": generation_id, "branch_point": branch_point,
        "overlay_targets": dict(overlay_targets), "role": list(role), "code": dict(code),
        "settings": dict(settings), "limit": limit, "batch": batch,
        "materialized": dict(materialized), "fallback_notes": dict(fallback_notes),
        "kinds": [dict(k) for k in kinds],
    }
    report["mismatch_count"] = sum(len(k["mismatches"]) for k in kinds)
    report["compared"] = {k["kind"]: k["compared"] for k in kinds}
    report["complete"] = all(k.get("complete") is True for k in kinds)
    report["generations"] = dict(generations)
    reasons: list[str] = []
    if report["mismatch_count"]:
        reasons.append("mismatches")
    if any(fallback_notes.values()):
        reasons.append("fallback_notes")
    if any(k["stopped"] is not None for k in kinds):
        reasons.append("stopped_early")
    if not report["complete"]:
        reasons.append("incomplete_walk")
    if any(k["compared"] == 0 for k in kinds):
        reasons.append("nothing_compared")
    if len(set(report["generations"].values())) != 1:
        reasons.append("generations_differ")
    report["failure_reasons"] = reasons
    report["ok"] = not reasons
    return report


# --------------------------------------------------------------------------
# bench
# --------------------------------------------------------------------------


def _bench(args: argparse.Namespace) -> int:
    import psycopg

    from okg.substrate.mcp import server as server_mod

    # PACT lane-s-storage-cutover: this tree has one read path, the graph
    # path. A "today" block comes from an older tree's own copy of this file.
    path = "graph"
    dsn = os.environ["OKG_MCP_RO_DSN"]
    shapes = json.load(open(args.shapes))
    selected = [s for s in shapes["shapes"] if not args.only or s["name"] in args.only.split(",")]
    pin = args.pin

    real_execute = psycopg.Cursor.execute
    mode = {"m": "off"}
    rec: list[dict[str, Any]] = []
    settings: list[str] = []

    def text_of(cur: Any, query: Any) -> str:
        if hasattr(query, "as_string"):
            return query.as_string(cur)
        return query.decode() if isinstance(query, bytes) else str(query)

    def hooked(self: Any, query: Any, params: Any = None, **kw: Any) -> Any:
        if mode["m"] == "off":
            return real_execute(self, query, params, **kw)
        sql = text_of(self, query)
        if sets_planner_option(sql):
            settings.append(sql[:200])
        if not is_read_statement(sql):
            return real_execute(self, query, params, **kw)
        if mode["m"] == "explain" and sql.lstrip().upper().startswith(("SELECT", "WITH")):
            t0 = time.perf_counter()
            try:
                real_execute(self, "EXPLAIN (ANALYZE, FORMAT JSON) " + sql, params,
                             **{k: v for k, v in kw.items() if k == "prepare"})
            except Exception:
                # Cancelled (the count budget): no plan, the time still spent.
                rec.append({"exec": (time.perf_counter() - t0) * 1000, "plan": None,
                            "failed": True})
                raise
            row = self.fetchone()
            plan = row[0] if not isinstance(row, dict) else next(iter(row.values()))
            plan = (json.loads(plan) if isinstance(plan, str) else plan)[0]
            rec.append({"exec": plan["Execution Time"], "plan": plan["Plan"], "failed": False})
            return real_execute(self, query, params, **kw)
        t0 = time.perf_counter()
        failed = True
        try:
            out = real_execute(self, query, params, **kw)
            failed = False
            return out
        finally:
            # A statement that fails (a count the count budget cancels) is
            # timed too: the database spent that time on the read.
            rec.append({"wall": (time.perf_counter() - t0) * 1000, "sql": sql,
                        "prepare": kw.get("prepare", "default"), "failed": failed})

    psycopg.Cursor.execute = hooked

    supplement: dict[str, Any] = {}
    try:
        from okg.substrate.mcp import search as search_mod
        real_supplement = search_mod._effective_view_lexical_candidates

        def captured_supplement(*a: Any, **kw: Any) -> Any:
            rows = real_supplement(*a, **kw)
            supplement.setdefault("calls", []).append({
                "ids": [str(r["node_id"]) for r in rows],
                "exclude": sorted(str(x) for x in kw.get("exclude_node_ids") or ()),
                "query": kw.get("query"),
            })
            return rows

        search_mod._effective_view_lexical_candidates = captured_supplement
    except (ImportError, AttributeError):
        pass

    mcp = server_mod.build_server(dsn=dsn, deployment_name=args.deployment)
    tools = mcp._tool_manager._tools

    def call(shape: Mapping[str, Any]) -> Any:
        kw = dict(shape["args"])
        kw["generation_id"] = pin
        try:
            return tools[shape["tool"]].fn(**kw)
        except Exception as exc:  # recorded and compared as an answer
            return {"exception": type(exc).__name__, "code": getattr(exc, "code", None),
                    "data_keys": sorted((getattr(exc, "data", None) or {}).keys()),
                    "message": str(exc)[:300]}

    with psycopg.connect(dsn, autocommit=True) as conn:
        role = conn.execute(
            "SELECT current_user, current_setting('plan_cache_mode'), "
            "current_setting('max_parallel_workers_per_gather')").fetchone()
    load_start = open("/proc/loadavg").read().split()[:3] if os.path.exists("/proc/loadavg") else None
    results: dict[str, Any] = {}
    for shape in selected:
        mode["m"] = "capture"
        rec.clear()
        supplement.clear()
        settings.clear()
        first = call(shape)
        statements = [{"sql": r["sql"][:4000], "prepare": r["prepare"], "failed": r["failed"]}
                      for r in rec]
        first_supplement = list(supplement.get("calls", []))
        digests = {digest(answer_of(first))}
        row_digests = {digest(rows_of(answer_of(first)))}
        counts = [count_of(first)]
        tool_ms: list[float] = []
        sql_ms: list[float] = []
        sql_ms_returned: list[float] = []
        failed_statements: list[int] = []
        for i in range(args.warm + args.runs):
            mode["m"] = "time"
            rec.clear()
            t0 = time.perf_counter()
            result = call(shape)
            ms = (time.perf_counter() - t0) * 1000
            digests.add(digest(answer_of(result)))
            row_digests.add(digest(rows_of(answer_of(result))))
            counts.append(count_of(result))
            if i >= args.warm:
                tool_ms.append(ms)
                sql_ms.append(sum(r["wall"] for r in rec))
                sql_ms_returned.append(sum(r["wall"] for r in rec if not r["failed"]))
                failed_statements.append(sum(1 for r in rec if r["failed"]))
        exec_ms: list[float] = []
        plans: list[Any] = []
        explain_counts: list[dict[str, Any] | None] = []
        for i in range(args.warm + args.runs):
            mode["m"] = "explain"
            rec.clear()
            result = call(shape)
            digests.add(digest(answer_of(result)))
            row_digests.add(digest(rows_of(answer_of(result))))
            # Not judged by rule A: EXPLAIN ANALYZE runs each statement twice
            # and slower, so a count that fits the budget may be cancelled here.
            explain_counts.append(count_of(result))
            if i >= args.warm:
                exec_ms.append(sum(r["exec"] for r in rec))
                plans = [r["plan"] for r in rec]
        mode["m"] = "off"
        results[shape["name"]] = {
            "tool": shape["tool"], "statements": statements,
            "answer_digest": digest(answer_of(first)), "rows_stable": len(digests) == 1,
            "row_digest": digest(rows_of(answer_of(first))), "ids_stable": len(row_digests) == 1,
            "counts": counts, "explain_counts": explain_counts,
            "ids": answer_ids(first), "groups": answer_groups(first),
            "projected": answer_projected(first, shape["args"].get("projection")),
            "total_count": first.get("total_count") if isinstance(first, dict) else None,
            "total_count_basis": first.get("total_count_basis") if isinstance(first, dict)
            else None,
            "budget_cut": isinstance(first, dict)
            and first.get("truncation_reason") == "response_byte_budget",
            "row_count": len(answer_groups(first) or {}) if answer_groups(first) is not None
            else len(answer_ids(first) or answer_projected(first, shape["args"].get("projection"))
                     or []),
            "exception": first.get("exception") if isinstance(first, dict) else None,
            "exception_code": first.get("code") if isinstance(first, dict) else None,
            "exception_data_keys": first.get("data_keys") if isinstance(first, dict) else None,
            "fallback_notes": [n for n in (first.get("notes") or [])
                               if isinstance(n, dict) and "read_path" in n]
            if isinstance(first, dict) else [],
            "supplement": first_supplement,
            "settings": list(settings),
            "tool_ms": tool_ms, "sql_ms": sql_ms, "exec_ms": exec_ms,
            "sql_ms_returned": sql_ms_returned, "failed_statements": failed_statements,
            "plans": plans,
        }
        line = {k: results[shape["name"]][k]
                for k in ("row_count", "exception", "rows_stable", "ids_stable")}
        line.update(shape=shape["name"], tool_p50=percentile(tool_ms, 50),
                    sql_p50=percentile(sql_ms, 50), exec_p50=percentile(exec_ms, 50))
        print(json.dumps(line), flush=True)
    psycopg.Cursor.execute = real_execute
    json.dump({"label": args.label, "path": path, "pin": pin, "state": args.state,
               "role": list(role), "server_file": server_mod.__file__,
               "load_start": load_start,
               "load_end": open("/proc/loadavg").read().split()[:3]
               if os.path.exists("/proc/loadavg") else None,
               "t_end": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "warm": args.warm, "runs": args.runs, "shapes": results},
              open(args.out, "w"), indent=1, default=str)
    return 0


# --------------------------------------------------------------------------
# recompute
# --------------------------------------------------------------------------


def _load_overlays(conn: Any, session: Any) -> dict[str, dict[str, dict[str, Any]]]:
    """The generation's overlay values by target kind (``node``, ``edge``) and
    target id, reserved keys left out."""
    from okg.substrate.attributes import RESERVED_EFFECTIVE_ATTR_KEYS

    overlays: dict[str, dict[str, dict[str, Any]]] = {"node": {}, "edge": {}}
    # A named (server-side) cursor needs a transaction block; the
    # connection is autocommit, so each read opens its own.
    with conn.transaction(), conn.cursor(name="overlays") as cur:
        cur.execute(
            "SELECT target_kind, target_id, attr_key, value FROM okg.attribute_effective_attrs "
            "WHERE generation_id = %s AND namespace = 'default'", (str(session.generation_id),))
        for kind, target, key, value in cur:
            if key not in RESERVED_EFFECTIVE_ATTR_KEYS:
                overlays[kind].setdefault(str(target), {})[key] = value
    return overlays


def _recompute_rows(conn: Any, segments: Mapping[str, str],
                    overlays: Mapping[str, Mapping[str, Mapping[str, Any]]], bp: Any,
                    kind: str, where: str, params: Mapping[str, Any],
                    limit: int | None = None) -> Iterable[dict[str, Any]]:
    """Every live row of one kind at the branch point in id order, streamed,
    with the overlays applied (graph rows plus overlays: the recompute)."""
    seg = segments[kind]
    cap = "" if limit is None else f" LIMIT {int(limit)}"
    if kind == "nodes":
        sql = (f"SELECT node_id AS id, subtype, attrs FROM {seg} WHERE deleted = false "
               f"AND live_lo <= %(bp)s AND %(bp)s < live_hi {where} ORDER BY node_id{cap}")
    else:
        sql = (f"SELECT edge_id::text AS id, edge_type, src, dst, attrs FROM {seg} "
               f"WHERE deleted = false AND live_lo <= %(bp)s AND %(bp)s < live_hi "
               f"{where} ORDER BY edge_id{cap}")
    with conn.transaction(), conn.cursor(name=f"rows_{kind}") as cur:
        cur.itersize = 5000
        cur.execute(sql, {"bp": bp, **params})
        names = [d.name for d in cur.description]
        for raw in cur:
            row = dict(zip(names, raw))
            row["attrs"] = {**(row["attrs"] or {}),
                            **overlays["node" if kind == "nodes" else "edge"].get(row["id"], {})}
            yield row


def _recompute(args: argparse.Namespace) -> int:
    import psycopg

    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    dsn = os.environ["OKG_MCP_RO_DSN"]
    shapes = json.load(open(args.shapes))
    bench = json.load(open(args.bench)) if args.bench else None
    out: dict[str, Any] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=args.deployment,
                               requested_generation=args.pin)
        bp = branch_point(conn, session)
        segments = {"nodes": resolve_node_segment(conn), "edges": resolve_edge_segment(conn)}
        overlays = _load_overlays(conn, session)

        def rows(kind: str, where: str, params: dict[str, Any]) -> Iterable[dict[str, Any]]:
            return _recompute_rows(conn, segments, overlays, bp, kind, where, params)

        for shape in shapes["shapes"]:
            spec = shape.get("recompute")
            if not spec:
                continue
            kind = spec.get("kind", "nodes")  # the supplement reads nodes
            scope_where, scope_params = "", {}
            if spec.get("subtype"):
                scope_where, scope_params = "AND subtype = %(st)s", {"st": spec["subtype"]}
            if spec.get("edge_type"):
                scope_where, scope_params = "AND edge_type = %(et)s", {"et": spec["edge_type"]}
            t0 = time.monotonic()
            if spec["read"] == "page":
                offset, limit = int(spec.get("offset", 0)), int(spec["limit"])
                found: list[str] = []
                projected: list[Any] = []
                total = 0
                # Every match is counted (the page's total_count); the page
                # keeps offset..offset+limit.
                for row in rows(kind, scope_where, scope_params):
                    if matches(row, spec.get("predicate")):
                        total += 1
                        if total <= offset + limit:
                            found.append(row["id"])
                            if spec.get("project") and total > offset:
                                projected.append([row["attrs"].get(k[6:])
                                                  for k in spec["project"]])
                out[shape["name"]] = {"ids": found[offset:offset + limit],
                                      "projected": projected, "total_count": total}
            elif spec["read"] == "groups":
                counts: Counter = Counter()
                for row in rows(kind, scope_where, scope_params):
                    if matches(row, spec.get("predicate")):
                        # Keyed as answer_groups keys the tool's group:
                        # {"attrs.k": value}, a missing key read as null.
                        value = row["attrs"].get(spec["by"][6:])
                        counts[json.dumps({spec["by"]: value}, sort_keys=True,
                                          default=str)] += 1
                out[shape["name"]] = {"groups": dict(counts)}
            elif spec["read"] == "supplement":
                calls = (bench or {}).get("shapes", {}).get(shape["name"], {}).get("supplement", [])
                expected = []
                for call_ in calls:
                    exclude = set(call_["exclude"])
                    terms = [t for t in (call_.get("query") or "").split() if t]
                    hits = [row["id"] for row in rows("nodes", "", {})
                            if row["id"] not in exclude
                            and supplement_matches(row["attrs"], spec["attr_filters"], terms)]
                    ordered = [r[0] for r in conn.execute(
                        "SELECT x FROM unnest(%s::text[]) AS x ORDER BY x LIMIT %s",
                        (hits, int(spec.get("cap", 200))))]
                    expected.append(ordered)
                out[shape["name"]] = {"supplement": expected}
            out[shape["name"]]["recompute_s"] = round(time.monotonic() - t0, 1)
            print(json.dumps({"shape": shape["name"], **{k: (len(v) if isinstance(v, (list, dict)) else v)
                                                        for k, v in out[shape["name"]].items()}}),
                  flush=True)
    json.dump({"pin": args.pin, "generation_id": str(session.generation_id), "branch_point": bp,
               "overlay_targets": {k: len(v) for k, v in overlays.items()}, "shapes": out},
              open(args.out, "w"), indent=1, default=str)
    return 0


# --------------------------------------------------------------------------
# parity
# --------------------------------------------------------------------------


def _code_tree(server_file: str) -> dict[str, Any]:
    """The code tree that ran: the server module's file and its git head and
    whether tracked files are modified (None with the error if git cannot say)."""
    import subprocess

    root = os.path.dirname(os.path.abspath(server_file))
    out: dict[str, Any] = {"server_file": server_file}
    for key, cmd in (("git_head", ["rev-parse", "HEAD"]),
                     ("git_dirty", ["status", "--porcelain", "--untracked-files=no"])):
        try:
            text = subprocess.run(["git", "-C", root, *cmd], capture_output=True, text=True,
                                  check=True).stdout.strip()
            out[key] = text if key == "git_head" else bool(text)
        except (OSError, subprocess.CalledProcessError) as exc:
            out[key] = None
            out[f"{key}_error"] = str(exc)[:200]
    return out


def _graph_rows(server: Any, conn: Any, session: Any, kind: str, mode: str, batch: int,
                limit: int | None, notes: list[Any]) -> Iterable[dict[str, Any]]:
    """Every row of one kind through the page read, in batches by keyset
    (``after_id``), so no page is past the offset cap. PACT
    lane-s-storage-cutover: the graph path is the only read path, so any
    ``mode`` but ``graph`` is refused. ``notes`` is kept for the report's
    shape; the graph path adds none."""
    if mode != "graph":
        raise RuntimeError(f"this code tree has only the graph read path, not {mode!r}")

    id_field = "node_id" if kind == "nodes" else "edge_id"
    after: str | None = None
    seen = 0
    while limit is None or seen < limit:
        size = batch if limit is None else min(batch, limit - seen)
        facts: dict[str, Any] = {}
        rows, more = server._okg_effective_view_sql_page(
            conn, session, kind=kind, predicate=None, limit=size, offset=0,
            after_id=after, page_facts=facts)
        if facts.get("read_filter_scan_bound"):
            raise RuntimeError(f"{mode} {kind} page stopped at the read-filter scan bound "
                               "(access rules on): the walk is incomplete")
        yield from rows
        seen += len(rows)
        if not rows or not more:
            return
        after = str(rows[-1][id_field])


def _parity(args: argparse.Namespace) -> int:
    import psycopg

    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    dsn = os.environ["OKG_MCP_RO_DSN"]
    limit = args.limit or None
    args.batch = max(1, min(args.batch, 1000))
    server = server_mod.build_server(dsn=dsn, deployment_name=args.deployment)
    conns = [psycopg.connect(dsn, autocommit=True) for _ in range(2)]
    try:
        recompute_conn, graph_conn = conns
        role = recompute_conn.execute(
            "SELECT current_user, current_setting('plan_cache_mode'), "
            "current_setting('max_parallel_workers_per_gather')").fetchone()
        if role[0] != "okg_mcp":
            print(f"parity must run as okg_mcp (OKG_MCP_RO_DSN), not {role[0]!r}", file=sys.stderr)
            return 2
        sessions = [open_session(c, deployment_name=args.deployment, requested_generation=args.pin)
                    for c in (recompute_conn, graph_conn)]
        rec_session, graph_session = sessions
        # Each connection resolved the pin on its own: record what each got.
        resolved = {"recompute": rec_session, "graph": graph_session}
        bp = branch_point(recompute_conn, rec_session)
        segments = {"nodes": resolve_node_segment(recompute_conn),
                    "edges": resolve_edge_segment(recompute_conn)}
        overlays = _load_overlays(recompute_conn, rec_session)
        # PACT lane-s-storage-cutover: no materialized read path is left.
        materialized: dict[str, Any] = {"status": "absent",
                                        "reason": "graph read path only"}
        notes: dict[str, list[Any]] = {"graph": []}
        kinds = []
        for kind in ("nodes", "edges"):
            streams: dict[str, Iterable[Mapping[str, Any]]] = {
                "recompute": _recompute_rows(recompute_conn, segments, overlays, bp, kind, "", {},
                                             limit=limit),
                "graph": _graph_rows(server, graph_conn, graph_session, kind, "graph",
                                     args.batch, limit, notes["graph"]),
            }
            t0 = time.monotonic()
            result = compare_streams(kind, streams, "recompute", limit=limit,
                                     max_mismatches=args.max_mismatches)
            result["seconds"] = round(time.monotonic() - t0, 1)
            kinds.append(result)
            print(json.dumps({k: result[k] for k in ("kind", "compared", "counts", "stopped",
                                                       "seconds")}
                             | {"mismatches": len(result["mismatches"])}), flush=True)
        report = build_parity_report(
            pin=args.pin, generation_id=str(rec_session.generation_id), branch_point=bp,
            overlay_targets={k: len(v) for k, v in overlays.items()}, kinds=kinds,
            materialized=materialized, fallback_notes=notes, role=role,
            code=_code_tree(server_mod.__file__),
            settings={"plan_cache_mode": role[1], "max_parallel_workers_per_gather": role[2]},
            limit=limit, batch=args.batch,
            generations={name: str(s.generation_id) for name, s in resolved.items()})
    finally:
        for c in conns:
            c.close()
        server._okg_pool.close()
        if getattr(server, "_okg_write_pool", None):
            server._okg_write_pool.close()
    json.dump(report, open(args.out, "w"), indent=1, default=str)
    print(json.dumps({"ok": report["ok"], "mismatch_count": report["mismatch_count"],
                      "complete": report["complete"],
                      "failure_reasons": report["failure_reasons"],
                      "compared": report["compared"], "materialized": materialized["status"]}))
    return 0 if report["ok"] else 1


# --------------------------------------------------------------------------
# verdict
# --------------------------------------------------------------------------

#: Today's refusal of a pin below its interval retention window (design 5.3).
BELOW_WINDOW = "pin_below_interval_retention_window"
#: Today's search supplement does not re-apply post-publish overlays (1.5).
STALE_SUPPLEMENT = "supplement_post_publish_overlay"


def _is_null_group(key: str) -> bool:
    """Whether an answer_groups key is the null group ({"attrs.k": null})."""
    try:
        value = json.loads(key)
    except ValueError:
        return False
    return isinstance(value, dict) and None in value.values()


def compare_rows(shape: str, today: Sequence[Mapping[str, Any]], graph: Sequence[Mapping[str, Any]],
                 expected: Mapping[str, Any] | None) -> dict[str, Any]:
    """Row checks for one shape in one state across every block. Rows (ids in
    order, paging, refusals) are compared without the count; the counts of
    every call follow rule A (``compare_counts``). A block written before
    the per-call counts were kept falls back to its whole-answer digest."""
    out: dict[str, Any] = {"listed": None}

    def rows_digest(b: Mapping[str, Any]) -> str:
        return b.get("row_digest") or b["answer_digest"]

    def stable(b: Mapping[str, Any]) -> bool:
        return b["ids_stable"] if "ids_stable" in b else b["rows_stable"]

    g_digests = {rows_digest(b) for b in graph}
    t_digests = {rows_digest(b) for b in today}
    out["graph_stable"] = all(stable(b) for b in graph) and len(g_digests) == 1
    today_refused = [b for b in today if b.get("exception")]
    if today_refused and all("retained_from_ordinal" in (b.get("exception_data_keys") or [])
                             for b in today_refused):
        out["listed"] = BELOW_WINDOW
        out["paths_equal"] = None
        t_counts: list[Any] = []
    else:
        out["today_stable"] = all(stable(b) for b in today) and len(t_digests) == 1
        out["paths_equal"] = g_digests == t_digests and len(g_digests) == 1
        t_counts = [c for b in today for c in b.get("counts", [])]
    g_counts = [c for b in graph for c in b.get("counts", [])]
    counts = compare_counts(t_counts, g_counts)
    out["counts_agree"] = counts["agree"] if (t_counts or g_counts) else None
    out["count_lost"] = counts["lost"]
    out["counts"] = {"today": counts["today"], "graph": counts["graph"]}
    g0 = graph[0]
    if expected is None:
        out["recompute_equal"] = None
    elif "groups" in expected:
        got = g0.get("groups") or {}
        want = expected["groups"]
        # Every served group has the recompute's count.
        ok = all(want.get(k) == n for k, n in got.items())
        total = g0.get("total_count")
        if total is not None and g0.get("total_count_basis") in (None, "exact"):
            # The tool groups the whole set and serves `limit` groups of it
            # (truncated, with no truncation_reason), so its group total must
            # be the recompute's. A missing key and JSON null are one group
            # here (answer_groups and the recompute both fold them) and may be
            # two in the tool's SQL.
            null_key = any(_is_null_group(k) for k in want)
            # (With every group served, these two checks already make the
            # served groups equal the recompute's.)
            ok = ok and (total == len(want) or (null_key and total == len(want) + 1))
        out["recompute_equal"] = ok
    elif "supplement" in expected:
        got = [c["ids"] for c in g0.get("supplement", [])]
        out["recompute_equal"] = got == expected["supplement"]
        if out.get("paths_equal") is False:
            today_sup = [c["ids"] for c in today[0].get("supplement", [])]
            if today_sup != expected["supplement"] and out["recompute_equal"]:
                out["listed"] = STALE_SUPPLEMENT
    else:
        # A map page's rows carry no id: its projected values are compared.
        projected = bool(expected.get("projected")) and not g0.get("ids")
        got = (g0.get("projected") or []) if projected else (g0.get("ids") or [])
        want = expected["projected"] if projected else expected["ids"]
        if g0.get("budget_cut"):
            # The response byte budget served the first rows of the page:
            # they must be the recompute's first rows, at least one of them.
            ok = bool(got) and got == want[:len(got)]
        else:
            ok = got == want
        known = [c for c in g_counts if c is not None and c["n"] is not None]
        if not g_counts and g0.get("total_count") is not None:
            known = [{"n": g0["total_count"], "basis": g0.get("total_count_basis") or "exact"}]
        if expected.get("total_count") is not None:
            for c in known:
                if c["basis"] == "lower_bound":
                    # A count past the tool's cap answers the cap as "at least".
                    ok = ok and expected["total_count"] >= c["n"]
                else:
                    ok = ok and c["n"] == expected["total_count"]
        out["recompute_equal"] = ok
    return out


def _ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def _flag(value: bool | None) -> str:
    return "-" if value is None else ("P" if value else "F")


def render_markdown(table: Sequence[Mapping[str, Any]], verdict: Mapping[str, Any]) -> str:
    """Counts and timings only: one row per state and shape. Timing columns
    are the first pair; the P/F letters are every pair in order. The "old"
    columns repeat the SQL checks on the statements that returned only, for
    disclosure; the verdict uses the gate measure."""
    lines = [
        "| state | shape | SQL p50 today / graph ms | SQL p95 today / graph ms "
        "| tool p50 today / graph ms | exec p50 today / graph ms | sql p50 | sql p95 | tool p50 "
        "| rows: paths / recompute | count | listed "
        "| old SQL p50 today / graph ms | old sql p50 | old sql p95 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in table:
        pair = row["pairs"][0] if row["pairs"] else None
        letters = {c: "".join(_flag(p[c]["pass"]) for p in row["pairs"]) or "-"
                   for c in ("sql_p50", "sql_p95", "tool_p50")}
        old_pairs = row.get("pairs_returned_only") or []
        old_letters = {c: "".join(_flag(p[c]["pass"]) for p in old_pairs) or "-"
                       for c in ("sql_p50", "sql_p95")}

        def cell(check: str, chosen: Any = pair) -> str:
            if chosen is None:
                return "-"
            return f"{_ms(chosen[check]['today'])} / {_ms(chosen[check]['graph'])}"

        rows_cell = f"{_flag(row.get('paths_equal'))} / {_flag(row.get('recompute_equal'))}"
        count_cell = ("differ" if row.get("counts_agree") is False
                      else "became unknown" if row.get("count_lost")
                      else "-" if row.get("counts_agree") is None else "ok")
        lines.append(
            f"| {row['state']} | {row['shape']} | {cell('sql_p50')} | {cell('sql_p95')} "
            f"| {cell('tool_p50')} | {_ms(row['exec_p50']['today'])} / {_ms(row['exec_p50']['graph'])} "
            f"| {letters['sql_p50']} | {letters['sql_p95']} | {letters['tool_p50']} "
            f"| {rows_cell} | {count_cell} | {row['listed'] or ''} "
            f"| {cell('sql_p50', old_pairs[0] if old_pairs else None)} "
            f"| {old_letters['sql_p50']} | {old_letters['sql_p95']} |")
    lines += ["", "| requirement | verdict |", "|---|---|"]
    for key in ("gate-sql-p50", "gate-sql-p95", "gate-tool-call", "gate-rows-equal",
                "gate-count-kept", "gate-production-settings", "gate-rounds-valid"):
        if key in verdict:
            lines.append(f"| {key} | {'PASS' if verdict[key] else 'FAIL'} |")
    only = verdict.get("sql_passes_only_with_failed_time") or []
    named = ", ".join(f"{state} {shape}" for state, shape in only) if only else "none"
    passes_only = ("SQL checks that pass only because failed statements are now timed "
                   f"(the old measure fails them): {named}")
    lines += ["", passes_only]
    rejected = verdict.get("rejected_rounds") or []
    if rejected:
        lines += ["", "Rejected rounds: " + "; ".join(rejected)]
    missing = verdict.get("missing_states") or []
    if missing:
        lines += ["", "Missing states: " + ", ".join(missing)]
    return "\n".join(lines) + "\n"


def round_problems(blocks: Sequence[Mapping[str, Any]]) -> list[str]:
    """Why one state's blocks are not a valid round (empty: valid). Every
    block runs as okg_mcp (the owner role sees what the read role may not),
    at the same pin, with the same recorded settings (plan_cache_mode and
    max_parallel_workers_per_gather, as the role sees them), each side from
    one code tree; and the blocks alternate today, graph, today, graph ...
    in time order (``t_end``), so each adjacent pair shared the host's load."""
    out = []
    for b in blocks:
        role = (b.get("role") or [None])[0]
        if role != "okg_mcp":
            out.append(f"{b.get('label')}: ran as {role!r}, not okg_mcp")
    for what, values in (
        ("pins", {b.get("pin") for b in blocks}),
        ("settings", {tuple((b.get("role") or [None])[1:]) for b in blocks}),
        ("today code trees", {b.get("server_file") for b in blocks if b["path"] != "graph"}),
        ("graph code trees", {b.get("server_file") for b in blocks if b["path"] == "graph"}),
    ):
        if len(values) > 1:
            out.append(f"blocks differ in {what}: {sorted(map(str, values))}")
    order = ["graph" if b["path"] == "graph" else "today"
             for b in sorted(blocks, key=lambda b: str(b.get("t_end") or ""))]
    expected = ["today" if i % 2 == 0 else "graph" for i in range(len(order))]
    if order != expected or len(order) % 2:
        out.append(f"blocks do not alternate today/graph in time order: {order}")
    return out


def _verdict(args: argparse.Namespace) -> int:
    states: dict[str, dict[str, list]] = {}
    for path in args.blocks:
        block = json.load(open(path))
        side = "graph" if block["path"] == "graph" else "today"
        states.setdefault(block["state"], {"today": [], "graph": []})[side].append(block)
    for sides in states.values():
        for side in ("today", "graph"):
            sides[side].sort(key=lambda b: str(b.get("t_end") or ""))
    rejected = [f"{state}: {p}" for state, sides in states.items()
                for p in round_problems(sides["today"] + sides["graph"])]
    required = [s for s in (args.require_states or "").split(",") if s]
    missing = [s for s in required if s not in states]
    recomputes = {}
    for path in args.recompute or []:
        state, _, file = path.partition("=")
        recomputes[state] = json.load(open(file))["shapes"]
    table: list[dict[str, Any]] = []
    settings: list[str] = []
    for state, sides in states.items():
        pairs = list(zip(sides["today"], sides["graph"]))
        names = list(sides["graph"][0]["shapes"]) if sides["graph"] else []
        for name in names:
            today = [b["shapes"][name] for b in sides["today"] if name in b["shapes"]]
            graph = [b["shapes"][name] for b in sides["graph"] if name in b["shapes"]]
            for b in today + graph:
                settings.extend(b.get("settings") or [])
            row: dict[str, Any] = {"state": state, "shape": name}
            row.update(compare_rows(name, today, graph, recomputes.get(state, {}).get(name)))
            gates = []
            for t_block, g_block in pairs:
                t, g = t_block["shapes"].get(name), g_block["shapes"].get(name)
                if t is None or g is None or t.get("exception") or g.get("exception"):
                    continue
                gates.append(gate_pair(t, g))
            row["pairs"] = gates
            for check in ("sql_p50", "sql_p95", "tool_p50"):
                row[check] = (all(gp[check]["pass"] for gp in gates) if gates else None)
            # Disclosure, shown beside the gate measure: the same SQL checks on
            # the walls of the statements that returned only (the measure
            # before failed statements were timed). Not part of the verdict.
            old = []
            for t_block, g_block in pairs:
                t, g = t_block["shapes"].get(name), g_block["shapes"].get(name)
                if (t is None or g is None or t.get("exception") or g.get("exception")
                        or "sql_ms_returned" not in t or "sql_ms_returned" not in g):
                    continue
                old.append(gate_pair({"sql_ms": t["sql_ms_returned"], "tool_ms": t["tool_ms"]},
                                     {"sql_ms": g["sql_ms_returned"], "tool_ms": g["tool_ms"]}))
            row["pairs_returned_only"] = old
            # A shape the gate measure passes only because a failed statement
            # (today's cancelled count) now counts: listed by name for review.
            row["sql_passes_only_with_failed_time"] = bool(old) and any(
                not p[c]["pass"] for p in old for c in ("sql_p50", "sql_p95")
            ) and row["sql_p50"] is not False and row["sql_p95"] is not False
            row["exec_p50"] = {
                "today": percentile([x for t in today for x in t["exec_ms"]], 50),
                "graph": percentile([x for g in graph for x in g["exec_ms"]], 50)}
            # Disclosure: the SQL wall of the statements that returned only
            # (the measure before failed statements were timed).
            row["sql_p50_returned"] = {
                "today": percentile([x for t in today for x in t.get("sql_ms_returned", [])], 50),
                "graph": percentile([x for g in graph for x in g.get("sql_ms_returned", [])], 50)}
            row["failed_statements"] = {
                "today": sum(sum(t.get("failed_statements", [])) for t in today),
                "graph": sum(sum(g.get("failed_statements", [])) for g in graph)}
            row["graph_fallbacks"] = sum(len(g.get("fallback_notes") or []) for g in graph)
            row["graph_prepared"] = sorted({str(s["prepare"]) for g in graph
                                            for s in g.get("statements", [])})
            table.append(row)
    verdict = {
        "gate-sql-p50": all(r["sql_p50"] is not False for r in table),
        "gate-sql-p95": all(r["sql_p95"] is not False for r in table),
        "gate-tool-call": all(r["tool_p50"] is not False for r in table),
        "gate-rows-equal": all(
            r["graph_stable"] and r["recompute_equal"] is not False
            and (r["paths_equal"] is not False or r["listed"] is not None)
            and r["counts_agree"] is not False
            for r in table),
        # Rule A: the graph path losing an exact count today's path had is
        # its own failure, not a row difference and not a pass.
        "gate-count-kept": not any(r["count_lost"] for r in table),
        "gate-production-settings": not settings,
        # The rounds are valid (okg_mcp, alternating) and every required state ran.
        "gate-rounds-valid": not rejected and not missing,
        "rejected_rounds": rejected,
        "missing_states": missing,
        "count_became_unknown": sorted({(r["state"], r["shape"]) for r in table if r["count_lost"]}),
        "sql_passes_only_with_failed_time": sorted(
            {(r["state"], r["shape"]) for r in table if r["sql_passes_only_with_failed_time"]}),
        "listed": sorted({(r["state"], r["shape"], r["listed"]) for r in table if r["listed"]}),
        "unmeasured_timing": sorted({(r["state"], r["shape"]) for r in table if r["sql_p50"] is None}),
    }
    json.dump({"table": table, "verdict": verdict, "settings": settings},
              open(args.out, "w"), indent=1, default=str)
    if args.markdown:
        open(args.markdown, "w").write(render_markdown(table, verdict))
    print(json.dumps(verdict, indent=1, default=str))
    return 0 if all(v for k, v in verdict.items() if k.startswith("gate-")) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bench")
    b.add_argument("--out", required=True)
    b.add_argument("--label", required=True)
    b.add_argument("--state", required=True)
    b.add_argument("--pin", required=True)
    b.add_argument("--shapes", required=True)
    b.add_argument("--deployment", required=True)
    b.add_argument("--only", default="")
    b.add_argument("--warm", type=int, default=2)
    b.add_argument("--runs", type=int, default=10)
    r = sub.add_parser("recompute")
    r.add_argument("--out", required=True)
    r.add_argument("--pin", required=True)
    r.add_argument("--shapes", required=True)
    r.add_argument("--deployment", required=True)
    r.add_argument("--bench", default="", help="a graph bench block (supplement exclusions)")
    p = sub.add_parser("parity", help="whole-graph overlay parity at one pin (read-only)")
    p.add_argument("--out", required=True)
    p.add_argument("--pin", required=True)
    p.add_argument("--deployment", required=True)
    p.add_argument("--limit", type=int, default=0,
                   help="rows per kind to compare (0 = the whole graph; a test aid)")
    p.add_argument("--batch", type=int, default=1000, help="rows per page (at most 1000)")
    p.add_argument("--max-mismatches", type=int, default=50)
    v = sub.add_parser("verdict")
    v.add_argument("--out", required=True)
    v.add_argument("--blocks", nargs="+", required=True)
    v.add_argument("--recompute", nargs="*", help="state=recompute.json")
    v.add_argument("--markdown", default="", help="also write the table as Markdown")
    v.add_argument("--require-states", default="",
                   help="comma-separated states that must all be present (else gate-rounds-valid fails)")
    args = parser.parse_args(argv)
    handlers: dict[str, Callable[[argparse.Namespace], int]] = {
        "bench": _bench, "recompute": _recompute, "parity": _parity,
        "verdict": _verdict}
    return handlers[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
