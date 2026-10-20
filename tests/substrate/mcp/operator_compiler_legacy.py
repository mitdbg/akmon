"""The pre-#3201 operator SQL, frozen for old-versus-new comparison.

Copied verbatim from 61b0acf373 (origin/dev f0da24a6a3 with #3226 and #3213
merged), the base PACT read-model-operator-parity-3201 builds on:

- ``legacy_effective_predicate_sql``: ``server._effective_view_predicate_sql``,
  the effective-view predicate renderer (path A in PARITY_MATRIX.md).
- ``sql_literal`` .. ``predicate_sql``: ``operator_sql``'s quoted-literal
  renderer, and ``aggregate_set``, ``filter_ordered`` and ``expand_filtered``
  from ``set_operators``, the run_sql path (path B).

One later addition: the read-model chain (#3209's query-less lookup, at
cb74b49b48) taught the renderer the internal ``_raw_text_contains`` leaf
before #3201 deleted it. That branch is copied verbatim from cb74b49b48, so
the compiler's lowering of it is compared with the renderer it replaces.

Tests compare the shared compiler and the rerouted operators with this copy;
nothing in ``src/`` imports it. Do not fix it: it is the old behavior.
"""
from __future__ import annotations

import json as _json
from typing import Any, Mapping, Optional

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.attr_filter_source import TsBound as _TsBound
from okg.substrate.mcp.operator_sql import (
    EFFECTIVE_VIEW_FIELD_COLUMNS as _EFFECTIVE_VIEW_FIELD_COLUMNS,
    as_operator_timestamp as _as_timestamp,
)
from okg.substrate.mcp.session import Session


def legacy_effective_predicate_sql(
    kind: str,
    predicate: Any,
    params: dict[str, Any],
    *,
    declared: Mapping[str, str] | None = None,
    relation: str | None = None,
) -> str | None:
    """Render the complete v1 predicate over effective attrs in SQL.

    Returns ``""`` for a no-op (None/True/{}), a SQL fragment, or ``None`` when
    ANY clause cannot be rendered. ``None`` is a refusal signal: production
    callers must not fall back to materialising a broad Python row set.

    Every leaf is two-valued even when a JSON key is absent, so ``not`` has
    the same semantics as the Python evaluator instead of inheriting SQL's
    NULL/unknown behavior.
    """
    if predicate in (None, True, {}):
        return ""
    if not isinstance(predicate, dict):
        return None
    op = predicate.get("op")

    def _bind(value: Any) -> str:
        index = len(params)
        name = f"q{index}"
        while name in params:
            index += 1
            name = f"q{index}"
        params[name] = value
        return f"%({name})s"

    def _field(field: Any) -> tuple[str, str | None] | None:
        if not isinstance(field, str):
            return None
        prefix = f"{relation}." if relation else ""
        if field == "attrs":
            return f"{prefix}attrs", ""
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            if not key:
                return None
            key_ref = _bind(key)
            return f"({prefix}attrs -> {key_ref})", key_ref
        column = _EFFECTIVE_VIEW_FIELD_COLUMNS[kind].get(field)
        return (f"{prefix}{column}", None) if column else None

    def _eq(field: Any, value: Any) -> str | None:
        resolved = _field(field)
        if resolved is None:
            return None
        expr, key_ref = resolved
        if key_ref is None:
            if value is None:
                return f"({expr} IS NULL)"
            if not isinstance(value, str):
                return "FALSE"
            return f"COALESCE({expr} = {_bind(value)}, FALSE)"
        if key_ref == "":
            return f"COALESCE({expr} = {_bind(Jsonb(value))}, FALSE)"
        if value is None:
            # dict.get(key) is None for both an absent key and JSON null.
            prefix = f"{relation}." if relation else ""
            return f"((NOT {prefix}attrs ? {key_ref}) OR {expr} = 'null'::jsonb)"
        declared_type = (
            declared.get(str(field).split(".", 1)[1])
            if declared is not None and str(field).startswith("attrs.")
            else None
        )
        if declared_type == "ts" and isinstance(value, str):
            parsed = _as_timestamp(value)
            text_expr = f"({expr} #>> '{{}}')"
            if parsed is None:
                return f"COALESCE({text_expr} = {_bind(value)}, FALSE)"
            # okg#3112 step 2 (design 2.2): stored values read through
            # okg.attr_ts_v4; the bound is v4's instant when v4 accepts
            # it, else today's Python-parsed one.
            raw_value = _bind(value)
            stored = f"okg.attr_ts_v4({text_expr})"
            return (
                f"CASE WHEN jsonb_typeof({expr}) = 'string' AND {stored} IS NOT NULL "
                f"THEN {stored} = COALESCE(okg.attr_ts_v4({raw_value}::text), "
                f"{_bind(parsed)}::timestamptz) ELSE {text_expr} = {raw_value} END"
            )
        return f"COALESCE({expr} = {_bind(Jsonb(value))}, FALSE)"

    def _range(field: Any, op_name: str, bounds: list[Any]) -> str | None:
        resolved = _field(field)
        if resolved is None or resolved[1] == "":
            return None
        expr, key_ref = resolved
        sql_op = {"lt": "<", "gt": ">"}.get(op_name)
        declared_type = (
            declared.get(str(field).split(".", 1)[1])
            if key_ref is not None and declared is not None
            else None
        )
        if key_ref is None:
            if not all(isinstance(v, str) for v in bounds):
                return "FALSE"
            value_expr = expr
            guard = f"{expr} IS NOT NULL"
        else:
            inferred = declared_type
            if inferred is None:
                if all(
                    isinstance(v, (int, float)) and not isinstance(v, bool)
                    for v in bounds
                ):
                    inferred = "num"
                elif all(isinstance(v, str) for v in bounds):
                    inferred = "text"
            if inferred == "num" and all(
                isinstance(v, (int, float)) and not isinstance(v, bool)
                for v in bounds
            ):
                # okg#3112 step 2 (design 2.4): a float bound compares in
                # double precision through okg.attr_num_f8, which never
                # raises 22003; an int bound compares exactly.
                value_expr = f"(({expr} #>> '{{}}')::numeric)"
                guard = f"jsonb_typeof({expr}) = 'number'"
                sides = (
                    [(">=", bounds[0]), ("<=", bounds[1])]
                    if op_name == "between"
                    else [(sql_op, bounds[0])]
                )
                comparison = " AND ".join(
                    (
                        f"okg.attr_num_f8({value_expr}) {cmp} {_bind(bound)}::float8"
                        if isinstance(bound, float)
                        else f"{value_expr} {cmp} {_bind(bound)}"
                    )
                    for cmp, bound in sides
                )
                return f"CASE WHEN {guard} THEN ({comparison}) ELSE FALSE END"
            elif inferred == "ts" and all(isinstance(v, str) for v in bounds):
                parsed_bounds = [_as_timestamp(v) for v in bounds]
                if any(v is None for v in parsed_bounds):
                    return "FALSE"
                # okg#3112 step 2 (design 2.2): as the ts equality above.
                bounds = [
                    _TsBound(raw, parsed) for raw, parsed in zip(bounds, parsed_bounds)
                ]
                text_expr = f"({expr} #>> '{{}}')"
                value_expr = f"okg.attr_ts_v4({text_expr})"
                guard = f"jsonb_typeof({expr}) = 'string' AND {value_expr} IS NOT NULL"
            elif inferred == "text" and all(isinstance(v, str) for v in bounds):
                value_expr = f"({expr} #>> '{{}}')"
                guard = f"jsonb_typeof({expr}) = 'string'"
            else:
                return "FALSE"
        def _bound(value: Any) -> str:
            if isinstance(value, _TsBound):
                return (
                    f"COALESCE(okg.attr_ts_v4({_bind(value.raw)}::text), "
                    f"{_bind(value.parsed)}::timestamptz)"
                )
            return _bind(value)

        if op_name == "between":
            comparison = (
                f"{value_expr} BETWEEN {_bound(bounds[0])} AND {_bound(bounds[1])}"
            )
        else:
            comparison = f"{value_expr} {sql_op} {_bound(bounds[0])}"
        # CASE prevents a malformed JSON scalar from reaching a cast.
        return f"CASE WHEN {guard} THEN {comparison} ELSE FALSE END"

    if op in ("and", "or"):
        clauses = predicate.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            return None
        parts = []
        for c in clauses:
            rendered = legacy_effective_predicate_sql(
                kind, c, params, declared=declared, relation=relation
            )
            if rendered is None or rendered == "":
                return None
            parts.append(rendered)
        joiner = " AND " if op == "and" else " OR "
        return "(" + joiner.join(parts) + ")"
    if op == "not":
        inner = legacy_effective_predicate_sql(
            kind,
            predicate.get("clause"),
            params,
            declared=declared,
            relation=relation,
        )
        if inner is None or inner == "":
            return None
        return f"(NOT {inner})"
    if op == "eq":
        return _eq(predicate.get("field"), predicate.get("value"))
    if op == "in":
        values = predicate.get("values")
        if not isinstance(values, list) or not values:
            return None
        parts = [_eq(predicate.get("field"), value) for value in values]
        if any(part is None for part in parts):
            return None
        return "(" + " OR ".join(str(part) for part in parts) + ")"
    if op == "exists":
        resolved = _field(predicate.get("field"))
        if resolved is None:
            return None
        expr, key_ref = resolved
        if key_ref is None:
            return f"({expr} IS NOT NULL)"
        if key_ref == "":
            return "TRUE"
        prefix = f"{relation}." if relation else ""
        return f"({prefix}attrs ? {key_ref})"
    if op == "contains":
        if predicate.get("field") != "attrs":
            return None
        value = predicate.get("value")
        if not isinstance(value, dict):
            return None
        if not value:
            return "TRUE"
        parts = [_eq(f"attrs.{key}", item) for key, item in value.items()]
        if any(part is None for part in parts):
            return None
        return "(" + " AND ".join(str(part) for part in parts) + ")"
    if op == "_raw_text_contains":
        # Verbatim from cb74b49b48 (`_search_where.RAW_TEXT_CONTAINS`).
        value = predicate.get("value")
        resolved = _field(predicate.get("field"))
        if not isinstance(value, str) or resolved is None or resolved[1] in (None, ""):
            return None
        expr = resolved[0]
        return (
            f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
            f"strpos(({expr} #>> '{{}}'), {_bind(value)}) > 0 ELSE FALSE END"
        )
    if op in ("suffix", "prefix"):
        value = predicate.get("value")
        resolved = _field(predicate.get("field"))
        if not isinstance(value, str) or not value or resolved is None:
            return None
        expr, key_ref = resolved
        bound = _bind(value)
        side = "right" if op == "suffix" else "left"
        if key_ref is None:
            return f"COALESCE({side}({expr}, char_length({bound})) = {bound}, FALSE)"
        if key_ref == "":
            return None
        return (
            f"CASE WHEN jsonb_typeof({expr}) = 'string' THEN "
            f"{side}(({expr} #>> '{{}}'), char_length({bound})) = {bound} "
            "ELSE FALSE END"
        )
    if op in {"lt", "gt"}:
        return _range(predicate.get("field"), op, [predicate.get("value")])
    if op == "between":
        values = predicate.get("values")
        if not isinstance(values, list) or len(values) != 2:
            return None
        return _range(predicate.get("field"), op, list(values))
    return None


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def json_literal(value: Any) -> str:
    return sql_literal(_json.dumps(value, sort_keys=True)) + "::jsonb"


def field_expr(kind: str, field: str) -> str:
    scalar_fields = {
        "nodes": {"node_id", "subtype", "text_hash", "valid_from_gen"},
        "edges": {
            "edge_id", "src", "dst", "edge_type",
            "valid_from_gen",
        },
    }
    if field in scalar_fields[kind]:
        return field
    if field == "attrs":
        return "attrs"
    if field.startswith("attrs."):
        key = field.split(".", 1)[1]
        if not key:
            raise ValueError("attrs field path must name a key")
        return f"attrs ->> {sql_literal(key)}"
    raise ValueError(
        f"unsupported {kind} field {field!r}; use stable view "
        "fields or attrs.<key>"
    )


def predicate_sql(kind: str, predicate: Any) -> str:
    if predicate in (None, True) or predicate == {}:
        return "TRUE"
    if not isinstance(predicate, dict):
        raise ValueError("predicate must be an object")
    op = predicate.get("op")
    if op in {"and", "or"}:
        clauses = predicate.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            raise ValueError(f"{op} predicate requires non-empty clauses")
        joined = f" {str(op).upper()} ".join(
            f"({predicate_sql(kind, clause)})" for clause in clauses
        )
        return joined
    if op == "not":
        return f"NOT ({predicate_sql(kind, predicate.get('clause'))})"
    if op == "eq":
        field = str(predicate.get("field") or "")
        value = predicate.get("value")
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            return f"attrs @> {json_literal({key: value})}"
        return f"{field_expr(kind, field)} = {sql_literal(value)}"
    if op == "in":
        field = str(predicate.get("field") or "")
        values = predicate.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("in predicate requires non-empty values")
        literals = ", ".join(sql_literal(value) for value in values)
        return f"{field_expr(kind, field)} IN ({literals})"
    if op == "exists":
        field = str(predicate.get("field") or "")
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            return f"attrs ? {sql_literal(key)}"
        return f"{field_expr(kind, field)} IS NOT NULL"
    if op == "contains":
        from okg.substrate.transform.operators import (
            CONTAINS_FORMS_MESSAGE,
        )

        field = str(predicate.get("field") or "")
        value = predicate.get("value")
        if field != "attrs" or not isinstance(value, dict):
            raise ValueError(CONTAINS_FORMS_MESSAGE)
        return f"attrs @> {json_literal(value)}"
    raise ValueError(
        f"unsupported predicate op {op!r}; v1 supports and, or, not, eq, "
        "in, exists, contains"
    )


_VIEW = {"nodes": "okg.v_nodes", "edges": "okg.v_edges"}
_MAX_BY = 4


def _fence_node_items(
    conn: psycopg.Connection,
    session: Session,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not items or not any("attrs" in item for item in items):
        return items
    from okg.substrate.mcp.provenance import (
        serialize_node_read_rows_at_pin,
    )

    return serialize_node_read_rows_at_pin(
        conn,
        items,
        deployment_name=session.deployment_name,
        generation_id=session.generation_id,
        branch_name=session.branch_name,
    )


def _serialize_edge_items(
    conn: psycopg.Connection,
    session: Session,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not items or not any("attrs" in item for item in items):
        return items
    from okg.substrate.mcp.provenance import (
        serialize_edge_read_rows_at_pin,
    )

    return serialize_edge_read_rows_at_pin(
        conn,
        items,
        deployment_name=session.deployment_name,
        generation_id=session.generation_id,
        branch_name=session.branch_name,
    )


def is_set_input(input: Any) -> bool:
    """A dict input naming a scope/subtype (not a single node anchor)."""
    if not isinstance(input, dict):
        return False
    if input.get("node_id") or input.get("start_node"):
        return False
    return bool(
        input.get("subtype")
        or input.get("filter")
        or input.get("kind") in {"nodes", "edges"}
    )


def _scope_where(kind: str, input: dict[str, Any]) -> list[str]:
    clauses: list[str] = []
    subtype = input.get("subtype")
    if subtype:
        if kind != "nodes":
            raise ValueError("set input: subtype applies to kind='nodes'")
        clauses.append(f"subtype = {sql_literal(str(subtype))}")
    edge_type = input.get("edge_type")
    if edge_type:
        if kind != "edges":
            raise ValueError("set input: edge_type applies to kind='edges'")
        clauses.append(f"edge_type = {sql_literal(str(edge_type))}")
    predicate = input.get("filter")
    if predicate:
        clauses.append(predicate_sql(kind, predicate))
    return clauses


def _order_clause(kind: str, order_by: Optional[str]) -> str:
    if not order_by:
        return ""
    parts = str(order_by).strip().split()
    if len(parts) > 2 or (
        len(parts) == 2 and parts[1].lower() not in {"asc", "desc"}
    ):
        raise ValueError(
            "order_by must be '<field>' or '<field> asc|desc'"
        )
    expr = field_expr(kind, parts[0])
    direction = parts[1].lower() if len(parts) == 2 else "asc"
    return f" ORDER BY {expr} {direction.upper()} NULLS LAST"


def _bounded_total(
    conn: psycopg.Connection,
    session: Session,
    *,
    count_sql: str,
    offset: int,
    page_rows: int,
    truncated: bool,
    operator: str,
) -> tuple[int, str]:
    """The true number of rows/groups the input matches (issue #1440).

    A complete page needs no second query -- the matches are what was already
    read. A truncated one runs a bounded COUNT over the same FROM/WHERE, so
    `total_count` never reports the capped page size back as if it were the
    size of the set. Generalized from `expand`'s `_filtered_match_count`
    (#904), which fixed exactly this for one operator.
    """
    if not truncated:
        return offset + page_rows, "exact"
    from okg.substrate.mcp.run_sql import run_sql

    out = run_sql(conn, session, count_sql, row_cap=1)
    rows = out.get("rows") or []
    if not rows:
        raise SubstrateError(
            f"{operator}: bounded match count returned no row",
            data={"count_sql": count_sql},
        )
    return int(rows[0][0]), "exact"


def aggregate_set(
    conn: psycopg.Connection,
    session: Session,
    *,
    input: dict[str, Any],
    by: list[str],
    metrics: list[str],
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """GROUP BY over a node/edge set: the drain dossier's status rollup
    and task-per-change rollup as single operator calls."""
    from okg.substrate.mcp.run_sql import run_sql

    if metrics != ["count"]:
        raise ValueError("aggregate set inputs support metrics=['count']")
    if not by or len(by) > _MAX_BY:
        raise ValueError(f"aggregate: by must name 1..{_MAX_BY} fields")
    kind = "edges" if input.get("kind") == "edges" else "nodes"
    exprs = [field_expr(kind, str(field)) for field in by]
    where = _scope_where(kind, input)
    grouped = (
        "SELECT "
        + ", ".join(
            f"{expr} AS g{i}" for i, expr in enumerate(exprs)
        )
        + ", count(*) AS count FROM "
        + _VIEW[kind]
        + ((" WHERE " + " AND ".join(where)) if where else "")
        + " GROUP BY "
        + ", ".join(f"g{i}" for i in range(len(exprs)))
    )
    # OFFSET without a total order is not pagination (issue #1440): `count
    # DESC` alone leaves every tie unordered, so Postgres is free to return
    # tied groups in a different order per plan and OFFSET pages then repeat
    # or skip them. The GROUP BY keys are unique per row by construction, so
    # appending them is exactly the tiebreak that makes the order total --
    # the same guarantee `expand` got from `e.edge_id` in #903/#904, lifted
    # here rather than written a third way.
    order = " ORDER BY count DESC, " + ", ".join(
        f"g{i} ASC NULLS LAST" for i in range(len(exprs))
    )
    sql = grouped + order + f" OFFSET {int(offset)}"
    out = run_sql(conn, session, sql, row_cap=int(limit))
    groups = [
        {**{by[i]: row[i] for i in range(len(by))}, "count": row[len(by)]}
        for row in out["rows"]
    ]
    if out["truncated"] and not out["rows"]:
        # Refuse BEFORE the bounded count: no rows and more to come means the
        # cursor cannot advance, and returning `offset` unchanged would hand
        # back a cursor that loops forever.
        raise SubstrateError(
            "aggregate: the page budget could not fit a single group, so "
            "there is no cursor that makes progress",
            data={"offset": int(offset), "limit": int(limit)},
            fix_hint="lower `limit` or narrow `by` so a page fits the budget",
        )
    total_count, total_basis = _bounded_total(
        conn,
        session,
        count_sql=f"SELECT count(*) AS n FROM ({grouped}) grouped",
        offset=int(offset),
        page_rows=len(out["rows"]),
        truncated=bool(out["truncated"]),
        operator="aggregate",
    )
    return {
        "groups": groups,
        "by": by,
        "metrics": metrics,
        "returned_count": len(groups),
        # The number of GROUPS this input matches -- not the size of the
        # capped page, which is what `run_sql` can report and what this
        # returned before (issue #1440).
        "total_count": total_count,
        "total_count_basis": total_basis,
        "truncated": out["truncated"],
        "limit": int(limit),
        "offset": int(offset),
        "next_offset": (
            int(offset) + len(out["rows"]) if out["truncated"] else None
        ),
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        **(
            {"source_freshness": out["source_freshness"]}
            if out.get("source_freshness")
            else {}
        ),
    }


def filter_ordered(
    conn: psycopg.Connection,
    session: Session,
    *,
    input: dict[str, Any],
    predicate: Optional[dict[str, Any]],
    order_by: str,
    limit: int,
    offset: int,
    select: Optional[list[str]],
) -> dict[str, Any]:
    """Ordered set listing: 'open changes by evidence_count desc' as one
    call (the third dossier acceptance query)."""
    from okg.substrate.mcp.envelope import project_row
    from okg.substrate.mcp.run_sql import run_sql

    kind = "edges" if input.get("kind") == "edges" else "nodes"
    where = _scope_where(kind, input)
    if predicate:
        where.append(predicate_sql(kind, predicate))
    cols = (
        ["node_id", "subtype", "attrs"]
        if kind == "nodes"
        else ["edge_id", "src", "dst", "edge_type", "attrs"]
    )
    identity = "node_id" if kind == "nodes" else "edge_id"
    # Same total-order requirement as aggregate above (issue #1440): the
    # caller's `order_by` names ONE field, and ties under it are unordered, so
    # OFFSET pages repeat or skip rows. The row identity is the unconditional
    # tiebreak -- a caller `order_by` refines the order but never leaves it
    # ambiguous.
    order = _order_clause(kind, order_by)
    order = (
        f"{order}, {identity} ASC" if order else f" ORDER BY {identity} ASC"
    )
    from_where = _VIEW[kind] + (
        (" WHERE " + " AND ".join(where)) if where else ""
    )
    sql = (
        "SELECT " + ", ".join(cols) + " FROM " + from_where
        + order
        + f" OFFSET {int(offset)}"
    )
    out = run_sql(conn, session, sql, row_cap=int(limit))
    always = {"node_id", "subtype"} if kind == "nodes" else {"edge_id", "edge_type"}
    items = []
    for raw in out["rows"]:
        source_row = dict(zip(cols, raw))
        projected = project_row(
            source_row, select,
            default=set(cols) - {"attrs"}, always=always,
        )
        items.append(projected)
    if kind == "nodes":
        items = _fence_node_items(conn, session, items)
    elif kind == "edges":
        items = _serialize_edge_items(conn, session, items)
    if out["truncated"] and not out["rows"]:
        # Refuse BEFORE the bounded count, for the same reason `expand` does:
        # a cursor that cannot advance is worse than an error.
        raise SubstrateError(
            "filter: the page budget could not fit a single row, so there is "
            "no cursor that makes progress",
            data={"offset": int(offset), "limit": int(limit)},
            fix_hint=(
                "drop `attrs` with select=[...] or lower `limit` so a page "
                "fits the response budget"
            ),
        )
    total_count, total_basis = _bounded_total(
        conn,
        session,
        count_sql="SELECT count(*) AS n FROM " + from_where,
        offset=int(offset),
        page_rows=len(out["rows"]),
        truncated=bool(out["truncated"]),
        operator="filter",
    )
    return {
        "items": items,
        "kind": kind,
        "order_by": order_by,
        "returned_count": len(items),
        # The number of rows this input matches -- not the size of the capped
        # page, which is what `run_sql` reports (issue #1440).
        "total_count": total_count,
        "total_count_basis": total_basis,
        "truncated": out["truncated"],
        "limit": int(limit),
        "offset": int(offset),
        "next_offset": (
            int(offset) + len(out["rows"]) if out["truncated"] else None
        ),
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
    }


def expand_filtered(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str],
    direction: str,
    dest_filter: Optional[dict[str, Any]],
    order_by: Optional[str],
    limit: int,
    offset: int,
    select: Optional[list[str]],
) -> dict[str, Any]:
    """1-hop expand with destination predicate, ordering, and offset -
    the wave's 'tracked follow-on' on expand, delivered.

    Emits the SAME edge-shaped rows as `expand_neighbors` and honors the same
    `select` grammar (PACT fix-expand-pagination-contract, issue #903). It used
    to emit node-shaped rows (`node_id`/`subtype`) and ignore `select`, so a
    caller that paged a neighborhood got edge rows for page 1 and node rows for
    every page after it. The destination node is still joined — that is what
    `dest_filter` and `order_by` range over — but it is a predicate target, not
    the row shape. The neighbor's id is `dst` (or `src` for direction='in').
    """
    from okg.substrate.mcp.envelope import project_row, validate_select
    from okg.substrate.mcp.run_sql import run_sql
    from okg.substrate.mcp.tools import (
        _LIST_NEIGHBORS_ALWAYS,
        _LIST_NEIGHBORS_DEFAULT,
        _LIST_NEIGHBORS_FIELDS,
    )

    if direction not in {"out", "in"}:
        raise ValueError("expand: direction must be 'out' or 'in'")
    validate_select(select, _LIST_NEIGHBORS_FIELDS, tool_name="expand")
    near, far = ("src", "dst") if direction == "out" else ("dst", "src")
    where = [f"e.{near} = {sql_literal(node_id)}"]
    # `is not None`, matching expand_neighbors: under truthiness an
    # edge_type of "" filters on one path and is ignored on the other.
    if edge_type is not None:
        where.append(f"e.edge_type = {sql_literal(str(edge_type))}")
    dest_where = ""
    if dest_filter:
        dest_where = " AND " + predicate_sql("nodes", dest_filter)
    # OFFSET without a total order is not pagination — Postgres may reorder
    # rows between plans, so pages would skip and repeat. `e.edge_id` is the
    # unconditional tiebreak (the same key the unfiltered path orders by), so
    # a caller `order_by` refines the order but never leaves it ambiguous.
    order = (
        _order_clause("nodes", order_by).replace(
            " ORDER BY ", " ORDER BY n.",
        ) + ", e.edge_id ASC"
        if order_by
        else " ORDER BY e.edge_id ASC"
    )
    # LEFT JOIN so the two paths range over the same edges: an edge whose far
    # endpoint is absent at the pin is visible to the unfiltered path, and
    # must not silently vanish here just because a caller passed order_by. A
    # `dest_filter` still excludes those rows on its own terms.
    from_join = (
        f" FROM okg.v_edges e LEFT JOIN okg.v_nodes n ON n.node_id = e.{far}"
    )
    predicate = " WHERE " + " AND ".join(where) + dest_where
    sql = (
        "SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs"
        + from_join
        + predicate
        + order
        + f" OFFSET {int(offset)}"
    )
    out = run_sql(conn, session, sql, row_cap=int(limit))
    from okg.substrate.attributes import apply_effective_attrs_to_rows

    # Same row pipeline as expand_neighbors — effective attrs, then read
    # provenance, then projection. Matching key sets is not enough: without
    # the overlay the same edge at the same pin would carry different `attrs`
    # depending only on whether the caller passed order_by.
    full_rows = [
        {
            "edge_id": row[0], "src": row[1], "dst": row[2],
            "edge_type": row[3], "attrs": row[4],
            # Per row, not the requested direction: a self-loop is "out" on
            # the unfiltered path, and the two must not disagree.
            "direction": ("out" if row[1] == node_id else "in"),
        }
        for row in out["rows"]
    ]
    full_rows = apply_effective_attrs_to_rows(
        conn,
        full_rows,
        target_kind="edge",
        target_id_field="edge_id",
        generation_id=session.generation_id,
    )
    # Serialize before projecting, exactly as expand_neighbors does, so
    # `read_provenance` is attached to the full row and survives the same
    # projection rules rather than being back-filled afterwards.
    full_rows = _serialize_edge_items(conn, session, full_rows)
    neighbors = []
    for row in full_rows:
        projected = project_row(
            row, select,
            default=_LIST_NEIGHBORS_DEFAULT,
            always=_LIST_NEIGHBORS_ALWAYS,
        )
        if "attrs" in projected and "read_provenance" not in projected:
            projected["read_provenance"] = row.get("read_provenance")
        neighbors.append(projected)
    total_count, total_basis = _filtered_match_count(
        conn, session,
        from_join=from_join, predicate=predicate,
        offset=int(offset), truncated=bool(out["truncated"]),
        page_rows=len(out["rows"]),
    )
    if out["truncated"] and not out["rows"]:
        # No rows and more to come means the cursor cannot advance. Refusing
        # is the honest answer; returning offset unchanged would hand back a
        # cursor that loops forever.
        raise SubstrateError(
            "expand: the page budget could not fit a single row, so there is "
            "no cursor that makes progress",
            data={"node_id": node_id, "offset": int(offset), "limit": int(limit)},
            fix_hint=(
                "drop `attrs` with select=[...] or lower `limit` so a page "
                "fits the response budget"
            ),
        )
    return {
        "node_id": node_id,
        "direction": direction,
        "neighbors": neighbors,
        "returned_count": len(neighbors),
        "total_count": total_count,
        "total_count_basis": total_basis,
        "offset": int(offset),
        "limit": int(limit),
        # Advance by rows actually RETURNED, never by `limit`. run_sql can
        # hand back fewer rows than asked for while still reporting
        # truncated=True — it pops rows off the end to fit the payload budget,
        # and it clamps row_cap at its own ceiling. A cursor that assumed
        # `limit` would step straight over every row run_sql dropped, and
        # those edges would be served by no page at all.
        "next_offset": (
            int(offset) + len(out["rows"]) if out["truncated"] else None
        ),
        "truncated": out["truncated"],
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        **(
            {"source_freshness": out["source_freshness"]}
            if out.get("source_freshness")
            else {}
        ),
    }


def _filtered_match_count(
    conn: psycopg.Connection,
    session: Session,
    *,
    from_join: str,
    predicate: str,
    offset: int,
    truncated: bool,
    page_rows: int,
) -> tuple[Optional[int], str]:
    """The true number of edges this filtered expand matches (issue #904).

    A complete page needs no second query — the matches are what was already
    read. A truncated one runs a bounded COUNT over the same join and
    predicate, so `total_count` never reports the page size back as if it
    were the answer.
    """
    if not truncated:
        return offset + page_rows, "exact"
    from okg.substrate.mcp.run_sql import run_sql

    out = run_sql(
        conn,
        session,
        "SELECT count(*) AS match_count" + from_join + predicate,
        row_cap=1,
    )
    rows = out.get("rows") or []
    if not rows:
        raise SubstrateError(
            "expand: bounded match count returned no row",
            data={"predicate": predicate},
        )
    return int(rows[0][0]), "exact"
