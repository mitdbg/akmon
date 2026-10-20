"""SQL lowering for the relational core (task.gtir.normalize-and-lower).

Compiles a read-only program in the canonical select shape into one SQL query
over the registered surface bindings:

    FROM(alias) [JOIN(how, on)]* [UNNEST(expr, field)]* [WHERE expr]
    (SELECT project | GROUP key+agg) [ORDER] [LIMIT [OFFSET]]

With a RANK op (the sixteenth operator, decision D1 of the graph-derivation
design) the canonical shape gains exactly one nesting step:

    <flat pre-shape>  RANK  [WHERE expr] [SELECT project] [ORDER] [LIMIT]

lowering to

    SELECT ... FROM (            -- L3: post-RANK ops, only when present
      SELECT __okg_r.*,          -- L2: window level (rank + basis provenance)
             ROW_NUMBER() OVER (...) AS <rank_as> [, CASE ... AS <basis_as>]
      FROM ( <flat pre-shape> ) __okg_r
    ) __okg_w WHERE ... ORDER ... LIMIT ...

The window ORDER pins ``NULLS LAST`` in BOTH directions explicitly (PG's bare
``DESC`` defaults NULLS FIRST, which would diverge from the interpreter).
The basis CASE repeats the window spec inline rather than using a named
WINDOW clause — inline ``OVER (...)`` is the form empirically proven to
round-trip the Chronos session rewriter (sqlglot) unmangled. RANK component
exprs are evaluated against the L1 output columns: with a pre-JOIN, project
the needed columns to unqualified names in a pre-SELECT (alias-qualified
references do not survive into L2 and fail loudly in PG).

The same expressions (expr.py) drive both this SQL and the reference
interpreter, so a lowered query and the interpreter agree by construction.
Shapes outside the canonical form raise ``LoweringError`` rather than
silently mis-compiling: GROUP does not compose with RANK (v1), and nothing
relational may precede FROM or follow LIMIT.

GROUP carries its expr form as ``{key: {alias: expr}, agg: {alias: {fn, expr?}}}``
(a dict, vs the interpreter's kernel-name string form). SELECT may carry
``distinct: true``. LIMIT may carry an ``offset``. ORDER carries one ``expr``
(+``desc``) or a ``keys`` list of ``{expr, desc?}`` for multi-key sorts.

UNNEST lowers to ``CROSS JOIN LATERAL jsonb_array_elements_text(expr)`` and
binds ``field`` as the element column. SQL applies WHERE after the lateral
join, so the canonical shape requires every UNNEST after all JOINs and before
the WHERE — other orders would mean the interpreter (which executes bindings
in order) and the SQL disagree, and are rejected instead of mis-compiled.
"""

from __future__ import annotations

import re

from .expr import render_sql
from .registry import OpKind, Registries
from .ir import _RANK_PEER_INTERNAL, TransformProgram


class LoweringError(Exception):
    pass


_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _require_ident(name: str, what: str) -> str:
    if not _IDENT.match(name):
        raise LoweringError(f"{what} {name!r} is not a plain identifier")
    return name


def lower_query(program: TransformProgram, registries: Registries) -> tuple[str, dict]:
    if len(program.blocks) != 1:
        raise LoweringError("SQL lowering supports a single-block read program")
    block = program.blocks[0]

    aliases: dict[str, str] = {}      # FROM output symbol -> SQL alias
    bindings: dict[str, str] = {}     # SQL alias -> table/view binding
    base_alias: str | None = None
    joins: list[tuple[str, str, dict]] = []   # (how, right_alias, on_expr)
    unnests: list[tuple[str, dict]] = []      # (field, array_expr)
    where_expr: dict | None = None
    project: dict | None = None
    distinct = False
    group: tuple[dict, dict] | None = None    # (keys, aggs)
    order = None
    limit = None
    offset = None
    rank_args: dict | None = None
    post_where: dict | None = None
    post_project: dict | None = None
    post_distinct = False

    def _after_rank() -> bool:
        return rank_args is not None

    for b in block.bindings:
        op = b.op
        a = op.args
        if op.kind is OpKind.FROM:
            if _after_rank():
                raise LoweringError("canonical shape requires FROM before RANK")
            surface = registries.surfaces.resolve(a["surface"])
            if surface is None:
                raise LoweringError(f"unregistered surface {a['surface']!r}")
            alias = a.get("alias", a["surface"])
            aliases[b.output] = alias
            bindings[alias] = surface.binding
            if base_alias is None:
                base_alias = alias
        elif op.kind is OpKind.JOIN:
            if _after_rank():
                raise LoweringError("canonical shape requires JOIN before RANK")
            if "on" not in a:
                raise LoweringError("SQL lowering requires JOIN to carry an `on` expression")
            if unnests:
                # A LEFT JOIN's null-extended rows would feed the lateral and
                # be dropped in SQL, while the interpreter (binding order)
                # unnested first. Reject rather than mis-compile.
                raise LoweringError("canonical shape requires JOIN before any UNNEST")
            left_sym, right_sym = b.inputs
            base_alias = aliases.get(left_sym, base_alias)  # left-deep base
            right_alias = aliases[right_sym]
            joins.append((a.get("how", "inner"), right_alias, a["on"]))
        elif op.kind is OpKind.UNNEST:
            if _after_rank():
                raise LoweringError("canonical shape requires UNNEST before RANK")
            if where_expr is not None:
                raise LoweringError("canonical shape requires UNNEST before the WHERE")
            field = _require_ident(str(a["field"]), "UNNEST field")
            unnests.append((field, a["expr"]))
        elif op.kind is OpKind.WHERE:
            if "expr" not in a:
                raise LoweringError("SQL lowering requires WHERE to carry an `expr`")
            if _after_rank():
                # The post-RANK tail renders in the fixed order WHERE ->
                # SELECT -> ORDER -> LIMIT; a program binding them in any
                # other order (or twice) would silently diverge from the
                # interpreter's binding-order execution. Reject instead.
                if post_where is not None:
                    raise LoweringError("canonical shape allows one WHERE after RANK")
                if post_project is not None or order is not None or limit is not None:
                    raise LoweringError(
                        "canonical shape requires the post-RANK WHERE before "
                        "SELECT/ORDER/LIMIT"
                    )
                post_where = a["expr"]
            else:
                where_expr = a["expr"]
        elif op.kind is OpKind.SELECT:
            if "project" not in a:
                raise LoweringError("SQL lowering requires SELECT to carry a `project` map")
            if _after_rank():
                if post_project is not None:
                    raise LoweringError("canonical shape allows one SELECT after RANK")
                if order is not None or limit is not None:
                    raise LoweringError(
                        "canonical shape requires the post-RANK SELECT before ORDER/LIMIT"
                    )
                post_project = a["project"]
                post_distinct = post_distinct or bool(a.get("distinct"))
            else:
                project = a["project"]
                distinct = distinct or bool(a.get("distinct"))
        elif op.kind is OpKind.GROUP:
            if rank_args is not None:
                raise LoweringError("GROUP does not compose with RANK in the canonical shape (v1)")
            keys, aggs = a.get("key"), a.get("agg")
            if not isinstance(keys, dict) or not isinstance(aggs, dict):
                raise LoweringError(
                    "SQL lowering requires GROUP `key` and `agg` as {alias: ...} maps"
                )
            group = (keys, aggs)
        elif op.kind is OpKind.RANK:
            if rank_args is not None:
                raise LoweringError("canonical shape allows a single RANK")
            if group is not None:
                raise LoweringError("GROUP does not compose with RANK in the canonical shape (v1)")
            if order is not None or limit is not None:
                raise LoweringError("canonical shape requires ORDER/LIMIT after RANK")
            rank_args = a
        elif op.kind is OpKind.ORDER:
            if order is not None:
                raise LoweringError("canonical shape allows a single ORDER")
            if limit is not None:
                raise LoweringError("canonical shape requires ORDER before LIMIT")
            order = a
        elif op.kind is OpKind.LIMIT:
            if limit is not None:
                raise LoweringError("canonical shape allows a single LIMIT")
            limit = int(a["n"])
            if a.get("offset") is not None:
                offset = int(a["offset"])
        elif op.kind in (OpKind.EMIT,):
            continue
        else:
            raise LoweringError(f"{op.kind.value} is not supported by the canonical SQL lowering")

    if base_alias is None:
        raise LoweringError("program has no FROM")

    params: dict = {}
    group_by_sql: str | None = None
    # SELECT list (L1)
    if group is not None:
        if distinct:
            # SELECT-distinct + GROUP silently MIS-COMPILES rather than being
            # wrong-but-visible: the interpreter dedups at the SELECT binding
            # and then aggregates, while this lowering ignores the pre-SELECT
            # projection (the branch below is `elif`) and hoists DISTINCT to
            # the outer keyword — emitting `SELECT DISTINCT k, COUNT(x) ...
            # GROUP BY k`, which counts DUPLICATED rows. Production would
            # over-count while every interpreter-based test passed. Reject the
            # shape; use the count_distinct aggregate instead.
            raise LoweringError(
                "SELECT distinct combined with GROUP is rejected: the "
                "interpreter dedups before aggregating but SQL would apply "
                "DISTINCT to the grouped output, so the two disagree "
                "silently. Use agg fn 'count_distinct' instead of "
                "de-duplicating with a distinct SELECT."
            )
        keys, aggs = group
        key_sql = {alias: render_sql(e, params) for alias, e in keys.items()}
        group_by_sql = ", ".join(key_sql.values())
        col_terms = [f"{sql} AS {alias}" for alias, sql in key_sql.items()]
        for alias, spec in aggs.items():
            fn = str(spec.get("fn", "count")).lower()
            arg = render_sql(spec["expr"], params) if "expr" in spec else "*"
            if fn == "count_distinct":
                if "expr" not in spec:
                    raise LoweringError("agg 'count_distinct' requires an expr")
                col_terms.append(f"COUNT(DISTINCT {arg}) AS {alias}")
            else:
                col_terms.append(f"{fn.upper()}({arg}) AS {alias}")
        cols = ", ".join(col_terms)
    elif project:
        cols = ", ".join(f"{render_sql(e, params)} AS {name}" for name, e in project.items())
    else:
        cols = "*"

    select_kw = "SELECT DISTINCT" if distinct else "SELECT"
    sql = f"{select_kw} {cols}\nFROM {bindings[base_alias]} {base_alias}"
    for how, right_alias, on_expr in joins:
        kw = {"inner": "JOIN", "left": "LEFT JOIN", "full": "FULL JOIN"}[how]
        sql += f"\n{kw} {bindings[right_alias]} {right_alias} ON {render_sql(on_expr, params)}"
    for i, (field, array_expr) in enumerate(unnests):
        sql += (
            f"\nCROSS JOIN LATERAL jsonb_array_elements_text("
            f"{render_sql(array_expr, params)}) __okg_unnest{i}({field})"
        )
    if where_expr is not None:
        sql += f"\nWHERE {render_sql(where_expr, params)}"
    if group_by_sql is not None:
        sql += f"\nGROUP BY {group_by_sql}"

    if rank_args is not None:
        sql = _lower_rank_level(sql, rank_args, params)

    # ORDER / LIMIT / post-RANK WHERE+SELECT apply at the outermost level.
    if rank_args is not None and (
        post_where is not None or post_project is not None
        or order is not None or limit is not None
    ):
        cols = (
            ", ".join(
                f"{render_sql(e, params)} AS {name}" for name, e in post_project.items()
            )
            if post_project
            else "*"
        )
        kw = "SELECT DISTINCT" if post_distinct else "SELECT"
        sql = f"{kw} {cols}\nFROM (\n{sql}\n) __okg_w"
        if post_where is not None:
            sql += f"\nWHERE {render_sql(post_where, params)}"
    if order is not None:
        if "keys" in order:
            terms = [
                f"{render_sql(k['expr'], params)} {'DESC' if k.get('desc') else 'ASC'}"
                for k in order["keys"]
            ]
            sql += "\nORDER BY " + ", ".join(terms)
        else:
            direction = "DESC" if order.get("desc") else "ASC"
            sql += f"\nORDER BY {render_sql(order['expr'], params)} {direction}"
    if limit is not None:
        sql += f"\nLIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"
    return sql, params


def _lower_rank_level(inner_sql: str, a: dict, params: dict) -> str:
    """The L2 window level: rank + (optional) basis comparison provenance."""
    partition = a.get("partition", [])
    comps = a["components"]
    tiebreak = a.get("tiebreak")
    rank_as = _require_ident(str(a.get("rank_as", "rnk")), "RANK rank_as")

    part_sql = [render_sql(e, params) for e in partition]
    part_clause = f"PARTITION BY {', '.join(part_sql)} " if part_sql else ""

    order_terms = []
    comp_sql: list[tuple[str, str]] = []  # (name, rendered expr)
    for c in comps:
        rendered = render_sql(c["expr"], params)
        # The name renders as an inline SQL literal in the basis CASE (same
        # sqlglot re-render hazard as json keys) — identifier charset only.
        comp_sql.append((_require_ident(str(c["name"]), "RANK component name"), rendered))
        direction = "DESC" if c.get("desc") else "ASC"
        order_terms.append(f"{rendered} {direction} NULLS LAST")
    if tiebreak is not None:
        direction = "DESC" if tiebreak.get("desc") else "ASC"
        order_terms.append(
            f"{render_sql(tiebreak['expr'], params)} {direction} NULLS LAST"
        )
    spec = f"({part_clause}ORDER BY {', '.join(order_terms)})"
    part_only = f"({part_clause.strip()})" if part_clause else "()"

    select_terms = ["__okg_r.*", f"ROW_NUMBER() OVER {spec} AS {rank_as}"]
    basis_as = a.get("basis_as")
    peer = a.get("peer")
    if basis_as is not None and peer is not None:
        return _lower_rank_peer_level(
            inner_sql, a, params, spec=spec, part_clause=part_clause,
            part_only=part_only, order_terms=order_terms, comp_sql=comp_sql,
            rank_as=rank_as, basis_as=_require_ident(str(basis_as),
                                                     "RANK basis_as"),
            peer_sql=render_sql(peer, params),
        )
    if basis_as is not None:
        basis_as = _require_ident(str(basis_as), "RANK basis_as")
        whens = "".join(
            f" WHEN ({rendered}) IS DISTINCT FROM (LEAD({rendered}) OVER {spec})"
            f" THEN '{name}'"
            for name, rendered in comp_sql
        )
        basis_case = (
            f"CASE WHEN ROW_NUMBER() OVER {spec} = 1 THEN"
            f" (CASE WHEN COUNT(*) OVER {part_only} = 1 THEN 'sole'{whens}"
            f" ELSE 'tie' END) ELSE NULL END AS {basis_as}"
        )
        select_terms.append(basis_case)
    return (
        f"SELECT {', '.join(select_terms)}\nFROM (\n{inner_sql}\n) __okg_r"
    )


def _lower_rank_peer_level(
    inner_sql: str, a: dict, params: dict, *, spec: str, part_clause: str,
    part_only: str, order_terms: list[str], comp_sql: list[tuple[str, str]],
    rank_as: str, basis_as: str, peer_sql: str,
) -> str:
    """RANK with a ``peer`` group: the basis compares the leader against the
    best row in a DIFFERENT peer group, not against the next row.

    ``LEAD`` cannot express that — it takes the immediately following row, and
    a leader's own peers sit between it and its first real rival. The leader's
    peer value is needed inside a window's ORDER BY, and SQL forbids nesting a
    window function there, so it is materialized one level down and consumed
    one level up:

      L2a: ROW_NUMBER + FIRST_VALUE(peer) OVER spec  AS __okg_leader_peer
      L2b: FIRST_VALUE(comp) OVER (PARTITION BY ... ORDER BY <is-own-peer>
           ASC, <original order>)  -- different-peer rows sort first, so
           FIRST_VALUE lands on the best rival

    A partition whose rows all share one peer has no rival and yields 'sole',
    matching the interpreter rather than reporting a meaningless 'tie'. That
    is counted at L2b, where the leader's peer is a plain column: postgres
    rejects DISTINCT inside a window function, and a FILTER at L2a could not
    reference a value computed in its own select list.
    """
    inner_select = [
        "__okg_r.*",
        f"ROW_NUMBER() OVER {spec} AS {rank_as}",
        f"FIRST_VALUE({peer_sql}) OVER {spec} AS {_RANK_PEER_INTERNAL}",
    ]
    level_a = (
        f"SELECT {', '.join(inner_select)}\nFROM (\n{inner_sql}\n) __okg_r"
    )

    # Different-peer rows first, then the original ranking order: FIRST_VALUE
    # over this window is the best row NOT restating the leader's peer.
    own = f"(CASE WHEN {peer_sql} IS NOT DISTINCT FROM {_RANK_PEER_INTERNAL}" \
          f" THEN 1 ELSE 0 END)"
    rival_spec = (
        f"({part_clause}ORDER BY {own} ASC, {', '.join(order_terms)})"
    )
    whens = "".join(
        f" WHEN ({rendered}) IS DISTINCT FROM"
        f" (FIRST_VALUE({rendered}) OVER {rival_spec}) THEN '{name}'"
        for name, rendered in comp_sql
    )
    rivals = (
        f"SUM(CASE WHEN {peer_sql} IS DISTINCT FROM {_RANK_PEER_INTERNAL}"
        f" THEN 1 ELSE 0 END) OVER {part_only}"
    )
    basis_case = (
        f"CASE WHEN {rank_as} = 1 THEN"
        f" (CASE WHEN ({rivals}) = 0 THEN 'sole'{whens}"
        f" ELSE 'tie' END) ELSE NULL END AS {basis_as}"
    )
    # NOTE: __okg_leader_peer stays in the output. Postgres has no
    # `SELECT * EXCEPT (col)`, and the lowering does not know the input column
    # list, so it cannot be projected away without enumerating columns it
    # cannot see. The interpreter therefore emits the SAME column (see
    # transform/interpreter.py `_rank`), keeping the two readers' schemas
    # identical — which is what the parity discipline actually requires. It is
    # part of the peer-RANK output contract, and reserved in ir.py so an input
    # surface cannot collide with it.
    return (
        f"SELECT __okg_w.*, {basis_case}\nFROM (\n{level_a}\n) __okg_w"
    )


__all__ = ["lower_query", "LoweringError"]
