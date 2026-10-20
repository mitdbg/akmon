#!/usr/bin/env python3
"""Read-path honesty gate for `okg schema quality`.

Answers one question about a migrated database: **does every promoted view
actually read something?** A view is promoted when a read role can SELECT it
--- that is the surface an agent sees, and the surface that freezes at OSS.

Two ways a promoted view can be a lie, and this gate refuses both:

    structurally_empty    the view CANNOT return a row, whatever the data,
                          because its own definition carries a constant-false
                          qualifier. A property of the definition, so it holds
                          on an empty database and a loaded one alike.
                          This is the shape `schema-canonical-read-path` was
                          written to remove --- the Chronos cutover replaced
                          okg.projectable_node_facts / projectable_edge_facts
                          with `WHERE false` stubs that stayed granted, so the
                          promoted surface carried two views that could not
                          answer. Always gating.

    constant_null_column  EVERY arm of the view projects the column as a bare
                          NULL literal, so it cannot carry a value on any
                          data. A frozen column that never carries a value is
                          a contract nobody can honour --- the constant-NULL
                          generation bounds on the retired history views were
                          exactly this. Definition-derived, so it holds
                          without reading a row; see `_literal_null_columns`
                          for why "every arm" rather than "every alias".

Deliberately NOT a static analysis of views.sql. The question is about the
shape a deployment actually has, and runtime-ensured DDL is where the text and
the catalog disagree; the same reasoning check_schema_identity.py records.

Both checks are definition-derived, and that is what lets them cover the
CHRONOS CHECKOUT SURFACES. `okg.graph_nodes` / `okg.graph_edges` are schema
templates whose rows live in interval storage, so a bare read of a view over
them raises through okg.require_graph_checkout() rather than answering. An
earlier version skipped those views entirely — which exempted `okg.v_nodes`,
`okg.v_edges`, the branch-layered pair and the whole document/chunk/agent
family, i.e. precisely the views whose fake generation columns motivated this
gate. Their ROWS are still unreadable from here; that half of the evidence
lives in tests/substrate/db/test_canonical_read_path.py, which sweeps the same
family through a Chronos checkout at a published generation. What is reported
here for those views is `status: chronos_checkout`, meaning "definition
checked, rows swept elsewhere" — not "skipped".

Usage:
    python scripts/check_view_reads.py --dsn postgres://...
    python scripts/check_view_reads.py --dsn ... --json
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any

import psycopg
from psycopg import sql as pgsql

#: The read role whose grants define the promoted surface. Same role
#: check_schema_identity.py uses, for the same reason: it is the surface an
#: agent reaches, not the surface a superuser can see.
_GRANTED_ROLE = "okg_mcp"

#: Marker that a view body defers its emptiness to a Chronos checkout.
_CHECKOUT_GUARD = "require_graph_checkout"

#: Views excluded from the promoted-surface sweep, each with the reason. Keep
#: this SMALL and justified; it is an exclusion list, and an exclusion list
#: that grows without reasons is how a gate stops gating.
_EXCLUDED: dict[str, str] = {}


def promoted_views(cur: psycopg.Cursor[Any]) -> list[str]:
    """Every view in schema ``okg`` the read role can SELECT."""
    cur.execute(
        """
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relkind = 'v'
           AND has_table_privilege(%s, c.oid, 'SELECT')
         ORDER BY c.relname
        """,
        (_GRANTED_ROLE,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def view_source_relations(cur: psycopg.Cursor[Any], view: str) -> list[str]:
    """Base relations (not views) a view reads, closed over view dependency."""
    cur.execute(
        """
        WITH RECURSIVE closure AS (
          SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'okg' AND c.relname = %s
          UNION
          SELECT d.refobjid
            FROM closure cl
            JOIN pg_rewrite r ON r.ev_class = cl.oid
            JOIN pg_depend d ON d.objid = r.oid
                            AND d.classid = 'pg_rewrite'::regclass
                            AND d.refclassid = 'pg_class'::regclass
           WHERE d.refobjid <> cl.oid
        )
        SELECT n.nspname || '.' || c.relname
          FROM closure cl
          JOIN pg_class c ON c.oid = cl.oid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p', 'm', 'f')
         ORDER BY 1
        """,
        (view,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _relation_has_rows(cur: psycopg.Cursor[Any], relation: str) -> bool | None:
    """``EXISTS`` over a relation. ``None`` when the probe itself refuses."""
    schema, name = relation.split(".", 1)
    try:
        cur.execute(
            pgsql.SQL("SELECT EXISTS (SELECT 1 FROM {} LIMIT 1)").format(
                pgsql.Identifier(schema, name)
            )
        )
        return bool(cur.fetchone()[0])
    except psycopg.Error:
        return None


def _view_definition(cur: psycopg.Cursor[Any], view: str) -> str:
    cur.execute("SELECT pg_get_viewdef(%s::regclass)", (f"okg.{view}",))
    row = cur.fetchone()
    return str(row[0]) if row else ""


def _reaches_checkout_guard(cur: psycopg.Cursor[Any], view: str) -> bool:
    """Does this view consult okg.require_graph_checkout, directly or via a view?

    The transitive half is load-bearing: `okg.v_branch_layered_nodes` is
    defined over `okg.v_nodes`, so its own body never names the guard while
    every read through it evaluates one.
    """
    seen: set[str] = set()
    pending = [view]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        if _CHECKOUT_GUARD in _view_definition(cur, current):
            return True
        cur.execute(
            """
            SELECT c.relname
              FROM pg_rewrite r
              JOIN pg_depend d ON d.objid = r.oid
                              AND d.classid = 'pg_rewrite'::regclass
                              AND d.refclassid = 'pg_class'::regclass
              JOIN pg_class c ON c.oid = d.refobjid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE r.ev_class = (
                     SELECT rc.oid
                       FROM pg_class rc
                       JOIN pg_namespace rn ON rn.oid = rc.relnamespace
                      WHERE rn.nspname = 'okg' AND rc.relname = %s
                   )
               AND c.relkind = 'v'
               AND n.nspname = 'okg'
               AND c.relname <> %s
            """,
            (current, current),
        )
        pending.extend(str(row[0]) for row in cur.fetchall())
    return False


#: A constant-false qualifier in a DEPARSED view body.
#:
#: Deliberately several spellings: `pg_get_viewdef` deparses the parse tree
#: WITHOUT constant-folding, so `WHERE 1 = 0` survives verbatim rather than
#: becoming `WHERE false`. Matching only the one idiom the Chronos cutover
#: happened to emit would let the same defect back in under a different
#: spelling. This is still a pattern and not a solver — `WHERE x AND false`,
#: `JOIN ... ON false` and `LIMIT 0` are not recognised — so the failure
#: direction is a missed finding, never a false accusation.
_CONSTANT_FALSE_QUAL = re.compile(
    r"\bWHERE\s+\(*\s*(?:false|1\s*=\s*0|0\s*=\s*1|NOT\s+true)\s*\)*\s*(?:$|;)",
    re.IGNORECASE | re.MULTILINE,
)

#: A top-level set operation in a deparsed body. Arms are split on these so a
#: constant-false qualifier in ONE arm does not condemn the whole view.
_TOP_LEVEL_SETOP = re.compile(
    r"^\s*(?:UNION(?:\s+ALL)?|INTERSECT|EXCEPT)\s*$", re.IGNORECASE | re.MULTILINE
)


def _union_arms(definition: str) -> list[str]:
    """Split a deparsed body into its top-level set-operation arms.

    `pg_get_viewdef` puts each top-level UNION / INTERSECT / EXCEPT on a line of
    its own, so a line-anchored split is exact for what PostgreSQL emits and
    cannot be confused by the word appearing inside a subquery (which is
    indented). A body with no set operation is one arm, which is what makes the
    single-arm case fall out of the same code.
    """
    arms = [arm for arm in _TOP_LEVEL_SETOP.split(definition) if arm.strip()]
    return arms or [definition]


def _is_structurally_empty(cur: psycopg.Cursor[Any], view: str) -> bool:
    """Can this view return a row AT ALL, whatever the data?

    Reads the deparsed definition, NOT the query plan. The plan looks like the
    right oracle — a `WHERE false` body folds to a Result node with
    ``One-Time Filter: false`` — but TimescaleDB emits exactly that plan for a
    scan of a hypertable with no chunks. On a fresh migration that makes every
    view over an empty hypertable (okg.mcp_call_log and its cost views, the
    telemetry span views) read as structurally empty, which is a statement
    about the fixture and not about the schema. The deparse cannot make that
    mistake: it is the definition.

    EVERY arm must be constant-false, not merely one. `okg.v_nodes` carries a
    constant arm so the honest-read guard is evaluable; condemning a view for
    having such an arm would refuse the very construct the read path depends
    on.
    """
    arms = _union_arms(_view_definition(cur, view))
    return all(_CONSTANT_FALSE_QUAL.search(arm) for arm in arms)


def _view_has_rows(cur: psycopg.Cursor[Any], view: str) -> bool | None:
    try:
        cur.execute(
            pgsql.SQL("SELECT EXISTS (SELECT 1 FROM {} LIMIT 1)").format(
                pgsql.Identifier("okg", view)
            )
        )
        return bool(cur.fetchone()[0])
    except psycopg.Error:
        return None


def _view_columns(cur: psycopg.Cursor[Any], view: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'okg' AND table_name = %s
         ORDER BY ordinal_position
        """,
        (view,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _literal_null_columns(
    cur: psycopg.Cursor[Any], view: str, columns: list[str]
) -> set[str]:
    """Columns whose EVERY projection in the view body is a bare NULL literal.

    The distinguishing evidence. A column can be all-NULL in the returned rows
    for three very different reasons, and only the first is a defect:

      * the view can only ever project a literal --- `NULL::bigint AS
        valid_from_gen`, the retired history views' fake generation bounds.
        The column can never carry a value on any data; that is a frozen
        contract nobody can honour, and it is what this gate refuses.
      * the underlying state is genuinely unknown right now --- e.g.
        `okg.v_runtime_queue_health` emits an explicit `status = 'unavailable'`
        sentinel row whose per-queue measurements are NULL because DBOS state
        is absent. NULL there means "unknown", which is precisely correct, and
        flagging it would punish a view for being honest.
      * one UNION arm projects a literal and another projects a value ---
        `okg.v_source_runs` falls back to watermark rows with NULL counts when
        no per-run evidence exists, and `okg.v_nodes` carries a constant arm
        purely to make the honest-read guard evaluable. Both columns CAN carry
        a value; today's rows just came from the other arm.

    Hence "every ARM", not "every alias occurrence". The distinction is
    load-bearing and an earlier version of this function got it wrong.
    `pg_get_viewdef` aliases a select-list item only when the alias differs
    from the expression's own name, so the FIRST arm of `okg.v_nodes` deparses
    as a bare `graph_nodes.node_id` with no `AS`, while its guard arm deparses
    as `NULL::text AS node_id`. Scanning for `AS <column>` therefore finds
    exactly one occurrence per column — the literal one — and concludes that
    every column of the flagship view can never carry a value. Per-arm
    analysis gets it right: arm 1 projects a column reference, so the column
    is cleared.

    Known limit, stated rather than implied: an arm is judged by the ONE
    select-list item at the column's ordinal position, matched textually. A
    column that is constant-NULL by a route the pattern cannot see — `CASE
    WHEN ... THEN NULL ELSE NULL END` — is NOT flagged. The direction of that
    error is the safe one (a missed finding, never a false accusation against
    an honest view), and the column still appears under `all_null_columns` in
    the per-view report, so it stays visible in the JSON even when it does not
    gate.
    """
    #: A select-list item that is nothing but a NULL literal, optionally cast,
    #: optionally parenthesised, optionally aliased.
    bare_null_item = re.compile(
        r"^\(*\s*NULL(?:::[\w ]+(?:\([^)]*\))?)?\s*\)*"
        r"(?:\s+AS\s+\"?[\w$]+\"?)?\s*$",
        re.IGNORECASE,
    )
    arms = _union_arms(_view_definition(cur, view))
    literal: set[str] = set()
    for index, column in enumerate(columns):
        items = [_select_item(arm, index) for arm in arms]
        if any(item is None for item in items):
            # An arm whose select list could not be split at this ordinal.
            # Unparsed is not "proven constant" — say nothing.
            continue
        if all(bare_null_item.match(item or "") for item in items):
            literal.add(column)
    return literal


def _select_item(arm: str, index: int) -> str | None:
    """The ``index``-th select-list item of one deparsed arm, or None.

    Splits on commas at paren depth zero, which is exactly what separates
    select-list items and what protects `COALESCE(a, b)` from being read as
    two of them. Returns None when the arm has no such item, when the select
    list cannot be located, or when parentheses do not balance — every "I do
    not know" answer is None rather than a guess, because the caller treats
    None as "not proven constant".
    """
    upper = arm.upper()
    start = upper.find("SELECT")
    if start < 0:
        return None
    body = arm[start + len("SELECT") :]
    depth = 0
    items: list[str] = []
    current: list[str] = []
    for position, char in enumerate(body):
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
            if depth < 0:
                return None
        elif depth == 0 and char == "," :
            items.append("".join(current))
            current = []
            continue
        elif depth == 0 and body.startswith("\n  FROM", position):
            break
        elif depth == 0 and body.startswith("\nFROM", position):
            break
        current.append(char)
    items.append("".join(current))
    # Trailing clauses (FROM / WHERE / ORDER BY) ride on the final item; strip
    # them so the last column is judged on its expression, not on the query.
    tail = re.split(r"\n\s*(?:FROM|WHERE|ORDER\s+BY|GROUP\s+BY)\b", items[-1], 1,
                    flags=re.IGNORECASE)
    # ... and the statement terminator, which rides the last item on a view
    # with no FROM clause and would otherwise make every final column look
    # unrecognised.
    items[-1] = tail[0].rstrip().rstrip(";")
    if index >= len(items):
        return None
    return items[index].strip()


def _constant_null_columns(
    cur: psycopg.Cursor[Any], view: str, columns: list[str]
) -> list[str]:
    """Columns that are NULL in EVERY row the view currently returns.

    One query, not one per column: `count(col)` counts non-nulls, so a zero
    count over a non-empty view is exactly "constant NULL here".
    """
    if not columns:
        return []
    counts = pgsql.SQL(", ").join(
        pgsql.SQL("count({})").format(pgsql.Identifier(col)) for col in columns
    )
    cur.execute(
        pgsql.SQL("SELECT {} FROM {}").format(
            counts, pgsql.Identifier("okg", view)
        )
    )
    row = cur.fetchone() or ()
    return [col for col, non_null in zip(columns, row, strict=True) if not non_null]


def run_view_read_checks(*, dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        return _run_view_read_checks_on(cur)


def _run_view_read_checks_on(cur: psycopg.Cursor[Any]) -> dict[str, Any]:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (_GRANTED_ROLE,))
    if cur.fetchone() is None:
        return {
            "ok": False,
            "detail": (
                f"read role {_GRANTED_ROLE!r} does not exist; the promoted "
                "surface cannot be determined, so this tier is uncertified"
            ),
            "findings": [],
            "view_count": 0,
            "checked": [],
        }

    findings: list[dict[str, Any]] = []
    checked: list[dict[str, Any]] = []
    views = promoted_views(cur)

    if not views:
        # Nothing to check is not the same as nothing wrong. A gating tier
        # that certifies an EMPTY surface is the "absence read as agreement"
        # failure: a database where views.sql never installed, or where the
        # grants were revoked, would sail through the one check written to
        # notice that the read layer is broken.
        return {
            "ok": False,
            "detail": (
                f"no okg.v_* view is readable by {_GRANTED_ROLE!r}; the read "
                "layer is absent or ungranted, so this tier cannot be "
                "certified"
            ),
            "findings": [],
            "view_count": 0,
            "checked": [],
        }

    for view in views:
        if view in _EXCLUDED:
            checked.append(
                {"view": view, "status": "excluded", "reason": _EXCLUDED[view]}
            )
            continue

        # Whether the view reaches the checkout guard decides only whether its
        # ROWS are readable from here — NOT whether it is checked. An earlier
        # version skipped guarded views outright, which exempted the entire
        # graph family: v_nodes, v_edges, the branch-layered pair and the
        # document/chunk/agent views. Those are exactly the views whose
        # constant-NULL generation columns motivated this gate, so the skip
        # made it strongest where it was needed least. The definition-level
        # checks below need no rows and run on every view.
        guarded = _reaches_checkout_guard(cur, view)

        if _is_structurally_empty(cur, view):
            checked.append({"view": view, "status": "structurally_empty"})
            findings.append(
                {
                    "check": "structurally_empty",
                    "view": f"okg.{view}",
                    "detail": (
                        "the view's own definition carries a constant-false "
                        "qualifier: it cannot return a row on any data, so a "
                        "reader asking it a question always gets 'none'"
                    ),
                }
            )
            continue

        columns = _view_columns(cur, view)
        # The definition-level half, which needs no rows and therefore covers
        # the guarded graph family too: a column every arm projects as a bare
        # NULL literal can never carry a value on ANY data. That is the
        # retired history views' shape, and it is a schema fact rather than a
        # fixture fact, so it gates on its own.
        literal = _literal_null_columns(cur, view, columns)

        # The data-level half, where the rows are reachable from here. A
        # guarded view refuses a bare read by design, so its rows are swept by
        # tests/substrate/db/test_canonical_read_path.py instead, which reads
        # the whole promoted graph family through a Chronos checkout at a
        # published generation.
        all_null: list[str] = []
        if guarded:
            status = "chronos_checkout"
        elif _view_has_rows(cur, view) is not True:
            # No rows TODAY. Distinct from structurally empty above: this view
            # would answer if its sources were populated, so on a fresh
            # migration it is a fixture fact. Sweeping it would report every
            # column of every unpopulated operational view as constant-NULL.
            status = "no_rows"
        else:
            try:
                all_null = _constant_null_columns(cur, view, columns)
                status = "populated"
            except psycopg.Error as exc:
                checked.append(
                    {"view": view, "status": "unreadable", "detail": str(exc).strip()}
                )
                continue

        entry: dict[str, Any] = {"view": view, "status": status}
        if all_null:
            entry["all_null_columns"] = all_null
        if literal:
            entry["literal_null_columns"] = sorted(literal)
        checked.append(entry)

        if literal:
            findings.append(
                {
                    "check": "constant_null_column",
                    "view": f"okg.{view}",
                    "columns": sorted(literal),
                    "detail": (
                        "every arm of this view projects the column as a bare "
                        "NULL literal, so it cannot carry a value on any data; "
                        "a frozen column that never carries a value is not a "
                        "contract"
                    ),
                }
            )

    return {
        "ok": not findings,
        "findings": findings,
        "view_count": len(views),
        "checked": checked,
    }


def _format_text(result: dict[str, Any]) -> str:
    lines = [
        "view reads: " + ("PASS" if result["ok"] else "FAIL"),
        f"  promoted views: {result['view_count']}",
    ]
    if result.get("detail"):
        lines.append(f"  {result['detail']}")
    for finding in result["findings"]:
        target = finding["view"]
        columns = finding.get("columns")
        if columns:
            target = f"{target} ({', '.join(columns)})"
        lines.append(f"  {finding['check']}: {target} — {finding['detail']}")
    if result["findings"]:
        lines.append(
            "  fix: give the view a populated source and real columns, or "
            "retire it from the granted surface — a promoted view that "
            "cannot return a value is a frozen lie"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True, help="migrated database DSN")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    result = run_view_read_checks(dsn=args.dsn)
    if args.as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
