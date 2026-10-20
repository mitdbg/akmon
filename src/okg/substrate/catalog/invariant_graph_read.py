"""Pinned Chronos graph reads for deployment-authored invariant SQL.

Two publish-time surfaces execute deployment-authored SQL: the publish gate's
``sql_body`` (``publisher/gates.py``) and the LLM-judge row source's
``candidates_sql`` (``catalog/invariants_llm.py``). Both ran that SQL on a bare
pooled connection, where the Chronos graph surfaces lie in two different ways:

* ``okg.v_nodes`` / ``okg.v_edges`` trip the ``okg.require_graph_checkout``
  guard in ``db/views.sql`` and raise ``object_not_in_prerequisite_state``,
  which escapes the gate and aborts the whole publish cycle (issue #1456);
* ``okg.graph_nodes`` / ``okg.graph_edges`` are logical stubs -- Chronos keeps
  the rows in a separate physical table and rewrites table names only in
  SUBMITTED SQL -- so a bare read returns the confident zero the guard exists
  to prevent, and the invariant passes vacuously.

Neither is a usable gate, so this module routes those bodies through the read
path the MCP ``run_sql`` tool already uses: inline composite ``okg.v_*`` views,
map the base views onto the physical graph tables, then execute inside a
Chronos checkout, which rewrites the registered names to the branch-visible
interval subquery. ``run_sql`` owns those rewrite helpers and several suites
already import them as the shared graph-read rewrite; they are imported rather
than re-implemented so there is one definition of "how OKG reads the graph".

A body that reads no graph relation is returned untouched and keeps its
original execution path, so non-graph invariants are unaffected.
"""
from __future__ import annotations

import re
from typing import Any

__all__ = [
    "DROPPED_GRAPH_RELATIONS",
    "dropped_graph_relations_in",
    "graph_branch_checkout",
    "prepare_invariant_sql",
    "references_graph_relation",
    "to_checkout_sql",
]


# Relations the Chronos cutover DROPPED (``retire_legacy_graph_storage`` in
# ``db/management.py`` issues DROP TABLE for both on every migrate), mapped to
# the maintained surface that replaces them. A body reading one of these fails
# with undefined_table at publish time and no checkout can repair it, so it is
# refused at authoring time instead.
DROPPED_GRAPH_RELATIONS: dict[str, str] = {
    "okg.nodes_live": "okg.v_nodes",
    "okg.edges_live": "okg.v_edges",
}

# Fail-closed backstop for SQL the PostgreSQL parser rejects. The parser walk
# below is the real detector -- it ignores comments and string literals for
# free -- but an unparseable body would otherwise slip through as "no dropped
# relation found". These mirror the comment/literal strippers `sql_contains_writes`
# already uses in `catalog/invariants.py`.
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_STRING_RE = re.compile(r"'(?:[^']|'')*'")
_DROPPED_RELATION_RE = re.compile(
    r"\bokg\.(nodes_live|edges_live)\b", re.IGNORECASE
)

# Names whose presence means the parser-based rewrite is worth attempting. The
# `okg.v_` prefix covers composite deployment views, which may be defined over
# the base graph views even when the invariant never names those directly.
_GRAPH_RELATION_MARKERS = (
    "graph_nodes",
    "graph_edges",
    "v_nodes",
    "v_edges",
    "okg.v_",
    'okg"."v_',
)


def _relation_keys(sql: str) -> list[str] | None:
    """Return every ``schema.relation`` the parser sees, or None if unparseable.

    Uses the same parser front end ``run_sql`` uses
    (`okg.substrate.postgres_sql`), so quoted spellings are canonicalized and
    mentions inside comments or string literals never appear.
    """
    try:
        from sqlglot import exp

        from okg.substrate.postgres_sql import iter_nodes, parse, table_name_parts

        # Mask psycopg placeholders first: PostgreSQL's parser has no `%(name)s`
        # syntax, so an unmasked body that binds one would fall to the regex,
        # which cannot see a quoted `"okg"."nodes_live"`.
        parsed = parse(_mask_pyformat_params(sql)[0])
    except Exception:  # noqa: BLE001 - unparseable SQL falls back to the regex
        return None
    if not parsed.statements:
        return None
    keys: list[str] = []
    for statement in parsed.statements:
        for node in iter_nodes(statement):
            if not isinstance(node, exp.Table):
                continue
            parts = table_name_parts(node)
            if parts is None:
                continue
            _catalog, schema, relname = parts
            schema = (schema or "").lower()
            relname = relname.lower()
            key = f"{schema}.{relname}" if schema else relname
            if key not in keys:
                keys.append(key)
    return keys


def dropped_graph_relations_in(sql: str) -> list[str]:
    """Return the dropped legacy graph relations this SQL reads, in order."""
    keys = _relation_keys(sql)
    if keys is not None:
        return [key for key in keys if key in DROPPED_GRAPH_RELATIONS]
    # Unparseable body: strip comments and string literals, then scan. This is
    # a declared fallback, not a silent one -- it can only ADD refusals, never
    # hide one, and an unparseable body is separately reported by the publish
    # preflight's planner pass.
    stripped = _BLOCK_COMMENT_RE.sub(" ", sql)
    stripped = _LINE_COMMENT_RE.sub(" ", stripped)
    stripped = _STRING_RE.sub("''", stripped)
    found: list[str] = []
    for match in _DROPPED_RELATION_RE.finditer(stripped):
        key = f"okg.{match.group(1).lower()}"
        if key not in found:
            found.append(key)
    return found


# Invariant bodies are psycopg SQL: the gate binds `%(pin)s` / `%(excluded_sources)s`
# and the LLM-judge row source binds `%(seed)s` / `%(cap)s`. PostgreSQL's own parser
# has no such syntax, so the parser rejects those statements outright. Mask
# each placeholder as a native positional parameter -- which every parser accepts in
# exactly the positions a parameter may appear, including LIMIT and ANY(...) -- then
# restore it after the rewrite.
_PYFORMAT_RE = re.compile(r"%\((\w+)\)s")
_POSITIONAL_RE = re.compile(r"\$(\d+)")


def _mask_pyformat_params(sql: str) -> tuple[str, dict[str, str]]:
    names: list[str] = []
    # Numbering starts above any positional parameter already in the text so the
    # restore below cannot capture one the author wrote.
    base = max(
        (int(m.group(1)) for m in _POSITIONAL_RE.finditer(sql)), default=0,
    )

    def _swap(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in names:
            names.append(name)
        return f"${base + names.index(name) + 1}"

    masked = _PYFORMAT_RE.sub(_swap, sql)
    restore = {
        f"${base + index + 1}": f"%({name})s"
        for index, name in enumerate(names)
    }
    return masked, restore


def _restore_pyformat_params(sql: str, restore: dict[str, str]) -> str:
    if not restore:
        return sql
    return _POSITIONAL_RE.sub(
        lambda m: restore.get(m.group(0), m.group(0)), sql,
    )


def references_graph_relation(sql: str) -> bool:
    """Textual fallback: could this statement read a Chronos graph relation?

    Consulted ONLY when the SQL parser cannot read the statement. It is NOT a
    sound test on its own: PostgreSQL accepts Unicode-escaped identifiers, so
    `okg.U&"graph_node\0073"` names `okg.graph_nodes` without the string
    "graph_nodes" appearing anywhere in the text. Treating that as "no graph
    read" would route it to the bare connection and return the confident zero
    this module exists to eliminate, so the parser is the authority wherever it
    can answer and this is used only where it cannot.
    """
    lowered = sql.lower()
    return any(name in lowered for name in _GRAPH_RELATION_MARKERS)


def prepare_invariant_sql(sql: str, conn: Any) -> tuple[str, bool]:
    """Return ``(prepared_sql, needs_checkout)`` for one invariant statement.

    ``sql`` is the statement the caller actually executes -- the count wrapper or
    the sample wrapper, not the bare body -- because the rewrite must see the
    whole statement. When the statement reads no graph relation the ORIGINAL
    string is returned unchanged, so the non-graph execution path stays
    byte-identical to what it was before.

    The returned SQL keeps psycopg ``%(name)s`` placeholders. That is what lets
    the publish preflight EXPLAIN it on a plain cursor: after the rewrite the
    statement names ``okg.graph_nodes`` / ``okg.graph_edges``, whose logical
    relations carry the same user columns Chronos copied into the physical
    table, so the planner resolves exactly the relations and columns the
    checkout will. Callers that actually execute it through a checkout convert
    the placeholders with `to_checkout_sql` first.
    """
    from okg.substrate.mcp.run_sql import (
        _expand_composite_views,
        _references_physical_graph_view,
        _rewrite_graph_views,
    )

    masked, restore = _mask_pyformat_params(sql)
    try:
        prepared = _rewrite_graph_views(_expand_composite_views(masked, conn))
    except Exception:
        # The parser could not read this statement. Fall back to the textual
        # check to decide what that means. A statement naming no graph relation
        # keeps its original execution path -- a body PostgreSQL accepts but
        # the parser front end cannot read must never become a publish failure
        # just by passing through here. One that DOES appear to name a graph
        # relation cannot be rewritten and so cannot be read honestly: let the
        # parser error surface, where the publish preflight reports it by name.
        if not references_graph_relation(sql):
            return sql, False
        raise
    if not _references_physical_graph_view(prepared):
        return sql, False
    return _restore_pyformat_params(_canonicalize(prepared), restore), True


def _canonicalize(sql: str) -> str:
    """Re-render through the PostgreSQL front end so relation names are canonical.

    `_rewrite_graph_views` returns its input untouched when it changed nothing,
    which leaves an exotic-but-legal spelling as the author wrote it. That is not
    good enough downstream: a Chronos checkout substitutes registered tables with
    sqlglot, and sqlglot cannot even parse `okg.U&"graph_node\\0073"` -- a
    spelling PostgreSQL resolves to `okg.graph_nodes`. Left as written it would
    either fail the checkout or slip through unsubstituted and read the empty
    stub. Normalizing decodes it to `okg."graph_nodes"`, which every downstream
    parser understands, and leaves every other byte as written.
    """
    try:
        from okg.substrate.postgres_sql import parse

        parsed = parse(sql)
        if len(parsed.statements) != 1:
            return sql
        return parsed.text
    except Exception:  # noqa: BLE001 - the caller already parsed this fine
        return sql


def to_checkout_sql(sql: str) -> str:
    """Convert psycopg ``%(name)s`` placeholders to the ``:name`` bind style a
    Chronos checkout session expects."""
    from okg.substrate.transform.operators import to_chronos_sql

    return to_chronos_sql(sql)


def graph_branch_checkout(conn: Any, *, branch_name: str) -> Any:
    """Check out the branch working head on the caller's own connection.

    Invariants run BEFORE the checkpoint exists, so the candidate they must
    validate is the branch head, not a published generation -- the same
    "transform-gate pattern" ``consistency/ir_executor.py`` falls back to. The
    checkout is bound to ``conn``, so the caller's ``SET LOCAL
    statement_timeout`` and ``SET LOCAL TRANSACTION READ ONLY`` still apply to
    every statement it runs.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    # `autocommit=False`: the caller is already inside an explicit
    # `conn.transaction()` (that is what scopes its SET LOCALs), and psycopg
    # forbids an explicit commit inside one.
    return ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=False,
    ).checkout(branch_name)
