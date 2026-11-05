"""A profile's excluded path patterns, applied to passages through their record (#4227).

PACT unit-grep-exclude-globs-4227. A search profile's `exclude_path_globs`
names paths. A record usually carries the `path`; its passages (the chunks
or comments it declares as `passage_rollup` children) usually carry none.
The exclusion read a row's own path only, so a passage of an excluded record
was still matched, counted and served by grep's unit form, lexical's passage
rows, semantic search and the alias method, while grep's record form, which
reads the record, left it out.

The rule. A passage is left out when its own path is excluded (as before),
or when it has a declaring record (through the declared relation edge) and
every such record has a path the patterns match. A record with no path, or
with a kept path, admits the passage: any declaring record that is not
excluded keeps it, as any visible lending record admits a unit under
`path_prefix` (`grep_search._scan_visible`). A passage with no declaring
record is judged on its own path only.

Two checks, as for a row's own path:

- `record_path_exclusion_sql` puts the rule into the SQL that ranks and
  bounds a read (grep's unit predicate, lexical's ranked read and its
  effective-attribute supplement), on the stored paths of every live
  declaring record, so excluded passages never fill a scan bound or a rank
  cap. Under access rules (Nomos `audit` or `enforce`) it is not used: a
  stored path of a record the reader may not see would decide what the
  reader is served, which tells the reader something about that record. The
  check is then a residual after the read, and the reply says so
  (`excluded_path_globs_application: post_fetch_residual`).
- `passages_excluded_by_record_path` checks what the reader reads: only a
  record and an edge the reader may see count (a hidden record decides
  nothing), on the record's path as read (overlaid; under a policy that can
  redact, a record whose path the read would change counts as excluded, the
  fail-closed rule of the own-path check in `grep_search._Admitter`).


Limits, stated. The SQL form reads a record's STORED path, as the own-path
SQL predicate does; the reader-view check reads the overlaid path. An
overlay that moves a record's path from excluded to kept leaves its passage
out (the SQL never reads it), one that moves it the other way is caught by
the check. Under access rules the read is not bounded by the rule, so
lexical ranks deeper (`search._PROFILE_RANK_CAP_MAX`) and a page whose cap
the dropped rows filled says it is cut; grep counts units one by one at its
scan site, which costs more statements than its SQL count.
"""
from __future__ import annotations

import weakref
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.bm25_node_search import branch_point
from okg.substrate.mcp.readable_text import read_unchanged, redaction_possible
from okg.substrate.mcp.search_profiles import (
    ResolvedScope,
    excluded_by_path_globs,
    glob_to_like,
)
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import visible_ids_by_decision_class

__all__ = [
    "RecordPathContext",
    "passage_types_in_scope",
    "record_path_context",
    "passages_excluded_by_record_path",
    "record_path_exclusion_is_residual",
    "record_path_exclusion_sql",
]

_ROW_KEYS = (
    "node_id", "subtype", "attrs", "text_hash", "source",
    "source_record_id", "source_revision",
)
_LIVE = "{a}.deleted = FALSE AND {a}.live_lo <= %({bp})s AND %({bp})s < {a}.live_hi"


@dataclass(frozen=True)
class RecordPathContext:
    """What one call reads once for the reader-view check: the declared
    passage relations, the node and edge segments, the branch point, and
    whether access rules run (when they do not, every record and edge is
    visible and no path is masked, so those checks are skipped)."""

    declared: tuple[tuple[str, str, str], ...]
    segment: str
    edge_segment: str
    branch_point: Any
    access_rules: bool


def record_path_context(conn: psycopg.Connection, session: Session) -> RecordPathContext:
    from okg.substrate.mcp.grep_search import resolve_trgm_segment
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment

    return RecordPathContext(
        declared=tuple(_declared(conn, session)),
        segment=resolve_trgm_segment(conn),
        edge_segment=resolve_edge_segment(conn),
        branch_point=branch_point(conn, session),
        access_rules=record_path_exclusion_is_residual(session),
    )


#: Declared passage relations, read once per session object (one call opens
#: one session; the entry goes when the session does). Keyed by identity: two
#: sessions on different databases can be equal.
_DECLARED: dict[int, tuple[tuple[str, str, str], ...]] = {}


def _declared(conn: psycopg.Connection, session: Session) -> list[tuple[str, str, str]]:
    from okg.substrate.mcp.grep_search import _declarations

    key = id(session)
    if key not in _DECLARED:
        _DECLARED[key] = tuple(_declarations(conn, session))
        weakref.finalize(session, _DECLARED.pop, key, None)
    return list(_DECLARED[key])


def passage_types_in_scope(
    conn: psycopg.Connection, session: Session, scope: Optional[ResolvedScope],
) -> list[str]:
    """The declared passage types a scope can hold (all of them when it names
    no subtypes)."""
    children = sorted({child for _p, _e, child in _declared(conn, session)})
    if scope is not None and scope.subtypes is not None:
        children = [child for child in children if child in scope.subtypes]
    return children


def record_path_exclusion_is_residual(session: Session) -> bool:
    """Whether the record-path rule runs only after the read (access rules)."""
    from okg.substrate.nomos.runtime_enforcement import deployment_requires_nomos_runtime

    return deployment_requires_nomos_runtime(session.deployment_name)


def record_path_exclusion_sql(
    conn: psycopg.Connection,
    session: Session,
    scope: Optional[ResolvedScope],
    params: dict[str, Any],
    *,
    segment: str,
    node: str = "n",
    prefix: str = "rpx",
) -> Optional[str]:
    """The rule as a SQL condition on the row alias `node` of `segment`, or
    None when the scope has no excluded paths or no passage type, or when the
    rule runs as a residual (access rules)."""
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment

    if scope is None or not scope.excluded_path_globs:
        return None
    if record_path_exclusion_is_residual(session):
        return None
    children = passage_types_in_scope(conn, session, scope)
    if not children:
        return None
    declared = [d for d in _declared(conn, session) if d[2] in children]
    bp = f"{prefix}_bp"
    params[bp] = branch_point(conn, session)
    params[f"{prefix}_children"] = children
    values = []
    for index, (parent, edge, child) in enumerate(declared):
        params[f"{prefix}_p{index}"] = parent
        params[f"{prefix}_e{index}"] = edge
        params[f"{prefix}_c{index}"] = child
        values.append(
            f"(%({prefix}_p{index})s::text, %({prefix}_e{index})s::text, "
            f"%({prefix}_c{index})s::text)"
        )
    likes = []
    for index, pattern in enumerate(scope.excluded_path_globs):
        params[f"{prefix}_g{index}"] = glob_to_like(pattern)
        likes.append(f"(xp.attrs->>'path') LIKE %({prefix}_g{index})s ESCAPE '\\'")
    edge_segment = resolve_edge_segment(conn)
    record = (
        f"SELECT 1 FROM {edge_segment} xe "
        f"JOIN {segment} xp ON xp.node_id = xe.src AND {_LIVE.format(a='xp', bp=bp)} "
        f"JOIN (VALUES {', '.join(values)}) AS xd(parent, edge, child) "
        f"ON xd.parent = xp.subtype AND xd.edge = xe.edge_type AND xd.child = {node}.subtype "
        f"WHERE xe.dst = {node}.node_id AND {_LIVE.format(a='xe', bp=bp)}"
    )
    # A path that is not a string is never excluded, as in
    # `excluded_by_path_globs` (`->>` would turn a number into text).
    excluded_path = (
        "(coalesce(jsonb_typeof(xp.attrs->'path') = 'string', FALSE) "
        f"AND ({' OR '.join(likes)}))"
    )
    # Kept unless it is a passage with a declaring record and no declaring
    # record that is not excluded.
    return (
        f"({node}.subtype <> ALL(%({prefix}_children)s::text[]) "
        f"OR NOT EXISTS ({record}) OR EXISTS ({record} AND NOT {excluded_path}))"
    )


def passages_excluded_by_record_path(
    conn: psycopg.Connection,
    session: Session,
    rows: Sequence[Mapping[str, Any]],
    globs: Sequence[str],
    *,
    context: Optional[RecordPathContext] = None,
) -> set[str]:
    """The ids among `rows` that the rule leaves out, judged on what the
    reader reads: each passage with a visible declaring record whose every
    visible declaring record has an excluded (or unreadable) path.

    `context` is read once per call by a caller that checks many batches
    (grep's scan site); without it, it is read here."""
    if not globs or not rows:
        return set()
    if context is None:
        context = record_path_context(conn, session)
    subtypes = {str(r.get("subtype") or "") for r in rows}
    declared = [d for d in context.declared if d[2] in subtypes]
    children = {child for _p, _e, child in declared}
    ids = sorted({str(r["node_id"]) for r in rows if str(r.get("subtype") or "") in children})
    if not ids:
        return set()
    params: dict[str, Any] = {"ids": ids, "bp": context.branch_point}
    values = []
    for index, (parent, edge, child) in enumerate(declared):
        params[f"pp{index}"], params[f"pe{index}"], params[f"pc{index}"] = parent, edge, child
        values.append(f"(%(pp{index})s::text, %(pe{index})s::text, %(pc{index})s::text)")
    segment = context.segment
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT n.node_id::text AS unit_id, e.edge_id::text AS edge_id, "
            f"p.node_id, p.subtype, p.attrs, p.text_hash, p.source, "
            f"p.source_record_id, p.source_revision "
            f"FROM {context.edge_segment} e "
            f"JOIN {segment} p ON p.node_id = e.src AND {_LIVE.format(a='p', bp='bp')} "
            f"JOIN {segment} n ON n.node_id = e.dst AND {_LIVE.format(a='n', bp='bp')} "
            f"JOIN (VALUES {', '.join(values)}) AS d(parent, edge, child) "
            f"ON d.parent = p.subtype AND d.edge = e.edge_type AND d.child = n.subtype "
            f"WHERE e.dst = ANY(%(ids)s) AND {_LIVE.format(a='e', bp='bp')}",
            params,
        )
        found = cur.fetchall()
    if not found:
        return set()
    records = {
        str(hit["node_id"]): {key: hit.get(key) for key in _ROW_KEYS} for hit in found
    }
    if context.access_rules:
        shown = visible_ids_by_decision_class(
            conn, session, list(records.values()), row_kind="node",
        )
        edges_shown: Optional[set[str]] = visible_ids_by_decision_class(
            conn, session, [{"edge_id": hit["edge_id"]} for hit in found], row_kind="edge",
        )
    else:
        # No access rules: every record and edge is visible.
        shown, edges_shown = set(records), None
    seen = [dict(records[i]) for i in sorted(shown)]
    if context.access_rules and redaction_possible(conn, session):
        readable = read_unchanged(conn, session, seen, decisive=lambda _row: {"path"})
    else:
        readable = apply_effective_attrs_to_rows(
            conn, seen, target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
    judged = {str(r["node_id"]): r for r in readable}
    # Per passage: is there a visible declaring record, and does one keep it?
    kept: set[str] = set()
    named: set[str] = set()
    for hit in found:
        record_id = str(hit["node_id"])
        if record_id not in shown:
            continue
        if edges_shown is not None and str(hit["edge_id"]) not in edges_shown:
            continue
        unit = str(hit["unit_id"])
        named.add(unit)
        record = judged.get(record_id)
        # Unreadable path: fail closed (counts as excluded).
        if record is not None and not excluded_by_path_globs(record.get("attrs"), globs):
            kept.add(unit)
    return named - kept
