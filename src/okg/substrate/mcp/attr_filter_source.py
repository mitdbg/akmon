"""Which candidate set narrows an attribute-filtered MCP read.

okg#3112 step 2, slice 2 (PACT lane-h-3112-step2-reader), step 3, the
cut-over (PACT lane-h-3112-step3), and step 4a (PACT lane-h-3112-step4a), which
took ``okg.attr_index`` out of every read; design
``pact/changes/lane-h-3112-step2/design.md`` sections 2.2, 2.4, 3 and 7.
``OKG_ATTR_FILTER_INDEX`` picks the candidate source of
``_effective_view_sql_page`` in ``mcp/server.py``:

``expression`` (also what an unset switch reads as)
    the declared node expression indexes: through slice 1's probe renderer
    (``declared_attr_indexes.render_candidate_select``), UNION the targets
    with an overlay row on a predicate key, inside the page statement (one
    statement, one snapshot). An attribute is narrowed only when its index is
    ready (``declared_attr_indexes.reader_readiness``), the read is at the
    branch head, and every bound passes its type's probe rule. Every other
    declared attribute the predicate narrows by -- an edge attribute
    (``kind_not_indexed``, decision 287d), an index that is not ready, a
    ``ts`` bound v4 refuses, a read pinned to a non-head generation -- is
    answered by the exact predicate alone and named in a ``{"pushdown":
    "not_covered", "attrs": [{"attr", "reason"}]}`` note, as is a top-level
    ``prefix`` clause on a declared key (``prefix_not_indexed``; no index
    narrows a prefix). Other clauses no index narrows (``exists``, a clause
    under ``or`` or ``not``) are answered the same way without a note.
``off``
    no candidate set: the exact predicate over the effective relation alone.

In both modes the exact predicate decides every row, so both return the rows
``off`` returns, in the same order. ``attr_index`` and ``dual``, the step-3
values, are refused on a filtered read with ``removed_value``
(:func:`filter_index_mode`); the MCP server still starts with one set and logs
it once (:func:`log_removed_settings`). Any other unknown value is refused.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Mapping, NamedTuple, Sequence

import psycopg
from psycopg.rows import dict_row

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.run_steps import names_run_values

log = logging.getLogger(__name__)

__all__ = [
    "MODES",
    "REMOVED_MODES",
    "SWITCH_ENV",
    "ExpressionCandidates",
    "PushableClause",
    "TsBound",
    "filter_index_mode",
    "log_removed_settings",
    "narrowable_predicate",
    "not_covered_note",
    "plan_expression_candidates",
    "prefix_attrs",
    "pushable_clauses",
    "ts_refused_attrs",
]

SWITCH_ENV = "OKG_ATTR_FILTER_INDEX"
MODES = ("expression", "off")
#: The step-3 values step 4a removed (okg#3112): refused by name, never mapped
#: to another mode.
REMOVED_MODES = ("attr_index", "dual")
#: The mode an unset switch reads as.
DEFAULT_MODE = "expression"
#: Step 3's dual-read settings; since step 4a they have no effect.
_REMOVED_SETTINGS = ("OKG_ATTR_FILTER_DUAL_SAMPLE_EVERY", "OKG_ATTR_FILTER_DUAL_TIMEOUT_MS")
#: Ops a conjunctive clause can be narrowed by (``exists`` is not: presence
#: is not type admission).
_PUSHABLE_OPS = frozenset({"eq", "in", "lt", "gt", "between"})


class TsBound(NamedTuple):
    """A ``ts`` range bound for the exact predicate: compared as
    ``okg.attr_ts_v4(raw)`` when v4 accepts it, else as ``parsed`` (today's
    Python reading). Decided in SQL, so no round trip."""

    raw: str
    parsed: Any


def filter_index_mode() -> str:
    """The configured mode, read on every call. A removed or unknown value is
    refused, never defaulted."""
    raw = os.environ.get(SWITCH_ENV)
    if raw is None:
        return DEFAULT_MODE
    mode = raw.strip()
    if mode in REMOVED_MODES:
        err = SubstrateError(
            f"{SWITCH_ENV}={mode} is no longer supported",
            data={"reason": "removed_value", "value": raw, "accepted": list(MODES)},
            fix_hint=f"unset {SWITCH_ENV}, or set it to expression or off",
        )
        err.code = "invalid_configuration"
        raise err
    if mode not in MODES:
        err = SubstrateError(
            f"unknown {SWITCH_ENV} value",
            data={"value": raw, "known": list(MODES)},
            fix_hint=(
                f"set {SWITCH_ENV} to one of {', '.join(MODES)}, or unset it for the "
                f"default ({DEFAULT_MODE})"
            ),
        )
        err.code = "invalid_configuration"
        raise err
    return mode


def log_removed_settings() -> None:
    """At MCP server start: one ERROR when the switch holds a removed or
    unknown value (every filtered read will refuse it; other reads still
    work), and one
    WARNING naming the dual-read settings that are set and have no effect."""
    raw = os.environ.get(SWITCH_ENV)
    if raw is not None and raw.strip() in REMOVED_MODES:
        log.error(
            "%s=%s is no longer supported: every attribute-filtered "
            "filter, map and search read is refused with invalid_configuration "
            "until it is unset or set to expression or off",
            SWITCH_ENV, raw.strip(),
        )
    elif raw is not None and raw.strip() not in MODES:
        log.error(
            "%s=%r is not a known value: every attribute-filtered filter, map "
            "and search read is refused with invalid_configuration until it is "
            "unset or set to expression or off",
            SWITCH_ENV, raw,
        )
    stale = [name for name in _REMOVED_SETTINGS if os.environ.get(name) is not None]
    if stale:
        log.warning(
            "%s %s no effect: the dual read was removed",
            " and ".join(stale), "has" if len(stale) == 1 else "have",
        )


@dataclass(frozen=True)
class PushableClause:
    attr: str
    value_type: str
    op: str
    value: Any


def pushable_clauses(predicate: Any, declared: Mapping[str, str]) -> list[PushableClause]:
    """The top-level conjuncts a candidate set may narrow: ``eq``, ``in``,
    ``lt``, ``gt`` and ``between`` on a declared key. A clause under ``or``
    or ``not`` never narrows: a candidate set is a superset of one
    conjunct's answer, which says nothing about a disjunct or a negation."""
    if predicate in (None, True, {}) or not declared:
        return []
    if isinstance(predicate, dict) and predicate.get("op") == "and":
        clauses = predicate.get("clauses")
        conjuncts = list(clauses) if isinstance(clauses, list) else [predicate]
    else:
        conjuncts = [predicate]
    out: list[PushableClause] = []
    for clause in conjuncts:
        if not isinstance(clause, dict) or clause.get("op") not in _PUSHABLE_OPS:
            continue
        field_name = clause.get("field")
        if not isinstance(field_name, str) or not field_name.startswith("attrs."):
            continue
        attr = field_name.split(".", 1)[1]
        value_type = declared.get(attr)
        if value_type is None:
            continue
        op = str(clause["op"])
        value = clause.get("values") if op in ("in", "between") else clause.get("value")
        if op == "in" and names_run_values(value):
            # A `run` step's value list (#3666) is a subquery, not values an
            # index can look up; the exact condition decides it.
            continue
        out.append(PushableClause(attr, value_type, op, value))
    return out


def prefix_attrs(predicate: Any, declared: Mapping[str, str]) -> list[str]:
    """The declared keys a top-level conjunctive ``prefix`` clause names:
    answered by the exact predicate alone, reported ``prefix_not_indexed``."""
    if not isinstance(predicate, dict) or not declared:
        return []
    if predicate.get("op") == "and" and isinstance(predicate.get("clauses"), list):
        conjuncts = predicate["clauses"]
    else:
        conjuncts = [predicate]
    out: list[str] = []
    for clause in conjuncts:
        if not isinstance(clause, dict) or clause.get("op") != "prefix":
            continue
        field_name = clause.get("field")
        if isinstance(field_name, str) and field_name.startswith("attrs."):
            attr = field_name.split(".", 1)[1]
            if attr in declared and attr not in out:
                out.append(attr)
    return out


def _ts_strings(clause: PushableClause) -> list[str] | None:
    values = clause.value if clause.op in ("in", "between") else [clause.value]
    if not isinstance(values, list) or not values:
        return None
    if not all(isinstance(v, str) for v in values):
        return None
    return list(values)


def ts_refused_attrs(conn: psycopg.Connection, clauses: Sequence[PushableClause]) -> list[str]:
    """The ``ts`` attributes with a bound ``okg.attr_ts_v4`` refuses (design
    section 2.2 rule 2): no candidate set may narrow them, in any mode.

    A bound that fails the v4 shape is refused without asking the database;
    the remaining bounds of every clause are checked in one round trip, and
    none is made when no ``ts`` clause has a well-shaped bound.
    """
    refused: list[str] = []
    to_check: list[tuple[str, list[str]]] = []
    for clause in clauses:
        if clause.value_type != "ts":
            continue
        bounds = _ts_strings(clause)
        if bounds is None:
            continue
        if not all(dai.TS_V4_RE.match(b) for b in bounds):
            if clause.attr not in refused:
                refused.append(clause.attr)
        else:
            to_check.append((clause.attr, bounds))
    flat = [b for attr, bounds in to_check if attr not in refused for b in bounds]
    if flat:
        with conn.cursor(row_factory=dict_row) as cur:
            accepted = dict(zip(flat, dai.ts_bounds_accepted(cur, flat)))
        for attr, bounds in to_check:
            if attr not in refused and not all(accepted.get(b, False) for b in bounds):
                refused.append(attr)
    return refused


def narrowable_predicate(predicate: Any, refused: Sequence[str]) -> Any:
    """``predicate`` without the conjuncts no candidate set may narrow by:
    the pushable clauses on a ``refused`` attribute (a ``ts`` bound v4
    refuses). The exact predicate still decides them; dropping a conjunct
    only widens a candidate set, so the other clauses keep narrowing.
    ``None`` when nothing is left to narrow by."""
    if not refused:
        return predicate
    drop = set(refused)

    def dropped(clause: Any) -> bool:
        if not isinstance(clause, dict) or clause.get("op") not in _PUSHABLE_OPS:
            return False
        field_name = clause.get("field")
        return (
            isinstance(field_name, str)
            and field_name.startswith("attrs.")
            and field_name.split(".", 1)[1] in drop
        )

    if isinstance(predicate, dict) and predicate.get("op") == "and" and isinstance(
        predicate.get("clauses"), list
    ):
        kept = [clause for clause in predicate["clauses"] if not dropped(clause)]
        if not kept:
            return None
        return {**predicate, "clauses": kept}
    return None if dropped(predicate) else predicate


def not_covered_note(uncovered: Sequence[tuple[str, str]]) -> dict[str, Any]:
    return {
        "pushdown": "not_covered",
        "attrs": [{"attr": attr, "reason": reason} for attr, reason in uncovered],
    }


@dataclass
class ExpressionCandidates:
    """The expression-index candidate set of one read: ``sql`` selects text
    target ids (named parameters in ``params``); ``attrs`` it narrows by."""

    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    attrs: tuple[str, ...] = ()


def _head_branch_point(conn: psycopg.Connection, generation_id: Any) -> int | None:
    """The branch point of the pinned generation when it is its branch's
    head, else None."""
    gen = str(generation_id)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chronos_branch_id, stab_ordinal FROM okg.v_generation_interval_stab "
            "WHERE generation_ref = %s",
            (gen,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cur.execute(
            "SELECT max(stab_ordinal) FROM okg.v_generation_interval_stab "
            "WHERE chronos_branch_id = %s",
            (row[0],),
        )
        head = cur.fetchone()[0]
        if head is None or int(head) != int(row[1]):
            return None
        cur.execute(
            "SELECT s.branch_point::bigint "
            "FROM public._chronos_branch_interval_checkpoints c "
            "JOIN public._chronos_branch_interval_segments s ON s.segment_id = c.segment_id "
            "WHERE c.checkpoint_id = %s",
            (gen,),
        )
        bp = cur.fetchone()
    return None if bp is None or bp[0] is None else int(bp[0])


_unreadable_warned: list[bool] = []


def plan_expression_candidates(
    conn: psycopg.Connection,
    *,
    generation_id: Any,
    predicate: Any,
    declared: Mapping[str, str],
    target_kind: str,
    param_prefix: str = "dai",
    refused: Sequence[str] | None = None,
    at_branch_point: int | None = None,
) -> tuple[ExpressionCandidates | None, list[tuple[str, str]]]:
    """The expression candidate set over the attributes whose index is ready
    (None when none narrows; the caller adds the overlay union), and
    ``(attr, reason)`` for each narrowing attribute that is not: its index is
    not ready (reasons from :func:`declared_attr_indexes.reader_readiness`),
    a ``ts`` bound v4 refuses (``bound_not_strict_iso``), or the read is not
    at the branch head (``pinned_generation``). Appends no note: the caller
    names the outcome. ``refused`` is the caller's :func:`ts_refused_attrs`
    answer when it already has one (it then passes the
    :func:`narrowable_predicate`); None asks here.

    ``at_branch_point`` is for a reader of the graph rows themselves (the
    graph read path, PACT lane-s-storage-redesign-s2): the probe then reads
    the index entries visible at that branch point, at any pin. The
    ``pinned_generation`` rule exists because the materialized reader
    filters the effective view of the pinned generation, while the candidate
    set comes from the graph rows at the head. A reader of the graph rows at
    the pin's own branch point has no such gap: the index holds every row
    version (its WHERE reads only the row's own ``deleted`` flag and value),
    so the probe at the pin admits every row visible there whose value the
    clause can match. None keeps the head-only rule."""
    planned = _plan_expression(
        conn, generation_id=generation_id, predicate=predicate, declared=declared,
        target_kind=target_kind, param_prefix=param_prefix, refused=refused,
        at_branch_point=at_branch_point,
    )
    if planned is None:
        return None, []
    return planned


def _plan_expression(
    conn: psycopg.Connection,
    *,
    generation_id: Any,
    predicate: Any,
    declared: Mapping[str, str],
    target_kind: str,
    param_prefix: str,
    refused: Sequence[str] | None,
    at_branch_point: int | None = None,
) -> tuple[ExpressionCandidates | None, list[tuple[str, str]]] | None:
    """None when no clause can narrow; else the candidate set over the ready
    attributes (None when none renders) and the not-ready ``(attr, reason)``."""
    clauses = pushable_clauses(predicate, declared)
    if not clauses:
        return None
    attrs = list(dict.fromkeys(c.attr for c in clauses))
    uncovered: list[tuple[str, str]] = []
    physical_table: str | None = None
    bp = (
        int(at_branch_point) if at_branch_point is not None
        else _head_branch_point(conn, generation_id)
    )
    if bp is None:
        uncovered = [(attr, "pinned_generation") for attr in attrs]
    else:
        refused_attrs = set(
            ts_refused_attrs(conn, clauses) if refused is None else refused
        )
        try:
            with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                ready, physical_table = dai.reader_readiness(
                    cur, target_kind=target_kind,
                    declared={attr: declared[attr] for attr in attrs},
                )
        except psycopg.errors.InsufficientPrivilege as exc:
            # The read role cannot see the manifest (a grant the registry
            # apply skipped): nothing vouches for the indexes, so they are not
            # used. Logged once; the response names the attributes.
            if not _unreadable_warned:
                _unreadable_warned.append(True)
                log.warning(
                    "declared attribute index manifest is unreadable by the MCP read role "
                    "(%s); expression-index filter reads scan every record and report "
                    "pushdown not_covered / index_missing until SELECT on "
                    "okg.declared_attr_indexes is granted to okg_mcp",
                    exc,
                )
            ready, physical_table = {attr: "index_missing" for attr in attrs}, None
        for attr in attrs:
            if attr in refused_attrs:
                uncovered.append((attr, "bound_not_strict_iso"))
            elif ready.get(attr) is not None:
                uncovered.append((attr, str(ready[attr])))
    not_ready = {attr for attr, _reason in uncovered}
    ready_clauses = [c for c in clauses if c.attr not in not_ready]
    if not ready_clauses:
        return None, uncovered
    assert bp is not None and physical_table is not None
    params: dict[str, Any] = {f"{param_prefix}_bp": bp}

    def bind(value: Any) -> str:
        name = f"{param_prefix}{len(params)}"
        params[name] = value
        return f"%({name})s"

    selects: list[str] = []
    narrowed: list[str] = []
    with conn.cursor(row_factory=dict_row) as cur:
        for clause in ready_clauses:
            try:
                selects.append(dai.render_candidate_select(
                    target_kind=target_kind,
                    physical_table=physical_table,
                    attr_id=clause.attr,
                    value_type=clause.value_type,
                    op=clause.op,
                    value=clause.value,
                    bp_ref=f"%({param_prefix}_bp)s",
                    bind=bind,
                    ts_accepts=lambda bounds: dai.ts_bounds_accepted(cur, bounds),
                ))
            except dai.NotPushable:
                # A bound of another JSON type, a range on a boolean, a NaN:
                # this clause does not narrow; the exact predicate still
                # decides it.
                continue
            narrowed.append(clause.attr)
    if not selects:
        return None, uncovered
    return ExpressionCandidates(
        sql=" INTERSECT ".join(f"({s})" for s in selects),
        params=params,
        attrs=tuple(dict.fromkeys(narrowed)),
    ), uncovered
