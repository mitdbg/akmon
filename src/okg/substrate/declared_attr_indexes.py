"""Declared-attribute expression indexes on the Chronos graph tables.

okg#3112 step 2, slice 1 (PACT lane-h-3112-step2; design
``pact/changes/lane-h-3112-step2/design.md`` revision 3). One partial
expression index per declared filterable attribute on the node graph table
(step 3, decision 287d: :data:`INDEXED_KINDS`; edges get none) will replace
``okg.attr_index``. Slice 1 builds and maintains them; slice 2 (PACT
lane-h-3112-step2-reader) lets the MCP filter read them through
:func:`render_candidate_select` and :func:`reader_readiness`
(``mcp/attr_filter_source.py``); step 3 (PACT lane-h-3112-step3) makes that
the default; step 4 (PACTs lane-h-3112-step4a and -step4b) removes
``attr_index``, and migration 0043 drops its tables.

What lives here:

* **One renderer** (:func:`index_ddl`, :func:`key_expression`,
  :func:`admission_predicate`, :func:`render_probe`). The index DDL and the
  reader's candidate probe come from the same expressions, so the planner can
  match them. Number probes follow design section 2.4 (``Decimal(repr(x))``
  for equality, one-ulp widening of a float range bound, never a SQL
  float-to-numeric cast); a ``ts`` bound the strict v4 reading refuses is not
  pushable; text probes compare the 512-character prefix.
* **The reconciler** (:func:`reconcile`), design section 4.3: builds with
  ``CREATE INDEX CONCURRENTLY`` under the graph-index DDL lock it shares with
  cold-history rotation and step 1's convergence, drops its own failed and
  retired indexes first, never builds on a table with any invalid index,
  checks the publish lock, source runs and old transactions before a build,
  waits a long lock timeout, retries with backoff and stops after five
  consecutive failures, and refuses text indexes unless the database orders
  strings by code point. Migrate and catalog apply preempt it
  (:func:`index_ddl_preemption`, :func:`preempt_in_transaction`).
* **The declaration registry** (:func:`declared_filterable_attrs`,
  :func:`declared_filterable_by_type`, :func:`flatten_declarations`): what the
  latest applied catalog declares filterable, one value type per attribute
  name.
* **Doctor** (:func:`doctor_report`).

The reconciler runs in its own DBOS workflow
(``runtime/declared_attr_index_workflow.py``) on the per-deployment
index-maintenance queue, every five minutes once an operator has run
``okg db attr-index reconcile --apply``; the publisher and catalog apply
only ask for a run.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Iterator, Mapping, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row, tuple_row

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import SubstrateError

_log = logging.getLogger(__name__)

__all__ = [
    "INDEXED_KINDS",
    "DROP_ALL_STATISTICS_SQL",
    "INDEX_NAME_PREFIX",
    "KEY_VERSION",
    "STATISTICS_NAME_PREFIX",
    "NotPushable",
    "Probe",
    "admission_predicate",
    "collation_orders_by_code_point",
    "database_collation",
    "declared_filterable_attrs",
    "declared_filterable_by_type",
    "definition_digest",
    "doctor_report",
    "flatten_declarations",
    "index_ddl",
    "index_ddl_preemption",
    "index_name",
    "key_expression",
    "maintenance_wanted",
    "named_probe_sql",
    "preempt_in_transaction",
    "reader_readiness",
    "reconcile",
    "record_maintenance_request",
    "render_candidate_select",
    "render_probe",
    "statistics_ddl",
    "statistics_expression",
    "statistics_expression_matches",
    "statistics_name",
    "ts_bounds_accepted",
]

#: Reserved index-name prefix. Step 1's post-publish rebuild never judges an
#: index with it, and the reconciler reports one its manifest does not list.
INDEX_NAME_PREFIX = "okg_dattr_"
#: Version of the key functions and expressions; part of the index name.
KEY_VERSION = 1
#: Characters of a text value the index key keeps (at most 2,048 bytes).
TEXT_KEY_CHARS = 512

#: Reserved name prefix of the per-attribute extended statistics objects
#: (PACT lane-s-redesign-s1; decision ``stats`` A of lane-s-storage-redesign).
#: The reconciler owns every object with it on the graph tables and works out
#: what exists from ``pg_statistic_ext``; no manifest row records them.
STATISTICS_NAME_PREFIX = "okg_dstat_"
#: Version of :func:`statistics_expression`; part of the object name.
STATISTICS_KEY_VERSION = 1
#: The kinds that get statistics objects. Decision edgestats A (PACT
#: lane-s-redesign-s1): node attributes only.
STATISTICS_KINDS = ("node",)

MANIFEST_TABLE = "okg.declared_attr_indexes"
STATE_TABLE = "okg.declared_attr_index_state"

#: The advisory lock every graph-table index change takes (cold-history
#: rotation, step 1's convergence and rebuilds, this reconciler).
INDEX_DDL_LOCK = "okg:cold-history-index-rotation"
#: Held by a migrate or catalog apply that wants the reconciler out of the
#: way. The reconciler checks it after taking the DDL lock and before every
#: index statement, and stops when someone holds it.
PREEMPT_LOCK = "okg:declared-attr-index-preempt"

MAX_CONSECUTIVE_FAILURES = 5
#: Runs to wait after the 1st, 2nd, 3rd and 4th consecutive failure.
BACKOFF_RUNS = (1, 2, 4, 8)
#: A failed build is retried after this long at most, whatever the run count.
BACKOFF_CAP_SECONDS = 24 * 3600
#: A build waits while any transaction in the database is older than this.
OLD_TRANSACTION_SECONDS = 60.0
DROP_LOCK_TIMEOUT_SECONDS = 30.0
BUILD_LOCK_TIMEOUT_ENV = "OKG_DECLARED_ATTR_INDEX_LOCK_TIMEOUT_SECONDS"
DEFAULT_BUILD_LOCK_TIMEOUT_SECONDS = 600.0
#: How long migrate and catalog apply wait for the DDL lock after cancelling
#: the reconciler.
PREEMPT_WAIT_SECONDS = 60.0
#: How long they wait when no holder of the DDL lock is the reconciler (a
#: cold-history rotation, step 1's convergence, another migrate). Nothing
#: they do frees such a holder, so they go on without the lock after this
#: short grace instead of the full wait. The grace covers a reconciler that
#: has taken the lock but not yet recorded its pid; it then sees the
#: preemption intent lock and stops on its own.
PREEMPT_OTHER_HOLDER_GRACE_SECONDS = 1.0
DRIFT_SLICES_ENV = "OKG_DECLARED_ATTR_DRIFT_SLICES"
DEFAULT_DRIFT_SLICES = 32
#: ``off`` turns the scheduled maintenance workflow into a no-op.
MAINTENANCE_ENV = "OKG_DECLARED_ATTR_INDEXES"

TARGET_KINDS = ("node", "edge")
#: The kinds that get declared expression indexes. Decision 287d (okg#3112
#: step 3, PACT lane-h-3112-step3): nodes only. No per-attribute index is
#: built on the edge table; edge filter reads are answered by the exact
#: predicate alone (step 4a) and the reconciler retires any edge index an
#: earlier run built (``last_reason`` ``kind_not_indexed``). Drift checks still
#: compare the view with the graph for both kinds.
INDEXED_KINDS = ("node",)
_LOGICAL_TABLE = {"node": "okg.graph_nodes", "edge": "okg.graph_edges"}
_ID_COLUMN = {"node": "node_id", "edge": "edge_id"}
_JSON_TYPE = {"text": "string", "num": "number", "bool": "boolean", "ts": "string"}
VALUE_TYPES = tuple(_JSON_TYPE)
_LIVE_STATES = ("pending", "building", "ready", "failed")
_OWN_DROP_STATES = ("failed", "drop_failed", "retiring")

#: The strict ISO-8601 shape ``okg.attr_ts_v4`` accepts (before its validity
#: check). A bound that fails it is refused without a database round trip.
TS_V4_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([Tt ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?"
    r"([Zz]|[+-][0-9]{2}(:?[0-9]{2})?)?)?$"
)

#: Collations under which ``left(v, 512)`` is monotone (code-point order).
_LIBC_CODE_POINT = frozenset({"C", "POSIX", "C.UTF-8", "C.utf8"})
_BUILTIN_CODE_POINT = frozenset({"C", "C.UTF-8"})


# --------------------------------------------------------------------------- #
# Renderer                                                                     #
# --------------------------------------------------------------------------- #


def _literal(text: str) -> str:
    """A SQL string literal (standard_conforming_strings is on everywhere)."""
    if "\x00" in text:
        raise SubstrateError(
            "a declared attribute name contains a NUL character",
            data={"attr_id": text},
        )
    return "'" + text.replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check_value_type(value_type: str) -> None:
    if value_type not in _JSON_TYPE:
        raise SubstrateError(
            "unknown declared value type",
            data={"value_type": value_type, "known": sorted(_JSON_TYPE)},
        )


def index_name(target_kind: str, attr_id: str, value_type: str) -> str:
    """``okg_dattr_<n|e>_<24 hex of sha256(attr | type | key version)>``."""
    if target_kind not in TARGET_KINDS:
        raise ValueError(f"target_kind must be one of {TARGET_KINDS}")
    _check_value_type(value_type)
    digest = hashlib.sha256(
        f"{attr_id}|{value_type}|{KEY_VERSION}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{INDEX_NAME_PREFIX}{target_kind[0]}_{digest}"


def key_expression(attr_id: str, value_type: str, *, alias: str = "") -> str:
    """The index key of one declared attribute (design section 2.1)."""
    _check_value_type(value_type)
    a = f"{alias}." if alias else ""
    k = _literal(attr_id)
    if value_type == "text":
        return f"left({a}attrs ->> {k}, {TEXT_KEY_CHARS})"
    if value_type == "num":
        return (
            f"(CASE WHEN jsonb_typeof({a}attrs -> {k}) = 'number' "
            f"THEN okg.attr_num_key(({a}attrs -> {k})::numeric) END)"
        )
    if value_type == "bool":
        return (
            f"(CASE WHEN jsonb_typeof({a}attrs -> {k}) = 'boolean' "
            f"THEN ({a}attrs -> {k})::boolean END)"
        )
    return f"okg.attr_ts_v4({a}attrs ->> {k})"


def admission_predicate(attr_id: str, value_type: str, *, alias: str = "") -> str:
    """The partial-index predicate: a live row whose value has the declared
    JSON type."""
    _check_value_type(value_type)
    a = f"{alias}." if alias else ""
    return (
        f"{a}deleted = false AND jsonb_typeof({a}attrs -> {_literal(attr_id)}) "
        f"= '{_JSON_TYPE[value_type]}'"
    )


def index_ddl(
    *,
    physical_table: str,
    target_kind: str,
    attr_id: str,
    value_type: str,
    concurrently: bool = True,
    name: str | None = None,
    schema: str = "public",
) -> str:
    """``CREATE INDEX [CONCURRENTLY] <name> ON <table> (<key>, live_lo,
    live_hi) WHERE <admission>``."""
    name = name or index_name(target_kind, attr_id, value_type)
    return (
        f"CREATE INDEX {'CONCURRENTLY ' if concurrently else ''}{_quote_ident(name)} "
        f"ON {_quote_ident(schema)}.{_quote_ident(physical_table)} "
        f"({key_expression(attr_id, value_type)}, live_lo, live_hi) "
        f"WHERE {admission_predicate(attr_id, value_type)}"
    )


def definition_digest(
    target_kind: str,
    attr_id: str,
    value_type: str,
    *,
    collation: Mapping[str, Any] | None,
) -> str:
    """Digest of what an index answers for: its key, predicate, key version
    and, for text, the database collation (provider, collate, locale)."""
    payload = {
        "target_kind": target_kind,
        "attr_id": attr_id,
        "value_type": value_type,
        "key": key_expression(attr_id, value_type),
        "predicate": admission_predicate(attr_id, value_type),
        "columns": ["live_lo", "live_hi"],
        "key_version": KEY_VERSION,
        "collation": (
            [collation.get("provider"), collation.get("collate"), collation.get("locale")]
            if value_type == "text" and collation is not None
            else None
        ),
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


class NotPushable(Exception):
    """A filter the candidate probe cannot cover; the exact predicate answers
    alone. ``reason`` is the ``not_covered`` reason a reader reports."""

    def __init__(self, reason: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = dict(detail or {})


@dataclass(frozen=True)
class Probe:
    """A boolean SQL fragment over the graph row (``%s`` placeholders) that is
    true for every row the exact filter accepts, and its parameters."""

    sql: str
    params: tuple[Any, ...]


_OPS = frozenset({"eq", "in", "lt", "lte", "gt", "gte", "between"})


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _num_probe_value(value: Any) -> Decimal:
    """The exact decimal an equality bound compares as: ``Decimal(repr(x))``
    for a float (what ``Jsonb(x)`` encodes), ``Decimal(x)`` for an int."""
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NotPushable("bound_not_finite", {"value": repr(value)})
        return Decimal(repr(value))
    return Decimal(value)


def _num_range_value(value: Any, *, upper: bool) -> Decimal:
    """A range bound widened one ulp outward when it is a float.

    The exact range filter compares a float bound in double precision, so a
    stored value just past ``x`` can round onto ``x``. Rounding to the nearest
    double is monotone, so every such value lies before ``nextafter(x)``; an
    int bound compares exactly and is not widened. Never cast a float to
    numeric in SQL: PostgreSQL keeps 15 significant digits.
    """
    if isinstance(value, float):
        if math.isnan(value):
            raise NotPushable("bound_not_finite", {"value": repr(value)})
        widened = math.nextafter(value, math.inf if upper else -math.inf)
        if math.isinf(widened):
            return Decimal("Infinity") if widened > 0 else Decimal("-Infinity")
        return Decimal(repr(widened))
    return Decimal(value)


def _bounds(op: str, value: Any) -> list[Any]:
    if op in ("in", "between"):
        if not isinstance(value, (list, tuple)) or not value:
            raise NotPushable("malformed_bound", {"op": op})
        if op == "between" and len(value) != 2:
            raise NotPushable("malformed_bound", {"op": op})
        return list(value)
    return [value]


def ts_bounds_accepted(cur: Any, values: Sequence[str]) -> list[bool]:
    """Whether ``okg.attr_ts_v4`` accepts each bound, asked of the database
    (the regex alone does not decide: ``2024-13-45`` matches it)."""
    if not values:
        return []
    cur.execute(
        "SELECT okg.attr_ts_v4(v) IS NOT NULL AS ok "
        "FROM unnest(%s::text[]) WITH ORDINALITY AS u(v, n) ORDER BY n",
        (list(values),),
    )
    rows = cur.fetchall()
    return [bool(row["ok"] if isinstance(row, Mapping) else row[0]) for row in rows]


def render_probe(
    attr_id: str,
    value_type: str,
    op: str,
    value: Any,
    *,
    alias: str = "",
    ts_accepts: Callable[[Sequence[str]], Sequence[bool]] | None = None,
) -> Probe:
    """The candidate probe for one filter on one declared attribute.

    The fragment repeats the index's admission predicate and compares its key
    with non-strict comparisons, so the planner can use the index and the
    exact filter decides every row. Raises :class:`NotPushable` when no probe
    can cover the exact filter (a bound of another JSON type, a ``ts`` bound
    ``okg.attr_ts_v4`` refuses, a range on a boolean). ``ts_accepts`` answers
    whether v4 accepts each ``ts`` bound (:func:`ts_bounds_accepted`).
    """
    _check_value_type(value_type)
    if op not in _OPS:
        raise NotPushable("op_not_supported", {"op": op})
    bounds = _bounds(op, value)
    key = key_expression(attr_id, value_type, alias=alias)
    predicate = admission_predicate(attr_id, value_type, alias=alias)
    params: list[Any] = []
    equality = op in ("eq", "in")

    if value_type == "num":
        if not all(_is_number(b) for b in bounds):
            raise NotPushable("bound_type_mismatch", {"value_type": value_type})
        if equality:
            for b in bounds:
                params.append(_num_probe_value(b))
            slots = ", ".join("okg.attr_num_key(%s::numeric)" for _ in bounds)
            return Probe(f"({predicate} AND {key} IN ({slots}))", tuple(params))
        return _range_probe(
            predicate, key, op, bounds,
            convert=lambda b, upper: _num_range_value(b, upper=upper),
            slot="okg.attr_num_key(%s::numeric)",
        )

    if value_type == "text":
        if not all(isinstance(b, str) for b in bounds):
            raise NotPushable("bound_type_mismatch", {"value_type": value_type})
        slot = f"left(%s::text, {TEXT_KEY_CHARS})"
        if equality:
            params.extend(bounds)
            slots = ", ".join(slot for _ in bounds)
            return Probe(f"({predicate} AND {key} IN ({slots}))", tuple(params))
        return _range_probe(predicate, key, op, bounds, convert=lambda b, upper: b, slot=slot)

    if value_type == "bool":
        if not equality:
            raise NotPushable("op_not_supported", {"op": op, "value_type": value_type})
        if not all(isinstance(b, bool) for b in bounds):
            raise NotPushable("bound_type_mismatch", {"value_type": value_type})
        params.extend(bounds)
        slots = ", ".join("%s::boolean" for _ in bounds)
        return Probe(f"({predicate} AND {key} IN ({slots}))", tuple(params))

    # ts
    if not all(isinstance(b, str) for b in bounds):
        raise NotPushable("bound_type_mismatch", {"value_type": value_type})
    shaped = [bool(TS_V4_RE.match(b)) for b in bounds]
    if not all(shaped):
        raise NotPushable(
            "bound_not_strict_iso",
            {"bounds": [b for b, ok in zip(bounds, shaped) if not ok]},
        )
    if ts_accepts is None:
        raise SubstrateError(
            "a ts probe needs the database to check its bounds",
            data={"attr_id": attr_id},
            fix_hint="pass ts_accepts (ts_bounds_accepted on a cursor)",
        )
    accepted = list(ts_accepts(bounds))
    if not all(accepted):
        raise NotPushable(
            "bound_not_strict_iso",
            {"bounds": [b for b, ok in zip(bounds, accepted) if not ok]},
        )
    slot = "okg.attr_ts_v4(%s::text)"
    if equality:
        params.extend(bounds)
        slots = ", ".join(slot for _ in bounds)
        return Probe(f"({predicate} AND {key} IN ({slots}))", tuple(params))
    return _range_probe(predicate, key, op, bounds, convert=lambda b, upper: b, slot=slot)


def _range_probe(
    predicate: str,
    key: str,
    op: str,
    bounds: list[Any],
    *,
    convert: Callable[[Any, bool], Any],
    slot: str,
) -> Probe:
    if op == "between":
        parts = [(False, bounds[0]), (True, bounds[1])]
    else:
        parts = [(op in ("lt", "lte"), bounds[0])]
    conds: list[str] = []
    params: list[Any] = []
    for upper, bound in parts:
        params.append(convert(bound, upper))
        conds.append(f"{key} {'<=' if upper else '>='} {slot}")
    return Probe(f"({predicate} AND {' AND '.join(conds)})", tuple(params))


_END = object()


def named_probe_sql(attr_id: str, probe: Probe, bind: Callable[[Any], str]) -> str:
    """``probe.sql`` with each ``%s`` slot replaced by ``bind(param)`` (a named
    placeholder such as ``%(dai0)s``), for a statement with named parameters.

    The attribute name is a literal inside the key expression (the planner
    matches the index by it), and a literal ``%`` must read ``%%`` in a
    statement with parameters. Slots come only from the renderer's templates,
    never from inside the attribute literal, so the text is split on that
    literal first.
    """
    literal = _literal(attr_id)
    params = iter(probe.params)
    parts: list[str] = []
    for segment in probe.sql.split(literal):
        pieces = segment.split("%s")
        out = pieces[0]
        for piece in pieces[1:]:
            out += bind(next(params)) + piece
        parts.append(out)
    if next(params, _END) is not _END:
        raise SubstrateError(
            "probe has more parameters than slots",
            data={"attr_id": attr_id, "sql": probe.sql},
        )
    return literal.replace("%", "%%").join(parts)


def render_candidate_select(
    *,
    target_kind: str,
    physical_table: str,
    attr_id: str,
    value_type: str,
    op: str,
    value: Any,
    bp_ref: str,
    bind: Callable[[Any], str],
    ts_accepts: Callable[[Sequence[str]], Sequence[bool]] | None = None,
) -> str:
    """The target ids one filter's candidate probe admits on one graph table,
    among the rows visible at the branch point ``bp_ref`` (a placeholder).

    ``target_id`` is text, as the effective-attribute relation keys it. Raises
    :class:`NotPushable` as :func:`render_probe` does.
    """
    if target_kind not in TARGET_KINDS:
        raise ValueError(f"target_kind must be one of {TARGET_KINDS}")
    probe = render_probe(attr_id, value_type, op, value, alias="g", ts_accepts=ts_accepts)
    target = "g.edge_id::text" if target_kind == "edge" else "g.node_id"
    return (
        f"SELECT {target} AS target_id "
        f"FROM public.{_quote_ident(physical_table).replace('%', '%%')} g "
        f"WHERE {named_probe_sql(attr_id, probe, bind)} "
        f"AND g.live_lo <= {bp_ref} AND {bp_ref} < g.live_hi"
    )


#: The ``not_covered`` reasons a reader reports for an (attribute, kind)
#: whose declared index it does not use (design section 3, readiness gate).
READINESS_REASONS = (
    "kind_not_indexed",
    "index_missing",
    "index_building",
    "index_failed",
    "declaration_changed",
)


def reader_readiness(
    cur: Any, *, target_kind: str, declared: Mapping[str, str]
) -> tuple[dict[str, str | None], str | None]:
    """Per declared attribute, None when a reader may use its index as a
    candidate set, else the ``not_covered`` reason; and the graph table.

    Ready means: the manifest row is ``ready`` for the declared value type
    and key version, its definition digest equals the one the declaration
    and the current database collation give, and the
    index exists on the registered graph table, valid, ready and live, with
    the definition the reconciler recorded. ``cur`` must return dict rows.

    A kind outside :data:`INDEXED_KINDS` (edges, decision 287d) is never
    ready: every attribute reports ``kind_not_indexed``, even while an edge
    index an earlier run built still waits for the reconciler to drop it.
    """
    if target_kind not in TARGET_KINDS:
        raise ValueError(f"target_kind must be one of {TARGET_KINDS}")
    if target_kind not in INDEXED_KINDS:
        return {attr: "kind_not_indexed" for attr in declared}, None
    out: dict[str, str | None] = {attr: "index_missing" for attr in declared}
    if not declared or not installed(cur):
        return out, None
    tables = _graph_tables(cur)
    if tables is None:
        return out, None
    physical_table = tables[target_kind]
    collation = database_collation(cur)
    cur.execute(
        """
        SELECT m.attr_id, m.value_type, m.key_version, m.state, m.definition_digest,
               m.indexdef AS recorded_def, m.physical_table,
               i.indexrelid IS NOT NULL AS present,
               coalesce(i.indisvalid AND i.indisready AND i.indislive, false) AS usable,
               i.indrelid = to_regclass(%s) AS on_table,
               pg_get_indexdef(i.indexrelid) AS indexdef
          FROM okg.declared_attr_indexes m
          LEFT JOIN pg_class c
            ON c.relname = m.index_name AND c.relnamespace = 'public'::regnamespace
          LEFT JOIN pg_index i ON i.indexrelid = c.oid
         WHERE m.target_kind = %s AND m.attr_id = ANY(%s::text[])
           AND m.state IN ('pending', 'building', 'ready', 'failed')
        """,
        (f"public.{_quote_ident(physical_table)}", target_kind, sorted(declared)),
    )
    for row in cur.fetchall():
        attr = str(row["attr_id"])
        value_type = declared[attr]
        expected = definition_digest(target_kind, attr, value_type, collation=collation)
        if (row["value_type"] != value_type or int(row["key_version"]) != KEY_VERSION
                or row["definition_digest"] != expected):
            out[attr] = "declaration_changed"
        elif row["state"] == "failed":
            out[attr] = "index_failed"
        elif row["state"] == "pending":
            out[attr] = "index_missing"
        elif row["state"] == "building":
            out[attr] = "index_building"
        elif not row["present"] or not row["on_table"] or row["physical_table"] != physical_table:
            out[attr] = "index_missing"
        elif not row["usable"]:
            out[attr] = "index_building"
        elif row["recorded_def"] and row["indexdef"] != row["recorded_def"]:
            out[attr] = "declaration_changed"
        else:
            out[attr] = None
    return out, physical_table


# --------------------------------------------------------------------------- #
# Collation                                                                    #
# --------------------------------------------------------------------------- #


def database_collation(cur: Any) -> dict[str, Any]:
    """``provider`` (c libc, b builtin, i ICU), ``collate`` and ``locale`` of
    the current database. ``locale`` reads ``datlocale`` (PostgreSQL 17) or
    ``daticulocale`` (15-16)."""
    cur.execute(
        "SELECT to_jsonb(d) AS d FROM pg_database d WHERE d.datname = current_database()"
    )
    row = cur.fetchone()
    data = row["d"] if isinstance(row, Mapping) else row[0]
    provider = data.get("datlocprovider")
    return {
        "provider": provider,
        "collate": data.get("datcollate"),
        "locale": data.get("datlocale", data.get("daticulocale")),
    }


def collation_orders_by_code_point(collation: Mapping[str, Any]) -> bool:
    """True for libc C, POSIX, C.UTF-8 or C.utf8, and builtin C or C.UTF-8:
    the only collations under which a 512-character prefix key is monotone."""
    provider = collation.get("provider")
    if provider == "c":
        return collation.get("collate") in _LIBC_CODE_POINT
    if provider == "b":
        return collation.get("locale") in _BUILTIN_CODE_POINT
    return False


def statistics_name(target_kind: str, attr_id: str) -> str:
    """``okg_dstat_<n|e>_<24 hex of sha256(attr | statistics key version)>``."""
    if target_kind not in TARGET_KINDS:
        raise ValueError(f"target_kind must be one of {TARGET_KINDS}")
    digest = hashlib.sha256(
        f"{attr_id}|{STATISTICS_KEY_VERSION}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{STATISTICS_NAME_PREFIX}{target_kind[0]}_{digest}"


def statistics_expression(attr_id: str, *, alias: str = "") -> str:
    """The expression a declared attribute's statistics object describes:
    ``attrs -> '<key>'``, the key an inlined literal.

    Defined once, here. The planner uses the object only for a query
    expression equal to it, and a statistics expression cannot take a bind
    parameter, so the split aggregate of stage S2 (PACT
    lane-s-storage-redesign section 1.6) groups on exactly this text, with
    the same inlined key; ``alias`` qualifies the column the same way
    :func:`key_expression` does.
    """
    a = f"{alias}." if alias else ""
    return f"{a}attrs -> {_literal(attr_id)}"


def statistics_ddl(
    *,
    physical_table: str,
    target_kind: str,
    attr_id: str,
    name: str | None = None,
    schema: str = "public",
) -> str:
    """``CREATE STATISTICS <name> ON (attrs -> '<key>') FROM <table>``."""
    name = name or statistics_name(target_kind, attr_id)
    return (
        f"CREATE STATISTICS {_quote_ident(schema)}.{_quote_ident(name)} "
        f"ON ({statistics_expression(attr_id)}) "
        f"FROM {_quote_ident(schema)}.{_quote_ident(physical_table)}"
    )


#: The rollback of the statistics objects (PACT lane-s-redesign-s1): one
#: statement that drops every object named with the reserved prefix, in any
#: schema, listed from ``pg_statistic_ext``.
DROP_ALL_STATISTICS_SQL = r"""DO $okg_dstat_rollback$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname, s.stxname
      FROM pg_statistic_ext s
      JOIN pg_namespace n ON n.oid = s.stxnamespace
     WHERE s.stxname LIKE 'okg\_dstat\_%'
  LOOP
    EXECUTE format('DROP STATISTICS %I.%I', r.nspname, r.stxname);
  END LOOP;
END
$okg_dstat_rollback$"""


# --------------------------------------------------------------------------- #
# Catalog reads                                                                #
# --------------------------------------------------------------------------- #


def _regclass(cur: Any, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (name,))
    row = cur.fetchone()
    return bool(row["present"] if isinstance(row, Mapping) else row[0])


def installed(cur: Any) -> bool:
    """True when migration 0037's manifest and state tables exist."""
    return _regclass(cur, MANIFEST_TABLE) and _regclass(cur, STATE_TABLE)


def _graph_tables(cur: Any) -> dict[str, str] | None:
    """``{kind: physical table}`` or None before the graph tables exist."""
    from okg.substrate.db import cold_history

    tables = cold_history.registered_graph_tables(cur)
    if tables is None:
        return None
    return {kind: tables[_LOGICAL_TABLE[kind]] for kind in TARGET_KINDS}


def _latest_applied_catalog(cur: Any) -> int | None:
    cur.execute(
        "SELECT max(catalog_version_id) FROM okg.catalog_versions "
        "WHERE status = 'applied'"
    )
    row = cur.fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _unrepresentable_value_type(attr_id: str, value_type: str, subtype: str) -> SubstrateError:
    return SubstrateError(
        "catalog declares a filterable value_type this build cannot represent",
        data={
            "attr_id": attr_id,
            "value_type": value_type,
            "subtype": subtype,
            "known": sorted(VALUE_TYPES),
        },
        fix_hint=(
            "recompose the catalog against this build, or upgrade the "
            "deployment to one that knows this value type"
        ),
    )


def declared_filterable_by_type(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int | None = None,
) -> dict[str, dict[str, str]]:
    """Return ``{type_name: {attr_key: value_type}}`` for a catalog version.

    Declarations live per subtype in ``okg.subtypes.metadata->'filterable_attrs'``
    (compiled from LinkML annotations by ``okg.substrate.catalog.modules_compose``).
    Defaults to the latest applied version. A value type outside
    :data:`VALUE_TYPES` raises: no index could serve it.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        if catalog_version_id is None:
            catalog_version_id = _latest_applied_catalog(cur)
        if catalog_version_id is None:
            return {}
        cur.execute(
            "SELECT name, metadata -> 'filterable_attrs' FROM okg.subtypes "
            "WHERE catalog_version_id = %s AND metadata ? 'filterable_attrs'",
            (catalog_version_id,),
        )
        rows = cur.fetchall()
    out: dict[str, dict[str, str]] = {}
    for type_name, mapping in rows:
        if not isinstance(mapping, Mapping):
            continue
        declared: dict[str, str] = {}
        for attr_key, value_type in mapping.items():
            if str(value_type) not in VALUE_TYPES:
                raise _unrepresentable_value_type(
                    str(attr_key), str(value_type), str(type_name)
                )
            declared[str(attr_key)] = str(value_type)
        if declared:
            out[str(type_name)] = declared
    return out


def flatten_declarations(
    by_type: Mapping[str, Mapping[str, str]],
) -> dict[str, str]:
    """``{attr_key: value_type}`` from one per-type read, refusing conflicts.

    One value type per attribute name, deployment-wide: an index and its
    reader's probe are typed by the name alone, so two subtypes declaring the
    same name with different types raise rather than pick a winner.
    """
    declared: dict[str, str] = {}
    origin: dict[str, str] = {}
    for type_name in sorted(by_type):
        for attr_key, value_type in by_type[type_name].items():
            prior = declared.get(attr_key)
            if prior is not None and prior != value_type:
                raise SubstrateError(
                    "conflicting filterable value_type for one attribute name",
                    data={
                        "attr_id": attr_key,
                        "declared": sorted([prior, value_type]),
                        "subtypes": sorted([origin[attr_key], type_name]),
                    },
                    fix_hint=(
                        "a declared attribute is typed by its name, so one "
                        "name carries one value_type deployment-wide. Rename "
                        "one of the attributes or align their value_type "
                        "annotations, then recompose the catalog."
                    ),
                )
            declared[attr_key] = value_type
            origin[attr_key] = type_name
    return declared


def declared_filterable_attrs(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int | None = None,
) -> dict[str, str]:
    """Return ``{attr_id: value_type}`` for the deployment's declared attributes
    (the latest applied catalog unless ``catalog_version_id`` is given).

    Reading declarations is a catalog SELECT; declaring one is a catalog
    INSERT under a new catalog version, never DDL. A conflicting or unknown
    value type raises (:func:`flatten_declarations`).
    """
    return flatten_declarations(
        declared_filterable_by_type(conn, catalog_version_id=catalog_version_id)
    )


def _declarations(conn: psycopg.Connection) -> dict[str, str]:
    """``{attr_id: value_type}`` of the latest applied catalog, one value type
    per attribute name (a conflict raises)."""
    with conn.cursor(row_factory=dict_row) as cur:
        if not _regclass(cur, "okg.catalog_versions"):
            return {}
    return declared_filterable_attrs(conn)


@dataclass(frozen=True)
class DesiredIndex:
    target_kind: str
    attr_id: str
    value_type: str
    index_name: str
    physical_table: str
    definition_digest: str


def desired_indexes(
    declared: Mapping[str, str],
    tables: Mapping[str, str],
    collation: Mapping[str, Any],
) -> tuple[list[DesiredIndex], list[dict[str, Any]]]:
    """Every (indexed kind, declared attribute) index, and the text ones the
    database collation refuses. Only :data:`INDEXED_KINDS` (nodes)."""
    text_ok = collation_orders_by_code_point(collation)
    desired: list[DesiredIndex] = []
    refused: list[dict[str, Any]] = []
    for kind in INDEXED_KINDS:
        for attr_id in sorted(declared):
            value_type = declared[attr_id]
            if value_type == "text" and not text_ok:
                refused.append({
                    "target_kind": kind,
                    "attr_id": attr_id,
                    "reason": "collation_not_code_point",
                })
                continue
            desired.append(DesiredIndex(
                target_kind=kind,
                attr_id=attr_id,
                value_type=value_type,
                index_name=index_name(kind, attr_id, value_type),
                physical_table=tables[kind],
                definition_digest=definition_digest(
                    kind, attr_id, value_type, collation=collation
                ),
            ))
    return desired, refused


def _dattr_catalog(cur: Any) -> dict[str, dict[str, Any]]:
    """Every index named with the reserved prefix, in any schema."""
    cur.execute(
        r"""
        SELECT c.relname AS name, n.nspname AS schema, t.relname AS table_name,
               i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
               pg_get_indexdef(i.indexrelid) AS indexdef,
               pg_relation_size(i.indexrelid) AS bytes
          FROM pg_index i
          JOIN pg_class c ON c.oid = i.indexrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_class t ON t.oid = i.indrelid
         WHERE c.relname LIKE 'okg\_dattr\_%%'
        """
    )
    return {str(row["name"]): dict(row) for row in cur.fetchall()}


@dataclass(frozen=True)
class DesiredStatistics:
    target_kind: str
    attr_id: str
    name: str
    physical_table: str


def desired_statistics(
    declared: Mapping[str, str], tables: Mapping[str, str]
) -> list[DesiredStatistics]:
    """One statistics object per declared attribute and statistics kind,
    whatever its value type (the expression does not depend on it, nor on
    the collation)."""
    return [
        DesiredStatistics(
            target_kind=kind,
            attr_id=attr_id,
            name=statistics_name(kind, attr_id),
            physical_table=tables[kind],
        )
        for kind in STATISTICS_KINDS
        for attr_id in sorted(declared)
    ]


def statistics_expression_matches(expressions: Any, attr_id: str) -> bool:
    """Whether a statistics object's expressions, as
    ``pg_get_statisticsobjdef_expressions`` prints them, are exactly the one
    :func:`statistics_expression` wants for ``attr_id``. PostgreSQL prints
    the key's type cast (``attrs -> 'k'::text``) and may wrap the expression
    in parentheses; both are accepted, nothing else is."""
    items = list(expressions or ())
    if len(items) != 1:
        return False
    text = str(items[0]).strip()
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    want = statistics_expression(attr_id)
    return text in {want, f"{want}::text"}


def _dstat_catalog(cur: Any) -> dict[str, dict[str, Any]]:
    """Every extended statistics object named with the reserved prefix, in
    any schema, with its table and its expressions."""
    cur.execute(
        r"""
        SELECT s.stxname AS name, n.nspname AS schema,
               t.relname AS table_name, tn.nspname AS table_schema,
               pg_get_statisticsobjdef_expressions(s.oid) AS expressions
          FROM pg_statistic_ext s
          JOIN pg_namespace n ON n.oid = s.stxnamespace
          JOIN pg_class t ON t.oid = s.stxrelid
          JOIN pg_namespace tn ON tn.oid = t.relnamespace
         WHERE s.stxname LIKE 'okg\_dstat\_%%'
        """
    )
    return {str(row["name"]): dict(row) for row in cur.fetchall()}


def _statistics_plan(
    catalog: Mapping[str, Mapping[str, Any]],
    desired: Sequence[DesiredStatistics],
    tables: Mapping[str, str],
) -> dict[str, list[str]]:
    """What a run creates, drops and redefines, and the prefixed objects it
    does not own.

    The reconciler owns a prefixed object on one of the graph tables in
    ``public``; it drops one that no declaration wants, and drops and
    recreates (``redefine``) a wanted one whose expression is not the one
    :func:`statistics_expression` gives. A prefixed object on any other table
    is foreign: reported, never touched. A wanted name taken by a foreign
    object cannot be created; it is listed under ``blocked`` (and under
    ``missing``, with every wanted name not yet created), so the run is not
    converged and doctor reports the attribute without statistics.
    """
    own_tables = {str(table) for table in tables.values()}
    wanted = {d.name: d for d in desired}
    foreign = sorted(
        name for name, row in catalog.items()
        if row["table_schema"] != "public" or row["table_name"] not in own_tables
        or (name in wanted and row["table_name"] != wanted[name].physical_table)
    )
    foreign_set = set(foreign)
    create = [d.name for d in desired if d.name not in catalog]
    blocked = [d.name for d in desired if d.name in foreign_set]
    redefine = [
        d.name for d in desired
        if d.name in catalog and d.name not in foreign_set
        and not statistics_expression_matches(catalog[d.name].get("expressions"), d.attr_id)
    ]
    drop = sorted(
        name for name in catalog if name not in wanted and name not in foreign_set
    )
    return {
        "create": create,
        "drop": drop,
        "redefine": redefine,
        "foreign": foreign,
        "blocked": blocked,
        "missing": create + blocked,
    }


def _invalid_indexes_on(cur: Any, physical_table: str) -> list[dict[str, Any]]:
    """Every invalid index on a graph table, whoever made it."""
    cur.execute(
        """
        SELECT c.relname AS name, i.indisready AS ready
          FROM pg_index i
          JOIN pg_class c ON c.oid = i.indexrelid
         WHERE i.indrelid = to_regclass(%s)
           AND NOT (i.indisvalid AND i.indisready AND i.indislive)
         ORDER BY 1
        """,
        (f"public.{_quote_ident(physical_table)}",),
    )
    return [dict(row) for row in cur.fetchall()]


def _load_state(cur: Any) -> dict[str, Any]:
    cur.execute(
        "INSERT INTO okg.declared_attr_index_state (singleton) VALUES (true) "
        "ON CONFLICT (singleton) DO NOTHING"
    )
    cur.execute("SELECT * FROM okg.declared_attr_index_state WHERE singleton")
    return dict(cur.fetchone())


def _manifest_rows(cur: Any) -> list[dict[str, Any]]:
    cur.execute("SELECT * FROM okg.declared_attr_indexes ORDER BY target_kind, attr_id, index_name")
    return [dict(row) for row in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Locks and preemption                                                         #
# --------------------------------------------------------------------------- #


def _try_lock(cur: Any, key: str, *, xact: bool = False) -> bool:
    fn = "pg_try_advisory_xact_lock" if xact else "pg_try_advisory_lock"
    cur.execute(f"SELECT {fn}(hashtext(%s)) AS got", (key,))
    row = cur.fetchone()
    return bool(row["got"] if isinstance(row, Mapping) else row[0])


def _unlock(cur: Any, key: str) -> None:
    conn = cur.connection
    if conn.closed or conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
        return
    try:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))
    except psycopg.Error as exc:  # noqa: BLE001 -- the session close releases it
        _log.warning("could not release advisory lock %s: %s", key, exc)


def _lock_held_elsewhere(cur: Any, key: str) -> bool:
    """True when another session holds ``key`` (taken and released at once)."""
    if _try_lock(cur, key):
        _unlock(cur, key)
        return False
    return True


def _lock_holders(cur: Any, key: str) -> list[int]:
    """Pids that hold the single-bigint advisory lock ``hashtext(key)``."""
    cur.execute(
        """
        SELECT l.pid
          FROM pg_locks l
         WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND l.classid::bigint = ((hashtext(%s)::bigint >> 32) & 4294967295)
           AND l.objid::bigint = (hashtext(%s)::bigint & 4294967295)
         ORDER BY 1
        """,
        (key, key),
    )
    return [int(row["pid"] if isinstance(row, Mapping) else row[0]) for row in cur.fetchall()]


def preempt_requested(cur: Any) -> bool:
    """True when a migrate or catalog apply asks the reconciler to stop."""
    return _lock_held_elsewhere(cur, PREEMPT_LOCK)


def _reconciler_identity(cur: Any) -> tuple[int, Any] | None:
    """``(pid, backend_start)`` the running reconciler recorded, or None."""
    cur.execute(
        "SELECT reconciler_pid, reconciler_backend_start "
        "FROM okg.declared_attr_index_state WHERE singleton"
    )
    row = cur.fetchone()
    if row is None:
        return None
    pid = row["reconciler_pid"] if isinstance(row, Mapping) else row[0]
    started = row["reconciler_backend_start"] if isinstance(row, Mapping) else row[1]
    return None if pid is None else (int(pid), started)


#: The statements of the reconciler worth cancelling: the only ones that can
#: wait long. Its bookkeeping statements are short and it checks the
#: preemption lock before every index statement, so they are never cancelled.
_INDEX_DDL_RE = re.compile(r"^\s*(CREATE|DROP)\s+INDEX\b", re.IGNORECASE)


def _backend_activity(cur: Any, pid: int) -> dict[str, Any] | None:
    # Inside a transaction (catalog apply's form) pg_stat_activity would
    # repeat its first reading; clear that snapshot so each look is fresh.
    cur.execute("SELECT pg_stat_clear_snapshot()")
    cur.execute(
        "SELECT backend_start, state, query_start, query FROM pg_stat_activity WHERE pid = %s",
        (pid,),
    )
    row = cur.fetchone()
    return dict(row) if row is not None else None


def _preempt_loop(
    cur: Any,
    *,
    actor: str,
    take: str | None,
    wait_seconds: float,
    cancelled_statements: set[tuple[int, str]] | None = None,
) -> dict[str, Any]:
    """Cancel a reconciler's index statement and wait for the DDL lock.

    ``take`` is ``session`` or ``xact`` to keep the lock, None to only wait
    until it is free. Only the reconciler is cancelled: its pid and backend
    start are recorded in the state table, it must hold the lock, and the
    backend with that pid must have started at the recorded time (a reused
    pid is never cancelled; it is listed under ``unverified_pids`` and
    waited for). Only a running ``CREATE INDEX`` or ``DROP INDEX`` is
    cancelled, once per statement (``cancelled_statements`` carries that
    across calls); the reconciler sees the preemption lock before its next
    index statement. When no holder is the recorded reconciler (a
    cold-history rotation, step 1's convergence, another migrate), it waits
    only :data:`PREEMPT_OTHER_HOLDER_GRACE_SECONDS` and returns
    ``not_acquired`` with reason ``held_by_another_index_change``: nothing it
    does frees that holder, and the caller goes on without the lock either
    way, so the full wait would only stall it. A wait that runs out has
    reason ``wait_expired``.
    """
    started = time.monotonic()
    deadline = started + max(0.0, wait_seconds)
    other_deadline = started + min(max(0.0, wait_seconds), PREEMPT_OTHER_HOLDER_GRACE_SECONDS)
    done = cancelled_statements if cancelled_statements is not None else set()
    cancelled: list[int] = sorted({pid for pid, _ in done})
    refused: list[int] = []
    unverified: list[int] = []
    holders: list[int] = []

    def result(status: str, **extra: Any) -> dict[str, Any]:
        return {
            "status": status,
            "actor": actor,
            "cancelled_pids": cancelled,
            "cancel_refused_pids": refused,
            "unverified_pids": unverified,
            **extra,
            "waited_seconds": round(time.monotonic() - started, 3),
        }

    while True:
        if take is None:
            got = not _lock_held_elsewhere(cur, INDEX_DDL_LOCK)
        else:
            got = _try_lock(cur, INDEX_DDL_LOCK, xact=(take == "xact"))
        if got:
            return result("acquired" if take else "free")
        holders = _lock_holders(cur, INDEX_DDL_LOCK)
        identity = _reconciler_identity(cur)
        if identity is not None and identity[0] in holders \
                and identity[0] not in refused and identity[0] not in unverified:
            pid, recorded_start = identity
            activity = _backend_activity(cur, pid)
            if activity is not None and (
                recorded_start is None or activity["backend_start"] is None
                or activity["backend_start"] != recorded_start
            ):
                unverified.append(pid)
                _log.warning(
                    "%s found pid %s holding the graph-index DDL lock, but its backend "
                    "start %s is not the reconciler's recorded %s; waiting for it "
                    "instead of cancelling it", actor, pid,
                    activity["backend_start"], recorded_start,
                )
            elif activity is not None and activity["state"] == "active" \
                    and _INDEX_DDL_RE.match(activity["query"] or ""):
                statement = (pid, str(activity["query_start"]))
                if statement not in done:
                    try:
                        with cur.connection.transaction():
                            cur.execute("SELECT pg_cancel_backend(%s) AS ok", (pid,))
                    except psycopg.errors.InsufficientPrivilege as exc:
                        # Another role's worker: wait for it instead, and say so.
                        refused.append(pid)
                        _log.warning(
                            "%s may not cancel the declared-attribute index reconciler "
                            "(pid %s): %s; waiting for the lock instead", actor, pid, exc,
                        )
                        continue
                    done.add(statement)
                    if pid not in cancelled:
                        cancelled.append(pid)
                    _log.warning(
                        "%s preempted the declared-attribute index reconciler (pid %s)",
                        actor, pid,
                    )
        reconciler_holds = identity is not None and identity[0] in holders
        if holders and not reconciler_holds and time.monotonic() >= other_deadline:
            _log.warning(
                "%s found the graph-index DDL lock held by another index change "
                "(holders=%s, none of them the reconciler); continuing without it",
                actor, holders,
            )
            return result(
                "not_acquired", holders=holders, reason="held_by_another_index_change",
            )
        if time.monotonic() >= deadline:
            _log.warning(
                "%s could not take the graph-index DDL lock within %.0f s; "
                "holders=%s; continuing without it",
                actor, wait_seconds, holders,
            )
            return result("not_acquired", holders=holders, reason="wait_expired")
        time.sleep(0.2)


class IndexDdlPreemption:
    """A migrate's hold on the reconciler (see :func:`index_ddl_preemption`)."""

    def __init__(self, conn: psycopg.Connection, cur: Any, actor: str, wait_seconds: float):
        self.conn = conn
        self.cur = cur
        self.actor = actor
        self.wait_seconds = wait_seconds
        self.report: dict[str, Any] = {"status": "not_installed", "actor": actor}
        self.holding_ddl_key = False
        self.active = False
        # One cancel per reconciler statement over the whole hold.
        self.cancelled_statements: set[tuple[int, str]] = set()

    def start(self) -> None:
        if not installed(self.cur):
            return
        self.active = True
        # The intent lock keeps a reconciler from starting a new statement;
        # a second preemptor may hold it already, which serves the same end.
        _try_lock(self.cur, PREEMPT_LOCK)
        self.report = _preempt_loop(
            self.cur, actor=self.actor, take=None, wait_seconds=self.wait_seconds,
            cancelled_statements=self.cancelled_statements,
        )

    def hold_ddl_key(self) -> dict[str, Any]:
        """Take the DDL lock for the rest of the context, cancelling a
        reconciler that took it meanwhile."""
        if not self.active:
            return self.report
        earlier = self.report
        self.report = _preempt_loop(
            self.cur, actor=self.actor, take="session", wait_seconds=self.wait_seconds,
            cancelled_statements=self.cancelled_statements,
        )
        # One account of the whole hold: a reconciler cancelled on entry
        # stays listed.
        for key in ("cancelled_pids", "cancel_refused_pids", "unverified_pids"):
            self.report[key] = sorted(
                set(earlier.get(key) or []) | set(self.report.get(key) or [])
            )
        self.report["waited_seconds"] = round(
            float(earlier.get("waited_seconds") or 0.0)
            + float(self.report.get("waited_seconds") or 0.0), 3
        )
        self.holding_ddl_key = self.report["status"] == "acquired"
        return self.report


@contextmanager
def index_ddl_preemption(
    dsn: str,
    *,
    actor: str = "migrate",
    wait_seconds: float = PREEMPT_WAIT_SECONDS,
) -> Iterator[IndexDdlPreemption]:
    """Keep the reconciler out of the graph tables for a migrate (rule 1).

    On entry it holds the preemption intent lock on its own connection and
    cancels a reconciler that holds the graph-index DDL lock (a waiting
    ``CREATE INDEX CONCURRENTLY`` would otherwise make the migrate's DDL
    queue behind it for up to ten minutes), then waits up to
    ``wait_seconds`` for the DDL lock to be free. ``hold_ddl_key()`` then
    keeps the DDL lock until the context ends. The cancelled build becomes
    ``failed`` with reason ``preempted``, is dropped first on the next run
    and is not counted. Without migration 0037 it does nothing.
    """
    conn = db_connect(dsn, autocommit=True)
    try:
        cur = conn.cursor(row_factory=dict_row)
        holder = IndexDdlPreemption(conn, cur, actor, wait_seconds)
        holder.start()
        yield holder
    finally:
        try:
            if not conn.closed:
                conn.execute("SELECT pg_advisory_unlock_all()")
        finally:
            conn.close()


def preempt_in_transaction(
    conn: psycopg.Connection,
    *,
    actor: str,
    wait_seconds: float = PREEMPT_WAIT_SECONDS,
) -> dict[str, Any]:
    """Catalog apply's form of rule 1, inside the caller's transaction.

    Takes the intent and DDL locks at transaction level, so both release when
    the transaction ends, cancelling a reconciler that holds the DDL lock.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        if not installed(cur):
            return {"status": "not_installed", "actor": actor}
        _try_lock(cur, PREEMPT_LOCK, xact=True)
        return _preempt_loop(cur, actor=actor, take="xact", wait_seconds=wait_seconds)


def record_maintenance_request(conn: psycopg.Connection, *, reason: str) -> dict[str, Any]:
    """Ask for a maintenance run (catalog apply). The next publisher cycle
    enqueues the workflow, or the five-minute schedule runs it.

    ``status`` is ``recorded``, ``not_installed`` (no migration 0037) or
    ``not_permitted`` (this role cannot write the state table; logged as a
    warning, and the schedule still runs).
    """
    with conn.cursor(row_factory=dict_row) as cur:
        if not installed(cur):
            return {"status": "not_installed"}
        try:
            with conn.transaction():
                cur.execute(
                    "INSERT INTO okg.declared_attr_index_state (singleton, requested_at, request_reason) "
                    "VALUES (true, now(), %s) ON CONFLICT (singleton) DO UPDATE "
                    "SET requested_at = now(), request_reason = excluded.request_reason, "
                    "updated_at = now()",
                    (reason,),
                )
        except psycopg.errors.InsufficientPrivilege as exc:
            _log.warning(
                "could not record a declared-attribute index request (%s): %s; "
                "the five-minute schedule still runs the maintenance", reason, exc,
            )
            return {"status": "not_permitted", "error": _error_text(exc)}
    return {"status": "recorded", "reason": reason}


# --------------------------------------------------------------------------- #
# Reconciler                                                                   #
# --------------------------------------------------------------------------- #


def _env_seconds(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{name} must be a number of seconds",
            data={"reason": "invalid_setting", "variable": name, "value": raw},
        ) from exc
    if value <= 0:
        raise SubstrateError(
            f"{name} must be positive",
            data={"reason": "invalid_setting", "variable": name, "value": raw},
        )
    return value


def maintenance_disabled() -> bool:
    raw = os.environ.get(MAINTENANCE_ENV)
    return raw is not None and raw.strip().lower() in {"0", "off", "false", "no"}


class _Preempted(Exception):
    pass


class _StopBuilds(Exception):
    def __init__(self, reason: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = dict(detail or {})


def _old_transactions(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT pid, state,
               round(extract(epoch FROM clock_timestamp() - xact_start)::numeric, 1)::float8
                 AS xact_seconds
          FROM pg_stat_activity
         WHERE datname = current_database()
           AND pid <> pg_backend_pid()
           AND backend_type = 'client backend'
           AND xact_start IS NOT NULL
           AND xact_start < clock_timestamp() - make_interval(secs => %s)
         ORDER BY xact_start
         LIMIT 10
        """,
        (OLD_TRANSACTION_SECONDS,),
    )
    return [dict(row) for row in cur.fetchall()]


def _check_build_preconditions(conn: psycopg.Connection, cur: Any, deployment: str) -> None:
    """Rule 5: a filter, not a guarantee. Raises ``_StopBuilds``."""
    from okg.substrate.db.index_reclaim import _source_runs, _try_publisher_lock

    if preempt_requested(cur):
        raise _Preempted()
    if not _try_publisher_lock(cur, deployment):
        raise _StopBuilds("publish_running")
    running = _source_runs(conn, deployment)
    if running:
        raise _StopBuilds("source_run_in_progress", {"in_progress": [
            {k: str(run.get(k)) for k in ("run_id", "source")} for run in running[:10]
        ]})
    old = _old_transactions(cur)
    if old:
        raise _StopBuilds("old_transaction_open", {"old_transactions": old})


def _set(cur: Any, name: str, **fields: Any) -> None:
    assignments = ", ".join(f"{column} = %({column})s" for column in fields)
    cur.execute(
        f"UPDATE okg.declared_attr_indexes SET {assignments}, updated_at = now() "
        "WHERE index_name = %(index_name)s",
        {**fields, "index_name": name},
    )


def _drop_index(cur: Any, schema: str, name: str) -> None:
    cur.execute(
        sql.SQL("SET lock_timeout = {}").format(
            sql.Literal(f"{int(DROP_LOCK_TIMEOUT_SECONDS * 1000)}ms")
        )
    )
    try:
        cur.execute(
            sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(schema, name))
        )
    finally:
        cur.execute("RESET lock_timeout")


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc).strip().splitlines()[0] if str(exc).strip() else ''}"


def _failure_reason(exc: psycopg.Error) -> str:
    return {
        "55P03": "lock_timeout",
        "57014": "cancelled",
        "54000": "row_too_large",
    }.get(getattr(exc, "sqlstate", None) or "", "build_error")


def reconcile(
    dsn: str,
    *,
    deployment: str | None,
    apply: bool = False,
    retry_failed: bool = False,
    enable: bool = False,
    require_enabled: bool = False,
    lock_timeout_seconds: float | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Converge the declared-attribute indexes (design section 4.3).

    Dry run unless ``apply``: the plan (builds, drops, capped failures,
    foreign ``okg_dattr_`` indexes, the collation) without taking a lock.
    With ``apply`` one run, in order: drop its own failed, drop_failed and
    retiring indexes; build the pending ones one at a time under rules 3-6.
    ``enable`` records the operator's opt-in (the CLI's ``--apply``); the
    scheduled workflow passes ``require_enabled`` and does nothing before it.

    ``status`` is ``dry_run``, ``converged``, ``partial`` (something is left
    pending or failed), ``preempted``, ``refused`` (another actor holds the
    graph-index DDL lock), ``not_enabled``, ``not_registered`` (no graph
    tables yet) or ``not_installed`` (migration 0037 not applied).

    ``apply`` needs ``deployment``: the publish-lock and source-run checks
    are per deployment, and none is assumed. A dry run may pass None.
    """
    if apply and not deployment:
        raise SubstrateError(
            "a reconcile run needs the deployment it checks the publish lock and "
            "source runs of",
            data={"reason": "deployment_required"},
            fix_hint="pass --deployment <name>",
        )
    say = on_progress or (lambda _msg: None)
    build_timeout = (
        float(lock_timeout_seconds)
        if lock_timeout_seconds is not None
        else _env_seconds(BUILD_LOCK_TIMEOUT_ENV, DEFAULT_BUILD_LOCK_TIMEOUT_SECONDS)
    )
    with db_connect(dsn, autocommit=True) as conn:
        cur = conn.cursor(row_factory=dict_row)
        if not installed(cur):
            return {"status": "not_installed", "deployment": deployment,
                    "fix_hint": "run `okg migrate` (migration 0037_declared_attr_indexes)"}
        tables = _graph_tables(cur)
        if tables is None:
            return {"status": "not_registered", "deployment": deployment}
        collation = database_collation(cur)
        declared = _declarations(conn)
        desired, refused = desired_indexes(declared, tables, collation)
        stats_wanted = desired_statistics(declared, tables)
        base: dict[str, Any] = {
            "deployment": deployment,
            "collation": collation,
            "text_indexes_allowed": collation_orders_by_code_point(collation),
            "indexed_kinds": list(INDEXED_KINDS),
            "declared_attributes": len(declared),
            "desired_indexes": len(desired),
            "desired_statistics": len(stats_wanted),
            "refused": refused,
        }
        if not apply:
            return {
                **base,
                **_plan(cur, desired),
                **_statistics_dry_run(cur, stats_wanted, tables),
                "status": "dry_run",
            }
        state = _load_state(cur)
        if require_enabled and state.get("enabled_at") is None:
            return {**base, "status": "not_enabled",
                    "fix_hint": "an operator runs `okg db attr-index reconcile --apply` first"}
        if not _try_lock(cur, INDEX_DDL_LOCK):
            holders = _lock_holders(cur, INDEX_DDL_LOCK)
            return {**base, "status": "refused", "reason": "index_ddl_lock_held",
                    "holders": holders}
        run_no: int | None = None
        try:
            cur.execute(
                "UPDATE okg.declared_attr_index_state SET run_count = run_count + 1, "
                "reconciler_pid = pg_backend_pid(), "
                "reconciler_backend_start = (SELECT backend_start FROM pg_stat_activity "
                "WHERE pid = pg_backend_pid()), last_run_started_at = now(), "
                "enabled_at = CASE WHEN %s THEN coalesce(enabled_at, now()) ELSE enabled_at END, "
                "updated_at = now() WHERE singleton RETURNING run_count",
                (bool(enable),),
            )
            run_no = int(cur.fetchone()["run_count"])
            # Rule 1: take the lock, record the pid, then look for a preemptor,
            # so a migrate that starts meanwhile either sees our pid or we see it.
            if preempt_requested(cur):
                report = {**base, "status": "preempted", "run": run_no}
            else:
                try:
                    report = _reconcile_run(
                        conn, cur,
                        deployment=deployment,
                        desired=desired,
                        stats_wanted=stats_wanted,
                        tables=tables,
                        run_no=run_no,
                        retry_failed=retry_failed,
                        build_timeout=build_timeout,
                        say=say,
                        base={**base, "run": run_no},
                    )
                except psycopg.errors.QueryCanceled as exc:
                    # A cancel that lands outside an index statement (a
                    # preemptor's cancel racing a statement boundary) is a
                    # preemption too when one is asked for: recorded as
                    # such and not counted. Any other cancel is an error.
                    if not preempt_requested(cur):
                        raise
                    report = _preempted_mid_run(cur, base={**base, "run": run_no}, exc=exc)
            _finish_run(cur, report)
            return report
        except BaseException as exc:
            _record_failed_run(conn, dsn, run_no=run_no, exc=exc)
            raise
        finally:
            if not conn.closed and conn.info.transaction_status != psycopg.pq.TransactionStatus.INERROR:
                try:
                    cur.execute("RESET lock_timeout")
                    cur.execute("RESET statement_timeout")
                except psycopg.Error as exc:  # noqa: BLE001 -- the session close resets them
                    _log.warning("could not reset the reconciler's timeouts: %s", exc)
            _unlock(cur, INDEX_DDL_LOCK)


def _finish_run(cur: Any, report: Mapping[str, Any]) -> None:
    cur.execute(
        "UPDATE okg.declared_attr_index_state SET reconciler_pid = NULL, "
        "reconciler_backend_start = NULL, last_run_finished_at = now(), "
        "last_run_report = %s, updated_at = now() WHERE singleton",
        (json.dumps(_summary(report), default=str),),
    )


def _preempted_mid_run(cur: Any, *, base: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    """Close a run a preemptor's cancel stopped between index statements: a
    row left ``building`` becomes failed with reason ``preempted`` and is not
    counted. This session holds the DDL lock and phase A already failed any
    row an earlier run left ``building``, so every such row is this run's
    (its ``backend_pid`` may not be set yet)."""
    cur.execute(
        "UPDATE okg.declared_attr_indexes SET state = 'failed', state_since = now(), "
        "backend_pid = NULL, last_reason = 'preempted', last_error = %s, updated_at = now() "
        "WHERE state = 'building' RETURNING index_name",
        (_error_text(exc),),
    )
    failed = [{"index_name": r["index_name"], "reason": "preempted", "counted": False}
              for r in cur.fetchall()]
    return {**base, "status": "preempted", "failed": failed,
            "cancelled_at": "between_index_statements", "pending": _pending_names(cur)}


def _record_failed_run(
    conn: psycopg.Connection, dsn: str, *, run_no: int | None, exc: BaseException
) -> None:
    """Record a run that raised in ``last_run_report`` (status ``error``),
    on a fresh session when the run's own one is gone. A failure to record
    is logged; the run's own error still propagates."""
    report = {"status": "error", "run": run_no, "error": _error_text(exc)}
    statement = (
        "UPDATE okg.declared_attr_index_state SET reconciler_pid = NULL, "
        "reconciler_backend_start = NULL, last_run_finished_at = now(), "
        "last_run_report = %s, updated_at = now() WHERE singleton"
    )
    payload = (json.dumps(report, default=str),)
    try:
        if not conn.closed and conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE:
            conn.execute(statement, payload)
            return
        with db_connect(dsn, autocommit=True) as fresh:
            fresh.execute(statement, payload)
    except Exception as record_exc:  # noqa: BLE001 -- the original error is raised
        _log.warning(
            "could not record the failed declared-attribute index run %s (%s): %s",
            run_no, report["error"], record_exc,
        )


def _summary(report: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("status", "run", "reason", "built", "dropped", "failed", "skipped",
            "capped", "foreign_dattr_indexes", "pending", "statistics_created",
            "statistics_dropped", "statistics_redefined", "foreign_statistics",
            "blocked_statistics", "statistics_pending")
    out: dict[str, Any] = {}
    for key in keys:
        value = report.get(key)
        if isinstance(value, list):
            out[key] = [
                item.get("index_name") or item.get("statistics_name") or item
                if isinstance(item, Mapping) else item
                for item in value
            ][:50]
        elif value is not None:
            out[key] = value
    return out


def _plan(cur: Any, desired: Sequence[DesiredIndex]) -> dict[str, Any]:
    rows = {row["index_name"]: row for row in _manifest_rows(cur)}
    catalog = _dattr_catalog(cur)
    wanted = {d.index_name: d for d in desired}
    build = [d.index_name for d in desired if d.index_name not in rows
             or rows[d.index_name]["state"] == "pending"
             or rows[d.index_name]["definition_digest"] != d.definition_digest]
    drop = sorted(
        name for name, row in rows.items()
        if name not in wanted or row["state"] in _OWN_DROP_STATES
        or row["definition_digest"] != wanted[name].definition_digest
    )
    capped = sorted(name for name, row in rows.items()
                    if row["state"] == "failed" and int(row["attempts"]) >= MAX_CONSECUTIVE_FAILURES)
    return {
        "would_build": build,
        "would_drop": drop,
        "capped": capped,
        "foreign_dattr_indexes": sorted(name for name in catalog if name not in rows),
        "states": _state_counts(rows.values()),
    }


def _statistics_dry_run(
    cur: Any, desired: Sequence[DesiredStatistics], tables: Mapping[str, str]
) -> dict[str, Any]:
    plan = _statistics_plan(_dstat_catalog(cur), desired, tables)
    return {
        "would_create_statistics": plan["create"],
        "would_drop_statistics": plan["drop"],
        "would_redefine_statistics": plan["redefine"],
        "foreign_statistics": plan["foreign"],
        "blocked_statistics": plan["blocked"],
    }


def _statistics_statement(cur: Any, statement: str | sql.Composable) -> None:
    """One statistics DDL statement with the drop's bounded lock wait.
    ``CREATE``/``DROP STATISTICS`` take a SHARE UPDATE EXCLUSIVE lock on the
    table, which graph writes do not wait for."""
    cur.execute(
        sql.SQL("SET lock_timeout = {}").format(
            sql.Literal(f"{int(DROP_LOCK_TIMEOUT_SECONDS * 1000)}ms")
        )
    )
    try:
        cur.execute(statement)
    finally:
        cur.execute("RESET lock_timeout")


def _reconcile_statistics(
    cur: Any,
    *,
    desired: Sequence[DesiredStatistics],
    tables: Mapping[str, str],
    say: Callable[[str], None],
    report: dict[str, Any],
) -> None:
    """Create the declared attributes' statistics objects and drop the ones
    no declaration wants (PACT lane-s-redesign-s1). Under the DDL lock the
    run holds, with the preemption check before every statement; a statement
    that fails is reported and left for the next run."""
    catalog = _dstat_catalog(cur)
    plan = _statistics_plan(catalog, desired, tables)
    wanted = {d.name: d for d in desired}
    report["statistics_created"] = []
    report["statistics_dropped"] = []
    report["statistics_redefined"] = []
    report["foreign_statistics"] = plan["foreign"]
    # A wanted name a foreign object holds cannot be created: pending, so
    # the run is not converged (doctor reports it as missing statistics).
    report["blocked_statistics"] = plan["blocked"]
    pending: list[str] = list(plan["blocked"])

    def run(name: str, action: str, statement: str | sql.Composable) -> bool:
        if preempt_requested(cur):
            raise _Preempted()
        say(f"{action} {name}")
        try:
            _statistics_statement(cur, statement)
        except psycopg.Error as exc:
            if getattr(exc, "sqlstate", None) == "57014" and preempt_requested(cur):
                raise _Preempted() from exc
            report["failed"].append({"statistics_name": name, "action": action,
                                     "error": _error_text(exc)})
            pending.append(name)
            return False
        return True

    for name in plan["drop"]:
        row = catalog[name]
        if run(name, "drop_statistics", sql.SQL("DROP STATISTICS IF EXISTS {}").format(
                sql.Identifier(str(row["schema"]), name))):
            report["statistics_dropped"].append(name)
    for name in plan["redefine"]:
        # An owned object of another expression: dropped and created again.
        row = catalog[name]
        want = wanted[name]
        if run(name, "drop_statistics", sql.SQL("DROP STATISTICS IF EXISTS {}").format(
                sql.Identifier(str(row["schema"]), name))) and run(
                name, "create_statistics", statistics_ddl(
                    physical_table=want.physical_table, target_kind=want.target_kind,
                    attr_id=want.attr_id, name=name)):
            report["statistics_redefined"].append(
                {"statistics_name": name, "attr_id": want.attr_id})
    for name in plan["create"]:
        want = wanted[name]
        if run(name, "create_statistics", statistics_ddl(
                physical_table=want.physical_table, target_kind=want.target_kind,
                attr_id=want.attr_id, name=name)):
            report["statistics_created"].append(
                {"statistics_name": name, "attr_id": want.attr_id})
    report["statistics_pending"] = pending


def _state_counts(rows: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["state"]] = counts.get(row["state"], 0) + 1
    return dict(sorted(counts.items()))


def _reconcile_run(
    conn: psycopg.Connection,
    cur: Any,
    *,
    deployment: str,
    desired: Sequence[DesiredIndex],
    stats_wanted: Sequence[DesiredStatistics],
    tables: Mapping[str, str],
    run_no: int,
    retry_failed: bool,
    build_timeout: float,
    say: Callable[[str], None],
    base: dict[str, Any],
) -> dict[str, Any]:
    wanted = {d.index_name: d for d in desired}
    catalog = _dattr_catalog(cur)
    rows = {row["index_name"]: row for row in _manifest_rows(cur)}
    report: dict[str, Any] = {
        **base,
        "built": [],
        "dropped": [],
        "failed": [],
        "skipped": [],
        "capped": [],
        "foreign_dattr_indexes": sorted(name for name in catalog if name not in rows),
    }

    # Phase A: classify what an earlier run, a crash, an operator or a
    # declaration change left.
    for name, row in rows.items():
        physical = catalog.get(name)
        want = wanted.get(name)
        state = row["state"]
        if want is None or want.definition_digest != row["definition_digest"] \
                or want.physical_table != row["physical_table"]:
            if state in _LIVE_STATES:
                if want is not None:
                    reason = "declaration_changed"
                elif row["target_kind"] not in INDEXED_KINDS:
                    reason = "kind_not_indexed"
                else:
                    reason = "undeclared"
                _set(cur, name, state="retiring", state_since=_now(cur),
                     last_reason=reason, backend_pid=None)
                row["state"] = "retiring"
            continue
        if state == "building":
            # We hold the DDL lock, so no build of ours is running: a worker
            # restart (or a crash) killed this one. Counted like a failure.
            attempts = int(row["attempts"]) + 1
            _set(cur, name, state="failed", state_since=_now(cur), attempts=attempts,
                 next_attempt_run=_next_attempt(run_no, attempts), backend_pid=None,
                 last_reason="interrupted", last_error="the build's session ended mid-build")
            row.update(state="failed", attempts=attempts)
        elif state == "ready":
            if physical is None:
                _set(cur, name, state="pending", state_since=_now(cur),
                     last_reason="index_missing")
                row["state"] = "pending"
            elif not (physical["valid"] and physical["ready"]):
                _set(cur, name, state="failed", state_since=_now(cur),
                     last_reason="index_invalid")
                row["state"] = "failed"
            elif row.get("indexdef") and physical["indexdef"] != row["indexdef"]:
                _set(cur, name, state="retiring", state_since=_now(cur),
                     last_reason="definition_changed")
                row["state"] = "retiring"

    # Phase B (rule 2): drop our own failed, drop_failed and retiring indexes.
    try:
        for name, row in sorted(rows.items()):
            physical = catalog.get(name)
            # A pending row's index can exist only as a leftover (a crash
            # after the build, an interrupted one): drop it and build again.
            if row["state"] not in _OWN_DROP_STATES and not (
                row["state"] == "pending" and physical is not None
            ):
                continue
            if physical is not None:
                if preempt_requested(cur):
                    raise _Preempted()
                try:
                    say(f"drop {name}")
                    _drop_index(cur, str(physical["schema"]), name)
                except psycopg.Error as exc:
                    if getattr(exc, "sqlstate", None) == "57014" and preempt_requested(cur):
                        raise _Preempted() from exc
                    if row["state"] == "failed":
                        _set(cur, name, last_error=f"drop: {_error_text(exc)}")
                    else:
                        _set(cur, name, state="drop_failed", state_since=_now(cur),
                             last_error=_error_text(exc))
                    report["failed"].append({"index_name": name, "action": "drop",
                                             "error": _error_text(exc)})
                    continue
                report["dropped"].append({"index_name": name, "previous_state": row["state"]})
            if row["state"] in ("retiring", "drop_failed"):
                cur.execute("DELETE FROM okg.declared_attr_indexes WHERE index_name = %s", (name,))

        # Phase C: rows for declarations that have none yet.
        for d in desired:
            cur.execute(
                """
                INSERT INTO okg.declared_attr_indexes
                  (index_name, target_kind, attr_id, value_type, key_version,
                   physical_table, definition_digest, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (index_name) DO NOTHING
                """,
                (d.index_name, d.target_kind, d.attr_id, d.value_type, KEY_VERSION,
                 d.physical_table, d.definition_digest),
            )

        # Phase S (PACT lane-s-redesign-s1): the statistics objects. Before
        # the builds: they are catalog-only and cheap, and a build that stops
        # the run early (a publish running) must not hold them back.
        _reconcile_statistics(cur, desired=stats_wanted, tables=tables, say=say,
                              report=report)

        # Phase D: builds, one at a time.
        _build_pending(conn, cur, deployment=deployment, wanted=wanted, run_no=run_no,
                       retry_failed=retry_failed, build_timeout=build_timeout,
                       say=say, report=report)
    except _Preempted:
        report["status"] = "preempted"
        report["pending"] = _pending_names(cur)
        return report

    rows_after = _manifest_rows(cur)
    report["states"] = _state_counts(rows_after)
    report["pending"] = [r["index_name"] for r in rows_after if r["state"] != "ready"]
    report.setdefault(
        "status",
        "converged" if all(r["state"] == "ready" for r in rows_after) else "partial",
    )
    if report.get("statistics_pending") and report["status"] == "converged":
        report["status"] = "partial"
    return report


def _pending_names(cur: Any) -> list[str]:
    return [r["index_name"] for r in _manifest_rows(cur) if r["state"] != "ready"]


def _now(cur: Any) -> Any:
    cur.execute("SELECT now() AS t")
    return cur.fetchone()["t"]


def _next_attempt(run_no: int, attempts: int) -> int | None:
    if attempts >= MAX_CONSECUTIVE_FAILURES:
        return None
    return run_no + BACKOFF_RUNS[min(attempts, len(BACKOFF_RUNS)) - 1]


def _eligible(row: Mapping[str, Any], *, run_no: int, retry_failed: bool, now: Any) -> str | None:
    """None when the row may build this run, else why not."""
    if row["state"] == "pending":
        return None
    if row["state"] != "failed":
        return "not_buildable"
    if row.get("last_reason") == "preempted":
        return None
    if retry_failed:
        return None
    attempts = int(row["attempts"])
    if attempts >= MAX_CONSECUTIVE_FAILURES:
        return "failure_cap"
    due_run = row.get("next_attempt_run")
    if due_run is not None and run_no >= int(due_run):
        return None
    since = row.get("state_since")
    if since is not None and (now - since).total_seconds() >= BACKOFF_CAP_SECONDS:
        return None
    return "backoff"


def _build_pending(
    conn: psycopg.Connection,
    cur: Any,
    *,
    deployment: str,
    wanted: Mapping[str, DesiredIndex],
    run_no: int,
    retry_failed: bool,
    build_timeout: float,
    say: Callable[[str], None],
    report: dict[str, Any],
) -> None:
    rows = [r for r in _manifest_rows(cur) if r["state"] in ("pending", "failed")]
    now = _now(cur)
    catalog = _dattr_catalog(cur)
    blocked_tables: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = row["index_name"]
        why = _eligible(row, run_no=run_no, retry_failed=retry_failed, now=now)
        if why == "failure_cap":
            report["capped"].append(name)
            continue
        if why is not None:
            report["skipped"].append({"index_name": name, "reason": why})
            continue
        want = wanted.get(name)
        if want is None:
            continue
        if name in catalog:
            # Our own failed index whose drop failed: rule 3 forbids a build
            # on its table until the next run drops it.
            report["skipped"].append({"index_name": name, "reason": "own_invalid_index"})
            continue
        table = want.physical_table
        if table in blocked_tables:
            report["skipped"].append({"index_name": name, **blocked_tables[table]})
            _set(cur, name, last_reason=blocked_tables[table]["reason"])
            continue
        try:
            _check_build_preconditions(conn, cur, deployment)
        except _StopBuilds as stop:
            cur.execute(
                "UPDATE okg.declared_attr_indexes SET last_reason = %s, updated_at = now() "
                "WHERE state = 'pending'",
                (stop.reason,),
            )
            report["status"] = "partial"
            report["reason"] = stop.reason
            report.update(stop.detail)
            return
        invalid = _invalid_indexes_on(cur, table)
        if invalid:
            blocked_tables[table] = {
                "reason": "foreign_invalid_index",
                "invalid_indexes": [item["name"] for item in invalid],
            }
            report["skipped"].append({"index_name": name, **blocked_tables[table]})
            _set(cur, name, last_reason="foreign_invalid_index")
            continue
        if row["state"] == "failed" and retry_failed:
            _set(cur, name, attempts=0)
            row["attempts"] = 0
        say(f"build {name} ({want.target_kind} {want.attr_id} {want.value_type})")
        _set(cur, name, state="building", state_since=_now(cur), last_reason=None,
             last_error=None)
        cur.execute("UPDATE okg.declared_attr_indexes SET backend_pid = pg_backend_pid() "
                    "WHERE index_name = %s", (name,))
        cur.execute(
            sql.SQL("SET lock_timeout = {}").format(sql.Literal(f"{int(build_timeout * 1000)}ms"))
        )
        # A statement timeout would leave an invalid index; the wait is what
        # is bounded.
        cur.execute("SET statement_timeout = 0")
        begun = time.monotonic()
        try:
            cur.execute(index_ddl(
                physical_table=table,
                target_kind=want.target_kind,
                attr_id=want.attr_id,
                value_type=want.value_type,
            ))
        except psycopg.Error as exc:
            cur.execute("RESET lock_timeout")
            if preempt_requested(cur):
                _set(cur, name, state="failed", state_since=_now(cur), backend_pid=None,
                     last_reason="preempted", last_error=_error_text(exc))
                report["failed"].append({"index_name": name, "reason": "preempted",
                                         "counted": False})
                raise _Preempted() from exc
            attempts = int(row["attempts"]) + 1
            reason = _failure_reason(exc)
            _set(cur, name, state="failed", state_since=_now(cur), backend_pid=None,
                 attempts=attempts, next_attempt_run=_next_attempt(run_no, attempts),
                 last_reason=reason, last_error=_error_text(exc))
            report["failed"].append({"index_name": name, "reason": reason,
                                     "attempts": attempts, "error": _error_text(exc)})
            _drop_leftover_after_failure(cur, name, report)
            if reason in ("lock_timeout", "cancelled"):
                report["status"] = "partial"
                report["reason"] = reason
                return
            continue
        cur.execute("RESET lock_timeout")
        cur.execute(
            """
            SELECT i.indisvalid AND i.indisready AS ok, pg_get_indexdef(i.indexrelid) AS def
              FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
             WHERE c.relname = %s
            """,
            (name,),
        )
        built = cur.fetchone()
        if built is None or not built["ok"]:
            _set(cur, name, state="failed", state_since=_now(cur), backend_pid=None,
                 attempts=int(row["attempts"]) + 1, last_reason="index_invalid_after_build")
            report["failed"].append({"index_name": name, "reason": "index_invalid_after_build"})
            continue
        _set(cur, name, state="ready", state_since=_now(cur), backend_pid=None, attempts=0,
             next_attempt_run=None, last_reason=None, last_error=None, indexdef=built["def"])
        report["built"].append({"index_name": name, "target_kind": want.target_kind,
                                "attr_id": want.attr_id, "value_type": want.value_type,
                                "seconds": round(time.monotonic() - begun, 3)})


def _drop_leftover_after_failure(cur: Any, name: str, report: dict[str, Any]) -> None:
    """Drop the invalid index a failed build left, at once (E8: one failed at
    its last wait is maintained by every write until dropped). A drop that
    fails is retried by the next run's rule-2 phase."""
    catalog = _dattr_catalog(cur)
    physical = catalog.get(name)
    if physical is None:
        return
    try:
        _drop_index(cur, str(physical["schema"]), name)
        report["dropped"].append({"index_name": name, "previous_state": "failed"})
    except psycopg.Error as exc:  # noqa: BLE001 -- the next run drops it first
        _set(cur, name, last_error=f"drop after failure: {_error_text(exc)}")


# --------------------------------------------------------------------------- #
# Requests from the publisher                                                  #
# --------------------------------------------------------------------------- #


def maintenance_wanted(conn: psycopg.Connection) -> tuple[bool, str]:
    """Whether the publisher should enqueue a maintenance run now: the
    deployment opted in, and a catalog apply asked for one or the declared
    set no longer matches the manifest."""
    with conn.cursor(row_factory=dict_row) as cur:
        if not installed(cur):
            return False, "not_installed"
        cur.execute("SELECT * FROM okg.declared_attr_index_state WHERE singleton")
        state = cur.fetchone()
        if state is None or state["enabled_at"] is None:
            return False, "not_enabled"
        if state["requested_at"] is not None and (
            state["last_run_started_at"] is None
            or state["requested_at"] > state["last_run_started_at"]
        ):
            return True, f"requested:{state['request_reason']}"
        tables = _graph_tables(cur)
        if tables is None:
            return False, "not_registered"
        desired, _refused = desired_indexes(_declarations(conn), tables, database_collation(cur))
        cur.execute(
            "SELECT index_name, definition_digest FROM okg.declared_attr_indexes "
            "WHERE state IN ('pending', 'building', 'ready', 'failed')"
        )
        live = {(r["index_name"], r["definition_digest"]) for r in cur.fetchall()}
        if live != {(d.index_name, d.definition_digest) for d in desired}:
            return True, "declaration_changed"
        cur.execute("SELECT count(*) AS n FROM okg.declared_attr_indexes WHERE state = 'pending'")
        if int(cur.fetchone()["n"]):
            return True, "pending_builds"
    return False, "up_to_date"


# --------------------------------------------------------------------------- #
# Typed text rendering (tests)                                                 #
# --------------------------------------------------------------------------- #


def _typed_text(value_type: str, json_expr: str) -> str:
    """One text column per typed value, comparable across the effective view
    and the graph rows."""
    if value_type == "text":
        return f"({json_expr} #>> '{{}}')"
    if value_type == "num":
        return f"(({json_expr})::numeric)::text"
    if value_type == "bool":
        return f"(({json_expr})::boolean)::text"
    return _utc_text(f"okg.attr_ts_v4({json_expr} #>> '{{}}')")


def _utc_text(ts_expr: str) -> str:
    return (
        f"(CASE WHEN isfinite({ts_expr}) THEN to_char({ts_expr} AT TIME ZONE 'UTC', "
        f"'YYYY-MM-DD\"T\"HH24:MI:SS.US') ELSE ({ts_expr})::text END)"
    )


# --------------------------------------------------------------------------- #
# Doctor                                                                       #
# --------------------------------------------------------------------------- #

#: A maintained invalid index of ours warns after this long or this many
#: attempts, and is an error after a day.
_INVALID_WARN_SECONDS = 3600
_INVALID_WARN_ATTEMPTS = 3
_INVALID_ERROR_SECONDS = 24 * 3600


#: okg#3112 step 4a (decision 304, D3): severity of a declared node attribute
#: without a ready index, by reason. Since 4a no other candidate set narrows
#: it, so a filter on it scans every record.
_UNINDEXED_SEVERITY = {
    "never_enabled": "ERROR",
    "collation_refuses_text": "ERROR",
    "maintenance_disabled": "ERROR",
    "failure_cap": "ERROR",
    "index_missing": "ERROR",
    "failed": "WARN",
    "declaration_changed": "WARN",
}
_UNINDEXED_UPGRADE_NOTE = (
    "only a ready declared expression index narrows an "
    "attribute filter (okg.attr_index no longer does); until this attribute's "
    "index is ready, filters on it are answered by scanning every record"
)


def _unindexed_attr_findings(
    cur: Any,
    declared: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    now: Any,
) -> list[dict[str, Any]]:
    """One ``attr_not_indexed`` finding per declared node attribute whose
    expression index is not ready, naming the reason. ``building`` and
    ``pending`` warn after an hour and are INFO before that; a ``ready``
    manifest row whose index is gone is ``index_missing``, an ERROR."""
    if not declared:
        return []
    ready, _table = reader_readiness(cur, target_kind="node", declared=declared)
    # The live row of each attribute: a retiring or drop_failed row of an
    # earlier value type can coexist with it, and must not answer for it.
    by_attr: dict[str, Mapping[str, Any]] = {}
    for r in rows:
        attr = str(r["attr_id"])
        if (r["target_kind"] == "node" and r["state"] in _LIVE_STATES
                and r["value_type"] == declared.get(attr)):
            by_attr[attr] = r
    text_allowed = bool(report["text_indexes_allowed"])
    out: list[dict[str, Any]] = []
    for attr in sorted(declared):
        readiness = ready.get(attr)
        if readiness is None:
            continue
        row = by_attr.get(attr) or {}
        if declared[attr] == "text" and not text_allowed:
            reason = "collation_refuses_text"
        elif not report["enabled"]:
            reason = "never_enabled"
        elif readiness == "index_failed":
            capped = int(row.get("attempts") or 0) >= MAX_CONSECUTIVE_FAILURES
            reason = "failure_cap" if capped else "failed"
        elif readiness == "declaration_changed":
            reason = readiness
        elif readiness == "index_missing" and row.get("state") == "ready":
            # The manifest says ready but the index is gone (dropped by hand,
            # or invalid): no reconcile run is coming to build it.
            reason = "index_missing"
        elif maintenance_disabled():
            # Read from THIS process's environment: the worker records
            # nothing when maintenance is off, so doctor run with the
            # worker's environment is what makes this exact.
            reason = "maintenance_disabled"
        elif readiness == "index_building":
            reason = "building"
        else:
            reason = "pending"
        severity = _UNINDEXED_SEVERITY.get(reason)
        if severity is None:
            since = row.get("state_since")
            age = (now - since).total_seconds() if since else _INVALID_WARN_SECONDS
            severity = "WARN" if age >= _INVALID_WARN_SECONDS else "INFO"
        finding: dict[str, Any] = {
            "code": "attr_not_indexed", "severity": severity,
            "attr_id": attr, "value_type": declared[attr], "reason": reason,
            "readiness": readiness, "message": _UNINDEXED_UPGRADE_NOTE,
            "fix": ("unset OKG_DECLARED_ATTR_INDEXES on the worker"
                    if reason == "maintenance_disabled" else
                    "use a database collation that orders by code point (C or C.UTF-8)"
                    if reason == "collation_refuses_text" else
                    "okg db attr-index reconcile --apply --retry-failed"
                    if reason == "failure_cap" else
                    "okg db attr-index reconcile --apply"),
        }
        if reason == "maintenance_disabled":
            finding["source"] = "doctor_environment"
        out.append(finding)
    return out


def doctor_report(dsn: str) -> dict[str, Any]:
    """Lifecycle, invalid indexes (every actor), the failure cap, the
    collation and value lengths. ``status`` is OK, WARN or ERROR; each
    finding names its code, severity and the fix."""
    with db_connect(dsn, autocommit=True) as conn:
        cur = conn.cursor(row_factory=dict_row)
        findings: list[dict[str, Any]] = []
        if not installed(cur):
            return {"status": "OK", "installed": False, "findings": [],
                    "note": "migration 0037_declared_attr_indexes is not applied"}
        tables = _graph_tables(cur)
        collation = database_collation(cur)
        cur.execute("SELECT * FROM okg.declared_attr_index_state WHERE singleton")
        state = dict(cur.fetchone() or {})
        rows = _manifest_rows(cur)
        catalog = _dattr_catalog(cur)
        cur.execute("SELECT now() AS t")
        now = cur.fetchone()["t"]
        report: dict[str, Any] = {
            "installed": True,
            "enabled": state.get("enabled_at") is not None,
            "enabled_at": state.get("enabled_at"),
            "collation": collation,
            "text_indexes_allowed": collation_orders_by_code_point(collation),
            "indexed_kinds": list(INDEXED_KINDS),
            "states": _state_counts(rows),
            "run_count": state.get("run_count"),
            "last_run_finished_at": state.get("last_run_finished_at"),
            "last_run": state.get("last_run_report"),
        }
        declared: dict[str, str] = {}
        try:
            declared = _declarations(conn)
        except SubstrateError as exc:
            findings.append({"code": "declaration_conflict", "severity": "ERROR",
                             "message": str(exc), "data": exc.data})
        if not report["enabled"] and not declared:
            findings.append({"code": "not_enabled", "severity": "INFO",
                             "message": "no declared-attribute index is maintained yet",
                             "fix": "okg db attr-index reconcile --apply"})
        findings.extend(_unindexed_attr_findings(cur, declared, rows, report, now))
        text_attrs = sorted(a for a, t in declared.items() if t == "text")
        if text_attrs and not report["text_indexes_allowed"]:
            findings.append({
                "code": "collation_refuses_text", "severity": "WARN",
                "message": (
                    f"the database collation (provider {collation.get('provider')}, "
                    f"collate {collation.get('collate')}, locale {collation.get('locale')}) "
                    "does not order strings by code point; text attributes get no index"
                ),
                "attrs": text_attrs,
            })
        for name in sorted(n for n in catalog if n not in {r["index_name"] for r in rows}):
            findings.append({"code": "foreign_dattr_index", "severity": "WARN",
                             "index": name,
                             "message": "an index uses the reserved okg_dattr_ prefix but the "
                                        "reconciler's manifest does not list it",
                             "fix": f"drop it: DROP INDEX CONCURRENTLY public.{name}"})
        if tables is not None:
            # PACT lane-s-redesign-s1: the per-attribute statistics objects.
            stat_catalog = _dstat_catalog(cur)
            stat_plan = _statistics_plan(
                stat_catalog, desired_statistics(declared, tables), tables
            )
            report["statistics"] = {
                "present": len(stat_catalog),
                "missing": stat_plan["missing"],
                "wrong_expression": stat_plan["redefine"],
                "undeclared": stat_plan["drop"],
                "foreign": stat_plan["foreign"],
            }
            for name in stat_plan["foreign"]:
                row = stat_catalog[name]
                findings.append({
                    "code": "foreign_dattr_statistics", "severity": "WARN",
                    "statistics": name,
                    "table": f"{row['table_schema']}.{row['table_name']}",
                    "message": "a statistics object uses the reserved okg_dstat_ prefix "
                               + "on a table the reconciler does not own",
                    "fix": f"drop it: DROP STATISTICS {row['schema']}.{name}",
                })
            if report["enabled"]:
                for name in stat_plan["create"]:
                    findings.append({
                        "code": "statistics_missing", "severity": "WARN",
                        "statistics": name,
                        "message": "a declared attribute has no statistics object",
                        "fix": "okg db attr-index reconcile --apply",
                    })
                for name in stat_plan["blocked"]:
                    findings.append({
                        "code": "statistics_missing", "severity": "WARN",
                        "statistics": name,
                        "message": "a declared attribute has no statistics object: "
                                   + "a statistics object the reconciler does not own "
                                   + "holds its name",
                        "fix": "drop the foreign object: DROP STATISTICS "
                               + f"{stat_catalog[name]['schema']}.{name}; then "
                               + "okg db attr-index reconcile --apply",
                    })
                for name in stat_plan["redefine"]:
                    findings.append({
                        "code": "statistics_wrong_expression", "severity": "WARN",
                        "statistics": name,
                        "observed": stat_catalog[name].get("expressions"),
                        "message": "a statistics object of a declared attribute "
                                   + "describes another expression; the next "
                                   + "reconcile drops and recreates it",
                        "fix": "okg db attr-index reconcile --apply",
                    })
                for name in stat_plan["drop"]:
                    findings.append({
                        "code": "statistics_undeclared", "severity": "INFO",
                        "statistics": name,
                        "message": "no declaration wants this statistics object; "
                                   + "the next reconcile drops it",
                        "fix": "okg db attr-index reconcile --apply",
                    })
        for row in rows:
            name = row["index_name"]
            physical = catalog.get(name)
            if row["target_kind"] not in INDEXED_KINDS and row["state"] in _LIVE_STATES:
                # Decision 287d: no edge index is declared. One an earlier run
                # built is not drift and no reader uses it; the next reconcile
                # drops it.
                findings.append({"code": "kind_not_indexed", "severity": "INFO", "index": name,
                                 "target_kind": row["target_kind"],
                                 "message": "no index is declared for this kind; the next "
                                            "reconcile drops it",
                                 "fix": "okg db attr-index reconcile --apply"})
            # An undeclared kind's row is retired by the next reconcile, so a
            # missing index or a failed build of it is not an error.
            undeclared = row["target_kind"] not in INDEXED_KINDS
            age = (now - row["state_since"]).total_seconds() if row.get("state_since") else 0.0
            if (not undeclared and row["state"] == "ready"
                    and (physical is None or not physical["valid"])):
                findings.append({"code": "ready_index_missing", "severity": "ERROR", "index": name,
                                 "fix": "okg db attr-index reconcile --apply"})
            if row["state"] == "failed" and not undeclared:
                capped = int(row["attempts"]) >= MAX_CONSECUTIVE_FAILURES
                findings.append({
                    "code": "failure_cap" if capped else "build_failed",
                    "severity": "ERROR" if capped else "WARN", "index": name,
                    "attr_id": row["attr_id"], "attempts": int(row["attempts"]),
                    "reason": row.get("last_reason"), "error": row.get("last_error"),
                    "fix": "okg db attr-index reconcile --apply --retry-failed" if capped else None,
                })
            if row["state"] == "drop_failed":
                findings.append({"code": "drop_failed", "severity": "WARN", "index": name,
                                 "error": row.get("last_error")})
            # An invalid index of ours that is still there: every write may
            # maintain it (when ready) and it blocks builds on its table,
            # whether its build is running, failed or its drop failed.
            if row["state"] in ("building", "failed", "drop_failed") \
                    and physical is not None and not physical["valid"]:
                severity = None
                if age >= _INVALID_ERROR_SECONDS:
                    severity = "ERROR"
                elif age >= _INVALID_WARN_SECONDS or int(row["attempts"]) >= _INVALID_WARN_ATTEMPTS:
                    severity = "WARN"
                if severity:
                    findings.append({"code": "maintained_invalid_index", "severity": severity,
                                     "index": name, "state": row["state"],
                                     "seconds": round(age),
                                     "ready_and_maintained": bool(physical["ready"]),
                                     "fix": "okg db attr-index reconcile --apply drops it; "
                                            "if the drop keeps failing, end the session "
                                            "that blocks it"})
            if row["state"] == "pending" and row.get("last_reason"):
                findings.append({"code": "build_waiting", "severity": "INFO", "index": name,
                                 "reason": row["last_reason"]})
            if row.get("max_value_chars") and int(row["max_value_chars"]) > TEXT_KEY_CHARS:
                findings.append({"code": "long_values", "severity": "INFO", "index": name,
                                 "attr_id": row["attr_id"],
                                 "max_value_chars": int(row["max_value_chars"]),
                                 "message": "values longer than the 512-character key share "
                                            "candidates by prefix"})
        if tables is not None:
            for kind, physical_table in tables.items():
                for bad in _invalid_indexes_on(cur, physical_table):
                    if str(bad["name"]).startswith(INDEX_NAME_PREFIX):
                        continue
                    findings.append({"code": "foreign_invalid_index", "severity": "WARN",
                                     "table": physical_table, "index": bad["name"],
                                     "message": "an invalid index of another actor blocks "
                                                "declared-index builds on this table"})
        severities = {f["severity"] for f in findings}
        report["status"] = "ERROR" if "ERROR" in severities else (
            "WARN" if "WARN" in severities else "OK")
        report["findings"] = findings
        return json.loads(json.dumps(report, default=str))
