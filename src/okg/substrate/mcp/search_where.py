"""`where` and record `scope` on `search` (PACT read-model-s3-where-scope).

Design: `pact/changes/read-model/design.md` sections 3.3 (rules), 3.4
(scope) and 6 (compatibility, decision D9). This module holds the parts that
need no database:

* :func:`normalize_where` checks a caller's predicate against the fields
  search can read (stable fields, catalog title/path and named dates, attrs) and
  rewrites the accepted spellings to one canonical form.
* :func:`plan_where` merges the old arguments (`subtype_filter`,
  `attr_filters`, `filter.subtype`, `filter.attr_filters`, `filter.source`)
  with `where` into ONE predicate, lifts the top-level equality clauses the
  ranked read already serves into the old filter keys (so `where` and the
  old arguments run the same code and return the same page), and keeps the
  rest as a residual predicate.
* :func:`evaluate` is the residual check: three-valued (decision D5). A
  clause on a masked value is unknown, `not` of unknown is unknown, and an
  unknown row is excluded.
* :func:`invariant_sql` renders the clauses on columns Nomos never changes
  (`id`, `type`, `source`) for the ranked SQL (decision D4). Other clauses
  on attributes stay out of SQL: until the reader's view lands (design slice
  7) any attribute may be masked. A top-level `attrs.<key>` equality is the
  one exception: :func:`plan_where` lifts it into `attr_filters`, which shape
  bounds from stored values exactly as that argument does on dev (D9; #3208).
* :func:`lookup_sql_clauses` turns the residual into extra clauses for the
  query-less lookup's paged SQL: exact clauses on `id`/`type`, and, only when
  no Nomos bundle can mask a value, superset clauses on the stored attribute
  each catalog title, path or date reads. Every row the SQL admits is still
  rechecked by :func:`evaluate` on the read values.
* :func:`lookup_path_prefixes` names the top-level `path` prefixes that
  lookup may also narrow through the bounded path-prefix index (issue
  #3194), again only when no Nomos bundle can mask a value.
* :func:`describe` is the short text of the applied predicate the payload
  carries as `where_applied`, so a filter that did not apply is visible.
"""
from __future__ import annotations

import json
import hashlib
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import runtime_enforcement as _runtime_enforcement
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER
from okg.substrate.mcp.date_conditions import MAX_DATE_TEXT, instant_of, leaf_windows
from okg.substrate.mcp.handle_fields import (
    DATE_MEANINGS,
    PRECISION_DATE,
    _parse_date,
    handle_fields,
    resolve_named_date,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.operator_sql import (
    CompiledPredicate,
    compile_read_predicate,
    string_field_lowerer,
    text_contains,
)

__all__ = [
    "SearchScopeUnsupported",
    "WherePlan",
    "WhereUnsupported",
    "admitted_types",
    "and_clauses",
    "describe",
    "evaluate",
    "invariant_conjuncts",
    "invariant_sql",
    "compile_where",
    "names_field",
    "matches",
    "normalize_record_scope",
    "normalize_where",
    "plan_where",
    "lift_passage_type",
    "profile_subtypes",
    "has_negation",
    "lookup_path_prefixes",
    "lookup_date_predicate",
    "lookup_sql_clauses",
    "lookup_sql_exact",
    "masking_possible",
    "parse_date",
    "reads_attrs",
    "to_view_predicate",
    "top_conjuncts",
    "WhereContext",
    "load_where_context",
]


class WhereUnsupported(SubstrateError):
    """A `where` on search names a field or shape search cannot apply."""

    code = "where_unsupported"


class SearchScopeUnsupported(SubstrateError):
    """A search `scope` that is malformed or not served on this method."""

    code = "search_scope_unsupported"


#: Logical operators and the leaf operators search's `where` accepts: the
#: v1 predicate set plus `prefix`.
_LOGICAL = frozenset({"and", "or", "not"})
_LEAVES = frozenset({
    "eq", "in", "exists", "contains", "suffix", "prefix", "lt", "gt", "between",
})

#: Accepted spellings -> canonical field. Catalog path and title keep their
#: own meaning; attrs.path explicitly names the raw attribute.
_FIELD_ALIASES = {
    "id": "id",
    "node_id": "id",
    "type": "type",
    "subtype": "type",
    "source": "source",
    "path": "path",
    "title": "title",
}

#: Fields the design names that this slice cannot read yet, with the reason
#: the refusal carries.
_UNAVAILABLE_FIELDS = {
    "date": (
        "name date.created, date.modified, date.published, date.resolved or "
        "date.effective; use date.attrs.<key> for another domain date"
    ),
    "source_family": (
        "`source_family` is not a `where` field on this server; pass the "
        "`source_family` argument"
    ),
}

#: Row key each canonical stable field reads.
_ROW_KEY = {"id": "node_id", "type": "subtype", "source": "source"}

#: Stable fields whose values Nomos never masks: a clause on them may go
#: into the ranked SQL and shape the rank cap (D4).
INVARIANT_FIELDS = frozenset({"id", "type", "source"})


def _text_contains_field(canonical: str) -> bool:
    """Fields `contains` reads text in (issue #3666): one stored attribute,
    the catalog `path` or `title`, or the record's `source` id, each a
    string on a read row."""
    return canonical in ("path", "title", "source") or (
        canonical.startswith("attrs.") and len(canonical) > len("attrs.")
    )


def _refuse(message: str, *, data: dict[str, Any], fix_hint: str) -> None:
    raise WhereUnsupported(message, data=data, fix_hint=fix_hint)


# -- named-date values: one comparison rule ---------------------------------
#
# `handle_fields` (PACT read-model-s1-catalog-fields) parses a date into a
# datetime and a precision: `date` when the source named a calendar day and
# `datetime` when it named a time of day. The comparison rule is the one
# every read shares (`date_conditions`, issue #3666; docs/reference/
# mcp-results.md, "Search predicates and named dates"):
#
# * a date compares as an instant; a value or an operand with no time zone
#   is read as UTC, and the reply says so once (`dates_read_as_utc`);
# * an operand is a day or a timestamp. A day means its first instant,
#   except as the upper bound of `between`, where it means its last, and
#   with `eq` or `in`, where it means "on that day";
# * a row value that is a day (zoned or not) is its first instant.
#
# No pair of values is incompatible any more: search used to refuse an aware
# operand beside a naive one and to count a naive row against an aware
# operand as `where_incompatible`. Both are now compared, as UTC.

#: `(datetime, precision)` of a parsed date value.
_DateValue = tuple[datetime, str]


def parse_date(raw: Any) -> Optional[_DateValue]:
    """`raw` as `(datetime, precision)`, or None when it is not a date.

    The public name for the parser `resolve_named_date` uses (PACT
    read-model-s1-catalog-fields keeps it private in `handle_fields`), so an
    operand and a row value read alike; `normalize_handle_date` keeps only the
    ISO text and drops the precision this policy needs.
    """
    return _parse_date(raw)


def _canonical_date(value: _DateValue) -> str:
    """The canonical operand text: a day keeps its day form, never a time.

    `2026-10-03` (naive day), `2026-10-03-07:00` (a day in that zone), or the
    ISO date-time of an instant. `parse_date` reads each back to the
    same value and precision.
    """
    when, precision = value
    if precision != PRECISION_DATE:
        return when.isoformat()
    day = when.date().isoformat()
    if when.utcoffset() is None:
        return day
    return day + when.isoformat()[len("0000-00-00T00:00:00"):]


def _canonical_field(raw: Any, *, op: str) -> str:
    if not isinstance(raw, str) or not raw:
        _refuse(
            f"a `{op}` clause in `where` needs a `field`",
            data={"op": op, "field": raw},
            fix_hint="name one of id, type, source, path or attrs.<key>",
        )
    if raw in _UNAVAILABLE_FIELDS:
        _refuse(
            f"`where` field {raw!r} is not available on search here",
            data={"field": raw, "reason": "field_not_available"},
            fix_hint=_UNAVAILABLE_FIELDS[raw],
        )
    if raw in _FIELD_ALIASES:
        return _FIELD_ALIASES[raw]
    if raw in {f"date.{meaning}" for meaning in DATE_MEANINGS} or (
        raw.startswith("date.attrs.") and raw[len("date.attrs."):]
    ):
        if op not in {"eq", "in", "lt", "gt", "between", "exists"}:
            _refuse("date fields support eq, in, lt, gt, between or exists",
                    data={"field": raw, "op": op}, fix_hint="use a supported date comparison")
        return raw
    if raw == "attrs":
        if op != "contains":
            _refuse(
                "`where` field 'attrs' (the whole attribute map) is only "
                "read by `contains`",
                data={"field": raw, "op": op},
                fix_hint="name one attribute as attrs.<key>",
            )
        return "attrs"
    if raw.startswith("attrs.") and raw.split(".", 1)[1]:
        return raw
    _refuse(
        f"unknown `where` field {raw!r}",
        data={"field": raw, "reason": "unknown_field"},
        fix_hint="name one of id, type, source, path or attrs.<key>",
    )
    raise AssertionError("unreachable")


def normalize_where(where: Any) -> Optional[dict[str, Any]]:
    """The caller's `where` in canonical form, or None for no predicate.

    Validates what the shared Predicate model does not: which field names
    search reads, and which operand each leaf needs. `{}` means no
    predicate, as everywhere else the predicate tree is taken.
    """
    if where is None or where == {}:
        return None
    if hasattr(where, "model_dump"):
        where = where.model_dump(exclude_none=True)
    if not isinstance(where, Mapping):
        _refuse(
            "`where` must be a predicate object",
            data={"got": type(where).__name__},
            fix_hint='e.g. {"op": "eq", "field": "type", "value": "ticket"}',
        )
    normalized = _normalize(dict(where))
    try:
        # The canonical encoding the compiler and scanner fingerprint. A NaN or
        # Infinity, or a string holding a lone surrogate, has none: refuse it
        # here, by name, instead of failing later inside fingerprinting.
        json.dumps(normalized, sort_keys=True, ensure_ascii=False,
                   allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        _refuse(
            "`where` values must be JSON values: finite numbers and valid "
            "Unicode text",
            data={"reason": "invalid_where_value"},
            fix_hint="drop NaN/Infinity operands and unpaired surrogate "
            "characters from the predicate",
        )
    return normalized


def _normalize(node: dict[str, Any]) -> dict[str, Any]:
    op = node.get("op")
    if op in ("and", "or"):
        clauses = node.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            _refuse(
                f"an `{op}` clause takes a non-empty `clauses` list",
                data={"op": op},
                fix_hint=f'{{"op": "{op}", "clauses": [...]}}',
            )
        return {"op": op, "clauses": [_normalize(dict(c)) for c in clauses]}
    if op == "not":
        clause = node.get("clause")
        if not isinstance(clause, Mapping) or not clause:
            _refuse(
                "a `not` clause takes one sub-predicate in `clause`",
                data={"op": op},
                fix_hint='{"op": "not", "clause": {...}}',
            )
        return {"op": "not", "clause": _normalize(dict(clause))}
    if op in ("like", "like_exact"):
        # loose-match-3666: like is filter's. Search's own `contains` finds
        # text inside one field; the grammar offers that call (or filter's,
        # on a field contains reads no text in) before a dispatched search
        # gets here.
        _refuse(
            f"search's `where` does not take `{op}`; `contains` with a text value "
            "finds text inside attrs.<key>, path, title or source",
            data={"op": op},
            fix_hint="write the test as contains, which ignores case but not "
            'accents, e.g. {"attrs.<key>": {"op": "contains", "value": "<text>"}}; '
            "filter's `where` takes like, which also ignores accents",
        )
    if op not in _LEAVES:
        _refuse(
            f"unsupported `where` op {op!r}",
            data={"op": op},
            fix_hint="use and, or, not, eq, in, exists, contains, suffix, "
            "prefix, lt, gt or between",
        )
    canonical = _canonical_field(node.get("field"), op=str(op))
    out: dict[str, Any] = {"op": op, "field": canonical}
    if op in ("in", "between"):
        values = node.get("values")
        if not isinstance(values, list) or not values or (
            op == "between" and len(values) != 2
        ):
            _refuse(
                f"a `{op}` clause takes `values`"
                + (" [low, high]" if op == "between" else " (a non-empty list)"),
                data={"op": op, "field": canonical},
                fix_hint=f'{{"op": "{op}", "field": ..., "values": [...]}}',
            )
        out["values"] = list(values)
    elif op == "exists":
        pass
    elif op == "contains" and not (
        isinstance(node.get("value"), str) and _text_contains_field(canonical)
    ):
        value = node.get("value")
        if canonical != "attrs" or not isinstance(value, Mapping):
            _refuse(
                "`contains` takes text on attrs.<key>, path, title or source (the "
                "value holds it, ASCII case ignored), or an object on field "
                "'attrs' (JSON containment)",
                data={"op": op, "field": canonical},
                fix_hint='{"op": "contains", "field": "attrs.<key>", "value": "text"} '
                'or {"op": "contains", "field": "attrs", "value": {"k": "v"}}',
            )
        out["value"] = dict(value)
    elif op in ("suffix", "prefix", "contains"):
        # `contains` here is text inside attrs.<key>, path, title or source
        # (#3666).
        value = node.get("value")
        if not isinstance(value, str) or not value:
            _refuse(
                f"a `{op}` clause takes a non-empty string `value`",
                data={"op": op, "field": canonical},
                fix_hint=f'{{"op": "{op}", "field": "path", "value": "slack/"}}',
            )
        if "\x00" in value or any("\ud800" <= ch <= "\udfff" for ch in value):
            # PostgreSQL text cannot hold NUL, and a lone surrogate is not
            # UTF-8: either would fail inside SQL instead of here, by name.
            _refuse(
                f"a `{op}` value cannot contain a NUL character or a lone surrogate",
                data={"op": op, "field": canonical},
                fix_hint=f"remove the character from the `{op}` value",
            )
        out["value"] = value
    else:
        out["value"] = node.get("value")
    if canonical.startswith("date.") and op != "exists":
        operands = out["values"] if op in {"in", "between"} else [out["value"]]
        parsed = [parse_date(value) for value in operands]
        if any(value is None for value in parsed):
            _refuse("date operands must be valid ISO-8601 or RFC 2822 strings",
                    data={"field": canonical, "reason": "invalid_date_operand"},
                    fix_hint="pass an explicit date string; numeric epoch values are ambiguous")
        normalized = [_canonical_date(value) for value in parsed]
        try:
            # The shared rule decides what the operands mean, including
            # whether a `between` runs low to high (a day as the upper bound
            # is its last instant).
            leaf_windows({**out, "values" if op in {"in", "between"} else "value": (
                normalized if op in {"in", "between"} else normalized[0]
            )}, operator="search")
        except InvalidOperatorArguments as exc:
            if (exc.data or {}).get("reason") == "date_bounds_reversed":
                _refuse("date between bounds must be ordered low to high",
                        data={"field": canonical, "reason": "date_bounds_reversed"},
                        fix_hint="pass values=[low, high]")
            _refuse("date operands must be valid ISO-8601 or RFC 2822 strings",
                    data={"field": canonical, "reason": "invalid_date_operand"},
                    fix_hint="pass an explicit date string; numeric epoch values are ambiguous")
        if op in {"in", "between"}:
            out["values"] = normalized
        else:
            out["value"] = normalized[0]
    return out


def _fields(predicate: Optional[Mapping[str, Any]]) -> set[str]:
    if not predicate:
        return set()
    op = predicate.get("op")
    if op in {"and", "or"}:
        return set().union(*(_fields(c) for c in predicate["clauses"]))
    if op == "not":
        return _fields(predicate["clause"])
    return {predicate["field"]}


@dataclass
class WhereContext:
    """One pinned catalog read and deduplicated bounded-candidate diagnostics."""

    metadata: Mapping[str, Mapping[str, Any]]
    fields: set[str]
    missing: dict[str, set[str]] = field(default_factory=dict)
    unparseable: dict[str, set[str]] = field(default_factory=dict)
    #: Date fields with an operand, and with a row value, that had no time
    #: zone and was read as UTC (issue #3666).
    utc_bounds: set[str] = field(default_factory=set)
    utc_stored: set[str] = field(default_factory=set)
    #: Per date field, records in scope whose value is present and is not a
    #: date, as counted in SQL by the query-less lookup, which then reads
    #: only the records its date conditions select (issue #3666).
    unparseable_counted: dict[str, int] = field(default_factory=dict)

    def disclosure(self, *, evaluation_scope: str) -> dict[str, Any]:
        mappings = []
        for subtype, meta in sorted(self.metadata.items()):
            for name in sorted(self.fields):
                meaning = name[5:] if name.startswith("date.") and not name.startswith("date.attrs.") else None
                key = DATE_MEANINGS.get(meaning) if meaning else f"{name}_from"
                attr = name[len("date.attrs."):] if name.startswith("date.attrs.") else meta.get(key)
                if attr:
                    mappings.append({"subtype": subtype, "field": name, "attr": attr,
                                     **({"meaning": meaning} if meaning else {})})
        out: dict[str, Any] = {
            "where_fields": mappings[:100], "where_fields_truncated": len(mappings) > 100,
            "where_evaluation_scope": evaluation_scope,
            "where_missing": {name: len(ids) for name, ids in sorted(self.missing.items())},
            "where_unparseable": {
                name: max(len(self.unparseable.get(name, ())), self.unparseable_counted.get(name, 0))
                for name in sorted(set(self.unparseable) | set(self.unparseable_counted))
            },
        }
        if self.utc_bounds or self.utc_stored:
            # The same note the set operators give (`date_conditions.
            # DateConditionReport`).
            out["dates_read_as_utc"] = {
                **({"bounds": sorted(self.utc_bounds)} if self.utc_bounds else {}),
                **({"stored": sorted(self.utc_stored)} if self.utc_stored else {}),
            }
        return out


def load_where_context(conn: Any, session: Any, predicate: Optional[Mapping[str, Any]],
                       *, subtypes: Optional[Sequence[str]] = None,
                       fold_parents: bool = False) -> Optional[WhereContext]:
    """The catalog field declarations a `where` reads, for `subtypes`.

    `fold_parents` (a records view or grep, issue #3466): a passage in scope matches
    through its record, which can lie outside `subtypes` (a content profile
    without `document`), so the declarations of the record types whose
    passage child is in scope are loaded too. The "no scoped subtype
    declares it" refusal still judges `subtypes` alone."""
    names = {name for name in _fields(predicate) if name in {"title", "path"} or name.startswith("date.")}
    if not names:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT name, metadata FROM okg.subtypes WHERE catalog_version_id = %s",
                    (session.catalog_version_id,))
        every = {str(name): meta or {} for name, meta in cur.fetchall()}
    scoped = {name: meta for name, meta in every.items()
              if subtypes is None or name in subtypes}
    metadata = dict(scoped)
    if fold_parents and subtypes is not None:
        for name, meta in every.items():
            rollup = meta.get("passage_rollup")
            if isinstance(rollup, Mapping) and rollup.get("child_subtype") in subtypes:
                metadata.setdefault(name, meta)
    for name in names:
        if name.startswith("date.attrs."):
            continue  # Explicit attributes have the existing attrs availability contract.
        key = DATE_MEANINGS[name[5:]] if name.startswith("date.") else f"{name}_from"
        if not any(meta.get(key) for meta in scoped.values()):
            _refuse(f"no scoped subtype declares {name!r}",
                    data={"field": name, "reason": "field_not_declared"},
                    fix_hint="declare the catalog field, select a declaring subtype, or use attrs.<key>")
    context = WhereContext(metadata, names)
    _note_utc_bounds(context, predicate)
    return context


def _note_utc_bounds(context: WhereContext, predicate: Optional[Mapping[str, Any]]) -> None:
    """Name the date fields with an operand that has no time zone. Decided
    by the operands alone, as the set operators decide it, so the note does
    not depend on whether any row was compared."""
    if not predicate:
        return
    op = predicate.get("op")
    if op in {"and", "or"}:
        for clause in predicate["clauses"]:
            _note_utc_bounds(context, clause)
    elif op == "not":
        _note_utc_bounds(context, predicate["clause"])
    elif str(predicate.get("field", "")).startswith("date.") and op != "exists":
        try:
            if leaf_windows(predicate, operator="search")[1]:
                context.utc_bounds.add(predicate["field"])
        except InvalidOperatorArguments:
            # normalize_where admits only operands the shared rule reads.
            raise AssertionError(f"unnormalized date operand in {predicate!r}") from None


_SCOPE_FIX_HINT = (
    'scope={"record": "<record id>"}, {"records": [...]} or '
    '{"children_of": "<record id>"}'
)


@dataclass(frozen=True)
class ChildrenScope:
    """`scope={"children_of": id, "edge_type": optional}` (issue #3363).

    The records `parent` reaches by its outgoing `edge_type` edges (unset:
    the parent's declared passage relation), never the parent itself.
    """

    parent: str
    edge_type: Optional[str] = None

    def applied(self) -> dict[str, str]:
        """The scope as the caller named it: a hidden parent and a missing
        one must read the same, so nothing resolved is echoed."""
        out = {"children_of": self.parent}
        if self.edge_type is not None:
            out["edge_type"] = self.edge_type
        return out


def _scope_mapping(scope: Any) -> Optional[Mapping[str, Any]]:
    if scope is None or scope == {}:
        return None
    if hasattr(scope, "model_dump"):
        scope = scope.model_dump(exclude_none=True)
    if not isinstance(scope, Mapping):
        raise SearchScopeUnsupported(
            "search `scope` must be an object",
            data={"got": type(scope).__name__},
            fix_hint=_SCOPE_FIX_HINT,
        )
    return scope or None


def assert_children_scope_served(
    children: Optional[ChildrenScope], *, rank_by: Optional[str],
    view: Optional[str], retain: bool,
) -> None:
    """`children_of` is served on lexical cards ranked `self`, and refused
    by name elsewhere, before any database work. `records` would fold a
    child into its declared parent, which can lie outside the scope (the
    excluded parent itself) or be hidden; a list, a windows read and a
    result reference hold records."""
    if children is None:
        return
    if rank_by not in (None, "self"):
        raise SearchScopeUnsupported(
            f"search: scope.children_of ranks each child by its own text; "
            f"rank_by={rank_by!r} is not served with it",
            data={"scope": "children_of", "rank_by": rank_by},
            fix_hint="drop rank_by, or pass rank_by='self'",
        )
    if view not in (None, "cards") or retain:
        raise SearchScopeUnsupported(
            "search: scope.children_of is served on cards only, not "
            + (f"view={view!r}" if view not in (None, "cards") else "retain"),
            data={"scope": "children_of", "view": view, "retain": retain},
            fix_hint="drop view and retain, or scope the records by id",
        )


def normalize_children_scope(scope: Any) -> Optional[ChildrenScope]:
    """`scope={"children_of": id[, "edge_type": name]}` -> the parent scope;
    None for any other scope (`normalize_record_scope` judges those)."""
    scope = _scope_mapping(scope)
    if scope is None or "children_of" not in scope:
        return None
    keys = set(scope)
    parent, edge_type = scope.get("children_of"), scope.get("edge_type")
    if (
        keys <= {"children_of", "edge_type"}
        and isinstance(parent, str) and parent
        and (edge_type is None or (isinstance(edge_type, str) and edge_type))
    ):
        return ChildrenScope(parent=parent, edge_type=edge_type)
    raise SearchScopeUnsupported(
        "search `scope.children_of` takes one record id and an optional "
        "`edge_type`, nothing else",
        data={"keys": sorted(str(k) for k in keys)},
        fix_hint='scope={"children_of": "<record id>", "edge_type": "<edge type>"}',
    )


def normalize_record_scope(scope: Any) -> Optional[list[str]]:
    """`scope={"record": id}` / `{"records": [...]}` -> the record ids.

    A `children_of` scope is not a list of named records: it answers None
    here once `normalize_children_scope` has accepted it.
    """
    scope = _scope_mapping(scope)
    if scope is None:
        return None
    if normalize_children_scope(scope) is not None:
        return None
    keys = set(scope)
    if keys == {"record"} and isinstance(scope["record"], str) and scope["record"]:
        return [scope["record"]]
    if keys == {"records"}:
        ids = scope["records"]
        if (
            isinstance(ids, list) and ids and len(ids) <= 100
            and all(isinstance(i, str) and i for i in ids)
        ):
            return sorted(set(ids))
    raise SearchScopeUnsupported(
        "search `scope` takes exactly one of `record` (an id), `records` "
        "(1 to 100 ids) or `children_of` (an id)",
        data={"keys": sorted(str(k) for k in keys)},
        fix_hint=_SCOPE_FIX_HINT,
    )


# ── planning: one predicate, legacy keys lifted ───────────────────────


@dataclass
class WherePlan:
    """What a search call applies, split by where it runs.

    `filter` is the legacy filter mapping the ranked paths read (`subtype`,
    `attr_filters`, `source`), extended with the lifted clauses. `residual`
    is the predicate evaluated on read rows (None when nothing remains).
    `applied` is the whole predicate, legacy arguments included, which
    `describe` renders.
    """

    filter: dict[str, Any]
    residual: Optional[dict[str, Any]]
    applied: Optional[dict[str, Any]]
    lifted: list[dict[str, Any]] = field(default_factory=list)



def invariant_conjuncts(residual: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    """Top-level conjuncts of a residual that read only id/type/source."""
    if residual is None:
        return []
    return [c for c in _conjuncts(residual) if _only_invariant(c)]


def top_conjuncts(predicate: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    """The top-level AND terms of a predicate (itself when not an AND)."""
    return [] if predicate is None else _conjuncts(predicate)


def has_negation(predicate: Mapping[str, Any]) -> bool:
    """Whether a `not` appears anywhere in a predicate."""
    op = predicate.get("op")
    if op == "not":
        return True
    if op in ("and", "or"):
        return any(has_negation(c) for c in predicate["clauses"])
    return False


def names_field(predicate: Optional[Mapping[str, Any]], fld: str) -> bool:
    """Whether any leaf of a predicate reads the canonical field `fld`."""
    if predicate is None:
        return False
    op = predicate.get("op")
    if op in ("and", "or"):
        return any(names_field(c, fld) for c in predicate["clauses"])
    if op == "not":
        return names_field(predicate["clause"], fld)
    return predicate.get("field") == fld


def reads_attrs(predicate: Optional[Mapping[str, Any]]) -> bool:
    """Whether a predicate reads any attribute (and so a masked value)."""
    if predicate is None:
        return False
    op = predicate.get("op")
    if op in ("and", "or"):
        return any(reads_attrs(c) for c in predicate["clauses"])
    if op == "not":
        return reads_attrs(predicate["clause"])
    name = str(predicate.get("field") or "")
    return name.startswith(("attrs", "date.")) or name in {"title", "path"}


def and_clauses(*predicates: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """AND of the non-empty predicates, flattened one level."""
    clauses: list[dict[str, Any]] = []
    for predicate in predicates:
        if predicate is not None:
            clauses.extend(_conjuncts(predicate))
    return _join(clauses)


def _conjuncts(predicate: dict[str, Any]) -> list[dict[str, Any]]:
    if predicate.get("op") == "and":
        out: list[dict[str, Any]] = []
        for clause in predicate["clauses"]:
            out.extend(_conjuncts(clause))
        return out
    return [predicate]


def _only_invariant(predicate: dict[str, Any]) -> bool:
    op = predicate.get("op")
    if op in ("and", "or"):
        return all(_only_invariant(c) for c in predicate["clauses"])
    if op == "not":
        return _only_invariant(predicate["clause"])
    return predicate.get("field") in INVARIANT_FIELDS


def _scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, bool)) and not (
        isinstance(value, str) and "\x00" in value
    )


def _legacy_clauses(filter: Mapping[str, Any]) -> list[dict[str, Any]]:
    clauses: list[dict[str, Any]] = []
    if filter.get("subtype") is not None:
        clauses.append({"op": "eq", "field": "type", "value": filter["subtype"]})
    if filter.get("source") is not None:
        clauses.append({"op": "eq", "field": "source", "value": filter["source"]})
    for key, value in (filter.get("attr_filters") or {}).items():
        clauses.append({"op": "eq", "field": f"attrs.{key}", "value": value})
    return clauses


def _join(clauses: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not clauses:
        return None
    if len(clauses) == 1:
        return dict(clauses[0])
    return {"op": "and", "clauses": [dict(c) for c in clauses]}


def masking_possible(deployment_name: str) -> bool:
    """Whether a Nomos bundle can hide or mask a value this deployment serves.

    Under Nomos `off` the read filter returns rows unchanged
    (`filter_node_rows_for_session`), so a stored value IS the read value and
    an attribute clause may shape SQL bounds without deciding anything a
    reader cannot see. `audit` and `enforce` evaluate the bundle, so any
    attribute may be masked (design 3.3 rule 3, D4). The same resolver the
    read filter asks; an unreadable manifest raises rather than reading as off.
    """
    return bool(_runtime_enforcement.deployment_requires_nomos_runtime(deployment_name))


def plan_where(
    filter: Optional[Mapping[str, Any]],
    where: Optional[dict[str, Any]],
    *,
    lift_source: bool,
    lift_type: bool = True,
) -> WherePlan:
    """Merge the old arguments and `where` into one plan.

    A top-level `eq` conjunct of `where` on `type`, on `source` (when the
    method's ranked read serves `filter.source`, i.e. `lift_source`) or on
    an `attrs.<key>` with a scalar value moves into the matching legacy key
    when that key is still free, under every Nomos mode. That is the whole
    equivalence argument (D9): `where={"op": "eq", "field": "type", "value":
    "ticket"}` and `subtype_filter="ticket"`, or an `attrs.path` equality and
    `attr_filters={"path": ...}`, then reach exactly the same code. The legacy
    `attr_filters` shape the ranked bounds and the lookup's SQL page from
    stored values, as on dev; under Nomos `audit`/`enforce` a stored value on
    a maskable attribute can therefore shape those bounds for both spellings
    alike (deferred to design slice 7, issue #3208). A conjunct that
    conflicts with a legacy key already set (a second subtype, a different
    value for the same attribute) stays in the residual and ANDs with it, as
    the caller wrote.

    `lift_type=False` (grep) keeps `type` equality in the residual: grep's
    `subtype` filter changes the result unit (a passage subtype lists
    passages), while a `where` clause judges the record, so `type = X` must
    behave as `type in [X]` does. A passage subtype is the exception: grep
    moves it into `subtype` itself (`lift_passage_type`), since judging the
    record by it drops every passage.
    """
    merged = dict(filter or {})
    legacy = _legacy_clauses(merged)
    if where is None:
        return WherePlan(filter=merged, residual=None, applied=_join(legacy))
    residual: list[dict[str, Any]] = []
    lifted: list[dict[str, Any]] = []
    attr_filters = dict(merged.get("attr_filters") or {})
    for clause in _conjuncts(where):
        if clause.get("op") == "eq":
            fld, value = clause["field"], clause.get("value")
            if (
                fld == "type" and lift_type and merged.get("subtype") is None
                and isinstance(value, str)
            ):
                merged["subtype"] = value
                lifted.append(clause)
                continue
            if (
                fld == "source" and lift_source and merged.get("source") is None
                and isinstance(value, str)
            ):
                merged["source"] = value
                lifted.append(clause)
                continue
            # Not a null or the mask token: the legacy attr_filters compare
            # two-valued (`attrs.get(k) == value`), so a key the read filter
            # dropped would equal null and a masked value would equal the
            # token. Those stay in the three-valued residual (D5).
            if (
                fld.startswith("attrs.") and _scalar(value)
                and value is not None and value != MASK_TOKEN
            ):
                key = fld.split(".", 1)[1]
                if key not in attr_filters and "\x00" not in key:
                    attr_filters[key] = value
                    lifted.append(clause)
                    continue
        residual.append(clause)
    if attr_filters:
        merged["attr_filters"] = attr_filters
    return WherePlan(
        filter=merged,
        residual=_join(residual),
        applied=_join(legacy + [*lifted, *residual]),
        lifted=lifted,
    )


def lift_passage_type(
    filter: Mapping[str, Any],
    is_passage_type: Callable[[str], bool],
) -> Optional[dict[str, Any]]:
    """Grep's filter with a `where` type clause that names a passage subtype
    moved into `subtype`, or None when there is none to move (#3666).

    `plan_where(lift_type=False)` keeps grep's `type` clauses in the
    residual, where they judge the record a passage folds into. A passage
    subtype (`document_chunk`) is never the type of a record a passage
    folds into, so judged that way every passage drops and the call answers
    "0 of 0" for words the passages hold. Named as the type, the caller
    means the passages themselves: what `subtype_filter` lists. So a
    top-level `type = X` or `type in [X]` (one value) with `X` a passage
    subtype moves into `subtype` when that key is free; the rest of the
    `where` stays the residual and judges the passages."""
    residual = filter.get("where")
    if residual is None or filter.get("subtype") is not None:
        return None
    clauses = top_conjuncts(dict(residual))
    for at, clause in enumerate(clauses):
        if clause.get("field") != "type":
            continue
        if clause.get("op") == "eq":
            value = clause.get("value")
        elif clause.get("op") == "in" and len(clause.get("values") or ()) == 1:
            value = clause["values"][0]
        else:
            continue
        if not isinstance(value, str) or not is_passage_type(value):
            continue
        rest = _join([c for i, c in enumerate(clauses) if i != at])
        lifted = {k: v for k, v in filter.items() if k != "where"}
        lifted["subtype"] = value
        if rest is not None:
            lifted["where"] = rest
        return lifted
    return None


def profile_subtypes(predicate: Optional[Mapping[str, Any]]) -> tuple[str, ...]:
    """The subtypes a `where` names for PROFILE decisions on grep.

    A top-level `type` equality and a top-level `type in [...]` both name
    subtypes, as `subtype_filter` does (D9): a deployment default profile
    then does not apply, and an explicit profile that leaves one out refuses
    `subtype_outside_profile`. Grep never lifts either into its subtype
    filter (that filter changes the result unit, see `plan_where`), so the
    two spellings must name the same subtypes here. Lexical keeps its own
    rule: only the equality, which it lifts. A `type` under `or` or `not`
    names nothing.
    """
    names: list[str] = []
    for clause in top_conjuncts(dict(predicate) if predicate is not None else None):
        if clause.get("field") != "type":
            continue
        if clause.get("op") == "eq" and isinstance(clause.get("value"), str):
            values = [clause["value"]]
        elif clause.get("op") == "in":
            values = [v for v in clause.get("values") or () if isinstance(v, str)]
        else:
            continue
        names.extend(v for v in values if v not in names)
    return tuple(names)


def admitted_types(predicate: Optional[Mapping[str, Any]]) -> Optional[frozenset[str]]:
    """The record types a `where` can admit, or None when it names none.

    Read from its top-level `type` equality and `type in [...]` conjuncts
    only (a `type` under `or` or `not` narrows nothing): every row the
    predicate admits has a type in each such conjunct's values, so in their
    intersection. A conjunct holding a non-string value is skipped rather
    than read (it narrows nothing). An empty set means no type passes.
    Grep uses it to keep a selective `where` from spending its page bound
    on rows of other types (Format v10 item G2)."""
    names: Optional[frozenset[str]] = None
    for clause in top_conjuncts(dict(predicate) if predicate is not None else None):
        if clause.get("field") != "type":
            continue
        if clause.get("op") == "eq":
            values = [clause.get("value")]
        elif clause.get("op") == "in":
            values = list(clause.get("values") or ())
        else:
            continue
        if not values or not all(isinstance(v, str) for v in values):
            continue
        names = frozenset(values) if names is None else names & frozenset(values)
    return names


_VIEW_FIELDS = {"id": "node_id", "type": "subtype"}


def to_view_predicate(predicate: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """A canonical predicate in the effective-view field names, or None.

    The query-less `attrs` lookup pages the effective-attribute relation,
    whose stable columns are `node_id` and `subtype` and which has no
    `source`; a clause on `source` cannot narrow it in SQL and is left to
    the read check.
    """
    op = predicate["op"]
    if op in ("and", "or"):
        parts = [to_view_predicate(c) for c in predicate["clauses"]]
        if any(p is None for p in parts):
            return None
        return {"op": op, "clauses": parts}
    if op == "not":
        inner = to_view_predicate(predicate["clause"])
        return None if inner is None else {"op": "not", "clause": inner}
    fld = predicate["field"]
    if fld == "source":
        return None
    out = dict(predicate)
    out["field"] = _VIEW_FIELDS.get(fld, fld)
    return out


# ── the query-less lookup's SQL page ──────────────────────────────────

#: Internal superset leaf for the lookup's paged SQL: true when the stored
#: attribute is a JSON string containing `value`. Rendered only by the
#: server's effective-view predicate renderer; the public `where` op set
#: (operator_models.Predicate, `_LEAVES`) cannot name it.
RAW_TEXT_CONTAINS = "_raw_text_contains"

#: Leaf ops whose SQL on the stable `node_id`/`subtype` columns is exact
#: (two-valued, COALESCEd) and agrees with :func:`evaluate`. Range ops stay
#: out: SQL text order follows the database collation, Python's does not.
_EXACT_STABLE_OPS = frozenset({"eq", "in", "exists", "prefix", "suffix"})

#: Most attribute groups one superset clause may OR together.
_MAX_SUPERSET_GROUPS = 16


def _exact_stable(predicate: Mapping[str, Any], fields: frozenset[str]) -> bool:
    op = predicate.get("op")
    if op in ("and", "or"):
        return all(_exact_stable(c, fields) for c in predicate["clauses"])
    if op == "not":
        return _exact_stable(predicate["clause"], fields)
    return op in _EXACT_STABLE_OPS and predicate.get("field") in fields


def _attr_text_clause(predicate: Mapping[str, Any]) -> bool:
    """A top-level `prefix`/`suffix`/`contains` on `attrs.<key>` with a
    usable string (`contains` with text, issue #3666)."""
    fld, value = predicate.get("field"), predicate.get("value")
    return (
        predicate.get("op") in ("prefix", "suffix", "contains")
        and isinstance(fld, str) and fld.startswith("attrs.") and len(fld) > len("attrs.")
        and "\x00" not in fld
        and isinstance(value, str) and bool(value) and "\x00" not in value
    )


def lookup_sql_exact(
    residual: Optional[dict[str, Any]], *, masking: bool,
) -> bool:
    """Whether :func:`lookup_sql_clauses` pushes every conjunct of `residual`
    to SQL exactly, so a COUNT over the page's own FROM and WHERE counts
    the matches (issue #3666). Only with nothing masked: under access rules
    the read check decides rows the SQL cannot see."""
    if masking:
        return False
    if residual is None:
        return True
    stable = frozenset({"id", "type"})
    return all(
        (_exact_stable(clause, stable) and to_view_predicate(clause) is not None)
        or _attr_text_clause(clause)
        for clause in _conjuncts(residual)
    )


def _catalog_attr(fld: str, meta: Mapping[str, Any]) -> Optional[str]:
    """The stored attribute a catalog field reads on one subtype, or None."""
    key = f"{fld}_from" if fld in ("title", "path") else DATE_MEANINGS[fld[len("date."):]]
    value = meta.get(key)
    return value.strip() if isinstance(value, str) and value.strip() else None


def _attr_superset(
    fld: str, op: str, predicate: Mapping[str, Any], attr: str,
) -> Optional[dict[str, Any]]:
    """A clause on stored `attrs.<attr>` admitting every row the leaf can.

    Title and path read the stripped string (title its first line), so a row
    whose read value equals, starts or ends with an operand stores a string
    CONTAINING it. Every other op, and every date op, needs a value, so the
    stored key must exist. A null operand (`eq null`, `in [.., null]`) admits
    rows with no value and is not narrowed.
    """
    target = f"attrs.{attr}"
    if fld in ("title", "path") and op == "contains":
        # Text inside the read value is text inside the stored string it was
        # read from, compared alike (ASCII case ignored), so the same test on
        # the stored attribute admits every such row.
        return {"op": "contains", "field": target, "value": predicate["value"]}
    if fld in ("title", "path") and op in ("eq", "prefix", "suffix", "in"):
        operands = predicate["values"] if op == "in" else [predicate.get("value")]
        if not operands or not all(isinstance(v, str) for v in operands):
            return None
        parts = [{"op": RAW_TEXT_CONTAINS, "field": target, "value": v} for v in operands]
        return parts[0] if len(parts) == 1 else {"op": "or", "clauses": parts}
    if op == "eq" and predicate.get("value") is None:
        return None
    if op == "in" and any(v is None for v in predicate["values"]):
        return None
    return {"op": "exists", "field": target}


def lookup_sql_clauses(
    residual: Optional[dict[str, Any]],
    context: Optional[WhereContext],
    *,
    masking: bool,
    explicit_subtypes: bool,
) -> list[dict[str, Any]]:
    """Extra clauses for the query-less lookup's paged SQL, view field names.

    The lookup pages the effective-attribute relation in SQL and checks the
    whole residual on the read rows afterwards. A residual clause kept out of
    SQL is checked only on whichever `limit` rows the page happened to hold,
    so on a graph larger than `limit` a match can sit on a later page while
    this one answers empty. These clauses narrow the page:

    * top-level conjuncts on `type` (and on `id` when nothing can be masked)
      whose SQL is exact (D4). Under Nomos `audit`/`enforce` an `id` clause
      stays out: the paged SQL runs before the read filter, so whether a
      hidden id exists would show as `truncated`;
    * when nothing can be masked, each top-level string `prefix`/`suffix`/
      `contains` on an `attrs.<key>`, which SQL decides exactly (issue #3666);
    * when nothing can be masked (design 3.3 rule 6), a superset clause per
      top-level conjunct on catalog `title`/`path` or a date field, on the
      stored attribute each scoped subtype declares, grouped by attribute.
      Title needs an explicit subtype scope whose every type declares it (an
      undeclared type falls back to a fixed attribute list); `path` and
      named dates read nothing on an undeclared type, so its rows cannot
      match.

    `source` has no column on that relation and stays residual. Each clause
    is exact or a superset; :func:`evaluate` still decides every row.
    """
    if residual is None:
        return []
    conjuncts = _conjuncts(residual)
    stable = frozenset({"type"} if masking else {"id", "type"})
    out: list[dict[str, Any]] = []
    for clause in conjuncts:
        if _exact_stable(clause, stable):
            view = to_view_predicate(clause)
            if view is not None:
                out.append(view)
    if masking:
        return out
    # Issue #3666: a string `prefix`/`suffix` on a stored attribute is exact
    # in SQL (a string value only, as :func:`_leaf` reads it), so it narrows
    # the page instead of being checked on whichever `limit` rows came first.
    # It needs no catalog context, so it is pushed before that return.
    out.extend(clause for clause in conjuncts if _attr_text_clause(clause))
    if context is None:
        return out
    scope, typed = _lookup_scope(conjuncts, context)
    explicit_subtypes = explicit_subtypes or typed
    for clause in conjuncts:
        fld, op = clause.get("field"), str(clause.get("op"))
        if not isinstance(fld, str):
            continue
        if fld.startswith("date.attrs."):
            superset = _attr_superset(fld, op, clause, fld[len("date.attrs."):])
            if superset is not None:
                out.append(superset)
            continue
        if fld not in ("title", "path") and not fld.startswith("date."):
            continue
        groups: dict[str, list[str]] = {}
        undeclared = False
        for subtype in sorted(scope):
            attr = _catalog_attr(fld, context.metadata.get(subtype) or {})
            if attr is None:
                undeclared = True
            else:
                groups.setdefault(attr, []).append(subtype)
        if not groups or len(groups) > _MAX_SUPERSET_GROUPS:
            continue
        if fld == "title" and (undeclared or not explicit_subtypes):
            continue
        parts: list[dict[str, Any]] = []
        for attr, subtypes in sorted(groups.items()):
            superset = _attr_superset(fld, op, clause, attr)
            if superset is None:
                parts = []
                break
            if len(groups) > 1:
                superset = {"op": "and", "clauses": [
                    {"op": "in", "field": "subtype", "values": subtypes}, superset,
                ]}
            parts.append(superset)
        if parts:
            out.append(parts[0] if len(parts) == 1 else {"op": "or", "clauses": parts})
    return out


def lookup_date_predicate(
    residual: Optional[dict[str, Any]], context: Optional[WhereContext],
) -> Optional[dict[str, Any]]:
    """The date comparisons that must hold for a lookup row, as a set
    operator's predicate (issue #3666): the top-level `date.<field>`
    conjuncts of the residual (not `exists`), and the record types a
    top-level `type` clause names. None when there is none.

    The query-less lookup pages in SQL and checks `where` on the rows it
    read. With only "the attribute exists" in SQL for a date clause, a page
    held whichever `limit` records came first, and a range none of them
    fell in answered an empty page with an unknown count while matching
    records lay further on. The caller compiles this predicate exactly, as
    `filter` does, so the page holds the records the dates select."""
    if residual is None or context is None:
        return None
    conjuncts = _conjuncts(residual)
    dated = [
        dict(clause) for clause in conjuncts
        if isinstance(clause.get("field"), str) and clause["field"].startswith("date.")
        and clause.get("op") in ("eq", "in", "lt", "gt", "between")
    ]
    if not dated:
        return None
    scope, _typed = _lookup_scope(conjuncts, context)
    if not scope:
        return None
    # The scoped types, and the exact top-level conjuncts on `id` and
    # `type`: with them the count of values that are not dates describes
    # the records those conditions select, as on the set operators.
    clauses: list[dict[str, Any]] = [
        {"op": "in", "field": "subtype", "values": sorted(scope)},
    ]
    for clause in conjuncts:
        if _exact_stable(clause, frozenset({"id", "type"})):
            view = to_view_predicate(clause)
            if view is not None:
                clauses.append(view)
    return {"op": "and", "clauses": [*clauses, *dated]}


def _lookup_scope(
    conjuncts: Sequence[dict[str, Any]], context: WhereContext,
) -> tuple[set[str], bool]:
    """The subtypes a lookup row may have: the context's, narrowed by every
    top-level `type` eq/in; and whether such a clause named them."""
    scope = set(context.metadata)
    typed = False
    for clause in conjuncts:
        if clause.get("field") == "type" and clause.get("op") in ("eq", "in"):
            values = clause["values"] if clause["op"] == "in" else [clause.get("value")]
            scope &= {v for v in values if isinstance(v, str)}
            typed = True
    return scope, typed


#: The stored attribute the bounded path-prefix index reads
#: (`node_path_prefix_visible`: `left(attrs->>'path', N)`).
PATH_INDEX_ATTR = "path"


def lookup_path_prefixes(
    residual: Optional[dict[str, Any]],
    context: Optional[WhereContext],
    *,
    masking: bool,
) -> list[str]:
    """The `prefix` operands the query-less lookup may answer through the
    path-prefix index (issue #3194), one per top-level conjunct.

    Only when nothing can be masked (Nomos `off`): under `audit`/`enforce`
    the paged SQL runs before the read filter, so a prefix tested there on
    a hidden path would show in the page facts (the #3208 leak class).
    A clause qualifies when it is a top-level `prefix` on `attrs.path`, or on
    catalog `path` where every scoped subtype that declares a path reads it
    from `attrs.path` (an undeclared subtype reads no path, so its rows
    cannot match). Each operand only narrows the page to a superset; the
    exact SQL clause and :func:`evaluate` still decide every row.
    """
    if masking or residual is None:
        return []
    conjuncts = _conjuncts(residual)
    # A predicate naming no catalog field loads no context; `attrs.path`
    # needs none.
    scope = _lookup_scope(conjuncts, context)[0] if context is not None else set()
    out: list[str] = []
    for clause in conjuncts:
        if clause.get("op") != "prefix":
            continue
        value = clause.get("value")
        if not isinstance(value, str) or not value:
            continue
        fld = clause.get("field")
        if fld == f"attrs.{PATH_INDEX_ATTR}":
            out.append(value)
            continue
        if fld != "path" or context is None:
            continue
        declared = {
            attr for subtype in scope
            if (attr := _catalog_attr("path", context.metadata.get(subtype) or {})) is not None
        }
        if declared == {PATH_INDEX_ATTR}:
            out.append(value)
    return out


def path_candidates_as_array(narrowed: bool, overlay_keys: Iterable[str]) -> bool:
    """Whether the lookup may match its candidate set as
    `target_id = ANY(ARRAY(...))` (issue #3194).

    The set the page reads is the path-prefix set (at most
    `PATH_PREFIX_CANDIDATE_BOUND` ids, after the probe) united with every
    target that has an accepted overlay at this generation on a key the
    predicate reads. Only when those keys are exactly `path` is the second
    part limited to path overlays; any other key adds every overlay target
    for that key, which nothing bounds, and the array form would compute
    that whole set first. Otherwise the lookup keeps `IN`, which still
    narrows by the path set."""
    return narrowed and set(overlay_keys) == {PATH_INDEX_ATTR}


# ── three-valued evaluation on read rows (D5) ─────────────────────────


def _row_value(row: Mapping[str, Any], fld: str, context: Optional[WhereContext] = None) -> tuple[bool, Any]:
    """`(known, value)` of a field on a read row; known=False when masked."""
    if fld in _ROW_KEY:
        return True, row.get(_ROW_KEY[fld])
    attrs = row.get("attrs") or {}
    if fld in {"title", "path"} or fld.startswith("date."):
        meta = context.metadata.get(str(row.get("subtype") or ""), {}) if context else {}
        if fld.startswith("date.attrs."):
            key = fld[len("date.attrs."):]
            known, raw = _row_value(row, f"attrs.{key}")
            if not known:
                return False, None
            # A string longer than any date form is not a date on any read.
            value = None if isinstance(raw, str) and len(raw) > MAX_DATE_TEXT else parse_date(raw)
            invalid = value is None and raw is not None and raw != ""
        else:
            resolved = (resolve_named_date(meta, attrs, fld[5:]) if fld.startswith("date.")
                        else getattr(handle_fields(meta, attrs), fld))
            if resolved.status == "masked":
                return False, None
            value = resolved.value
            if fld.startswith("date.") and value is not None and len(
                str(attrs.get(resolved.attr) or "")
            ) > MAX_DATE_TEXT:
                # As above: too long to be a date on any read.
                value, resolved = None, None
            if fld.startswith("date.") and value is not None:
                # A day's value is written as its midnight; the precision is
                # what keeps it from reading as an instant.
                value = (datetime.fromisoformat(value), resolved.precision)
            if fld == "title" and value is not None:
                # Display truncation must not change predicate meaning.
                # The resolver still chooses the attr and governs masking.
                raw_title = attrs.get(resolved.attr)
                if isinstance(raw_title, str):
                    value = raw_title.strip().splitlines()[0]
            invalid = resolved is None or resolved.status == "unparseable"
        if value is None and context:
            context.missing.setdefault(fld, set()).add(str(row.get("node_id")))
            if invalid:
                context.unparseable.setdefault(fld, set()).add(str(row.get("node_id")))
        return True, value
    if fld == "attrs":
        return True, attrs
    key = fld.split(".", 1)[1]
    if key in attrs:
        value = attrs[key]
        if value == MASK_TOKEN:
            return False, None
        return True, value
    if attrs.get(REDACTION_MARKER) is True:
        # The read filter drops some withheld keys outright (free text), so
        # on a redacted row an absent key may be a hidden one. Unknown, not
        # absent: `not exists` must not reveal which keys were withheld.
        return False, None
    return True, None


def _compare(value: Any, bound: Any) -> Optional[int]:
    if isinstance(value, bool) or isinstance(bound, bool):
        return None
    if isinstance(value, (int, float)) and isinstance(bound, (int, float)):
        return -1 if value < bound else (1 if value > bound else 0)
    if isinstance(value, str) and isinstance(bound, str):
        return -1 if value < bound else (1 if value > bound else 0)
    return None


def _leaf(row: Mapping[str, Any], predicate: Mapping[str, Any], context: Optional[WhereContext] = None) -> Optional[bool]:
    op = predicate["op"]
    fld = predicate["field"]
    if op == "contains" and isinstance(predicate["value"], Mapping):
        verdicts = [
            _leaf(row, {"op": "eq", "field": f"attrs.{key}", "value": value}, context)
            for key, value in predicate["value"].items()
        ]
        return _and(verdicts)
    known, value = _row_value(row, fld, context)
    if not known:
        return None
    if op == "exists":
        if fld.startswith("attrs."):
            return fld.split(".", 1)[1] in (row.get("attrs") or {})
        return value is not None
    if fld.startswith("date."):
        if value is None:
            return None
        try:
            windows, bound_read_as_utc = leaf_windows(predicate, operator="search")
        except InvalidOperatorArguments:
            # normalize_where admits only operands the shared rule reads.
            raise AssertionError(f"unnormalized date operand in {predicate!r}") from None
        instant, value_read_as_utc = instant_of(value)
        if context is not None:
            if bound_read_as_utc:
                context.utc_bounds.add(fld)
            if value_read_as_utc:
                context.utc_stored.add(fld)
        return any(window.holds(instant) for window in windows)
    if op == "eq":
        return value == predicate.get("value")
    if op == "in":
        return value in predicate["values"]
    if op == "suffix":
        return isinstance(value, str) and value.endswith(predicate["value"])
    if op == "prefix":
        return isinstance(value, str) and value.startswith(predicate["value"])
    if op == "contains":
        # Text inside the read value (#3666), as filter compares it.
        return text_contains(value, predicate["value"])
    if op == "between":
        low = _compare(value, predicate["values"][0])
        high = _compare(value, predicate["values"][1])
        return low is not None and high is not None and low >= 0 and high <= 0
    ordering = _compare(value, predicate.get("value"))
    if ordering is None:
        return False
    return ordering < 0 if op == "lt" else ordering > 0


def _and(verdicts: Sequence[Optional[bool]]) -> Optional[bool]:
    if any(v is False for v in verdicts):
        return False
    if any(v is None for v in verdicts):
        return None
    return True


def _or(verdicts: Sequence[Optional[bool]]) -> Optional[bool]:
    if any(v is True for v in verdicts):
        return True
    if any(v is None for v in verdicts):
        return None
    return False


def evaluate(row: Mapping[str, Any], predicate: Optional[Mapping[str, Any]], context: Optional[WhereContext] = None) -> Optional[bool]:
    """Kleene three-valued truth of a canonical predicate on one read row."""
    if predicate is None:
        return True
    op = predicate["op"]
    if op == "and":
        return _and([evaluate(row, c, context) for c in predicate["clauses"]])
    if op == "or":
        return _or([evaluate(row, c, context) for c in predicate["clauses"]])
    if op == "not":
        inner = evaluate(row, predicate["clause"], context)
        return None if inner is None else not inner
    return _leaf(row, predicate, context)


def matches(row: Mapping[str, Any], predicate: Optional[Mapping[str, Any]], context: Optional[WhereContext] = None) -> bool:
    """Whether a row passes: only a TRUE verdict admits it (unknown excludes)."""
    return evaluate(row, predicate, context) is True


# ── policy-invariant SQL (D4) ─────────────────────────────────────────


def invariant_sql(
    conjuncts: Sequence[Mapping[str, Any]],
    params: dict[str, Any],
    *,
    columns: Mapping[str, str],
    prefix: str = "w",
) -> list[str]:
    """SQL for clauses on id/type/source only; each is also rechecked."""
    out: list[str] = []
    for clause in conjuncts:
        out.append(_sql(clause, params, columns=columns, prefix=prefix))
    return out


def compile_where(
    predicate: Mapping[str, Any] | None, *, columns: Mapping[str, str],
    prefix: str = "w", params: Mapping[str, Any] | None = None,
) -> CompiledPredicate:
    """Shared prefilter over positively invariant fields; full residual otherwise.

    Actual adapter membership determines which columns are available. Raw attrs,
    canonical title/path and typed dates never enter this invariant mapping.
    """
    invariant_columns = {key: value for key, value in columns.items() if key in INVARIANT_FIELDS}
    mapping_identity = hashlib.sha256(json.dumps(
        {"lowerer": "string_field_v1", "columns": invariant_columns},
        sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()
    return compile_read_predicate(
        predicate, leaf_lowerer=string_field_lowerer(invariant_columns),
        field_map_identity=mapping_identity, prefix=prefix, params=params,
    )


def _sql(
    predicate: Mapping[str, Any],
    params: dict[str, Any],
    *,
    columns: Mapping[str, str],
    prefix: str,
) -> str:
    compiled = compile_where(predicate, columns=columns, prefix=prefix, params=params)
    if compiled.qualification != "exact":
        raise WhereUnsupported("predicate cannot run on invariant source columns",
                               data={"op": predicate.get("op")}, fix_hint="use reader residual evaluation")
    return compiled.bind_into(params)


# ── the applied predicate, as one short line ──────────────────────────


_BARE_VALUE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./:@-]*")
_WORDS = frozenset({"and", "or", "not", "in", "between", "exists", "prefix",
                    "suffix", "contains", "true", "false", "null"})


def _value_text(value: Any) -> str:
    """A value bare when it is one plain token, else as JSON."""
    if (
        isinstance(value, str) and _BARE_VALUE.fullmatch(value)
        and value.casefold() not in _WORDS
        and not value.lstrip("-").replace(".", "", 1).isdigit()
    ):
        return value
    if isinstance(value, list):
        return "[" + ", ".join(_value_text(v) for v in value) + "]"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def describe(predicate: Optional[Mapping[str, Any]], *, limit: int = 240) -> Optional[str]:
    """`type = "ticket" and attrs.path prefix "slack/"`, or None."""
    if predicate is None:
        return None
    text = _describe(predicate, top=True)
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


def _describe(predicate: Mapping[str, Any], *, top: bool = False) -> str:
    op = predicate["op"]
    if op in ("and", "or"):
        inner = f" {op} ".join(_describe(c) for c in predicate["clauses"])
        return inner if top else f"({inner})"
    if op == "not":
        return f"not {_describe(predicate['clause'])}"
    fld = predicate["field"]
    if op == "exists":
        return f"{fld} exists"
    if op == "eq":
        return f"{fld} = {_value_text(predicate.get('value'))}"
    if op in ("lt", "gt"):
        sign = "<" if op == "lt" else ">"
        return f"{fld} {sign} {_value_text(predicate.get('value'))}"
    if op in ("in", "between"):
        return f"{fld} {op} {_value_text(predicate['values'])}"
    return f"{fld} {op} {_value_text(predicate.get('value'))}"
