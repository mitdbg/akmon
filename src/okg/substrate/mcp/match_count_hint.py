"""The aggregate call that counts each distinct text a grep regex matches
(issue #3666, Format v10 item C).

Counting how many records hold each distinct text a regex matches takes one
call, `aggregate(where={..., "text": {"op": "matches", "value":
"re:/<regex>/"}}, by=["match"], metrics=["count"])`: one group per distinct
matched text, with the records holding it and its occurrences. A grep with
that regex returns a cut page of passages, from which the per-match counts
cannot be read. A grep page whose query holds a regex and whose rows are
cut therefore names that aggregate call, built from the caller's own
condition and regex, over the same records.

The call is named only where the deployment would serve it
(:func:`count_call_offerable`): under access rules (audit or enforce)
`aggregate` refuses every count and a text condition is refused, so no call
is named there, and none when that mode cannot be read. A regex with no
literal run the trigram index can use is counted only over a record set of
at most `text_match.SCAN_LIMIT`; whether the call's set is that small is
not known when the page is built, so the line says the count may be
refused for scan size.

What "the same records" takes. Grep judges its `where` on each row's
record: a passage row (a `passage_rollup` child) is judged through the
record holding it. Aggregate's text condition reads a record's own text and
its declared passages. So the call counts RECORDS of the types the grep
judged:

* the types the caller's `where` names; else the search profile's types
  (`scoped_subtypes`), else every type, with each passage type replaced by
  the record types that hold it (counting a passage beside its record would
  count one match twice);
* `id`/`node_id`, `type`/`subtype`, `attrs.<key>` and `date.<name>` are the
  same fields on both tools;
* `title` and `path` are catalog fields of search only: each record type
  reads them from the attribute it declares (`title_from`, `path_from`).
  One declared attribute is written as `attrs.<it>`; several are written
  per type (`{"op":"or"}` of type and attribute). A type that declares none
  reads search's title from a fallback list aggregate cannot express: it is
  left out of that clause and the line says so (the count can be lower);
* `source` has no aggregate field: left out and named (the count can be
  higher).

The call is never offered as the count of the grep's set when it is not:

* every part it cannot carry is printed on the same line, with which way
  the count moves;
* when the caller narrowed the grep (a `where`, `path_prefix` or `scope`)
  and NONE of that narrowing carries over, no call is offered at all
  (:func:`count_each_match_call` returns None): a call over every record of
  the profile's types would answer a different question, and its top group
  is often another key. A partly carried call is printed with its warning,
  because the parts that carry (a type and a title prefix, say) usually
  decide the set, and the warning names what is missing.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Sequence

from okg.substrate.mcp.grep_query import GrepQueryInvalid, parse_grep_query

__all__ = [
    "COUNT_EACH_MATCH_KEY",
    "count_call_offerable",
    "count_each_match_call",
    "has_regex_term",
    "record_types",
    "render_count_each_match",
]

#: The payload key a cut grep page carries the call under.
COUNT_EACH_MATCH_KEY = "count_each_match"

#: Search field -> aggregate field, where the two only differ in name.
_SAME = {"id": "node_id", "node_id": "node_id", "type": "type", "subtype": "type"}
#: Search's catalog fields, read through a declared attribute per type.
_CATALOG = ("title", "path")


def count_call_offerable(server: Any) -> bool:
    """Whether `server` would serve an `aggregate` count as offered.

    The server's own decision (`ByteBoundedFastMCP._okg_aggregate_refused`):
    access rules on, or a mode that cannot be read, refuse it. A server
    without that check is never offered the call (no offer, never a refused
    one). A tool profile or allowlist that leaves `aggregate` out removes
    the tool, so a server that does not list it in `_okg_served_tools` is
    never offered the call either (an unknown tool is no call)."""
    served = getattr(server, "_okg_served_tools", None)
    if not isinstance(served, (tuple, list, set, frozenset)) or "aggregate" not in served:
        return False
    refused = getattr(server, "_okg_aggregate_refused", None)
    if not callable(refused):
        return False
    try:
        return not refused()
    except Exception:  # noqa: BLE001 - unknown means no offer.
        return False


def _scan_bound(query: str) -> bool:
    """Whether the regex holds no literal the trigram index can narrow by,
    so aggregate counts it only over a set of at most `SCAN_LIMIT` records."""
    from okg.substrate.mcp import text_match as tm

    try:
        return tm.narrowing_query(parse_grep_query(query)) is None
    except GrepQueryInvalid:
        return True


def has_regex_term(query: Any) -> bool:
    """Whether a grep query holds a positive regex term (`re:/.../`) and no
    `path:` term (which a text condition refuses). Plain words never do."""
    if not isinstance(query, str) or not query.strip():
        return False
    try:
        parsed = parse_grep_query(query)
    except GrepQueryInvalid:
        return False
    if parsed.path_terms:
        return False
    return any(term.kind == "regex" for term in parsed.positive)


def _conjuncts(node: Any) -> list[Any]:
    if isinstance(node, Mapping) and node.get("op") == "and" and isinstance(
        node.get("clauses"), list,
    ):
        return [c for clause in node["clauses"] for c in _conjuncts(clause)]
    return [] if node in (None, {}) else [node]


def _named_types(conjuncts: Sequence[Any]) -> list[str]:
    """The record types a top-level `type` clause names (eq or in)."""
    named: list[str] = []
    for clause in conjuncts:
        if not isinstance(clause, Mapping) or clause.get("field") not in ("type", "subtype"):
            continue
        if clause.get("op") == "eq" and isinstance(clause.get("value"), str):
            named.append(clause["value"])
        elif clause.get("op") == "in" and isinstance(clause.get("values"), list):
            named += [v for v in clause["values"] if isinstance(v, str)]
    return named


def _holders(metadata: Mapping[str, Mapping[str, Any]]) -> dict[str, list[str]]:
    """Passage type -> the record types whose `passage_rollup` holds it."""
    out: dict[str, list[str]] = {}
    for parent, meta in sorted(metadata.items()):
        rollup = (meta or {}).get("passage_rollup")
        child = rollup.get("child_subtype") if isinstance(rollup, Mapping) else None
        if isinstance(child, str) and child:
            out.setdefault(child, []).append(parent)
    return out


def record_types(
    scoped: Optional[Sequence[str]], metadata: Mapping[str, Mapping[str, Any]],
) -> Optional[list[str]]:
    """The record types a grep over `scoped` (None: every type) judges, each
    passage type replaced by the types holding it. None when that is every
    type and no passage type has to be left out (no type clause needed)."""
    holders = _holders(metadata)
    if scoped is None:
        if not holders:
            return None
        return sorted(t for t in metadata if t not in holders)
    out: set[str] = set()
    for subtype in scoped:
        out.update(holders.get(subtype) or [subtype])
    return sorted(out)


def _type_clause(types: Sequence[str]) -> dict[str, Any]:
    if len(types) == 1:
        return {"op": "eq", "field": "type", "value": types[0]}
    return {"op": "in", "field": "type", "values": list(types)}


class _Reading:
    """What a translation read, for the line: catalog fields as attributes,
    and the types a catalog clause had to leave out."""

    def __init__(self) -> None:
        self.read_as: dict[str, str] = {}
        self.left_out: dict[str, list[str]] = {}


def _catalog_leaf(
    clause: Mapping[str, Any], field: str, *, types: Sequence[str],
    metadata: Mapping[str, Mapping[str, Any]], negated: bool, reading: _Reading,
) -> Optional[dict[str, Any]]:
    groups: dict[str, list[str]] = {}
    undeclared: list[str] = []
    for subtype in types:
        value = (metadata.get(subtype) or {}).get(f"{field}_from")
        if isinstance(value, str) and value.strip():
            groups.setdefault(value.strip(), []).append(subtype)
        else:
            undeclared.append(subtype)
    if not groups or (undeclared and negated):
        # Nothing declares the field, or a `not` would let every record of
        # an undeclared type through: not carried.
        return None
    if undeclared:
        reading.left_out[field] = sorted(set(reading.left_out.get(field, [])) | set(undeclared))
    if len(groups) == 1 and not undeclared:
        (attr,) = groups
        reading.read_as[field] = f"attrs.{attr}"
        return {**clause, "field": f"attrs.{attr}"}
    reading.read_as[field] = ", ".join(
        f"attrs.{attr} ({'/'.join(subtypes)})" for attr, subtypes in sorted(groups.items())
    )
    parts = [
        {"op": "and", "clauses": [_type_clause(subtypes), {**clause, "field": f"attrs.{attr}"}]}
        for attr, subtypes in sorted(groups.items())
    ]
    return parts[0] if len(parts) == 1 else {"op": "or", "clauses": parts}


def _translate(
    clause: Any, *, types: Sequence[str], metadata: Mapping[str, Mapping[str, Any]],
    reading: _Reading, negated: bool = False,
) -> Optional[dict[str, Any]]:
    """`clause` with aggregate's field names, or None when it cannot carry."""
    if not isinstance(clause, Mapping):
        return None
    op = clause.get("op")
    if op in ("and", "or") and isinstance(clause.get("clauses"), list):
        parts = [
            _translate(c, types=types, metadata=metadata, reading=reading, negated=negated)
            for c in clause["clauses"]
        ]
        return None if any(p is None for p in parts) else {"op": op, "clauses": parts}
    if op == "not":
        inner = _translate(
            clause.get("clause"), types=types, metadata=metadata, reading=reading,
            negated=not negated,
        )
        return None if inner is None else {"op": "not", "clause": inner}
    field = clause.get("field")
    if not isinstance(field, str):
        return None
    if field in _SAME:
        return {**clause, "field": _SAME[field]}
    if field.startswith(("attrs.", "date.")):
        return dict(clause)
    if field in _CATALOG:
        return _catalog_leaf(
            clause, field, types=types, metadata=metadata, negated=negated, reading=reading,
        )
    return None


def _short(clauses: list[dict[str, Any]]) -> dict[str, Any]:
    """The condition in the short form agents write (`{"<field>": value}`),
    or the canonical `and` when a clause has no short form."""
    out: dict[str, Any] = {}
    for clause in clauses:
        field, op = clause.get("field"), clause.get("op")
        if not isinstance(field, str) or field in out:
            return {"op": "and", "clauses": clauses}
        value = clause.get("value")
        if op == "eq" and not isinstance(value, (list, dict)) and value is not None:
            out[field] = value
        elif op == "in" and isinstance(clause.get("values"), list) and all(
            isinstance(v, (str, int, float)) and not isinstance(v, bool)
            for v in clause["values"]
        ):
            out[field] = list(clause["values"])
        else:
            out[field] = {k: v for k, v in clause.items() if k != "field"}
    return out


def _said(clause: Any) -> str:
    """A part left out, as compact JSON."""
    if isinstance(clause, Mapping) and isinstance(clause.get("field"), str):
        clause = _short([dict(clause)])
    return json.dumps(clause, separators=(",", ":"), ensure_ascii=False)


def count_each_match_call(
    query: str,
    where: Any,
    *,
    metadata: Mapping[str, Mapping[str, Any]],
    scoped: Optional[Sequence[str]] = None,
    path_prefix: Optional[str] = None,
    record_scoped: bool = False,
) -> Optional[dict[str, Any]]:
    """The aggregate call counting each distinct match of `query` over the
    records the grep judged, or None when the caller's narrowing does not
    carry over at all (see the module docstring).

    `metadata` is the catalog metadata of every record type (for
    `title_from`, `path_from` and `passage_rollup`); `scoped` the types the
    grep read when `where` names none (the search profile's, None for
    every type). `path_prefix` is grep's argument of that name, read as a
    `path` prefix; `record_scoped` says grep's `scope` named records.
    """
    conjuncts = _conjuncts(where)
    if isinstance(path_prefix, str) and path_prefix:
        conjuncts.append({"op": "prefix", "field": "path", "value": path_prefix})
    named = _named_types(conjuncts)
    judged = named or record_types(scoped, metadata)
    types = judged or sorted(metadata)
    reading = _Reading()
    kept: list[dict[str, Any]] = []
    dropped: list[str] = []
    for clause in conjuncts:
        served = _translate(clause, types=types, metadata=metadata, reading=reading)
        if served is None:
            dropped.append(_said(clause))
        else:
            kept.append(served)
    if record_scoped:
        dropped.append("scope")
    narrowed = len(conjuncts) + int(record_scoped)
    if narrowed and len(dropped) == narrowed:
        # The caller narrowed the grep and none of it carries: a call over
        # every record of the types would count a different set.
        return None
    if not named and judged:
        kept.insert(0, _type_clause(judged))
    kept.append({"op": "matches", "field": "text", "value": query})
    out: dict[str, Any] = {
        "tool": "aggregate",
        "arguments": {"where": _short(kept), "by": ["match"], "metrics": ["count"]},
    }
    if reading.read_as:
        out["read_as"] = dict(sorted(reading.read_as.items()))
    if dropped:
        out["dropped"] = dropped
    if reading.left_out:
        out["types_left_out"] = dict(sorted(reading.left_out.items()))
    if _scan_bound(query):
        out["scan_bound"] = True
    return out


def render_count_each_match(value: Any) -> Optional[str]:
    """The one line a cut grep page prints for the call, or None."""
    if not isinstance(value, Mapping) or value.get("tool") != "aggregate":
        return None
    arguments = value.get("arguments")
    if not isinstance(arguments, Mapping):
        return None
    compact = {k: json.dumps(v, separators=(",", ":"), ensure_ascii=False)
               for k, v in arguments.items()}
    line = "count each distinct match: aggregate(" + ", ".join(
        f"{k}={v}" for k, v in compact.items()
    ) + ")"
    read_as = value.get("read_as")
    if isinstance(read_as, Mapping) and read_as:
        line += "; " + ", ".join(f"{k} read as {v}" for k, v in read_as.items())
    dropped = value.get("dropped")
    if isinstance(dropped, list) and dropped:
        line += (
            "; NOT the grep's set, it may count more records: aggregate cannot take "
            + ", ".join(str(part) for part in dropped)
        )
    left_out = value.get("types_left_out")
    if isinstance(left_out, Mapping) and left_out:
        line += "; it may count fewer records: " + "; ".join(
            f"{', '.join(map(str, types))} declare no {field} and are left out "
            f"(name the type and its attribute, e.g. attrs.{field})"
            for field, types in left_out.items() if isinstance(types, list)
        )
    if value.get("scan_bound") is True:
        from okg.substrate.mcp.text_match import SCAN_LIMIT

        line += (
            "; it may be refused for scan size: the regex holds no run of 3 or "
            "more letters or digits for the index, so the records the rest of "
            f"the condition names must number at most {SCAN_LIMIT:,}"
        )
    return line
