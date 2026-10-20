"""The served description of the `query` tool (issue #3666, step M12).

An agent offered `query` with a seven-word description used it in a third
of its tries and was refused for one call in thirteen; one sentence naming
the views, their columns and a few fields per type made the same agent the
best okg setup on the benchmark. This module writes that sentence for the
deployment the server serves, from the facts the dataset card already reads
at server start (`okg.substrate.mcp.dataset_card.gather_dataset_facts`):

* the two views and their key columns, how to read a record's text and a
  field, `$$` regexes, and how to fit a reply (fixed text);
* the fields of the largest types, from a bounded sample of rows read only
  when the Nomos mode is `off` (under any other mode `query` over the graph
  is refused, and the card reads no row), each typed by the catalog where it
  declares a type;
* three examples that run as written on this graph: a count per value, a
  join through `okg.v_edges`, and a text regex.

With no facts (no database, the card switched off, or a withheld mode) the
description is the fixed text and three examples that run on any graph.
Field names come from row data, so only identifier-shaped names are stated.
"""
from __future__ import annotations

import re
from typing import Any, Optional

__all__ = [
    "QUERY_FIELDS_MAX_BYTES",
    "publish_query_description",
    "query_examples",
    "render_query_description",
]

#: The generated fields line stops near this many UTF-8 bytes.
QUERY_FIELDS_MAX_BYTES = 480
#: How many types and how many fields per type the fields line names.
QUERY_FIELD_TYPES = 5
QUERY_FIELDS_PER_TYPE = 5

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,39}$")
#: Keys okg itself writes into attrs (access-rule lineage and the like),
#: which describe the record's handling, not the source's fields.
_INTERNAL_KEY_PREFIXES = ("nomos_", "_")
_TYPE_WORDS = {"num": "number", "ts": "date", "bool": "bool"}

_HEAD = (
    "Read-only SQL on one pinned generation: okg.v_nodes(node_id, subtype, "
    "attrs jsonb), okg.v_edges(edge_id, src, dst, edge_type, attrs). Text is "
    "attrs->>'text', a field attrs->>'name'. Regex in $$...$$ (word "
    "boundary \\y, not \\b). Fit max_bytes with LIMIT and left()."
)
_COUNT_ANY = "SELECT subtype, count(*) FROM okg.v_nodes GROUP BY 1"
_JOIN = (
    "SELECT edge_type, d.subtype, count(*) FROM okg.v_edges "
    "JOIN okg.v_nodes d ON node_id = dst GROUP BY 1, 2"
)
_REGEX = (
    "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~* $$fail(ed|ure)$$ LIMIT 20"
)


_FIELDS_LEAD = "Fields by type (sampled at start; cast a typed one, e.g. ::date): "


def _typed(facts: Any) -> dict[str, dict[str, str]]:
    """subtype -> {attr: type word} from the catalog (`typed_attrs`)."""
    out: dict[str, dict[str, str]] = {}
    for subtype, attrs in getattr(facts, "typed_attrs", ()) or ():
        out[str(subtype)] = {str(a): str(t) for a, t in attrs}
    return out


def _sampled(facts: Any) -> list[tuple[str, int, list[tuple[str, int, int]]]]:
    """(subtype, rows sampled, [(key, rows with it, distinct values)]),
    largest type first, identifier-shaped keys only, `text` left out."""
    out = []
    for subtype, sampled, keys in getattr(facts, "attr_keys", None) or ():
        kept = [
            (str(k), int(n), int(d)) for k, n, d in keys
            if _IDENT.match(str(k)) and str(k) != "text"
            and not str(k).startswith(_INTERNAL_KEY_PREFIXES)
        ]
        kept.sort(key=lambda item: (-item[1], item[0]))
        if _IDENT.match(str(subtype)):
            out.append((str(subtype), int(sampled), kept))
    return out


def _count_example(facts: Any) -> str:
    """A count per value of a repeated, few-valued field of the largest type
    that has one; else the count per type."""
    typed = _typed(facts)
    for subtype, sampled, keys in _sampled(facts):
        candidates = [
            (d, k) for k, n, d in keys
            if n * 2 >= sampled and 2 <= d <= 20 and d < n
            and typed.get(subtype, {}).get(k) != "date"
        ]
        if candidates:
            _d, key = min(candidates)
            return (
                f"SELECT attrs->>'{key}', count(*) FROM okg.v_nodes "
                f"WHERE subtype = '{subtype}' GROUP BY 1"
            )
    return _COUNT_ANY


def query_examples(facts: Any = None) -> tuple[str, str, str]:
    """The three examples the description prints, in order."""
    return (_count_example(facts), _JOIN, _REGEX)


def _fields_line(facts: Any) -> Optional[str]:
    typed = _typed(facts)
    parts: list[str] = []
    for subtype, _sampled_rows, keys in _sampled(facts)[:QUERY_FIELD_TYPES]:
        if not keys:
            continue
        names = []
        for key, _n, _d in keys[:QUERY_FIELDS_PER_TYPE]:
            word = typed.get(subtype, {}).get(key)
            names.append(f"{key} ({word})" if word else key)
        part = f"{subtype}: {', '.join(names)}"
        candidate = _FIELDS_LEAD + "; ".join([*parts, part]) + "."
        if len(candidate.encode("utf-8")) > QUERY_FIELDS_MAX_BYTES:
            break
        parts.append(part)
    if not parts:
        return None
    return _FIELDS_LEAD + "; ".join(parts) + "."


def render_query_description(facts: Any = None) -> str:
    """The `query` description for these dataset facts (None: none read)."""
    lines = [_HEAD]
    fields = _fields_line(facts) if facts is not None else None
    if fields:
        lines.append(fields)
    lines[-1] += " Examples:"
    lines.extend(query_examples(facts))
    return "\n".join(lines)


def publish_query_description(mcp: Any, facts: Any) -> None:
    """Set the registered `query` tool's description, when it is registered."""
    manager = getattr(mcp, "_tool_manager", None)
    registered = getattr(manager, "_tools", None)
    if not isinstance(registered, dict) or "query" not in registered:
        return
    registered["query"].description = render_query_description(facts)
