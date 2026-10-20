"""`source` conditions on `search`: a repository name, and an empty page.

Issue #3666. Graph records carry a source id of the form `<repo>.<family>`
(`<repo>.doc-corpus`, `<repo>.git-history`), and agents write the repository
name alone (`where={"source": "<repo>"}`). Two things follow:

* :func:`read_repo_names`: a top-level `source` equality whose value is no
  source id but is the part before the first `.` of one or more source ids
  is read as `source in [those ids]`, with an `adjusted` note naming them.
  An exact source id is never rewritten.
* :func:`source_hint`: when a search with a top-level `source` `eq` or
  `contains` still returns no rows, the reply names up to
  `SOURCE_HINT_LIMIT` source ids that hold the given text (ASCII case
  ignored, as `contains` compares), or says none does.

Both read the names `search_arguments.load_source_names` states: with
access rules off the count rollup at the pin and the producer registry;
under access rules the producer registry only, which is policy state and not
derived from rows. No graph row is read here.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Optional, Sequence

from okg.substrate.mcp.operator_sql import ascii_casefold, text_contains

__all__ = [
    "RULE_SOURCE_REPO_AS_SOURCES",
    "SOURCE_HINT_LIMIT",
    "read_repo_names",
    "source_conditions",
    "source_hint",
]

RULE_SOURCE_REPO_AS_SOURCES = "source_repo_as_sources"
#: Most source ids one hint names; the rest are counted.
SOURCE_HINT_LIMIT = 5
#: Most source ids one repository name is read as; a larger set is left
#: as sent (the hint then names some of them).
MAX_REPO_SOURCES = 50
#: A name a note may echo on one line of the text reply.
_PLAIN_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:/\-]{0,95}\Z")


def _conjuncts(where: Optional[Mapping[str, Any]]) -> list[Any]:
    if not where:
        return []
    if where.get("op") == "and" and isinstance(where.get("clauses"), list):
        return list(where["clauses"])
    return [where]


def source_conditions(where: Optional[Mapping[str, Any]]) -> list[tuple[str, str]]:
    """`(op, text)` for each top-level `source` `eq` or `contains` with text.

    `where` is canonical (`search_where.normalize_where`). A clause under
    `or` or `not` is not read: an empty page says nothing about it alone."""
    out: list[tuple[str, str]] = []
    for clause in _conjuncts(where):
        if not isinstance(clause, Mapping) or clause.get("field") != "source":
            continue
        op, value = clause.get("op"), clause.get("value")
        if op in ("eq", "contains") and isinstance(value, str) and value:
            out.append((str(op), value))
    return out


def read_repo_names(
    where: Optional[Mapping[str, Any]], names: Iterable[str],
) -> tuple[Optional[dict[str, Any]], list[dict[str, str]]]:
    """`where` with each top-level `source = <repo>` read as `source in
    [<repo>.<...>, ...]`, and the `adjusted` notes that say so. Pure.

    Read only when `<repo>` is no source id in `names` and is the part
    before the first `.` of at least one; every id it is read as is a plain
    name, and there are at most `MAX_REPO_SOURCES`."""
    if not where:
        return (dict(where) if where else None), []
    known = frozenset(names)
    clauses = _conjuncts(where)
    notes: list[dict[str, str]] = []
    out: list[Any] = []
    for clause in clauses:
        value = clause.get("value") if isinstance(clause, Mapping) else None
        if not (
            isinstance(clause, Mapping) and clause.get("field") == "source"
            and clause.get("op") == "eq" and isinstance(value, str) and value
            and "." not in value and value not in known and _PLAIN_NAME.match(value)
        ):
            out.append(clause)
            continue
        ids = sorted(name for name in known if name.split(".", 1)[0] == value and "." in name)
        if not ids or len(ids) > MAX_REPO_SOURCES or not all(_PLAIN_NAME.match(i) for i in ids):
            out.append(clause)
            continue
        out.append({"op": "in", "field": "source", "values": ids})
        notes.append({
            "rule": RULE_SOURCE_REPO_AS_SOURCES,
            "sent": f"where source = `{value}`",
            "served": "where source in [" + ", ".join(f"`{i}`" for i in ids) + "]",
        })
    if not notes:
        return dict(where), []
    if len(out) == 1:
        return dict(out[0]), notes
    return {"op": "and", "clauses": out}, notes


def source_hint(
    conditions: Sequence[tuple[str, str]], names: Iterable[str],
) -> Optional[list[dict[str, Any]]]:
    """One entry per condition: the source ids holding its text, or None
    when there is no condition. `names` empty means no list was available,
    and each entry says so instead of claiming that no source matches."""
    if not conditions:
        return None
    ids = sorted(name for name in set(names) if _PLAIN_NAME.match(name))
    if not ids:
        return [
            {"op": op, "sent": text, "source_ids": None,
             "unavailable": "this server lists no source ids"}
            for op, text in conditions
        ]
    out: list[dict[str, Any]] = []
    for op, text in conditions:
        holding = [source for source in ids if text_contains(source, text)]
        entry: dict[str, Any] = {
            "op": op, "sent": text, "source_ids": holding[:SOURCE_HINT_LIMIT],
        }
        if len(holding) > SOURCE_HINT_LIMIT:
            entry["more"] = len(holding) - SOURCE_HINT_LIMIT
        if not holding:
            entry["note"] = "no source id this server lists holds this text"
        elif op == "eq" and text not in ids:
            exact = [s for s in holding if ascii_casefold(s) == ascii_casefold(text)]
            entry["note"] = (
                "no source id is exactly this text"
                + ("; one differs only in letter case" if exact else "")
                + "; write one of these, or use contains"
            )
        out.append(entry)
    return out
