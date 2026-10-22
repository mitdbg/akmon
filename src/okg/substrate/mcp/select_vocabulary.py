"""One `select` vocabulary per read tool (issue #3666, step 3a).

`search` and `expand` each return rows of several shapes: a search row is a
lexical card, a grep card, an attribute match, an alias, a span of the query
text, a list line or a record's windows; an expand row is a linked record, a
link or a path record. Each shape has its own selectable fields, kept where
the shape is built. This module joins them: a tool's vocabulary is every field any of its
shapes can return, mapped to the shapes that return it. Nothing is listed
here by hand except which shapes exist; the field sets are read from the
code that serves them.

:func:`plan_select` decides, from the arguments alone, what one call's
`select` means:

* every name is a field of the call's own shape: the call is left as sent;
* a name is in the tool's vocabulary but another shape returns it: the call
  is served with the fields this shape returns, and one note names the rest
  and what to call for each (never on identifier_mentions, whose rows are
  spans of the query text: a record field asked there is left for the
  tool's refusal, because a page of another kind of row reads as "no such
  record"; a 1-hop expand serves record fields since issue #3666, as the
  linked record's);
* no name is a field of this shape: the call is left for the refusal the
  tool gives it (which lists the shape's fields);
* a search `where` field that is no row field (`title`, `path`, `source`)
  on a search row that has the field holding it: read as that field, with a
  note (:data:`WHERE_NAMES_IN_ROWS`);
* a name is in no shape's vocabulary: the call is left for that same
  refusal. Nothing is guessed. A corrected `select` is worked out only for
  another letter case of a field and for the two second names
  (:data:`SECOND_NAMES`); failing that, on search, it is the caller's
  `select` without the names the rows lack, as the refusal's words say it.
  Either is offered only when the server proves it serves.

Two shapes have fixed rows and took no `select` (a grep list, a record's
windows). For them the `select` is taken out of the call, the page is built
exactly as without it, and each row is then cut to the selected keys.

A continuation is planned exactly as its first page was: the caller repeats
the call's arguments beside a `continuation_token`, so the shape, the
fields and the note are the same. `expand(next=...)` carries no arguments;
its token records the caller's own `select`, from which the tool and the
reply rebuild the same plan (:func:`path_select`).

A 1-hop expand has two shapes and its `select` picks between them (issue
#3666): names that are all the link's own old fields
(`tools.EXPAND_LINK_SELECT`) ask for one row per link, and anything else, or
no `select`, for one row per linked record. On a record row `attrs` is the
record's; the link's are `link.attrs`. A link-row call that selects `attrs`
is served as before with one fixed note (`LINK_ROWS_ATTRS_NOTE`) saying the
rows are links and `attrs` is each link's own.

Access rules are untouched: `select` only ever narrows a row the reader was
already served, and the note names fields of the static vocabulary, never a
value or a record.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

__all__ = [
    "ATTR_PATH_SHAPES",
    "FIXED_ROW_ALWAYS",
    "LINEAGE_SHAPES",
    "LINEAGE_TOKEN",
    "NOT_RECORD_SHAPES",
    "OMITTED_WHY",
    "SEARCH_LINEAGE_METHODS",
    "SEARCH_METHODS",
    "SECOND_NAMES",
    "SERVED_SECOND_NAMES",
    "SHAPE_CALL",
    "SHAPE_METHOD",
    "SHAPE_WORDS",
    "SelectPlan",
    "WHERE_DATES_IN_ROWS",
    "WHERE_NAMES_IN_ROWS",
    "attr_path_key",
    "call_shape",
    "path_select",
    "plan_select",
    "project_rows",
    "served_as_selected",
    "served_second_names",
    "shape_fields",
    "vocabulary",
]

#: `response_shape.LINEAGE_SELECT_TOKEN`, restated so this module imports no
#: operator code at import time; a test pins the two.
LINEAGE_TOKEN = "source_lineage"
#: What `search(method=...)` may name. The other search shapes are views.
SEARCH_METHODS: tuple[str, ...] = (
    "lexical", "grep", "attrs", "semantic", "alias", "identifier_mentions",
)
#: The search methods that return a row's lineage on request (issue #2674).
SEARCH_LINEAGE_METHODS: tuple[str, ...] = ("lexical", "semantic")
#: Per tool, the shapes whose rows can carry the lineage token.
LINEAGE_SHAPES: Mapping[str, tuple[str, ...]] = {
    "search": SEARCH_LINEAGE_METHODS,
    "expand": ("records", "path"),
}

#: The note on a 1-hop call whose `select` names only link fields, `attrs`
#: among them (issue #3666): the rows are links and `attrs` is each link's
#: own, where on a record row it is the linked record's. Fixed words.
LINK_ROWS_ATTRS_NOTE: Mapping[str, str] = {
    "rule": "link_rows_attrs",
    "sent": "select `attrs`",
    "served": (
        "each link's own attrs, because a select of only link fields "
        "returns one row per link"
    ),
}
#: Every `adjusted` rule this module writes: a closed set.
SELECT_NOTE_RULES: tuple[str, ...] = ("select_not_returned", LINK_ROWS_ATTRS_NOTE["rule"])

#: How a note or a refusal names each shape.
SHAPE_WORDS: Mapping[str, str] = {
    "lexical": "method lexical",
    "grep": "method grep",
    "attrs": "method attrs",
    "semantic": "method semantic",
    "alias": "method alias",
    "identifier_mentions": "method identifier_mentions",
    "list": "view list",
    "grep_list": "view list on method grep",
    "windows": "view windows",
    "records": "1-hop record rows",
    "edges": "1-hop link rows",
    "path": "path rows",
}

#: A second name for a field, exact. Used to work out a corrected `select`
#: a refusal may offer; only those in `SERVED_SECOND_NAMES` are served.
SECOND_NAMES: Mapping[str, str] = {"id": "node_id", "type": "subtype"}

#: The second names a `select` is served as their field (issue #3666): agents
#: named `type` for `subtype` in select. Read as the field, with a note, only
#: on rows that have the field and when the name is no field of the tool
#: (:func:`served_second_names`). `id` stays a refusal with a corrected call.
SERVED_SECOND_NAMES: Mapping[str, str] = {"type": "subtype"}

#: `search`'s `where` fields that are no row field, and the one row field
#: that holds each value (a record's title and path are in its `handle`, its
#: producer in `provenance.source`). Agents sent them in `select` in 84 of
#: 126 comp-ops tries (#3666, after M10). A search row that has the field is
#: served it with a note; elsewhere the name is refused as before.
WHERE_NAMES_IN_ROWS: Mapping[str, str] = {
    "title": "handle", "path": "handle", "source": "provenance",
}
#: `where` date names read the same way (issue #3666, lane RD). The handle's
#: date is the one `date.created` reads: the catalog's created date, or its
#: modified date where only that is declared, and the handle says which. Not
#: in the published select text: it names the three fields above.
WHERE_DATES_IN_ROWS: Mapping[str, str] = {"date.created": "handle"}
#: On a grep list row (fixed rows, no attributes), the attribute names
#: agents sent for a record's title and path, read as the handle that holds
#: them (PACT read-names-list-3666; lane X2 arms B and F were refused).
GREP_LIST_ATTR_NAMES: Mapping[str, str] = {"attrs.title": "handle", "attrs.path": "handle"}
#: What each of those row fields holds, as the note says it.
_HOLDS: Mapping[str, str] = {
    "handle": "the title, path and date", "provenance": "the source and trust",
}

#: Shapes whose rows are not records: a link (two ends) and a span of the
#: caller's own query text. A record field asked of one is never served
#: with the rest of the select, and is refused by the tool in its own words.
NOT_RECORD_SHAPES: frozenset[str] = frozenset({"edges", "identifier_mentions"})

#: The call that returns each shape's rows, as a note says it: "call <this>".
SHAPE_CALL: Mapping[str, str] = {
    "lexical": "method lexical",
    "grep": "method grep",
    "attrs": "a where with no query",
    "semantic": "method semantic",
    "alias": "method alias",
    "identifier_mentions": "method identifier_mentions",
    "list": "view list",
    "grep_list": "view list on method grep",
    "windows": "view windows on method grep",
    "records": "a 1-hop expand",
    "edges": "a 1-hop expand that selects only link fields",
    "path": "a path or depth>1 expand",
}

#: The search method a caller names to get each search shape's rows. A note
#: points only at shapes whose method this server serves.
SHAPE_METHOD: Mapping[str, str] = {
    "lexical": "lexical", "grep": "grep", "semantic": "semantic", "alias": "alias",
    "identifier_mentions": "identifier_mentions", "list": "lexical",
    "grep_list": "grep", "windows": "grep",
}

#: Why a shape has no such field, where the reason tells the caller what to
#: do: `(shape, field)` -> one clause. Everything else gets "call <shape>".
OMITTED_WHY: Mapping[tuple[str, str], str] = {
    ("grep", "score"): "grep matches exactly and returns no score",
    ("grep_list", "score"): "grep matches exactly and returns no score",
    ("grep", "recency"): "grep computes no recency",
    ("attrs", "excerpt"): "a lookup with no query has nothing to excerpt",
    ("attrs", "score"): "a lookup with no query ranks nothing",
    ("list", "excerpt"): "a list carries no excerpt",
    ("lexical", "match_windows"): "lexical ranks by terms and returns no match windows",
}

#: Shapes whose rows are records with their attributes: a `select` there may
#: name one attribute as `attrs.<key>`, as `filter` and `map` take it. On
#: search it is each row's `selected` (D6; alias rows refuse it); on a 1-hop
#: record row since issue #3666 (lane RD).
ATTR_PATH_SHAPES: Mapping[str, tuple[str, ...]] = {
    "search": ("lexical", "grep", "attrs", "semantic"),
    "expand": ("path", "records"),
}
#: `date.attrs.<key>`, the `where` name of one stored date attribute.
_DATE_ATTR_PREFIX = "date.attrs."


def attr_path_key(name: Any) -> Optional[str]:
    """The key `attrs.<key>` names, or None for any other name."""
    if isinstance(name, str) and name.startswith("attrs.") and len(name) > len("attrs."):
        return name[len("attrs."):]
    return None


def _attr_paths(tool: str, shape: str, names: Any) -> frozenset[str]:
    """The `attrs.<key>` names among `names` that `shape` serves."""
    if shape not in ATTR_PATH_SHAPES.get(tool, ()):
        return frozenset()
    return frozenset(name for name in names if attr_path_key(name) is not None)


#: Most field names one note spells out, and most it says a call for; the
#: rest are counted, so one note stays inside the `adjusted` size cap.
_MAX_NAMED = 8
_MAX_HINTS = 4
_PLAIN_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:\-]{0,63}\Z")


@functools.lru_cache(maxsize=None)
def shape_fields(tool: str) -> Mapping[str, frozenset[str]]:
    """Each shape of `tool`, in the order agents use them, with its fields.

    Read from the constants the shapes validate and project with, so a
    shape that gains a field gains it here.
    """
    if tool == "search":
        from okg.substrate.mcp.grep_search import GREP_FIELDS
        from okg.substrate.mcp.record_window_search import WINDOW_ROW_FIELDS
        from okg.substrate.mcp.search import (
            _ALIAS_SELECTABLE,
            _LEXICAL_FIELDS,
            _SEMANTIC_FIELDS,
            ATTRS_LOOKUP_FIELDS,
        )
        from okg.substrate.mcp.search_views import GREP_LIST_SELECTABLE, LIST_FIELDS
        from okg.substrate.mcp.tools import _FIND_MENTIONS_FIELDS

        shapes: dict[str, frozenset[str]] = {
            "lexical": frozenset(_LEXICAL_FIELDS),
            "grep": frozenset(GREP_FIELDS),
            "attrs": frozenset(ATTRS_LOOKUP_FIELDS),
            "semantic": frozenset(_SEMANTIC_FIELDS),
            "alias": frozenset(_ALIAS_SELECTABLE),
            "identifier_mentions": frozenset(_FIND_MENTIONS_FIELDS),
            "list": frozenset(LIST_FIELDS),
            "grep_list": frozenset(GREP_LIST_SELECTABLE),
            "windows": frozenset(WINDOW_ROW_FIELDS),
        }
    elif tool == "expand":
        from okg.substrate.mcp.tools import (
            _TRAVERSE_FIELDS,
            EXPAND_LINK_SELECT,
            EXPAND_RECORD_FIELDS,
        )

        shapes = {
            "records": frozenset(EXPAND_RECORD_FIELDS),
            "edges": frozenset(EXPAND_LINK_SELECT),
            "path": frozenset(_TRAVERSE_FIELDS),
        }
    else:
        return {}
    for shape in LINEAGE_SHAPES.get(tool, ()):
        shapes[shape] = shapes[shape] | {LINEAGE_TOKEN}
    return shapes


@functools.lru_cache(maxsize=None)
def vocabulary(tool: str) -> Mapping[str, tuple[str, ...]]:
    """Every field `tool` can return, mapped to the shapes that return it."""
    out: dict[str, list[str]] = {}
    for shape, fields in shape_fields(tool).items():
        for name in sorted(fields):
            out.setdefault(name, []).append(shape)
    return {name: tuple(shapes) for name, shapes in sorted(out.items())}


@functools.lru_cache(maxsize=None)
def _fixed_row_always() -> Mapping[str, frozenset[str]]:
    from okg.substrate.mcp.record_window_search import WINDOW_ROW_ALWAYS
    from okg.substrate.mcp.search_views import GREP_LIST_ALWAYS

    return {
        "grep_list": frozenset(GREP_LIST_ALWAYS),
        "windows": frozenset(WINDOW_ROW_ALWAYS),
    }


class _FixedRowAlways(Mapping[str, frozenset[str]]):
    """The fixed-row shapes and the keys a projected row always keeps."""

    def __getitem__(self, key: str) -> frozenset[str]:
        return _fixed_row_always()[key]

    def __iter__(self):
        return iter(("grep_list", "windows"))

    def __len__(self) -> int:
        return 2


#: Shapes whose rows are fixed: a `select` cuts the finished row. Each maps
#: to the keys every row keeps (its identity and its trust), as the `always`
#: fields of the other shapes are kept.
FIXED_ROW_ALWAYS: Mapping[str, frozenset[str]] = _FixedRowAlways()


def _search_shape(arguments: Mapping[str, Any]) -> Optional[str]:
    method = arguments.get("method")
    if method is None:
        method = "lexical"
    if not isinstance(method, str) or method not in SEARCH_METHODS:
        return None
    view = arguments.get("view")
    if view == "windows":
        return "windows" if method == "grep" else None
    query = arguments.get("query")
    if query is not None and not isinstance(query, str):
        return None
    if method == "lexical" and not query:
        # A query-less call with a condition is the attribute lookup
        # (`server.search`). Anything less clear is left as sent.
        legacy = arguments.get("filter")
        conditioned = (
            bool(arguments.get("where")) or bool(arguments.get("attr_filters"))
            or (isinstance(legacy, Mapping) and bool(legacy.get("attr_filters")))
        )
        if not conditioned:
            return None
        method = "attrs"
    elif method == "attrs" and query:
        # The attribute lookup takes no query text: the tool refuses it.
        return None
    if method == "attrs":
        legacy = arguments.get("filter")
        filtered = bool(arguments.get("attr_filters")) or (
            isinstance(legacy, Mapping) and bool(legacy.get("attr_filters"))
        )
        if not filtered and not arguments.get("where") and not arguments.get("subtype_filter"):
            # No condition at all: the lookup refuses the call. A type
            # alone lists that type (issue #3666).
            return None
    if view is None or view == "cards":
        return method
    if view == "list":
        # The query-less lookup's list delivers the lookup's own rows.
        return {"lexical": "list", "grep": "grep_list", "attrs": "attrs"}.get(method)
    return None


def _expand_shape(arguments: Mapping[str, Any]) -> Optional[str]:
    if arguments.get("next") is not None:
        # `next` stands alone: the tool rebuilds the call its token signed.
        return None
    if arguments.get("path") is not None:
        return "path"
    depth = arguments.get("depth")
    if depth is not None:
        if isinstance(depth, bool) or not isinstance(depth, int):
            return None
        if depth > 1:
            return "path"
    from okg.substrate.mcp.tools import expand_select_is_links

    # The `select` picks the 1-hop shape (#3666): only the link's own old
    # names ask for link rows; anything else, or none, is the linked records.
    return "edges" if expand_select_is_links(arguments.get("select")) else "records"


def call_shape(tool: str, arguments: Mapping[str, Any]) -> Optional[str]:
    """The shape of the rows `tool(**arguments)` returns, or None.

    None when the arguments do not settle it (an unknown method, a `next`
    token, a view with no rows): such a call is left exactly as sent.
    """
    if tool == "search":
        return _search_shape(arguments)
    if tool == "expand":
        return _expand_shape(arguments)
    return None


@dataclass(frozen=True)
class SelectPlan:
    """What one call's `select` means.

    `select` is the list the tool is given (None: the argument is taken out,
    for a fixed-row shape). `row_keys` is set for a fixed-row shape: the keys
    each finished row keeps. `notes` are the `adjusted` records.
    `refusal` is `(message, fix_hint, unknown names, the row's fields)` for
    a fixed-row call that selects nothing its rows have, or a name that is
    no field of the tool. `retry_select` is a corrected `select` the input
    determines, for a call that is left to be refused; the caller offers it
    only when the server proves it serves.
    """

    shape: str
    select: Optional[tuple[str, ...]] = None
    changed: bool = False
    row_keys: Optional[frozenset[str]] = None
    notes: tuple[dict[str, str], ...] = ()
    refusal: Optional[tuple[str, str, tuple[str, ...], tuple[str, ...]]] = None
    retry_select: Optional[tuple[str, ...]] = None
    #: Set when the offered call is another method: the records the caller
    #: asked for are what that method returns. The offer is then a guess.
    retry_method: Optional[str] = None


def _instead(
    tool: str, shape: str, name: str, served: Optional[frozenset[str]],
) -> Optional[str]:
    """What to call for the value `name` asks for, and why this shape has none.

    `served` is the search methods this server serves, or None when that is
    not known: a note never sends the caller to a method it would be refused.
    """
    producers = [other for other in vocabulary(tool).get(name, ()) if other != shape]
    if not producers:
        return None
    if served is not None and not isinstance(served, (set, frozenset)):
        raise TypeError("served must be a set of method names, or None")
    if served is not None:
        producers = [
            other for other in producers
            if SHAPE_METHOD.get(other) is None or SHAPE_METHOD[other] in served
        ]
        if not producers:
            return "not available on this server"
    call = f"call {SHAPE_CALL[producers[0]]}"
    why = OMITTED_WHY.get((shape, name))
    return f"{why}, so {call}" if why is not None else call


def _named(names: list[str]) -> str:
    shown = ", ".join(f"`{name}`" for name in names[:_MAX_NAMED])
    more = len(names) - _MAX_NAMED
    return shown + (f" and {more} more" if more > 0 else "")


def _note(
    tool: str, shape: str, omitted: list[str], served: Optional[frozenset[str]],
) -> dict[str, str]:
    words = SHAPE_WORDS[shape]
    verb = "do" if words.endswith("rows") else "does"
    # (Not named `served`: that is the set of methods, and a string in its
    # place would be searched as text.)
    said = f"not returned, {words} {verb} not return " + (
        "it" if len(omitted) == 1 else "them"
    )
    hints: list[str] = []
    for name in omitted[:_MAX_HINTS]:
        hint = _instead(tool, shape, name, served)
        if hint is not None:
            single = hint if hint.startswith("not available") else f"{hint} for it"
            hints.append(single if len(omitted) == 1 else f"{name}: {hint}")
    if hints:
        # Commas only: the text reply joins the records of one note with "; ".
        said += " (" + ", ".join(hints) + ")"
    return {
        "rule": SELECT_NOTE_RULES[0],
        "sent": f"select {_named(omitted)}",
        "served": said,
    }


def served_second_names(
    select: list[str], own: frozenset[str] | set[str], known: Any = (),
) -> tuple[list[str], list[dict[str, str]]]:
    """`select` with each served second name read as its field, and a note.

    A name is read only when it is no field of the tool (`known`, or `own`
    when the tool has one row shape) and the rows have its field; the field
    is not named twice when the caller also named it. Anything else is left
    as sent.
    """
    read = {
        name: field for name, field in SERVED_SECOND_NAMES.items()
        if name in select and name not in known and name not in own and field in own
    }
    if not read:
        return list(select), []
    out: list[str] = []
    for name in select:
        served = read.get(name, name)
        if served in read.values() and served in out:
            continue
        out.append(served)
    notes = [
        {
            "rule": "select_second_name",
            "sent": f"select `{name}`",
            "served": f"`{field}`, the record's type",
        }
        for name, field in read.items()
    ]
    return out, notes


def _where_names(
    tool: str, shape: str, select: list[str],
    own: frozenset[str], known: Mapping[str, Any],
) -> tuple[list[str], list[dict[str, str]]]:
    """`select` with each `where`-only name read as the row field holding it.

    Only on a search shape whose rows have that field (`WHERE_NAMES_IN_ROWS`),
    for a name that is no field of the tool: each such name means one field,
    so the call is served with a note per field. Any other name, and the
    same name on a shape without the field, is left as sent.
    """
    if tool != "search" or shape in NOT_RECORD_SHAPES:
        return select, []
    out: list[str] = []
    sent: dict[str, list[str]] = {}
    for name in select:
        target = WHERE_NAMES_IN_ROWS.get(name) or WHERE_DATES_IN_ROWS.get(name)
        if target is None and shape == "grep_list":
            # PACT read-names-list-3666: a grep list row names its record by
            # its handle; `attrs.title` and `attrs.path` were refused there.
            target = GREP_LIST_ATTR_NAMES.get(name)
        if target is None or name in known or target not in own:
            out.append(name)
            continue
        sent.setdefault(target, []).append(name)
        if target not in select and target not in out:
            out.append(target)
    notes = [
        {
            "rule": "select_renamed",
            "sent": f"select {_named(names)}",
            "served": f"`{target}`, which holds {_HOLDS[target]}",
        }
        for target, names in sent.items()
    ]
    return out, notes


def _date_attr_names(
    tool: str, shape: str, select: list[str],
) -> tuple[list[str], list[dict[str, str]]]:
    """`select` with each `date.attrs.<key>` read as `attrs.<key>`, a note each.

    `date.attrs.<key>` is how `where` names one stored date attribute; a row
    serves that attribute as `attrs.<key>`. Only on a shape that serves
    `attrs.<key>`; elsewhere the name is left for the refusal."""
    if shape not in ATTR_PATH_SHAPES.get(tool, ()):
        return select, []
    out: list[str] = []
    notes: list[dict[str, str]] = []
    for name in select:
        key = name[len(_DATE_ATTR_PREFIX):] if name.startswith(_DATE_ATTR_PREFIX) else ""
        if not key or not _PLAIN_NAME.match(key):
            out.append(name)
            continue
        served = f"attrs.{key}"
        if served not in select and served not in out:
            out.append(served)
        notes.append({
            "rule": "select_renamed",
            "sent": f"select `{name}`",
            "served": f"`{served}`, the stored attribute",
        })
    return out, notes


def _kept(tool: str, shape: str, select: list[str]) -> Optional[tuple[str, ...]]:
    """A refused search `select` as the tool's own refusal words it: each
    name this shape's rows lack is dropped, or replaced where the refusal
    says `use '<field>'` (`envelope.select_instead`). None when nothing is
    left but the lineage token, and on a shape whose rows are not records or
    on another tool, where a dropped name may change what the rows mean (a
    1-hop row is a link)."""
    if tool != "search" or shape in NOT_RECORD_SHAPES:
        # An identifier mention is a span of the query text: a record field
        # dropped from its select would read as "no such record".
        return None
    from okg.substrate.mcp.envelope import _retry_select, select_instead

    own = shape_fields(tool)[shape]
    lacking = [name for name in select if name not in own]
    kept = tuple(
        name for name in _retry_select(select, lacking, select_instead(lacking, own))
        if name in own
    )
    if set(kept) <= {LINEAGE_TOKEN} or list(kept) == list(select):
        # Alone, the lineage token is the default row plus the lineage:
        # fields nobody selected.
        return None
    return kept


def _corrected(
    tool: str, shape: str, select: list[str], arguments: Mapping[str, Any],
) -> Optional[tuple[str, ...]]:
    """The `select` this one stands for, when the input alone decides it.

    Another letter case of a field of the tool, and an exact second name
    (on a 1-hop row too: its rows are records, so `id` is the record's id
    and `type` its type). Any other name that this shape does not return
    has no single correction.
    """
    own = shape_fields(tool)[shape]
    known = vocabulary(tool)
    by_case: dict[str, list[str]] = {}
    for name in known:
        by_case.setdefault(name.casefold(), []).append(name)
    out: list[str] = []
    changed = False
    for name in select:
        fixed: Optional[str] = name
        if name not in known:
            variants = by_case.get(name.casefold(), [])
            fixed = SECOND_NAMES.get(name) or (variants[0] if len(variants) == 1 else None)
            changed = True
        if fixed is None or fixed not in own:
            return None
        if fixed not in out:
            out.append(fixed)
    return tuple(out) if changed and out else None


def plan_select(
    tool: str, arguments: Mapping[str, Any],
    *, search_methods: Optional[frozenset[str]] = None,
) -> Optional[SelectPlan]:
    """What `tool(**arguments)`'s `select` means, or None to leave the call.

    Pure. None for a tool with no vocabulary here, a call with no `select`,
    a `select` that is not a list of names (the tool refuses it), and a call
    whose shape the arguments do not settle. `search_methods` is the search
    methods the server serves, when known: it changes only the words of a
    note (never which fields are served) and whether another method's call
    is offered.
    """
    if not isinstance(arguments, Mapping) or tool not in ("search", "expand"):
        return None
    select = arguments.get("select")
    if not isinstance(select, (list, tuple)):
        return None
    if not all(isinstance(name, str) for name in select):
        return None
    shape = call_shape(tool, arguments)
    if shape is None:
        return None
    if not select:
        # An empty select on a view with fixed rows asks for nothing less
        # than the row: the same as no select. Elsewhere the tool refuses it.
        if shape in FIXED_ROW_ALWAYS:
            return SelectPlan(shape=shape, select=None, changed=True)
        return None
    own = shape_fields(tool)[shape]
    known = vocabulary(tool)
    seconds, second_notes = served_second_names(list(select), own, known)
    names, renamed = _where_names(tool, shape, seconds, own, known)
    names, dated = _date_attr_names(tool, shape, names)
    renamed = second_notes + renamed + dated
    # `attrs.<key>` names the shape serves (expand), after the `where`
    # names are read as their row fields (search): the two never overlap.
    attrs = _attr_paths(tool, shape, names)
    own = own | attrs
    known = {**known, **{name: (shape,) for name in attrs}}
    fixed_rows = shape in FIXED_ROW_ALWAYS
    unknown = [name for name in names if name not in known]
    returned = [name for name in names if name in own]
    # Sorted, so a page continued from a token that keeps the select sorted
    # says exactly what its first page said.
    omitted = sorted({name for name in names if name in known and name not in own})
    # The lineage token alone means "the default row plus the lineage": left
    # beside a field that is dropped it would return fields nobody selected.
    only_lineage = bool(omitted) and set(returned) == {LINEAGE_TOKEN}
    not_records = shape in NOT_RECORD_SHAPES and bool(omitted)
    if unknown or not returned or only_lineage or not_records:
        # Decided here, from the select and the vocabulary alone, before any
        # read. An identifier mention is a span of the query text: a record
        # field asked of one is never served with the rest, where an empty
        # page would read as "no such record". (1-hop link rows are in the
        # set too, but a select that names a record field asks for record
        # rows, issue #3666, so a link-row call omits nothing.)
        retry = _corrected(tool, shape, names, arguments)
        if not fixed_rows:
            # Refused by the tool, as before, in words that list its fields.
            method = None
            if retry is None and shape == "identifier_mentions" and not unknown:
                # The records whose text holds the identifier are grep's:
                # the same query there, with the fields grep rows have.
                grep = shape_fields(tool)["grep"]
                kept = tuple(dict.fromkeys(name for name in names if name in grep))
                if kept and (search_methods is None or "grep" in search_methods):
                    retry, method = kept, "grep"
            if retry is None:
                retry = _kept(tool, shape, names)
            return SelectPlan(
                shape=shape, select=tuple(names), retry_select=retry, retry_method=method,
            )
        words = SHAPE_WORDS[shape]
        fields = ", ".join(sorted(own))
        said = _named([
            name if _PLAIN_NAME.match(name) else "a name"
            for name in (unknown or names)
        ])
        return SelectPlan(
            shape=shape, select=tuple(names), retry_select=None,
            refusal=(
                f"{tool}: select names {said}, which "
                + ("is no field of this tool" if unknown else f"{words} does not return")
                + f"; {words} returns {fields}",
                f"select from {fields}, or drop select for the whole row",
                tuple(unknown or names), tuple(sorted(own)),
            ),
        )
    notes: list[dict[str, str]] = list(renamed)
    if omitted:
        notes.append(_note(tool, shape, omitted, search_methods))
    if shape == "edges" and "attrs" in names:
        # Served as sent; the note says whose `attrs` these are.
        notes.append(dict(LINK_ROWS_ATTRS_NOTE))
    if fixed_rows:
        return SelectPlan(
            shape=shape, select=None, changed=True, notes=tuple(notes),
            row_keys=frozenset(returned) | FIXED_ROW_ALWAYS[shape],
        )
    if not notes:
        return None
    return SelectPlan(
        shape=shape, select=tuple(returned), changed=bool(omitted or renamed),
        notes=tuple(notes),
    )


def path_select(select: Any) -> tuple[Any, tuple[dict[str, str], ...]]:
    """`(select, notes)` for a path read whose caller sent `select`.

    For `expand(next=...)`: the token records the caller's own `select`, and
    the continued page is given the fields and the note its first page had.
    A `select` the plan leaves alone comes back as it is, with no note.
    """
    plan = plan_select("expand", {"depth": 2, "select": select})
    if plan is None or not plan.changed or plan.select is None:
        return select, ()
    return list(plan.select), plan.notes


def served_as_selected(tool: str, arguments: Mapping[str, Any]) -> bool:
    """Whether the tool returns exactly the fields this `select` names.

    True only for a non-empty list of field names that the call's own shape
    returns, on a shape that projects its rows itself. Used before a refusal
    offers a call with a `select`.
    """
    select = arguments.get("select")
    if not isinstance(select, (list, tuple)) or not select:
        return False
    if not all(isinstance(name, str) for name in select):
        return False
    shape = call_shape(tool, arguments)
    if shape is None or shape in FIXED_ROW_ALWAYS:
        return False
    return set(select) <= shape_fields(tool)[shape] | _attr_paths(tool, shape, select)


def project_rows(payload: Any, keys: frozenset[str]) -> Any:
    """A served fixed-row page with each row cut to `keys`.

    Only `results` changes: the page's order, counts, coverage and
    continuation are the unprojected page's own. A refusal and a payload
    with no row list come back as they are.
    """
    if not isinstance(payload, dict) or payload.get("status") == "refused":
        return payload
    rows = payload.get("results")
    if not isinstance(rows, list):
        return payload
    payload["results"] = [
        {key: value for key, value in row.items() if key in keys}
        if isinstance(row, dict) else row
        for row in rows
    ]
    return payload
