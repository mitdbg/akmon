"""The shared read grammar, as a rewrite of one call's arguments (issue #3666).

Five read tools spell the same condition four ways: `search.where`,
`filter.predicate`, `input.filter` on `filter`/`map`/`aggregate` and
`expand.filter`. This module reads the spellings the grammar adds and turns
each call into the call those tools already accept, before anything else
looks at it:

* the short condition `{field: value}` (a plain value is `eq`, a list is
  `in`, an object with `op` and no `field` is that test on the key's field,
  several keys are `and`);
* `where` at the top level of `filter`, `map`, `aggregate` and `expand`, and
  `select` at the top level of `map`;
* `type` for `subtype` and `id` for `node_id`, in a top-level `where` and in
  the keys of a short form on a node set.

An empty `where` (`{}`) is no condition. Beside an older argument it adds
nothing and is never a conflict; alone it is the empty condition, as `{}`
always was.

Those are accepted spellings and say nothing in the reply. Two more rewrites
serve a call that is not a valid spelling but can mean only one valid call;
each one is recorded and the reply says so (`adjusted`):

* a clause with `field` and `value` and no `op` is read as `eq`;
* `select` or `max_bytes` inside `target` (`inspect`) or `input` (`filter`,
  `map`, `aggregate`) is lifted to the top level when the top level does not
  set it to something else.

A third serves a `select` that names fields of the tool which this call's
rows do not have (step 3a, `select_vocabulary`): the call is served with the
fields its rows do have, and the note names the rest. A `select` on a view
whose rows are fixed (a grep list, a record's windows) is taken out of the
call and the finished rows are cut to the selected keys (`row_keys`). A name
that is a field of no shape of the tool is left for the tool's own refusal.

One rule covers every new spelling: a field name the tool cannot evaluate is
refused by name, never passed on to match nothing. That is every short-form
key, every field of a clause in a top-level `where`, and the field of a
clause served without its `op`. On `filter`, `map` and `aggregate` over a
set of records or links (step 2c) it covers every long-form clause too, in
`predicate` and `input.filter`: one vocabulary in every argument, the second
names included, and two near-miss clauses are served with a note (`in` or
`between` with its list in `value`; `gte`/`lte`). Each such clause was
refused before. Elsewhere (search, `expand`, a saved result) a long-form
clause in an argument the tool already had is passed on as sent, with one
exception everywhere: a regex in a text value written without its slashes
(`re:CMSSW_.*`) is served as `re:/CMSSW_.*/` with a note, as in a grep query.

Every rewrite is a function of the arguments alone. The rewritten call is
the one that is validated, authorized and run, so no rewrite can reach a row
the call it produces could not. None drops a condition or widens a set: a
call that would need that is refused here. A refusal carries a corrected
call only when that call is itself served as written.
"""

from __future__ import annotations

import dataclasses
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from okg.substrate.mcp.aggregate_each import EACH_FORM, each_attribute, is_each_shaped
from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS

__all__ = [
    "DATE_CONDITION_TOOLS",
    "EDGE_FIELDS",
    "LIVE_FIELDS",
    "MAX_ADJUSTED",
    "MAX_ADJUSTED_BYTES",
    "MAX_CONDITION_DEPTH",
    "MORE_NOT_LISTED",
    "NODE_FIELDS",
    "NODE_FIELD_ALIASES",
    "RESERVED_KEYS",
    "SEARCH_FIELDS",
    "Fields",
    "GrammarRefusal",
    "RewrittenCall",
    "cap_adjusted",
    "lower_condition",
    "render_call",
    "rewrite_call",
]

#: Keys of the long form. They are never field names, so an object with one
#: of them and no `op` is a long-form clause that lost its `op`, never a
#: short form.
RESERVED_KEYS: frozenset[str] = frozenset({
    "op", "field", "value", "values", "clause", "clauses",
})

#: One field, two names, on a node set.
NODE_FIELD_ALIASES: Mapping[str, str] = {"type": "subtype", "id": "node_id"}

#: The tools whose conditions compare a `date.<name>` field: every read
#: operator that takes a condition over graph records or links. A live
#: result set holds an outside system's rows and has no date field on any
#: tool; a date condition on one is refused by name.
DATE_CONDITION_TOOLS: frozenset[str] = frozenset({
    "search", "aggregate", "filter", "map", "expand",
})

#: Deepest condition the rewrite walks. The predicate model recurses the
#: same tree afterwards; a deeper one is refused here by name.
MAX_CONDITION_DEPTH = 32

#: Most `adjusted` records one reply lists, and their largest total size as
#: JSON. The rest are counted in one last record (`MORE_NOT_LISTED`), so the
#: note fits the smallest response budget with room for a row.
MAX_ADJUSTED = 8
MAX_ADJUSTED_BYTES = 768
MORE_NOT_LISTED = "more_not_listed"

#: Prefix of a live result set's id (`live_result_sets.ID_PREFIX`; a test
#: pins the two). Its rows are an outside system's, with their own columns.
_LIVE_REF_PREFIX = "l1."
#: The reply key a live call's rows come back under (issue #3666): agents
#: sent it in `select` as if it were a field.
LIVE_ROWS_KEY = "external_live"

_PLAIN_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:\-]{0,95}\Z")
_BARE_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]{0,95}\Z")
_LOGICAL_OPS = ("and", "or", "not")
#: The comparisons one field takes (`operator_models.Predicate`); `matches`
#: tests the field `text` only (`text_match`).
_LEAF_OPS = (
    "eq", "in", "exists", "contains", "suffix", "prefix", "lt", "gt", "between", "matches",
    "like", "like_exact",
)
#: The substring tests (loose-match-3666): folded, and as written in NFC.
_LIKE_OPS = ("like", "like_exact")
#: The field a text condition names, and where it is not a field.
_TEXT = "text"
_TEXT_ELSEWHERE = (
    "`text` is matched on a set of records (filter, map, aggregate, expand); "
    "search's query already matches words"
)

#: Longest corrected clause a refusal spells out in its hint.
_SPELLED_CHARS = 400


@dataclass(frozen=True)
class Fields:
    """The field names a condition may read, for one kind of set.

    `names` are the set's own columns and `aliases` second names for some of
    them; any `attrs.<name>` is a field, and any `date.<name>` when `dates`.
    `instead` maps a mistaken name to the column it means on this set, and
    `elsewhere` says, for a name another tool or argument reads, where.
    `tool` is the tool the condition was sent to, for messages.
    """

    names: frozenset[str] = frozenset()
    aliases: Mapping[str, str] = dataclasses.field(default_factory=dict)
    instead: Mapping[str, str] = dataclasses.field(default_factory=dict)
    elsewhere: Mapping[str, str] = dataclasses.field(default_factory=dict)
    dates: bool = False
    #: Whether the set has the named dates (`date.created`, ...). A set of
    #: links has none: a date there is an attribute, `date.attrs.<name>`.
    date_meanings: bool = True
    tool: str = ""

    @staticmethod
    def is_date(key: str) -> bool:
        return key.startswith("date.") and len(key) > len("date.")

    def known(self, key: str) -> bool:
        if key in self.names or key in self.aliases:
            return True
        if key.startswith("attrs.") and len(key) > len("attrs."):
            return True
        if not (self.dates and self.is_date(key)):
            return False
        return self.date_meanings or (
            key.startswith("date.attrs.") and len(key) > len("date.attrs.")
        )

    def accepted(self) -> str:
        names = sorted(set(self.names) | set(self.aliases))
        dates = "date.<name>" if self.date_meanings else "date.attrs.<name>"
        return ", ".join([*names, "attrs.<name>", *([dates] if self.dates else [])])

    def on(self, tool: str) -> "Fields":
        """These fields as `tool` reads them."""
        return dataclasses.replace(
            self, tool=tool, dates=self.dates or tool in DATE_CONDITION_TOOLS,
        )


_SEARCH_ONLY = {
    name: (
        f"`{name}` is a field of search's `where` only; this tool compares "
        "stored attributes, written with the attrs. prefix"
    )
    for name in ("source", "title", "path")
}
_SEARCH_ARGUMENT = {
    "source_family": "`source_family` is an argument of search, not a field of a condition",
}
_TEXT_ON_RECORDS = {_TEXT: _TEXT_ELSEWHERE}
_EDGE_ONLY = {
    name: (
        f'`{name}` is a field of an edge set (input kind "edges"); a set of '
        "records compares node_id, subtype and stored attributes"
    )
    for name in EFFECTIVE_VIEW_FIELD_COLUMNS["edges"]
}
_NODE_ONLY = {
    name: (
        f"`{name}` is a field of a record; an edge set compares edge_id, src, "
        "dst, edge_type and stored attributes"
    )
    for name in ("subtype", "node_id", "text_hash")
}

#: The effective-attribute relation's stable columns (`operator_sql`), which
#: `filter`, `map`, `aggregate` and `expand`'s far end compare.
NODE_FIELDS = Fields(
    # `text` takes only `matches` (`text_match`), checked by the tool.
    names=frozenset({*EFFECTIVE_VIEW_FIELD_COLUMNS["nodes"], _TEXT}),
    aliases=NODE_FIELD_ALIASES,
    elsewhere={**_SEARCH_ONLY, **_SEARCH_ARGUMENT, **_EDGE_ONLY},
)
EDGE_FIELDS = Fields(
    names=frozenset(EFFECTIVE_VIEW_FIELD_COLUMNS["edges"]),
    instead={"type": "edge_type"},
    elsewhere={
        **_SEARCH_ONLY, **_SEARCH_ARGUMENT, **_NODE_ONLY, **_TEXT_ON_RECORDS,
        # The edge's own id, or the record at either end: not one field.
        "id": (
            "`id` on an edge set can mean the edge or the record at either "
            "end: name `edge_id`, `src` or `dst`"
        ),
    },
)
#: A live result set's rows carry an outside system's fields under `attrs`.
LIVE_FIELDS = Fields()
#: The stable fields search's `where` reads (`search_where`), under the names
#: it already accepts; it keeps both spellings itself, and compares dates.
SEARCH_FIELDS = Fields(
    names=frozenset({"id", "node_id", "type", "subtype", "source", "path", "title"}),
    # Search says itself where this one goes (`_search_guidance`); listed
    # here so no repair renames it.
    elsewhere={**_SEARCH_ARGUMENT, **_TEXT_ON_RECORDS},
    dates=True,
    tool="search",
)


class GrammarRefusal(ValueError):
    """A call the grammar cannot read as exactly one valid call.

    `fix_hint` says the change in words built from the caller's own input.
    `retry_arguments` is a whole call that is served as written, or None;
    `retry_mode` is `corrected` when the caller's input determines that call
    and `guess` when it rests on an assumption `fix_hint` states.
    """

    def __init__(
        self,
        message: str,
        *,
        fix_hint: str,
        fields: tuple[str, ...] = (),
        repairable: bool = False,
        retry_arguments: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.fix_hint = fix_hint
        self.fields = fields
        self.repairable = repairable
        self.retry_arguments = retry_arguments
        self.retry_mode = "corrected"
        #: Arguments the caller repeats beside `retry_arguments`, which does
        #: not carry them.
        self.retry_repeat: tuple[str, ...] = ()
        #: The refusal's code when it is not the argument check's own: a
        #: `select` no row of a fixed-row view can answer keeps the code
        #: every other `select` refusal has. `select_data` is that
        #: refusal's `unknown_fields` and `valid_fields`.
        self.code: Optional[str] = None
        self.select_data: Optional[dict[str, Any]] = None
        #: The words served instead of `fix_hint`, and with no call, where
        #: the deployment's access rules refuse every call the hint names
        #: (an `aggregate` count under audit or enforce).
        self.access_rules_hint: Optional[str] = None
        #: The tool `retry_arguments` are for, when not the tool refused: a
        #: substring test in search's `where` that search's own `contains`
        #: cannot stand for is offered as filter's call (loose-match-3666).
        self.retry_tool: Optional[str] = None
        #: A like in search's `where` on a field search's `contains` reads
        #: text in: the offered call is search's own, each like written as
        #: `contains` with the same text (review of read stack PR 2, #3666).
        self.likes_as_contains = False


@dataclass(frozen=True)
class RewrittenCall:
    """One call after the rewrite.

    `adjusted` holds a `{"rule", "sent", "served"}` record for each rewrite
    the reply must disclose (capped by :func:`cap_adjusted`). `spellings`
    names each accepted spelling that was read as an older argument, as
    `(sent, read as)`, so a refusal of the rewritten call can say which
    argument it is talking about.
    """

    arguments: Any
    adjusted: tuple[dict[str, str], ...] = ()
    spellings: tuple[tuple[str, str], ...] = ()
    #: For a view with fixed rows: the keys each served row keeps. The
    #: `select` that named them is no longer in `arguments`.
    row_keys: Optional[frozenset[str]] = None
    #: For a call whose `select` the tool will refuse: the same call with
    #: the `select` the caller's input determines, or None. Offered beside
    #: that refusal only when the server proves it serves the call.
    select_retry: Optional[dict[str, Any]] = None
    #: Arguments the caller repeats beside `select_retry`.
    select_retry_repeat: tuple[str, ...] = ()
    #: `corrected` when the caller's input determines `select_retry`; `guess`
    #: when it is another method's call for the records the caller asked for.
    select_retry_mode: str = "corrected"
    #: The caller's own `select` when the tool was given a narrower one. A
    #: path read signs it into its token, so `next` rebuilds this plan.
    sent_select: Optional[tuple[str, ...]] = None


def _spell(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def render_call(tool: str, arguments: Mapping[str, Any]) -> str:
    """`arguments` spelled as one call of `tool`."""
    return f"{tool}({', '.join(f'{name}={_spell(value)}' for name, value in arguments.items())})"


def cap_adjusted(records: list[dict[str, str]]) -> tuple[dict[str, str], ...]:
    """The records a reply lists, and one more counting any it does not."""
    kept: list[dict[str, str]] = []
    size = 0
    for record in records:
        cost = len(_spell(record).encode("utf-8"))
        if len(kept) >= MAX_ADJUSTED or (kept and size + cost > MAX_ADJUSTED_BYTES):
            break
        kept.append(record)
        size += cost
    more = len(records) - len(kept)
    if more:
        kept.append({
            "rule": MORE_NOT_LISTED,
            "sent": f"{more} more",
            "served": "not listed",
        })
    return tuple(kept)


def _name(value: Any) -> str:
    """A caller's field name for a one-line note, or a neutral word.

    A name that is not a plain identifier is not echoed: the note is printed
    on its own line of the text reply.
    """
    if isinstance(value, str) and _PLAIN_NAME.match(value):
        return f"`{value}`"
    return "a field"


def _place(path: str) -> str:
    """Where in the call a clause sits, kept short for the note."""
    return path if len(path) <= 64 else path[:24] + "..." + path[-32:]


def _refuse_non_string_key(node: Mapping[Any, Any], path: str) -> None:
    if all(isinstance(key, str) for key in node):
        return
    raise GrammarRefusal(
        f"{path}: every key of a condition is a field name, and a field "
        "name is a string",
        fix_hint="write each field name as a string key of the condition object",
        fields=(path,),
    )


# -- a name that is not a field ---------------------------------------------


def _spelling(key: str, fields: Fields) -> Optional[str]:
    """The name this set's vocabulary has for `key`, ignoring letter case.

    Its own columns and second names, the names it replaces, and the names
    it points elsewhere. None when `key` is none of them in any case.
    """
    known = set(fields.names) | set(fields.aliases) | set(fields.instead) | set(fields.elsewhere)
    if key in known:
        return key
    variants = sorted(name for name in known if name.casefold() == key.casefold())
    return variants[0] if variants else None


def _replacement(
    key: str, fields: Fields, taken: frozenset[str],
) -> Optional[tuple[str, str, str]]:
    """`(mode, name, words)` for the one name `key` can be replaced by.

    `corrected` when the set itself decides it: another letter case of one
    of its fields, or the column a second name means here. `guess` for a
    bare word that may be a stored attribute, which the words say. None for
    a name with no single replacement, and for one that would collide with a
    key already written (`taken`).
    """
    if fields.is_date(key):
        return None
    name = _spelling(key, fields)
    if name is not None:
        # A name this tool's view knows, in whatever letter case: it is
        # that name, or it has a home elsewhere. Never a guess.
        if name in fields.elsewhere:
            return None
        column = fields.instead.get(name, name)
        if column == key or column in taken:
            return None
        return "corrected", column, f"this set's field is `{column}`"
    if (
        _BARE_WORD.match(key)
        and key.casefold() not in _LOGICAL_OPS
        and key.casefold() not in ("attrs", "date")
        and f"attrs.{key}" not in taken
    ):
        return (
            "guess", f"attrs.{key}",
            f"if `{key}` is a stored attribute, write `attrs.{key}`",
        )
    return None


def _search_guidance(key: str) -> Optional[tuple[str, str]]:
    """Search's own `(message, fix_hint)` for a field it names a remedy for.

    Search already says where `source_family` and a bare `date` go; that
    guidance is passed on rather than replaced. None for a name search
    simply does not know.
    """
    from okg.substrate.mcp.search_where import WhereUnsupported, normalize_where

    try:
        normalize_where({"op": "exists", "field": key})
    except WhereUnsupported as refused:
        if refused.data.get("reason") != "unknown_field" and refused.fix_hint:
            return refused.message, refused.fix_hint
    return None


def _unknown_field(key: str, fields: Fields, path: str, taken: frozenset[str]) -> GrammarRefusal:
    """The refusal for a name that is no field of this set.

    Passed on, the name would reach the tool as a field no record has, and
    the reply would be a confident empty page.
    """
    said = f"`{key}`" if _PLAIN_NAME.match(key) else "a key"
    message = (
        f"{path}: {said} is not a field {fields.tool or 'this tool'} "
        f"compares; a condition here reads {fields.accepted()}"
    )
    if fields.is_date(key) and fields.dates:
        # A named date on a set that has none (a set of links).
        return GrammarRefusal(
            f"{path}: {said} is a date declared per record type; this set "
            "has none",
            fix_hint="a date attribute of this set is written with the date.attrs. prefix",
            fields=(path,),
        )
    if fields.is_date(key):
        # A set with no date field: a live result set's rows.
        return GrammarRefusal(
            f"{path}: {said} is a date field, which reads graph records; "
            "this set holds an outside system's rows",
            fix_hint=(
                "compare the set's own column, written with the attrs. "
                "prefix (compared as text)"
            ),
            fields=(path,),
        )
    if fields.tool == "search" and key:
        guidance = _search_guidance(key)
        if guidance is not None:
            return GrammarRefusal(f"{path}: {guidance[0]}", fix_hint=guidance[1], fields=(path,))
    name = _spelling(key, fields)
    if name in fields.elsewhere:
        return GrammarRefusal(message, fix_hint=fields.elsewhere[name], fields=(path,))
    fix = _replacement(key, fields, taken)
    return GrammarRefusal(
        message,
        fix_hint=fix[2] if fix is not None else (
            "name one of the fields the message lists; which one is meant "
            "cannot be told from the name"
        ),
        fields=(path,),
        repairable=fix is not None,
    )


def _clause_field(field: str, fields: Fields, path: str, *, alias: bool) -> str:
    """The field a clause compares, checked; second names only when `alias`."""
    if field == "attrs":
        # The whole attribute map, which `contains` reads.
        return field
    if not fields.known(field):
        raise _unknown_field(field, fields, path, frozenset())
    column = fields.aliases.get(field, field)
    if column != field and not alias:
        # A second name belongs to the new spelling; in an argument the tool
        # already had, the field is written as the tool reads it.
        raise GrammarRefusal(
            f"{path}: {_name(field)} is written `{column}` in this argument",
            fix_hint=f"this set's field is `{column}`",
            fields=(path,),
            repairable=True,
        )
    return column


# -- lowering ---------------------------------------------------------------


def _operator_object(value: Mapping[str, Any]) -> bool:
    """`{"gt": 3}`: an object whose every key is the name of an operator."""
    return bool(value) and all(key in _LEAF_OPS or key in _LOGICAL_OPS for key in value)


def _operator_fix(value: Mapping[str, Any]) -> Optional[tuple[str, Any]]:
    """What `{"<op>": operand}` under a field stands for, when it is one thing.

    `("test", test)` for one comparison whose operand fits its operator, and
    `("absent", None)` for `{"exists": false}`, which is the negation of a
    test and so not a test on the field. None otherwise: no operand is
    dropped and none is read as something else. `contains` reads the whole
    attribute map, never one field, so it has no form here. A key that is a
    word of `OPERATOR_WORDS` (a synonym, `gte`, `ne`) is read as that table
    says. `("either", (strict, eq))` is "at least" or "at most" (`gte`, `lte`):
    the strict test or `eq`. `("negated", test)` is `ne` or `nin`: `not` of
    the test, which also keeps a record without the field, so it is only
    ever offered as a guess.
    """
    if len(value) != 1:
        return None
    (op, operand), = value.items()
    if op not in _LEAF_OPS:
        kind, meant = OPERATOR_WORDS.get(op, ("none", None))
        if kind == "synonym":
            op = meant
        elif kind == "inclusive":
            if operand is None or isinstance(operand, (dict, list, bool)):
                return None
            return "either", ({"op": meant, "value": operand}, {"op": "eq", "value": operand})
        elif kind == "negated":
            inner = _operator_fix({meant: operand})
            return ("negated", inner[1]) if inner is not None and inner[0] == "test" else None
        else:
            return None
    if op == "exists":
        if operand is True:
            return "test", {"op": "exists"}
        return ("absent", None) if operand is False else None
    if op == "in":
        fits = isinstance(operand, list) and bool(operand)
        return ("test", {"op": op, "values": operand}) if fits else None
    if op == "between":
        fits = isinstance(operand, list) and len(operand) == 2
        return ("test", {"op": op, "values": operand}) if fits else None
    if op in ("prefix", "suffix", "matches"):
        fits = isinstance(operand, str) and bool(operand)
        return ("test", {"op": op, "value": operand}) if fits else None
    if op in _LIKE_OPS:
        served = _like_operand(op, operand)
        return ("test", {"op": op, "value": served[0]}) if served[1] is None else None
    if op in ("eq", "lt", "gt"):
        return "test", {"op": op, "value": operand}
    return None


def _operator_fixed(key: str, fix: tuple[str, Any]) -> dict[str, Any]:
    """The condition `{key: {"<op>": operand}}` stands for."""
    if fix[0] == "absent":
        return {"op": "not", "clause": {key: {"op": "exists"}}}
    if fix[0] == "either":
        return {"op": "or", "clauses": [{key: fix[1][0]}, {key: fix[1][1]}]}
    if fix[0] == "negated":
        return {"op": "not", "clause": {key: fix[1]}}
    return {key: fix[1]}


#: Every operator word both condition forms read, and what it is here
#: (issue #3790): `(kind, operator)`. `op` is an operator of this grammar;
#: a `synonym` means one operator exactly (`ilike` is like, which also
#: ignores accents); `inclusive` is "at least" or "at most", the strict
#: test or `eq`; `negated` is `not` of one test, which also keeps records
#: without the field, so it is offered, never served; `pattern` and `none`
#: have no one operator here. One table, so each word gives one answer in
#: the long form, the short form with `op`, and as a key.
OPERATOR_WORDS: Mapping[str, tuple[str, Optional[str]]] = {
    **{op: ("op", op) for op in _LEAF_OPS},
    "equal": ("synonym", "eq"), "equals": ("synonym", "eq"),
    "startswith": ("synonym", "prefix"), "starts_with": ("synonym", "prefix"),
    "endswith": ("synonym", "suffix"), "ends_with": ("synonym", "suffix"),
    "ilike": ("synonym", "like"),
    "gte": ("inclusive", "gt"), "ge": ("inclusive", "gt"),
    "lte": ("inclusive", "lt"), "le": ("inclusive", "lt"),
    "ne": ("negated", "eq"), "neq": ("negated", "eq"), "not_eq": ("negated", "eq"),
    "nin": ("negated", "in"), "not_in": ("negated", "in"),
    "regex": ("pattern", None), "regexp": ("pattern", None), "match": ("pattern", None),
    "isnull": ("none", None), "is_null": ("none", None), "range": ("none", None),
    "substring": ("none", None),
}


#: Words a near spelling is matched to: the operators of four letters or
#: more and the words served for one (`gte`, `lte`, `ilike`, ...) of three
#: or more. `eq`, `in`, `lt`, `gt`, `ge` and `le` match only exactly, since
#: many short words sit one letter from them (`seq`, `min`, `get`) and are
#: real keys.
_NEAR_MATCHED_OPS = tuple(dict.fromkeys([
    *(op for op in _LEAF_OPS if len(op) >= 4),
    *(word for word, (kind, _) in OPERATOR_WORDS.items()
      if kind in ("synonym", "inclusive") and len(word) >= 3),
]))
#: The difflib ratio a near spelling needs.
_NEAR_CUTOFF = 0.85


def _operator_word(word: Any) -> Optional[tuple[str, str, bool]]:
    """`(table word, kind, exact)` for an operator word as written, or None.

    The word is matched as a bare word with an optional leading `$`, in any
    letter case. `exact` is False for a near spelling of one of
    `_NEAR_MATCHED_OPS`: it may also be a real key, so its reading is a
    guess. Two near words: kind `near` and no table word.
    """
    if not isinstance(word, str):
        return None
    bare = word[1:] if word.startswith("$") else word
    if not _BARE_WORD.match(bare) or word in RESERVED_KEYS:
        return None
    lowered = bare.lower()
    if lowered in OPERATOR_WORDS:
        return lowered, OPERATOR_WORDS[lowered][0], True
    import difflib

    close = difflib.get_close_matches(lowered, _NEAR_MATCHED_OPS, n=2, cutoff=_NEAR_CUTOFF)
    if not close:
        return None
    if len(close) > 1:
        return "", "near", False
    return close[0], OPERATOR_WORDS[close[0]][0], False


#: Longest like operand, after the ends' % are stripped (`text_fold`).
_LIKE_MAX_CHARS = 200


def _like_operand(op: str, value: Any) -> tuple[Any, Optional[str]]:
    """`(served operand, problem)` for a like or like_exact operand.

    One string of 1 to 200 characters with no NUL. A run of % at either end
    is stripped (% is not a wildcard here; the caller is told); an interior
    % is a problem, as is an operand that is empty once stripped or folded.
    `problem` is None when the served operand is one to compare.
    """
    from okg.substrate.mcp.text_fold import needle

    if not isinstance(value, str):
        return value, f"`{op}` takes one string"
    if "\x00" in value:
        return value, f"`{op}` takes no NUL character"
    served = value.strip("%")
    if "%" in served:
        return served, "interior"
    if not served:
        return served, f"`{op}` needs text besides %"
    if len(served) > _LIKE_MAX_CHARS:
        return served, f"`{op}` takes at most {_LIKE_MAX_CHARS} characters"
    if not needle(op, served):
        return served, (
            f"`{op}` needs a letter, digit or sign that is not only an accent mark"
        )
    return served, None


def _misspelled_operator(
    value: Mapping[str, Any],
) -> Optional[tuple[str, Optional[str], bool]]:
    """`(key, operator, exact)` when a one-key object reads as a test with a
    wrong operator name (#3666), else None, and the object is a value to match.

    The rule: exactly one key, which is a word of `OPERATOR_WORDS` in any
    case and with an optional leading `$` (`_operator_word`), or a near
    spelling (difflib ratio `_NEAR_CUTOFF`) of one of `_NEAR_MATCHED_OPS`.
    `operator` is the table word it stands for, or None when no one
    operator here means it. `exact` is False for a near spelling: it may
    also be a real key, so its correction is a guess. Any other one-key
    object with a bare-word key is refused as `_unknown_key_object`.
    """
    if len(value) != 1:
        return None
    (key,) = value
    word = _operator_word(key)
    if word is None:
        return None
    table, kind, exact = word
    return key, table if kind in ("op", "synonym", "inclusive", "negated") else None, exact


def _misspelled_fix(value: Mapping[str, Any], operator: Optional[str]) -> Optional[tuple[str, Any]]:
    """What `{"<misspelled>": operand}` stands for (`_operator_fix`), when
    exactly one thing."""
    if operator is None:
        return None
    fix = _operator_fix({operator: next(iter(value.values()))})
    return fix if fix is not None and fix[0] != "absent" else None


def _object_match(key: str, value: Mapping[str, Any]) -> str:
    """The long-form clause that matches `value` itself on `key`."""
    return _spell({"op": "eq", "field": key, "value": dict(value)})


def _misspelled_note(
    key: str, value: Mapping[str, Any], miss: tuple[str, Optional[str], bool],
) -> tuple[str, str]:
    """`(mode, words)` for the repair of `{key: {"<word>": operand}}`.

    An exact word (a `$` form, a synonym) is `corrected`. A near spelling is
    a `guess`, and its words keep the object-match alternative, because the
    served text of a guess is these words and then the call.
    """
    word, operator, exact = miss
    kind = OPERATOR_WORDS.get(operator or "", ("none", None))[0]
    if kind == "negated":
        # Two readings: "not equal", which also keeps a record without the
        # field, or a test written wrong. Offered as a guess, said whole.
        return "guess", (
            f"`{word}` on {_name(key)} is read as not `{OPERATOR_WORDS[operator][1]}`, "
            "which also keeps records without that field; to match the object "
            f"itself write {_object_match(key, value)}"
        )
    if exact:
        meant = OPERATOR_WORDS[operator][1] if operator in OPERATOR_WORDS else operator
        if kind == "inclusive":
            return "corrected", f"on {_name(key)} `{word}` is `{meant}` or `eq`, each in `op`"
        return "corrected", f"on {_name(key)} write `{word}` as `op`: `{meant}`"
    return "guess", (
        f"to match the object itself write {_object_match(key, value)}; if "
        f"`{word}` on {_name(key)} means `{operator}`, write it in `op`"
    )


def _raise_misspelled_operator(
    key: str, value: Mapping[str, Any], miss: tuple[str, Optional[str], bool], path: str,
    *, tool: str = "",
) -> dict[str, Any]:
    word, operator, _exact = miss
    fix = _misspelled_fix(value, operator)
    as_object = _object_match(key, value)
    # Search's `where` takes no like (`_like_fitted`): not listed there.
    known = ", ".join(
        name for name in _LEAF_OPS if not (tool == "search" and name in _LIKE_OPS)
    )
    if fix is not None:
        spelled = _spell(_operator_fixed(key, fix))
        hint = f"write {spelled}" if len(spelled) <= _SPELLED_CHARS else (
            f"write the operator `{operator}` in `op`")
    else:
        hint = (f"the operators are {known}, each written in `op`; none of them "
                f"is `{word}` exactly")
    raise GrammarRefusal(
        f"{path}: the value of {_name(key)} names `{word}`, which is not an "
        f"operator; the operators are {known}",
        fix_hint=f"{hint}; to match the object itself write {as_object}",
        fields=(path,),
        repairable=fix is not None,
    )


def _unknown_key_object(key: str, value: Mapping[str, Any], path: str) -> dict[str, Any]:
    """Refuse `{key: {"<word>": operand}}` whose one key is a bare word and
    no operator word (D1, loose-match-3666): it reads as a test whose
    operator is misspelled as often as an object to match, and an object
    match that finds nothing reads as an empty answer. The long form is the
    one way to match an object; it is the call offered."""
    (word,) = value
    raise GrammarRefusal(
        f"{path}: the value of {_name(key)} is an object whose one key "
        f"{_name(word)} is not an operator",
        fix_hint=(
            f"to match the object itself write {_object_match(key, value)}; for a "
            "test, name its operator in `op`"
        ),
        fields=(path,),
        repairable=True,
    )


def _is_unknown_key_object(value: Any) -> bool:
    """Whether `value` is the shape `_unknown_key_object` refuses."""
    if not isinstance(value, Mapping) or len(value) != 1:
        return False
    (word,) = value
    if not isinstance(word, str) or word in RESERVED_KEYS:
        return False
    bare = word[1:] if word.startswith("$") else word
    return bool(_BARE_WORD.match(bare)) and _misspelled_operator(value) is None


def _holds_conditions(key: str, value: Any) -> bool:
    """Whether a logical word used as a key holds what its long form takes."""
    if key == "not":
        return isinstance(value, Mapping) and bool(value)
    return (
        isinstance(value, list) and bool(value)
        and all(isinstance(clause, Mapping) and clause for clause in value)
    )


def _short_clause(
    key: str, value: Any, *, node: Mapping[str, Any], fields: Fields, path: str,
) -> dict[str, Any]:
    """One `field: value` pair of the short form as a long-form clause."""
    if key in _LOGICAL_OPS and not fields.known(key):
        fits = _holds_conditions(key, value)
        raise GrammarRefusal(
            f"{path}: `{key}` is not a field; it is written with `op`",
            fix_hint=(
                f"write it in the long form: an object with \"op\": \"{key}\" and "
                + ("the one condition in `clause`" if key == "not"
                   else "the conditions as a list in `clauses`")
                + ("" if fits else f"; `{key}` here does not hold "
                   + ("one condition" if key == "not" else "a list of conditions"))
            ),
            fields=(path,),
            repairable=fits,
        )
    if not fields.known(key):
        raise _unknown_field(key, fields, path, frozenset(node) - {key})
    field = fields.aliases.get(key, key)
    if isinstance(value, (list, tuple)):
        if not value:
            raise GrammarRefusal(
                f"{path}: {_name(key)} has an empty list; a list means any "
                "of; an empty list matches nothing",
                fix_hint=(
                    "name at least one value, or drop the key to leave the "
                    "field unconstrained; which is meant cannot be told from "
                    "the call"
                ),
                fields=(path,),
            )
        return {"op": "in", "field": field, "values": list(value)}
    if not isinstance(value, Mapping):
        return {"op": "eq", "field": field, "value": value}
    _refuse_non_string_key(value, f"{path}.{key}")
    if "op" in value:
        if value["op"] in _LOGICAL_OPS:
            # `and`, `or` and `not` join conditions; they do not test a
            # field, and a tool given one beside `field` ignores the field.
            raise GrammarRefusal(
                f"{path}: the test on {_name(key)} uses `{value['op']}`, which "
                "joins conditions; a test on a field is one comparison",
                fix_hint=(
                    f"write `{value['op']}` at the level of the conditions it "
                    "joins, each of them naming its own field; which fields "
                    "the inner conditions test cannot be told from this value"
                ),
                fields=(path,),
            )
        if "field" in value:
            same = value["field"] == key
            raise GrammarRefusal(
                f"{path}: the test on {_name(key)} names a `field` of its own; "
                "a key of the short form is already the field",
                fix_hint=(
                    "drop `field` from the test"
                    if same else
                    "the key and the test's `field` differ, so which field is "
                    "meant cannot be told: drop `field` from the test to test "
                    "the key, or write one long-form clause on the other field"
                ),
                fields=(path,),
                repairable=same,
            )
        _refuse_stray_operand(key, value, path)
        return {
            "op": value["op"], "field": field,
            **{k: v for k, v in value.items() if k != "op"},
        }
    if _operator_object(value):
        # `{"<field>": {"gt": 3}}` is a test written without `op`, not an
        # object to match exactly.
        return _raise_operator_object(key, value, _operator_fix(value), path)
    miss = _misspelled_operator(value)
    if miss is not None:
        # `{"<field>": {"ilike": "x"}}` was served as an exact match on that
        # object: 0 rows and no word why (#3666).
        return _raise_misspelled_operator(key, value, miss, path, tool=fields.tool)
    if RESERVED_KEYS & set(value):
        # `{"<field>": {"value": 3}}` is a test that lost its `op`, or an
        # exact match on that object. Two readings: refused with both.
        as_test = _spell({key: {"op": "eq", **value}})
        as_object = _spell({"op": "eq", "field": key, "value": dict(value)})
        both = f"for a test write {as_test}; to match the object itself write {as_object}"
        raise GrammarRefusal(
            f"{path}: the value of {_name(key)} has no `op`, so it is either "
            "a test missing its `op` or an object to match exactly",
            fix_hint=(
                both if len(both) <= _SPELLED_CHARS else
                'add "op" to the value to make it a test, or write a '
                'long-form "eq" clause with the object as its `value`'
            ),
            fields=(path,),
        )
    if _is_unknown_key_object(value):
        return _unknown_key_object(key, value, path)
    return {"op": "eq", "field": field, "value": dict(value)}


#: The operand each comparison reads. Any other reserved key beside it is
#: ignored by the tools, so a test that carries one says more than is run.
_OPERAND = {
    "exists": (), "in": ("values",), "between": ("values",),
    **{op: ("value",) for op in (
        "eq", "lt", "gt", "prefix", "suffix", "contains", "matches", "like", "like_exact",
    )},
}


def _refuse_stray_operand(key: str, value: Mapping[str, Any], path: str) -> None:
    """Refuse `{"op": "exists", "value": false}`: the tools would drop `value`."""
    reads = _OPERAND.get(value["op"]) if isinstance(value["op"], str) else None
    if reads is None:
        return
    stray = sorted(k for k in value if k in ("value", "values") and k not in reads)
    if not stray:
        return
    named = ", ".join(f"`{k}`" for k in stray)
    absent = value["op"] == "exists" and value.get("value") is False
    spelled = _spell({"op": "not", "clause": {key: {"op": "exists"}}})
    raise GrammarRefusal(
        f"{path}: the test on {_name(key)} carries {named}, which "
        f"`{value['op']}` does not read",
        fix_hint=(
            f"for \"does not exist\" write {spelled}; for \"exists\" drop `value`"
            if absent and len(spelled) <= _SPELLED_CHARS else
            f"drop {named}, or use the operator that reads it; which is meant "
            "cannot be told from the test"
        ),
        fields=(path,),
    )


def _raise_operator_object(
    key: str, value: Mapping[str, Any],
    fix: Optional[tuple[str, Optional[dict[str, Any]]]], path: str,
) -> dict[str, Any]:
    names = ", ".join(f"`{name}`" for name in value)
    if fix is None:
        raise GrammarRefusal(
            f"{path}: the value of {_name(key)} names {names} as keys; a test "
            "names its operator in `op`",
            fix_hint=(
                "write one test per operator, each an object with `op` and "
                "the operand that operator takes (`value`; a list in `values` "
                "for in and between, two for between; nothing for exists); "
                "this value does not give one such test"
            ),
            fields=(path,),
        )
    spelled = _spell(_operator_fixed(key, fix))
    raise GrammarRefusal(
        f"{path}: the value of {_name(key)} names {names} as a key; a test "
        "names its operator in `op`",
        fix_hint=(
            f"write {spelled}" if len(spelled) <= _SPELLED_CHARS
            else "move the operator into `op` and its operand into `value`"
        ),
        fields=(path,),
        repairable=True,
    )


#: Longest value a text note quotes; a longer one is cut with "...".
_TEXT_NOTE_CHARS = 80


def _as_text_matches(
    value: Any, *, sent: str, path: str, adjusted: list[dict[str, str]],
) -> Optional[dict[str, Any]]:
    """A near-miss text test served as `matches` on `value`, read the one
    way `matches` reads it: grep's grammar, so plain words must all occur.

    The note quotes the value and says it was read as words. None for a
    value that is not a non-empty string: no reading, left for the refusal."""
    if not (isinstance(value, str) and value.strip()):
        return None
    adjusted.append({
        "rule": "text_as_matches",
        "sent": f"{_place(path)} {sent}",
        "served": f"text matches {_shown(value)} (grep grammar: every word must occur)",
    })
    return _text_regex_slashed(
        {"op": "matches", "field": _TEXT, "value": value}, path=path, adjusted=adjusted,
    )


def _shown(value: Any) -> str:
    """`value` spelled for a note, cut to `_TEXT_NOTE_CHARS` with "..."."""
    shown = _spell(value)
    if len(shown) > _TEXT_NOTE_CHARS:
        shown = shown[:_TEXT_NOTE_CHARS - 3] + "..."
    return shown


#: The characters that make an unslashed `re:` term a pattern. `re:jenkins`
#: holds none: it is a valid literal word (a subject line "RE:Jenkins"),
#: searched as sent. `+`, `$` and `^` alone do not count: `re:C++`,
#: `re:$100` and `re:a+b` read as words a person writes, and as a regex they
#: would match nothing or be refused for a pattern the caller never wrote.
_REGEX_META = frozenset("\\.?*()[]{}|")
#: `re:<pattern>/` or `re:<pattern>/<flags>`: a regex with its opening slash
#: missing: one unescaped slash, at the end or before flags. A pattern with
#: another slash (`re:https://host/x`) is not this form.
_SLASH_FORM = re.compile(r"((?:[^/\\]|\\.)+)/[gimsux]{0,3}")


def _grep_spans(query: str) -> Optional[list[tuple[int, int, str]]]:
    """`(start, end, kind)` for each term of a grep query, as
    `grep_query.parse_grep_query` delimits them (an exclusion's `-` is in
    the span); kind is word, near, phrase, regex or path. None when the
    query cannot be split (an unbalanced quote, an unterminated regex):
    the grep parser refuses that query by name."""
    from okg.substrate.mcp.grep_query import (
        PATH_TERM_PREFIX, GrepQueryInvalid, read_regex_term,
    )

    spans: list[tuple[int, int, str]] = []
    i, n = 0, len(query)

    def word_end(at: int) -> int:
        while at < n and not query[at].isspace():
            at += 1
        return at

    while i < n:
        if query[i].isspace():
            i += 1
            continue
        start = i
        if query[i] == "-" and i + 1 < n and not query[i + 1].isspace():
            i += 1
        if query.startswith(PATH_TERM_PREFIX, i):
            at = i + len(PATH_TERM_PREFIX)
            if at < n and query[at] == '"':
                close = query.find('"', at + 1)
                if close < 0:
                    return None
                i = close + 1
            else:
                i = word_end(at)
            spans.append((start, i, "path"))
            continue
        try:
            regex = read_regex_term(query, i)
        except GrepQueryInvalid:
            return None
        if regex is not None:
            i = regex[1]
            spans.append((start, i, "regex"))
            continue
        if query[i] == '"':
            close = query.find('"', i + 1)
            if close < 0:
                return None
            i = close + 1
            spans.append((start, i, "phrase"))
            continue
        end = word_end(i)
        word = query[i:end]
        near = start == i and (word == "NEAR" or word.startswith("NEAR/"))
        spans.append((start, end, "near" if near else "word"))
        i = end
    return spans


def _unslashed_regex(word: str) -> Optional[tuple[str, bool]]:
    """`(pattern, misslashed)` for a word that writes a regex without its
    opening slash, else None (#3666).

    `re:<pattern>` holding one of `. * ? [ ] ( ) { } | \\` can mean only
    the regex (`misslashed` False: served as `re:/<pattern>/`). A pattern
    with exactly one unescaped slash, followed by up to three of the
    letters gimsux (`re:foo/`, `re:foo/i`), is a regex missing its opening
    slash (`misslashed` True: refused with `re:/<pattern>/` to send, since
    grep's regex takes no flags and always ignores case). Any other word,
    such as `re:jenkins`, `re:C++` or `re:$100`, is a literal word and None."""
    body = word[1:] if word.startswith("-") else word
    if not body.startswith("re:") or body.startswith("re:/"):
        return None
    pattern = body[len("re:"):]
    if not pattern or '"' in pattern:
        return None
    slash = _SLASH_FORM.fullmatch(pattern)
    if slash is not None:
        return slash.group(1), True
    if any(char in _REGEX_META for char in pattern):
        return pattern, False
    return None


@dataclass(frozen=True)
class _RegexTerms:
    """The unslashed regex terms of one grep value (#3666)."""

    #: The value with each `re:<pattern>` that has a metacharacter slashed.
    served: str
    #: `served` with each misslashed term written `re:/<pattern>/` too.
    fixed: str
    #: Misslashed terms as sent, each with the term to send instead.
    misslashed: tuple[tuple[str, str], ...]
    #: Unslashed regex terms (either kind) written beside NEAR, as sent.
    beside_near: tuple[str, ...]


def read_regex_terms(value: str) -> Optional[_RegexTerms]:
    """The unslashed regex terms in a grep value, None when it has none.

    Terms are delimited as grep delimits them, so a quoted phrase, a
    slashed regex and a path term are never touched, wherever they stand."""
    spans = _grep_spans(value)
    if spans is None:
        return None
    served: list[str] = []
    fixed: list[str] = []
    misslashed: list[tuple[str, str]] = []
    beside_near: list[str] = []
    last = 0
    for index, (start, end, kind) in enumerate(spans):
        served.append(value[last:start])
        fixed.append(value[last:start])
        last = end
        word = value[start:end]
        found = _unslashed_regex(word) if kind == "word" else None
        if found is None:
            served.append(word)
            fixed.append(word)
            continue
        pattern, wrong = found
        sign = "-" if word.startswith("-") else ""
        slashed = f"{sign}re:/{pattern}/"
        near = any(
            0 <= other < len(spans) and spans[other][2] == "near"
            for other in (index - 1, index + 1)
        )
        if near:
            beside_near.append(word)
        if wrong:
            misslashed.append((word, slashed))
        served.append(word if wrong or near else slashed)
        fixed.append(word if near else slashed)
    served.append(value[last:])
    fixed.append(value[last:])
    if not (misslashed or beside_near) and "".join(served) == value:
        return None
    return _RegexTerms(
        "".join(served), "".join(fixed), tuple(misslashed), tuple(beside_near),
    )


def _regex_terms_refusal(
    terms: _RegexTerms, *, place: str, fields: tuple[str, ...] = (),
) -> Optional[GrammarRefusal]:
    """The refusal for a value with a regex term that has more than one
    reading, or None. Only the caller's own words are quoted."""
    if terms.beside_near:
        named = ", ".join(f"`{word}`" for word in terms.beside_near)
        return GrammarRefusal(
            f"{place} writes {named} beside NEAR; NEAR's operands are words "
            "or quoted phrases, so a regex cannot be one",
            fix_hint=(
                "match the regex as a term of its own (re:/<pattern>/, no NEAR), "
                "or quote the literal text as a NEAR operand"
            ),
            fields=fields,
        )
    if terms.misslashed:
        named = ", ".join(f"`{word}`" for word, _ in terms.misslashed)
        fixes = "; ".join(f"write {slashed} for {word}" for word, slashed in terms.misslashed)
        return GrammarRefusal(
            f"{place} writes {named}, a regex missing its opening slash; grep "
            "reads a regex only as re:/<pattern>/, which takes no flags and "
            "always ignores case",
            fix_hint=fixes,
            fields=fields,
            repairable=True,
        )
    return None


def _text_regex_slashed(
    clause: Any, *, path: str, adjusted: list[dict[str, str]],
) -> Any:
    """A text clause whose value writes a regex without slashes, served
    with them and a note (`read_regex_terms`), or refused when that term
    has more than one reading. Any other clause unchanged."""
    if not (
        isinstance(clause, Mapping) and clause.get("field") == _TEXT
        and clause.get("op") == "matches" and isinstance(clause.get("value"), str)
    ):
        return clause
    terms = read_regex_terms(clause["value"])
    if terms is None:
        return clause
    refusal = _regex_terms_refusal(
        terms, place=f"{path}: the text value", fields=(path,),
    )
    if refusal is not None:
        raise refusal
    adjusted.append({
        "rule": "regex_without_slashes",
        "sent": f"{_place(path)} text value {_shown(clause['value'])}",
        "served": f"text matches {_shown(terms.served)} (a regex is written re:/<pattern>/)",
    })
    return {**clause, "value": terms.served}


def _text_values_fixed(node: Any, notes: list[tuple[str, str]], depth: int = 0) -> Any:
    """A repaired condition with each misslashed regex term in a text value
    written `re:/<pattern>/` (and each unslashed one slashed), as the
    documented `matches` test, so the call offered is served as written."""
    if not isinstance(node, Mapping) or depth > MAX_CONDITION_DEPTH:
        return node

    def fixed(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        terms = read_regex_terms(value)
        if terms is None or terms.beside_near or terms.fixed == value:
            return None
        for word, slashed in terms.misslashed:
            notes.append(("corrected", f"write {slashed} for {word}"))
        return terms.fixed

    def again(child: Any) -> Any:
        return _text_values_fixed(child, notes, depth + 1)

    if "op" in node:
        out = dict(node)
        if isinstance(node.get("clauses"), list):
            out["clauses"] = [again(clause) for clause in node["clauses"]]
        if isinstance(node.get("clause"), Mapping):
            out["clause"] = again(node["clause"])
        if node.get("field") == _TEXT and node["op"] in ("matches", "contains"):
            value = fixed(node.get("value"))
            if value is not None:
                out.update(op="matches", value=value)
        return out
    if node.get("field") == _TEXT and set(node) == {"field", "value"}:
        value = fixed(node["value"])
        if value is not None:
            return {"op": "matches", "field": _TEXT, "value": value}
        return dict(node)
    out = dict(node)
    for key, value in node.items():
        if key != _TEXT:
            if key in _LOGICAL_OPS and isinstance(value, list):
                out[key] = [again(clause) for clause in value]
            elif key in _LOGICAL_OPS and isinstance(value, Mapping):
                out[key] = again(value)
            continue
        if isinstance(value, Mapping) and value.get("op") in ("matches", "contains"):
            sent = value.get("value")
        elif isinstance(value, Mapping) and len(value) == 1:
            sent = value.get("matches", value.get("contains"))
        else:
            sent = value
        served = fixed(sent)
        if served is not None:
            out[key] = {"op": "matches", "value": served}
    return out


def _text_contains(
    node: Mapping[str, Any], *, fields: Fields, path: str, adjusted: list[dict[str, str]],
) -> Optional[dict[str, Any]]:
    """`contains` on `text` with a string: served as `matches` (`_as_text_matches`).

    On attrs `contains` is JSON containment, and on attrs.<key> text inside
    the value; on `text` a string can only mean "the text holds this"."""
    if not (
        node.get("op") == "contains" and node.get("field") == _TEXT
        and set(node) <= {"op", "field", "value"} and fields.known(_TEXT)
    ):
        return None
    return _as_text_matches(
        node.get("value"), sent="contains on `text`", path=path, adjusted=adjusted,
    )


def _near_miss(
    node: Mapping[str, Any], path: str, adjusted: list[dict[str, str]], *, tool: str = "",
) -> Optional[dict[str, Any]]:
    """The one clause a near-miss test can mean, or None.

    Served with a note (`adjusted`):

    * `in` or `between` with its list in `value` and no `values`: the list
      is the operand (`between` takes exactly two);
    * an operator word that means one test (`_word_reading`): `gte`/`ge`
      and `lte`/`le` with a `value` are `gt` (or `lt`) or `eq`, which every
      path compares in the same type, so the two together are "at least"
      (or "at most") that value; `equals`, `startswith`, `endswith`, an
      operator in another case or behind a `$` are that operator; `ilike`
      is like, which also ignores accents.

    A word with two readings (`ne`, a near spelling) is refused with the
    call it most likely means; one with none is refused in words.
    """
    op = node.get("op")
    field = node.get("field")
    if not isinstance(op, str) or not isinstance(field, str) or not field:
        return None
    if op not in ("in", "between"):
        return _served_word(node, path, adjusted, tool=tool)
    if set(node) != {"op", "field", "value"}:
        return None
    value = node["value"]
    if op in ("in", "between"):
        if not isinstance(value, list) or not value or (op == "between" and len(value) != 2):
            return None
        adjusted.append({
            "rule": "values_in_value",
            "sent": f"{_place(path)} {op} on {_name(field)} with its list in `value`",
            "served": f"{op} with that list in `values`",
        })
        return {"op": op, "field": field, "values": list(value)}
    return None


def _word_reading(
    node: Mapping[str, Any], *, tool: str = "",
) -> Optional[tuple[str, Any, str, Optional[dict[str, str]]]]:
    """How a test whose `op` is an operator word but not an operator reads
    (`OPERATOR_WORDS`, issue #3790), or None for one this does not read.

    `(mode, clause, words, note)`: `serve` (served as `clause` with `note`),
    `corrected` or `guess` (`clause` is offered in a refusal; `words` say
    what it assumes), or `unfixable` (refused in `words`; no clause). One
    reading, the same in the long form and the short form with `op`, and the
    same as the word written as a key (`_operator_fix`). `tool` is the tool
    the condition was sent to: search's `where` takes no like, so its words
    name search's own `contains` and list its operators without like.
    """
    op, field = node.get("op"), node.get("field")
    if not isinstance(op, str) or op in _LEAF_OPS or op in _LOGICAL_OPS:
        return None
    if not isinstance(field, str) or not field or not set(node) <= {
        "op", "field", "value", "values",
    }:
        return None
    word = _operator_word(op)
    if word is None:
        return None
    table, kind, exact = word
    shown = _name(op) if _PLAIN_NAME.match(op) else "this operator"
    if kind in ("pattern", "none", "near"):
        search = tool == "search"
        substring = (
            "for text inside one field use `contains` with a text value on "
            "attrs.<key>, path or title"
            if search else "for a substring of a field use `like`"
        )
        operators = ", ".join(
            name for name in _LEAF_OPS if not (search and name in _LIKE_OPS)
        )
        return "unfixable", None, (
            f"{shown} has no one meaning here: {substring}; for words or a regex "
            "in a record's text, `matches` on `text`"
            if kind == "pattern" else
            f"{shown} is not an operator here; the operators are {operators}"
        ), None
    meant = OPERATOR_WORDS[table][1]
    if kind == "inclusive":
        value = node.get("value")
        if set(node) != {"op", "field", "value"} or value is None or isinstance(
            value, (dict, list, bool),
        ) or Fields.is_date(field):
            # No single reading (or a date, whose `eq` has its own rules):
            # left for the tool's refusal.
            return None
        clause: Any = {"op": "or", "clauses": [
            {"op": meant, "field": field, "value": value},
            {"op": "eq", "field": field, "value": value},
        ]}
        words = f"`{op}` on {_name(field)} is `{meant}` or `eq`"
        note = {"rule": "inclusive_comparison", "served": f"{meant} or eq"}
    elif kind == "negated":
        inner = {**node, "op": meant}
        return "guess", {"op": "not", "clause": inner}, (
            f"{shown} on {_name(field)} is read as not `{meant}`, which also "
            "keeps records without that field"
        ), None
    else:
        clause = {**node, "op": meant}
        words = f"`{op}` is written `{meant}`"
        note = {
            "rule": "ilike_as_like" if table == "ilike" else "operator_word",
            "served": "like, which also ignores accents" if table == "ilike" else meant,
        }
    if not exact:
        return "guess", clause, f"if {shown} on {_name(field)} means `{table}`, {words}", None
    return "serve", clause, words, note


def _served_word(
    node: Mapping[str, Any], path: str, adjusted: list[dict[str, str]], *, tool: str = "",
) -> Optional[Any]:
    """`node` served by `_word_reading`: the clause with its note, a refusal
    for an offer or a word with no reading, or None when it reads nothing."""
    reading = _word_reading(node, tool=tool)
    if reading is None:
        return None
    mode, clause, words, note = reading
    if mode == "serve":
        adjusted.append({
            "rule": note["rule"],
            "sent": f"{_place(path)} {node['op']} on {_name(node['field'])}",
            "served": note["served"],
        })
        return clause
    raise GrammarRefusal(
        f"{path}: the test on {_name(node['field'])} names "
        f"{_name(node['op']) if _PLAIN_NAME.match(node['op']) else 'an operator'}, "
        "which is not an operator here",
        fix_hint=words, fields=(path,), repairable=mode != "unfixable",
    )


#: What `matches` on `text` reads differently from like: the words a like
#: on `text` is refused with, beside the matches call it is offered.
_LIKE_ON_TEXT = (
    "`like` compares one attribute or column; a record's text is matched "
    "with `matches` on `text`, which ignores case but not accents, and whose "
    "`.` does not cross a line feed"
)


def _like_as_contains(node: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """A like or like_exact test written as search's `contains` with the
    same text (its ends' % stripped), or None when search's `contains`
    reads no text in its field (`search_where._text_contains_field`) or its
    operand is not one to compare."""
    from okg.substrate.mcp.search_where import _text_contains_field

    field = node.get("field")
    if not isinstance(field, str) or not _text_contains_field(field):
        return None
    served, problem = _like_operand(str(node.get("op")), node.get("value"))
    if problem is not None:
        return None
    return {"op": "contains", "field": field, "value": served}


def _likes_as_contains(condition: Any, key: Optional[str] = None, depth: int = 0) -> Any:
    """`condition` (long or short form) with every like or like_exact test
    written as search's `contains` (`_like_as_contains`), or None when one
    of them cannot be. `key` is the short-form field a test sits under."""
    if depth > MAX_CONDITION_DEPTH:
        return None
    if isinstance(condition, list):
        out_list = [_likes_as_contains(item, None, depth + 1) for item in condition]
        return None if any(item is None for item in out_list) else out_list
    if not isinstance(condition, Mapping):
        return condition
    op = condition.get("op")
    if op in _LIKE_OPS:
        test = dict(condition)
        if key is not None and "field" not in test:
            converted = _like_as_contains({**test, "field": key})
            return None if converted is None else {
                k: v for k, v in converted.items() if k != "field"
            }
        return _like_as_contains(test)
    out: dict[str, Any] = {}
    for name, value in condition.items():
        if name in ("op", "field", "value", "values"):
            out[name] = value
            continue
        child_key = name if "op" not in condition else None
        converted = _likes_as_contains(value, child_key, depth + 1)
        if converted is None and value is not None:
            return None
        out[name] = converted
    return out


def _like_fitted(
    node: Mapping[str, Any], fields: Fields, path: str, adjusted: list[dict[str, str]],
) -> Mapping[str, Any]:
    """A like or like_exact test once its field and operand fit (loose-match-3666).

    Served as sent, served with its ends' % stripped and a note, or refused
    by name with what to write: on `text` (the `matches` call is offered),
    on the whole `attrs`, on a date, on a link id or digest, with an
    interior % (two likes joined by `and` match more broadly), with an
    operand that is not 1 to 200 characters of text, and in search's
    `where` (search's own call with `contains` is offered on a field it
    reads text in, attrs.<key>, path, title or source; filter's call on any
    other). Every argument, so a substring test means one thing wherever it
    is written.
    """
    op, field = node.get("op"), node.get("field")
    if op not in _LIKE_OPS or not isinstance(field, str) or not field:
        return node
    if fields.tool == "search" and _like_as_contains(node) is not None:
        # Search's own `contains` finds text inside this field: offered as
        # search's call, so the ranking is kept (a guess: contains ignores
        # case but not accents).
        refusal = GrammarRefusal(
            f"{path}: search's `where` does not take `{op}`; `contains` with a "
            f"text value finds text inside {_name(field)}",
            fix_hint=(
                f"write the test on {_name(field)} as `contains` with the same text, "
                "which ignores case but not accents; filter's `where` takes like, "
                "which also ignores accents, and lists every match unranked"
            ),
            fields=(path,), repairable=True,
        )
        refusal.likes_as_contains = True
        raise refusal
    if fields.tool == "search":
        refusal = GrammarRefusal(
            f"{path}: search's `where` does not take `{op}`; filter lists every "
            "record whose field holds a substring",
            fix_hint=(
                "filter lists the records that match, not ranked by a query; "
                "send the condition to filter in `where`"
            ),
            fields=(path,), repairable=True,
        )
        refusal.retry_tool = "filter"
        raise refusal
    if field == _TEXT:
        raise GrammarRefusal(
            f"{path}: `{op}` does not compare `text`", fix_hint=_LIKE_ON_TEXT,
            fields=(path,), repairable=True,
        )
    unfit = None
    if field == "attrs":
        unfit = "`attrs` is the whole attribute object; compare one attribute as attrs.<name>"
    elif Fields.is_date(field):
        unfit = f"{_name(field)} compares dates; `{op}` compares text"
    elif field in _INTEGER_COLUMNS or field == "text_hash":
        unfit = f"{_name(field)} is not text a substring is read in"
    if unfit is not None:
        raise GrammarRefusal(f"{path}: `{op}` on {_name(field)}: {unfit}", fix_hint=unfit,
                             fields=(path,))
    served, problem = _like_operand(op, node.get("value"))
    if problem == "interior":
        raise GrammarRefusal(
            f"{path}: `{op}` on {_name(field)} has % inside its text; % is not a "
            "wildcard here",
            fix_hint=(
                "write the text without %; two like tests joined by `and`, one "
                "per piece, match more broadly, since they ignore the order of the pieces"
            ),
            fields=(path,),
        )
    if problem is not None:
        raise GrammarRefusal(f"{path}: {problem}", fix_hint=problem, fields=(path,))
    if served == node.get("value"):
        return node
    adjusted.append({
        "rule": "like_percent_stripped",
        "sent": f"{_place(path)} {op} on {_name(field)} with % at its ends",
        "served": f"{op} {_shown(served)} (% is not a wildcard here)",
    })
    return {**node, "value": served}


def _text_matches_value(value: str) -> Optional[str]:
    """The `matches` value a like on `text` with `value` is offered as: the
    word itself, or a regex of its pieces in order (`re:/a.*b/`)."""
    from okg.substrate.mcp.grep_query import _escape_literal

    pieces = [piece for piece in value.split("%") if piece]
    if not pieces:
        return None
    if len(pieces) == 1 and re.fullmatch(r"[A-Za-z0-9]+", pieces[0]):
        return pieces[0]
    return "re:/" + ".*".join(
        _escape_literal(piece).replace("/", "\\/") for piece in pieces
    ) + "/"


def _repaired_like(
    node: Mapping[str, Any], fields: Fields, notes: list[tuple[str, str]],
) -> Mapping[str, Any]:
    """A like test as a refusal offers it: its ends' % stripped, and on
    `text` the `matches` call (a guess: matches ignores case, not accents)."""
    op, field, value = node.get("op"), node.get("field"), node.get("value")
    if op not in _LIKE_OPS or not isinstance(field, str):
        return node
    if fields.tool == "search":
        served, problem = _like_operand(op, value)
        if problem is None and served != value:
            notes.append(("corrected", "% is not a wildcard here"))
            return {**node, "value": served}
        return node
    if field == _TEXT:
        offered = _text_matches_value(value) if isinstance(value, str) else None
        if offered is None:
            notes.append(("unfixable", _LIKE_ON_TEXT))
            return node
        notes.append(("guess", _LIKE_ON_TEXT))
        return {"op": "matches", "field": _TEXT, "value": offered}
    served, problem = _like_operand(op, value)
    if problem == "interior":
        notes.append((
            "unfixable",
            "write the text without %; two like tests joined by `and`, one per "
            "piece, match more broadly, since they ignore the order of the pieces",
        ))
        return node
    if problem is not None:
        notes.append(("unfixable", problem))
        return node
    if served != value:
        notes.append(("corrected", "% is not a wildcard here"))
        return {**node, "value": served}
    return node


#: The column whose values are integers: a link's id (stored as its digits,
#: compared as a number). Every other column of a set compares as text.
_INTEGER_COLUMNS = frozenset({"edge_id"})
#: A link id written as text: its digits (ids are below 2**63 < 10**19).
_ID_DIGITS = re.compile(r"[0-9]{1,19}")
#: How sure a repair is, surest first; `unfixable` is a refusal in words.
_FIT_MODES = ("serve", "corrected", "guess")

#: `(mode, fixed clause, problem, words)`; see `_operand_fit`.
_Fit = tuple[str, Any, str, str]


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _exact_float(value: Any) -> bool:
    """False for a float that is not exactly the number written as one: not
    finite, or past 2**53, where floats skip whole numbers. True otherwise."""
    if not isinstance(value, float):
        return True
    return math.isfinite(value) and abs(value) < 2 ** 53


def _unfixable(problem: str, words: str) -> _Fit:
    return "unfixable", None, problem, words


def _operand_fit(node: Mapping[str, Any], fields: Fields) -> Optional[_Fit]:
    """How a clause's operand fits the field it names (issue #3666, 2c).

    One rule over operator, field kind and value type, so no clause of a
    set read compiles to "matches nothing" because of its operand's type:

    * a text column (`subtype`, `node_id`, `src`, ...) compares text, and
      `null` in `eq` or `in` is "has no value"; a whole number is offered
      as its digits (a guess); any other value is refused;
    * the link id (`edge_id`) compares whole numbers and their digits; a
      whole number written as a float is served with a note, a fractional
      range bound is offered as the whole number it means, and any other
      value is refused;
    * `eq` with a list on a column is offered as `in`;
    * one attribute (`attrs.<name>`) holds any JSON value, so `eq` and `in`
      compare it exactly; a range bound is a number or text, and the two
      bounds of `between` are the same kind;
    * the whole attribute object (`attrs`) equals only an object and takes
      no range or text test;
    * a named date (`date.<name>`) refuses every operand that is not a
      date itself (`date_conditions`), so it is not read here.

    search's `where` takes the column rule on its own fields, every one of
    them text (`id`, `type`, `source`, `path`, `title`); its dates and
    attributes keep the rules search gives them.

    None when the operand fits, or for a clause this does not read (a live
    result set's rows keep their own rules). Otherwise ``(mode, fixed,
    problem, words)``: `serve` (served as `fixed` with a note), `corrected`
    or `guess` (`fixed` is offered in a refusal), or `unfixable` (refused in
    words; `fixed` is None).
    """
    if not fields.names:
        return None
    field = node.get("field")
    if not isinstance(field, str) or not field:
        return None
    if fields.tool == "search":
        return _column_fit(node, field) if field in fields.names else None
    if field == "attrs":
        return _whole_attrs_fit(node)
    if field.startswith("attrs.") and len(field) > len("attrs."):
        return _attribute_fit(node, field)
    if field in fields.names:
        return _column_fit(node, field)
    return None


def _operands(node: Mapping[str, Any]) -> Optional[tuple[str, list[Any]]]:
    """`(key, operands)` of a comparison; None for a shape the tool refuses."""
    op = node.get("op")
    if op in ("eq", "lt", "gt"):
        return "value", [node.get("value")]
    if op in ("in", "between"):
        raw = node.get("values")
        if not isinstance(raw, list) or not raw or (op == "between" and len(raw) != 2):
            return None
        return "values", list(raw)
    return None


def _column_fit(node: Mapping[str, Any], column: str) -> Optional[_Fit]:
    op = node.get("op")
    if op == "eq" and isinstance(node.get("value"), list):
        values = node["value"]
        if not values:
            return _unfixable(
                f"`eq` compares `{column}` with one value, and the list is empty",
                "name at least one value",
            )
        as_in = {"op": "in", "field": column, "values": list(values)}
        inner = _column_fit(as_in, column)
        if inner is not None and inner[0] == "unfixable":
            return inner
        fixed = inner[1] if inner is not None else as_in
        return (
            "guess" if inner is not None and inner[0] == "guess" else "corrected",
            fixed,
            f"`eq` compares `{column}` with one value, and {_spell(values)} is a list",
            f"for any of these values write {_spell(fixed)}",
        )
    value = node.get("value")
    if (
        op in ("prefix", "suffix") and column in _INTEGER_COLUMNS
        and isinstance(value, str) and value and not _ID_DIGITS.fullmatch(value)
    ):
        # A link id's digits start or end with digits only. (A value that is
        # not text is the tool's own refusal.)
        return _unfixable(
            f"`{column}` is a whole number, and {_spell(value)} is not digits",
            "write the digits the id starts or ends with",
        )
    taken = _operands(node)
    if taken is None:
        return None
    key, bounds = taken
    integer = column in _INTEGER_COLUMNS
    ranged = op in ("lt", "gt", "between")
    fixed: list[Any] = []
    mode = "serve"
    for index, bound in enumerate(bounds):
        if bound is None and not ranged:
            # "Has no value": a correct test, which no stored id or type meets.
            fixed.append(bound)
        elif integer:
            if _is_int(bound) or (
                not ranged and isinstance(bound, str) and _ID_DIGITS.fullmatch(bound)
            ):
                fixed.append(bound)
            elif not _exact_float(bound):
                # A float past 2**53 is not the number written (a link id is
                # up to 19 digits): which id was meant cannot be told.
                return _unfixable(
                    f"`{column}` is a whole number, and {_spell(bound)} as a float "
                    "is not exact",
                    "write the value as a whole number, not a float",
                )
            elif isinstance(bound, float) and bound.is_integer():
                fixed.append(int(bound))
            elif isinstance(bound, float) and ranged:
                # The integers past a fractional bound: exact for whole ids.
                low_side = op == "gt" or (op == "between" and index == 0)
                fixed.append(math.floor(bound) if op == "gt" else (
                    math.ceil(bound) if low_side or op == "lt" else math.floor(bound)))
                mode = max(mode, "corrected", key=_FIT_MODES.index)
            elif ranged and isinstance(bound, str) and _ID_DIGITS.fullmatch(bound):
                fixed.append(int(bound))
                mode = "guess"
            else:
                return _unfixable(
                    f"`{column}` is a whole number, and {_spell(bound)} is not one",
                    "write the value as a whole number",
                )
        elif isinstance(bound, str):
            fixed.append(bound)
        elif _is_int(bound):
            fixed.append(str(bound))
            mode = "guess"
        else:
            return _unfixable(
                f"`{column}` compares as text, and {_spell(bound)} is not text",
                "write the value as text",
            )
    if fixed == bounds and all(type(a) is type(b) for a, b in zip(fixed, bounds)):
        return None
    out = dict(node)
    out[key] = fixed if key == "values" else fixed[0]
    kind = "a whole number" if integer else "text"
    problem = f"`{column}` compares as {kind}, and {_spell(node.get(key))} does not fit"
    words = (
        f"write {_spell(out[key])}" if mode != "guess" else
        f"if {_spell(out[key])} is meant, write it so"
    )
    return mode, out, problem, words


def _attribute_fit(node: Mapping[str, Any], field: str) -> Optional[_Fit]:
    """A range on one attribute: its bounds are numbers, or text.

    `eq` and `in` compare the stored JSON value exactly, so every operand
    is a correct test there. A range compares numbers with numbers and text
    with text; a bound that is neither, or a `between` with one of each,
    holds for no value, and which was meant cannot be told.

    `eq` with a list can mean any of those values (`in`) or a stored list
    equal to this one: it is refused, offering `in` as a guess, and the
    words name the `contains` test for the whole list.
    """
    if node.get("op") == "eq" and isinstance(node.get("value"), list):
        values = node["value"]
        whole = {"op": "contains", "field": "attrs",
                 "value": {field[len("attrs."):]: list(values)}}
        problem = (
            f"`eq` with a list on {_name(field)} can mean any of these values or a "
            "stored list equal to this one"
        )
        if not values:
            return _unfixable(problem, f"for a stored empty list write {_spell(whole)}")
        as_in = {"op": "in", "field": field, "values": list(values)}
        return (
            "guess", as_in, problem,
            f"for a stored list equal to this one write {_spell(whole)}; for any of "
            f"these values write {_spell(as_in)}",
        )
    if node.get("op") not in ("lt", "gt", "between"):
        return None
    taken = _operands(node)
    if taken is None:
        return None
    bounds = taken[1]
    for bound in bounds:
        if not (_is_number(bound) or isinstance(bound, str)):
            return _unfixable(
                f"a range on {_name(field)} compares numbers or text, and "
                f"{_spell(bound)} is neither",
                "write the bound as a number, or as text",
            )
    if len(bounds) == 2 and _is_number(bounds[0]) != _is_number(bounds[1]):
        return _unfixable(
            f"between on {_name(field)} has one number and one text bound; a "
            "value compares as one or the other",
            "write both bounds as numbers, or both as text",
        )
    return None


def _whole_attrs_fit(node: Mapping[str, Any]) -> Optional[_Fit]:
    """`attrs` is the whole attribute object: it equals only an object."""
    op = node.get("op")
    words = (
        "compare one attribute as attrs.<name>, or match stored values with "
        '{"op": "contains", "field": "attrs", "value": {"<name>": <value>}}'
    )
    if op in ("lt", "gt", "between", "prefix", "suffix"):
        return _unfixable(
            f"`attrs` is the whole attribute object, which `{op}` does not compare",
            words,
        )
    taken = _operands(node) if op in ("eq", "in") else None
    if taken is None:
        return None
    for bound in taken[1]:
        if not isinstance(bound, Mapping):
            return _unfixable(
                f"`attrs` is the whole attribute object, and {_spell(bound)} is not an object",
                words,
            )
    return None


def _fitted(
    clause: Mapping[str, Any], fields: Fields, path: str, adjusted: list[dict[str, str]],
) -> Mapping[str, Any]:
    """`clause` once its operand meets `_operand_fit`: as sent, served with a
    note (a whole number written as a float on the link id), or refused."""
    if clause.get("op") not in _LEAF_OPS:
        return clause
    fit = _operand_fit(clause, fields)
    if fit is None:
        return clause
    mode, fixed, problem, words = fit
    if mode == "serve":
        adjusted.append({
            "rule": "integral_bound",
            "sent": f"{_place(path)} {clause['op']} on {_name(clause['field'])} with a float",
            "served": "the whole number it equals",
        })
        return fixed
    raise GrammarRefusal(
        f"{path}: {problem}", fix_hint=words, fields=(path,),
        repairable=mode != "unfixable",
    )


def _lower(
    node: Any, *, fields: Fields, new_spelling: bool, path: str,
    adjusted: list[dict[str, str]], depth: int, check_fields: bool = False,
    read_words: bool = False,
) -> Any:
    if not isinstance(node, Mapping):
        return node
    if depth > MAX_CONDITION_DEPTH and (new_spelling or "op" not in node):
        # A limit on what this grammar adds. A long-form tree in an argument
        # a tool already had is walked to any depth and passed on, as before.
        raise GrammarRefusal(
            f"{path}: a condition nests at most {MAX_CONDITION_DEPTH} levels",
            fix_hint="flatten the condition: `and` and `or` take any number of clauses",
            fields=(path,),
        )
    if not node:
        return {}
    _refuse_non_string_key(node, path)
    keys = set(node)
    contained = _text_contains(node, fields=fields, path=path, adjusted=adjusted)
    if contained is not None:
        return contained
    if "op" in keys:
        checked = new_spelling or check_fields
        # Over a saved result (`read_words`) an operator word reads as it
        # does on the set the explicit call names (issue #3790); its fields
        # stay unchecked, since its rows are the reference's own.
        near_miss = (
            _near_miss(node, path, adjusted, tool=fields.tool) if checked
            else _served_word(node, path, adjusted, tool=fields.tool) if read_words else None
        )
        if near_miss is not None:
            node = near_miss
        out = dict(node)
        clauses = node.get("clauses")
        if isinstance(clauses, list):
            out["clauses"] = [
                _lower(clause, fields=fields, new_spelling=new_spelling,
                       path=f"{path}.clauses[{index}]", adjusted=adjusted, depth=depth + 1,
                       check_fields=check_fields, read_words=read_words)
                for index, clause in enumerate(clauses)
            ]
        if isinstance(node.get("clause"), Mapping):
            out["clause"] = _lower(
                node["clause"], fields=fields, new_spelling=new_spelling,
                path=f"{path}.clause", adjusted=adjusted, depth=depth + 1,
                check_fields=check_fields, read_words=read_words,
            )
        if (
            checked
            and node["op"] in _LEAF_OPS
            and isinstance(node.get("field"), str) and node["field"]
        ):
            # In a top-level `where`, and in every condition of a set read
            # (`check_fields`, issue #3666 step 2c), a clause's field is
            # checked and takes the second names, like a short-form key.
            # Elsewhere the clause keeps its field as sent.
            out["field"] = _clause_field(node["field"], fields, path, alias=True)
            if out["field"] != node["field"] and not new_spelling:
                # A second name in an argument the tool already had: served,
                # and the note says so (step 2c, "lenient yes").
                adjusted.append({
                    "rule": "second_name",
                    "sent": f"{_place(path)} clause on {_name(node['field'])}",
                    "served": f"on `{out['field']}`",
                })
            out = _fitted(out, fields, path, adjusted)
        elif (
            fields.tool == "search" and node["op"] in _LEAF_OPS
            and isinstance(node.get("field"), str)
        ):
            # search's `where` keeps its long form as sent, but its operands
            # meet the column rule: no type mismatch matches nothing.
            out = _fitted(out, fields, path, adjusted)
        # In any argument a substring test means one thing: its field and
        # operand are checked, and its ends' % stripped with a note.
        out = _like_fitted(out, fields, path, adjusted)
        # In any argument, a text value's regex written without its slashes
        # is served with them and a note, or refused when it has two readings.
        return _text_regex_slashed(out, path=path, adjusted=adjusted)
    if keys & RESERVED_KEYS:
        field = node.get("field")
        if keys == {"field", "value"} and isinstance(field, str) and field:
            # Served without its `op`, so the grammar answers for the field
            # too: it is checked, and the note says everything that changed.
            column = _clause_field(field, fields, path, alias=new_spelling or check_fields)
            value = node["value"]
            if column == _TEXT:
                # A record's whole text equal to a value is not a test anyone
                # means; with no `op` it can only mean the text holds it.
                served = _as_text_matches(
                    value, sent="clause on `text` with no op", path=path,
                    adjusted=adjusted,
                )
                if served is not None:
                    return served
            adjusted.append({
                "rule": "clause_without_op",
                "sent": f"{_place(path)} clause on {_name(field)} with no op",
                "served": "eq" if column == field else f"eq on `{column}`",
            })
            return {"op": "eq", "field": column, "value": node["value"]}
        if keys == {"field", "values"} and isinstance(field, str) and field:
            as_in = _spell({"op": "in", **node})
            as_between = _spell({"op": "between", **node})
            both = f"for any of the values write {as_in}; for a range write {as_between}"
            raise GrammarRefusal(
                f"{path}: the clause on {_name(field)} has `values` and no "
                "`op`; it can mean `in` or `between`",
                fix_hint=(
                    both if len(both) <= _SPELLED_CHARS else
                    'add "op": "in" to that clause for any of the values, or '
                    '"op": "between" for a range [low, high]'
                ),
                fields=(path,),
            )
        # Any other clause without `op` has no single reading; it is left as
        # sent for the refusal the predicate model gives it.
        return dict(node)
    clauses = []
    for key, value in node.items():
        if key == _TEXT and isinstance(value, Mapping):
            contained = _text_contains(
                {**value, "field": _TEXT}, fields=fields, path=path, adjusted=adjusted,
            )
            if contained is not None:
                clauses.append(contained)
                continue
            keyed = next(iter(value)) if len(value) == 1 else None
            if (
                keyed in ("matches", "contains") and fields.known(_TEXT)
                and isinstance(value[keyed], str)
            ):
                # `{"text": {"matches": "..."}}`: the operator written as a
                # key. On `text` both can mean only the text condition.
                served = _as_text_matches(
                    value[keyed], sent=f"`{keyed}` written as a key of `text`",
                    path=path, adjusted=adjusted,
                )
                if served is not None:
                    clauses.append(served)
                    continue
        if key == _TEXT and fields.known(_TEXT) and isinstance(value, str):
            # `{"text": "reaper pods"}`: a record's whole text equal to a
            # value is not a test anyone means; it can only mean `matches`.
            served = _as_text_matches(
                value, sent="`text` given a plain value", path=path, adjusted=adjusted,
            )
            if served is not None:
                clauses.append(served)
                continue
        # The short form is new in every argument, so its operands always
        # meet the one rule (`_operand_fit`): a short-form test never
        # compiles to "matches nothing" because of its value's type (issue
        # #3666, 2c).
        leaf = _short_clause(key, value, node=node, fields=fields, path=path)
        # An operator word in `op` reads as it does in the long form and as
        # a key (`OPERATOR_WORDS`, issue #3790); what it is served as is
        # read again, as any long-form condition is.
        served = _served_word(leaf, path, adjusted, tool=fields.tool)
        if served is not None:
            clauses.append(_lower(
                served, fields=fields, new_spelling=True, path=path,
                adjusted=adjusted, depth=depth + 1, check_fields=check_fields,
                read_words=read_words,
            ))
            continue
        clauses.append(_text_regex_slashed(
            _like_fitted(_fitted(leaf, fields, path, adjusted), fields, path, adjusted),
            path=path, adjusted=adjusted,
        ))
    if len(clauses) == 1:
        return clauses[0]
    return {"op": "and", "clauses": clauses}


def lower_condition(
    condition: Any,
    *,
    fields: Fields = NODE_FIELDS,
    new_spelling: bool = False,
    argument: str = "where",
    check_fields: bool = False,
    read_words: bool = False,
) -> tuple[Any, list[dict[str, str]]]:
    """`condition` in the long form, and the rewrites the reply must disclose.

    `fields` says which names a condition may read; a short-form key outside
    them is refused. `new_spelling` is a top-level `where` on a tool that
    had none: there a long-form clause's field is checked and takes the
    second names as well. Otherwise a long-form clause comes back as sent,
    except that a text value's regex written without its slashes is served
    with them and a note. `{}` stays `{}`; a value that is not an object is returned as it
    is. `argument` is the argument the condition was sent in, for messages.
    `check_fields` reads every long-form clause as the new spelling does,
    and serves the near-miss clauses `_near_miss` names (issue #3666, 2c).
    `read_words` reads an operator word in `op` (`OPERATOR_WORDS`) where
    fields are not checked: a saved result's condition (issue #3790).
    """
    adjusted: list[dict[str, str]] = []
    lowered = _lower(
        condition, fields=fields, new_spelling=new_spelling, path=argument,
        adjusted=adjusted, depth=0, check_fields=check_fields, read_words=read_words,
    )
    return lowered, adjusted


# -- the corrected condition a refusal offers -------------------------------


def _repaired_field(
    field: str, fields: Fields, notes: list[tuple[str, str]], *, alias: bool,
) -> str:
    if field == "attrs":
        return field
    if fields.known(field):
        column = fields.aliases.get(field, field)
        if alias or column == field:
            return field
        notes.append(("corrected", f"this set's field is `{column}`"))
        return column
    fix = _replacement(field, fields, frozenset())
    if fix is None:
        notes.append(("unfixable", _unknown_field(field, fields, "", frozenset()).fix_hint))
        return field
    notes.append((fix[0], fix[2]))
    return fix[1]


def _repaired(
    node: Any, fields: Fields, notes: list[tuple[str, str]], *,
    new_spelling: bool, depth: int = 0, aliases: Optional[bool] = None,
) -> Any:
    """The caller's condition with each mistake that has one fix, fixed.

    `new_spelling` checks every long-form clause's field; `aliases` (by
    default the same) keeps a second name as written. Without it a second
    name is written as the column, so the repaired call needs no note.

    A logical word used as a key becomes the long form it stands for, a name
    with one replacement takes it, a test that repeats its key as `field`
    loses the repeat, an operator written as a key moves into `op`, and a
    clause served without `op` gets it. `notes` gets `(mode, words)` for
    every change, so a refusal can say each one and whether any is a guess.
    Everything else is kept as written; whether the result is a call the
    server serves is decided by rewriting it again.
    """
    if not isinstance(node, Mapping) or depth > MAX_CONDITION_DEPTH:
        return node
    if not all(isinstance(key, str) for key in node):
        return node
    if aliases is None:
        aliases = new_spelling

    def again(child: Any) -> Any:
        return _repaired(
            child, fields, notes, new_spelling=new_spelling, depth=depth + 1, aliases=aliases,
        )

    if "op" in node:
        out = dict(node)
        reading = _word_reading(node, tool=fields.tool) if new_spelling else None
        if reading is not None:
            # An operator word (issue #3790): the clause it reads as, said.
            mode, clause, words, _note = reading
            notes.append(("corrected" if mode == "serve" else mode, words))
            if clause is not None and clause["op"] in _LOGICAL_OPS:
                return again(clause)
            if clause is not None:
                out = node = dict(clause)
        if isinstance(node.get("clauses"), list):
            out["clauses"] = [again(clause) for clause in node["clauses"]]
        if isinstance(node.get("clause"), Mapping):
            out["clause"] = again(node["clause"])
        out = dict(_repaired_like(out, fields, notes))
        if (
            new_spelling and out["op"] in _LEAF_OPS
            and isinstance(out.get("field"), str) and out["field"]
        ):
            out["field"] = _repaired_field(out["field"], fields, notes, alias=aliases)
            fit = _operand_fit(out, fields)
            if fit is not None:
                mode, fixed, _problem, words = fit
                if mode == "unfixable":
                    notes.append(("unfixable", words))
                else:
                    # An integral float is written as its integer, so the
                    # offered call needs no note either.
                    notes.append(("guess" if mode == "guess" else "corrected", words))
                    out = fixed
        elif (
            fields.tool == "search" and out["op"] in _LEAF_OPS
            and isinstance(out.get("field"), str)
        ):
            fit = _operand_fit(out, fields)
            if fit is not None:
                mode, fixed, _problem, words = fit
                if mode == "unfixable":
                    notes.append(("unfixable", words))
                else:
                    notes.append(("guess" if mode == "guess" else "corrected", words))
                    out = fixed
        return out
    if set(node) & RESERVED_KEYS:
        if set(node) == {"field", "value"} and isinstance(node["field"], str) and node["field"]:
            notes.append((
                "corrected", f'add "op": "eq" to the clause on {_name(node["field"])}',
            ))
            return {
                "op": "eq",
                "field": _repaired_field(node["field"], fields, notes, alias=aliases),
                "value": node["value"],
            }
        return dict(node)
    plain: dict[str, Any] = {}
    logical: list[dict[str, Any]] = []
    taken = frozenset(node)
    for key, value in node.items():
        if key in _LOGICAL_OPS and not fields.known(key) and _holds_conditions(key, value):
            notes.append(("corrected", f"write `{key}` with `op`"))
            logical.append(
                {"op": "not", "clause": again(value)} if key == "not"
                else {"op": key, "clauses": [again(clause) for clause in value]}
            )
            continue
        if key in _LOGICAL_OPS and not fields.known(key):
            notes.append(("unfixable", f"`{key}` here does not hold " + (
                "one condition" if key == "not" else "a list of conditions"
            )))
        elif not fields.known(key):
            fix = _replacement(key, fields, taken - {key})
            if fix is not None:
                notes.append((fix[0], fix[2]))
                key = fix[1]
            else:
                notes.append((
                    "unfixable", _unknown_field(key, fields, "", taken - {key}).fix_hint,
                ))
        if key in plain:
            # Two keys that come to one name: writing either over the other
            # would drop a condition the caller sent.
            notes.append((
                "unfixable",
                f"two keys both name {_name(key)}; a field is written once, so "
                "which of the two tests is meant cannot be told",
            ))
            continue
        if isinstance(value, Mapping) and "op" in value and value.get("field") == key:
            notes.append(("corrected", f"drop `field` from the test on {_name(key)}"))
            value = {k: v for k, v in value.items() if k != "field"}
        elif isinstance(value, Mapping) and "op" not in value and _operator_object(value):
            operator = _operator_fix(value)
            if operator is not None:
                notes.append((
                    "corrected", f"on {_name(key)} write the operator in `op`",
                ))
                if operator[0] != "test":
                    # "Does not exist" negates a test; it is a condition of
                    # its own, beside the other keys.
                    logical.append(_operator_fixed(key, operator))
                    continue
                value = operator[1]
        elif isinstance(value, Mapping) and "op" not in value and _misspelled_operator(value):
            miss = _misspelled_operator(value)
            fix = _misspelled_fix(value, miss[1])
            if fix is None:
                notes.append(("unfixable", f"no operator here is `{miss[0]}` exactly"))
            else:
                notes.append(_misspelled_note(key, value, miss))
                if fix[0] != "test":
                    # "At least" is two tests, and "not equal" a `not`:
                    # each a condition of its own, beside the other keys.
                    logical.append(_operator_fixed(key, fix))
                    continue
                value = fix[1]
        elif isinstance(value, Mapping) and "op" not in value and _is_unknown_key_object(value):
            # D1: the long form is the one way to match an object.
            column = fields.aliases.get(key, key)
            notes.append(("corrected", f"to match the object itself write {_object_match(key, value)}"))
            logical.append({"op": "eq", "field": column, "value": dict(value)})
            continue
        elif (
            isinstance(value, Mapping) and "op" in value and "field" not in value
            and (reading := _word_reading({**value, "field": key}, tool=fields.tool)) is not None
        ):
            # An operator word in `op` (issue #3790), as in the long form.
            mode, clause, words, _note = reading
            notes.append(("corrected" if mode == "serve" else mode, words))
            if clause is not None and clause["op"] in _LOGICAL_OPS:
                logical.append(_shortened(clause))
                continue
            if clause is not None:
                value = {k: v for k, v in clause.items() if k != "field"}
        plain[key] = _repaired_operand(key, value, fields, notes)
    if not logical:
        return plain
    if not plain and len(logical) == 1:
        return logical[0]
    return {"op": "and", "clauses": ([plain] if plain else []) + logical}


def _shortened(clause: Mapping[str, Any]) -> dict[str, Any]:
    """A long-form tree written with short-form leaves: `{field: test}`."""
    if clause.get("op") in ("and", "or"):
        return {"op": clause["op"], "clauses": [_shortened(c) for c in clause["clauses"]]}
    if clause.get("op") == "not":
        return {"op": "not", "clause": _shortened(clause["clause"])}
    return {clause["field"]: {k: v for k, v in clause.items() if k != "field"}}


def _repaired_operand(
    key: str, value: Any, fields: Fields, notes: list[tuple[str, str]],
) -> Any:
    """A short-form value once its operand meets `_operand_fit`, still short.

    The test the pair stands for is checked as `_lower` checks it; a fix is
    written back in the form the caller used (a value, a list, or a test
    object), and its words go to `notes`.
    """
    field = fields.aliases.get(key, key)
    if isinstance(value, list):
        clause: dict[str, Any] = {"op": "in", "field": field, "values": value}
    elif isinstance(value, Mapping) and value.get("op") in _LIKE_OPS and "field" not in value:
        # A substring test as offered: its ends' % stripped, or on `text`
        # the matches call (`_repaired_like`).
        liked = _repaired_like({**value, "field": field}, fields, notes)
        return {name: operand for name, operand in liked.items() if name != "field"}
    elif isinstance(value, Mapping) and value.get("op") in _LEAF_OPS and "field" not in value:
        clause = {**value, "field": field}
    elif isinstance(value, Mapping) and "op" in value:
        return value
    else:
        clause = {"op": "eq", "field": field, "value": value}
    fit = _operand_fit(clause, fields)
    if fit is None:
        return value
    mode, fixed, _problem, words = fit
    if mode == "unfixable":
        notes.append(("unfixable", words))
        return value
    notes.append(("guess" if mode == "guess" else "corrected", words))
    if isinstance(value, Mapping):
        return {name: operand for name, operand in fixed.items() if name != "field"}
    if fixed["op"] == "in":
        return fixed["values"]
    return fixed["value"]


def _given(value: Any) -> bool:
    """Whether an old spelling carries a condition (`{}` and null do not)."""
    return value is not None and value != {}


def _differ(left: Any, right: Any) -> bool:
    """Whether two argument values differ as JSON.

    Python calls `1` and `True` equal, and `1` and `1.0`; as stored values
    they are not the same, so two spellings that differ only so do differ.
    """
    def canonical(value: Any) -> str:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=repr)

    return canonical(left) != canonical(right)


def _is_type_clause(clause: Any) -> bool:
    return (
        isinstance(clause, Mapping)
        and set(clause) == {"op", "field", "value"}
        and clause["op"] == "eq"
        and clause["field"] == "subtype"
        and isinstance(clause["value"], str)
        and bool(clause["value"])
    )


def _take_type(condition: Any) -> tuple[Optional[str], Any]:
    """`(type, rest)` when the condition pins the type at its top level.

    The type is taken only from an `eq` that is the whole condition or one
    clause of a top-level `and`, so the set is the same conjunction either
    way. `rest` is `{}` when nothing else was asked.
    """
    if _is_type_clause(condition):
        return condition["value"], {}
    if not (
        isinstance(condition, Mapping)
        and set(condition) == {"op", "clauses"}
        and condition["op"] == "and"
        and isinstance(condition["clauses"], list)
        and all(isinstance(c, Mapping) and c for c in condition["clauses"])
    ):
        return None, condition
    clauses = condition["clauses"]
    for index, clause in enumerate(clauses):
        if _is_type_clause(clause):
            rest = clauses[:index] + clauses[index + 1:]
            if not rest:
                return clause["value"], {}
            if len(rest) == 1:
                return clause["value"], rest[0]
            return clause["value"], {"op": "and", "clauses": rest}
    return None, condition


def _both_spellings(tool: str, new: str, old: str) -> GrammarRefusal:
    return GrammarRefusal(
        f"{tool}: `{new}` and `{old}` both carry a condition and they differ; "
        f"`{new}` means what `{old}` means",
        fix_hint=(
            f"send only `{new}` or only `{old}`; they differ, so which one is "
            "meant cannot be told from the call"
        ),
        fields=(new, old),
    )


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


#: As many ranges as a text read takes (`record_text.MAX_LINE_RANGES`; not
#: imported, as record_text imports this module through response_bounds).
_MAX_LINE_RANGES = 20

#: A whole line number written as text: "146".
_DIGITS = re.compile(r"[0-9]{1,8}\Z")

#: The key pairs a range object may be written with: `{"start": 1, "end": 4}`.
_RANGE_KEYS = (("start", "end"), ("first", "last"))


def _number_like(value: Any) -> bool:
    return _whole(value) or (isinstance(value, str) and bool(_DIGITS.match(value)))


def _line_number(value: Any) -> tuple[Any, bool]:
    """`value` as a whole line number, and whether it was written as text."""
    if isinstance(value, str) and _DIGITS.match(value):
        return int(value), True
    return value, False


def _range_object(item: Mapping[Any, Any]) -> Optional[list[Any]]:
    """`{"start": a, "end": b}` (or first/last) as `[a, b]`; None otherwise."""
    for first, last in _RANGE_KEYS:
        if set(item) == {first, last}:
            return [item[first], item[last]]
    return None


def _ascending(ranges: list[list[Any]]) -> bool:
    """Each range starts after the one before ends (what the text read takes)."""
    return all(
        prev[1] is not None and cur[0] > prev[1]
        for prev, cur in zip(ranges, ranges[1:])
    )


def _in_order(ranges: list[list[Any]]) -> tuple[list[list[Any]], bool]:
    """`ranges` sorted by first line with overlapping ones merged, and
    whether any merged. An open end (null) reaches past every later range."""
    merged: list[list[Any]] = []
    joined = False
    for first, last in sorted(ranges, key=lambda pair: pair[0]):
        if merged and (merged[-1][1] is None or first <= merged[-1][1]):
            joined = True
            if merged[-1][1] is not None:
                merged[-1][1] = None if last is None else max(merged[-1][1], last)
            continue
        merged.append([first, last])
    return merged, joined


def _normal_lines(lines: Any, path: str) -> tuple[Any, list[dict[str, str]]]:
    """A text read's `lines` in the form the read takes, with one note per
    change (issue #3666, Format v10 B2); as sent with no notes when it is
    already that form or no shape read here.

    Read: one flat `[first, last]` pair; a flat even-length list of numbers
    (`[a, b, c, d]` is `[[a, b], [c, d]]`); `{"start", "end"}` range objects;
    whole numbers written as text; line 0 (lines count from 1, so 0 is 1);
    ranges out of order or overlapping (served ascending, overlaps merged).
    Anything else -- an odd flat list, first > last, more than 20 ranges, a
    value that is no number -- is left as sent for the range check to
    refuse, by name.
    """
    if not isinstance(lines, (list, tuple)) or not lines:
        return lines, []
    notes: list[dict[str, str]] = []
    items: list[Any] = list(lines)
    if all(not isinstance(item, (list, tuple, Mapping)) for item in items):
        if not (
            len(items) % 2 == 0 and len(items) <= 2 * _MAX_LINE_RANGES
            and all(_number_like(item) for item in items[:-1])
            and (items[-1] is None or _number_like(items[-1]))
        ):
            return lines, []
        if len(items) == 2:
            notes.append(_flat_lines_note(path))
        else:
            notes.append({
                "rule": "lines_flat_list",
                "sent": f"{path} as one flat list of numbers",
                "served": "consecutive pairs [[first, last], ...]",
            })
        items = [items[index:index + 2] for index in range(0, len(items), 2)]
    elif any(isinstance(item, Mapping) for item in items):
        pairs: list[Any] = []
        for item in items:
            pair = _range_object(item) if isinstance(item, Mapping) else item
            if pair is None:
                return lines, []
            pairs.append(pair)
        items = pairs
        notes.append({
            "rule": "lines_range_objects",
            "sent": f"{path} ranges as start/end objects",
            "served": "[first, last] pairs",
        })
    if not (
        len(items) <= _MAX_LINE_RANGES
        and all(isinstance(item, (list, tuple)) and len(item) == 2 for item in items)
    ):
        return lines, []
    ranges: list[list[Any]] = []
    as_text = False
    for item in items:
        first, first_text = _line_number(item[0])
        last, last_text = (None, False) if item[1] is None else _line_number(item[1])
        as_text = as_text or first_text or last_text
        ranges.append([first, last])
    if not all(_whole(first) and (last is None or _whole(last)) for first, last in ranges):
        return lines, []
    if as_text:
        notes.append({
            "rule": "lines_numbers_as_text",
            "sent": f"{path} numbers written as text",
            "served": "whole numbers",
        })
    if any(first == 0 for first, _ in ranges):
        ranges = [
            [1, 1 if last == 0 else last] if first == 0 else [first, last]
            for first, last in ranges
        ]
        notes.append({
            "rule": "lines_start_zero",
            "sent": f"{path} line 0",
            "served": "line 1 (lines count from 1)",
        })
    if not _ascending(ranges) and all(
        first >= 1 and (last is None or first <= last) for first, last in ranges
    ):
        ranges, joined = _in_order(ranges)
        notes.append({
            "rule": "lines_in_order",
            "sent": f"{path} ranges out of order" + (" or overlapping" if joined else ""),
            "served": "ascending order" + (", overlaps merged" if joined else ""),
        })
    if not notes:
        return lines, []
    return ranges, notes


def _as_ranges(lines: Any) -> Any:
    """`lines` in the read's form, so two spellings of one value compare equal."""
    return _normal_lines(lines, "lines")[0]


def _flat_lines_note(path: str) -> dict[str, str]:
    return {
        "rule": "lines_one_range",
        "sent": f"{path} as one [first, last] pair",
        "served": "a list of that one range [[first, last]]",
    }


def _replaced(arguments: Mapping[str, Any], argument: str, value: Any) -> dict[str, Any]:
    """`arguments` with the condition at `argument` (`input.filter` is nested)."""
    out = dict(arguments)
    if argument == "input.filter":
        out["input"] = {**arguments["input"], "filter": value}
    else:
        out[argument] = value
    return out


#: The tools that read a set of records or links through one condition
#: vocabulary in every argument (issue #3666, step 2c).
_SET_TOOLS = frozenset({"filter", "map", "aggregate"})


# -- aggregate's group keys ---------------------------------------------------

_PERIOD_WORDS = "with <period> one of day, week, month, year"

#: What each kind of set is grouped by, as a refusal names it.
_GROUP_KEYS = {
    "nodes": (
        ", ".join(sorted(EFFECTIVE_VIEW_FIELD_COLUMNS["nodes"]))
        + f", attrs.<key>, {EACH_FORM}, or <period>(date.<name>) {_PERIOD_WORDS}"
    ),
    "edges": (
        ", ".join(sorted(EFFECTIVE_VIEW_FIELD_COLUMNS["edges"]))
        + f", attrs.<key>, {EACH_FORM}, or <period>(date.attrs.<key>) {_PERIOD_WORDS}"
    ),
}

#: What a group-key refusal says, instead of naming a call, where access
#: rules are on: `aggregate` refuses every count there.
_NO_COUNT_UNDER_ACCESS_RULES = (
    "counting is not available on this deployment: access rules are on "
    "(audit or enforce), and aggregate refuses every count under them; "
    "filter or map read a bounded page"
)

#: The older whole-graph count of links by their ends' types: its own path,
#: served only as this whole `by`, with no condition and no edge_type.
_LINK_TYPE_TRIPLE = ["src_subtype", "edge_type", "dst_subtype"]

_LINK_OPEN = ("link:", "link(")
#: Comparisons that mean the same on a link's end id as on a record's id.
_ID_OPS = frozenset({"eq", "in", "prefix", "suffix"})


_KEY_TEXT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:()<>\-]{0,95}\Z")


def _key_name(key: str) -> str:
    """A group key for a one-line message: quoted when it is plain text."""
    return f"`{key}`" if _KEY_TEXT.match(key) else "a key"


def link_key(key: str) -> Optional[tuple[str, str]]:
    """`(link type, direction)` for a key naming the far end of a link.

    `link:<type>:<in|out>` and `link(<type>:<in|out>)`. None for a key not
    written as a link; `("", "")` for one written as a link that names no
    single type and direction.
    """
    if key.startswith("link:"):
        body: Optional[str] = key[len("link:"):]
    elif key.startswith("link("):
        body = key[len("link("):-1] if key.endswith(")") else None
    else:
        return None
    if body is None:
        return "", ""
    edge_type, colon, direction = body.rpartition(":")
    if not colon or direction not in ("in", "out") or not _PLAIN_NAME.match(edge_type):
        return "", ""
    return edge_type, direction


def _grouped_set(scope: Any, by: Any) -> Optional[str]:
    """`nodes` or `edges` for the set an aggregate groups; None when the
    call is not a grouped set (one record's links, a live result set, the
    older count of links by end types) or its input is for the tool to judge.
    """
    if not isinstance(scope, Mapping):
        return None
    if "ref" in scope:
        ref = scope.get("ref")
        if not isinstance(ref, str) or ref.startswith(_LIVE_REF_PREFIX):
            return None
        return "nodes"
    if scope.get("node_id") or scope.get("start_node") or scope.get("kind") == "node":
        return None
    if scope.get("kind") == "edges":
        if by == _LINK_TYPE_TRIPLE and not scope.get("filter") and not scope.get("edge_type"):
            return None
        return "edges"
    if scope.get("kind") == "nodes" or scope.get("subtype") or scope.get("filter"):
        return "nodes"
    return None


def _group_key_verdict(
    key: str, kind: str, fields: Fields, taken: frozenset[str],
) -> Optional[tuple[str, Optional[str], str]]:
    """None for a key the set is grouped by; else `(mode, key, words)`.

    `mode` is `corrected` or `guess` with the one replacement key, `link`
    for the far end of a link on a set of records, or `unfixable`.
    """
    if kind == "nodes" and key == "match":
        # The text a text condition matched (#3666); the tool refuses it
        # without one.
        return None
    if key == _TEXT:
        return (
            "corrected", "match",
            "a record's text is matched, not grouped; `match` groups the text it matched",
        ) if kind == "nodes" else ("unfixable", None, _TEXT_ELSEWHERE)
    if key in fields.names:
        return None
    if key.startswith("attrs.") and len(key) > len("attrs."):
        return None
    if is_each_shaped(key):
        if each_attribute(key) is not None:
            return None
        inner = key[len("each("):-1] if key.endswith(")") else ""
        if _BARE_WORD.match(inner) and inner.casefold() not in ("attrs", "date"):
            # Words, not a call: whether the attribute holds a list is known
            # only to the read, which refuses each() on one that does not.
            return (
                "unfixable", None,
                f"if `{inner}` is a stored attribute holding a list, write "
                f"`each(attrs.{inner})`",
            )
        return "unfixable", None, f"each() takes one attribute: {EACH_FORM}"
    if key.startswith(_LINK_OPEN):
        if kind == "nodes":
            return "link", None, ""
        return (
            "unfixable", None,
            "a set of links is grouped by its own ends: `src` or `dst`",
        )
    if "(" in key or ")" in key:
        # A date period: the read's own parser judges it, and refuses a
        # malformed one by name with the form.
        return None
    if Fields.is_date(key):
        date = key if kind == "nodes" else "date.attrs.<key>"
        return (
            "unfixable", None,
            f"a date is grouped by a period: `month({date})` (or day, week, year)",
        )
    if kind == "edges" and key in _LINK_TYPE_TRIPLE:
        return (
            "unfixable", None,
            "src_subtype and dst_subtype are served only as the whole by "
            '["src_subtype", "edge_type", "dst_subtype"], over every link with '
            "no condition and no edge_type",
        )
    if key.endswith("_subtype") or key.startswith(("src.", "dst.")):
        return (
            "unfixable", None,
            "a field of the record at a link's end is not a group key yet; "
            "group the links by `src` or `dst`",
        )
    column = fields.aliases.get(key)
    if column is not None:
        if column in taken:
            return "unfixable", None, f"`{column}` is already in by"
        return "corrected", column, f"this set's field is `{column}`"
    name = _spelling(key, fields)
    if name in fields.elsewhere:
        return "unfixable", None, fields.elsewhere[name]
    fix = _replacement(key, fields, taken)
    if fix is None:
        return (
            "unfixable", None,
            "name one of the keys the message lists; which one is meant cannot "
            "be told from the name",
        )
    return fix


def _refuse_each_beside(by: list[str]) -> None:
    """One list per call, and not beside a date period."""
    each = [key for key in by if each_attribute(key) is not None]
    if len(each) > 1:
        raise GrammarRefusal(
            "aggregate: by holds two each() keys; one call counts the "
            "elements of one list",
            fix_hint="keep one each() key, and send one call per list",
            fields=("by",),
        )
    periods = [
        key for key in by
        if each_attribute(key) is None and ("(" in key or ")" in key)
        and not key.startswith("attrs.")
    ]
    if each and periods:
        raise GrammarRefusal(
            "aggregate: by holds each() beside a date period; the two are not "
            "counted together in one call",
            fix_hint="send one call with the each() key and one with the period",
            fields=("by",),
        )


def _onto_link_end(scope: Any, by: list[str], near: str) -> tuple[Optional[Any], str]:
    """A record set's condition as a condition on the near end of its links.

    `(condition, "")`, `{}` when there is none; `(None, what)` when part of
    the call cannot be carried over exactly, `what` naming it.
    """
    if len(by) != 1:
        return None, "other group keys"
    if not isinstance(scope, Mapping):
        return None, "input"
    if "ref" in scope:
        return None, "result reference"
    if scope.get("subtype"):
        return None, "type"
    if set(scope) - {"kind", "filter"} or scope.get("kind", "nodes") != "nodes":
        return None, "input"
    condition = scope.get("filter")
    if condition in (None, {}):
        return {}, ""
    moved = _onto_end(condition, near)
    return (moved, "") if moved is not None else (None, "conditions")


def _onto_end(condition: Any, near: str) -> Optional[Any]:
    if not isinstance(condition, Mapping):
        return None
    op = condition.get("op")
    if op in ("and", "or") and isinstance(condition.get("clauses"), list):
        clauses = [_onto_end(clause, near) for clause in condition["clauses"]]
        if not clauses or any(clause is None for clause in clauses):
            return None
        return {**condition, "clauses": clauses}
    if op == "not" and isinstance(condition.get("clause"), Mapping):
        clause = _onto_end(condition["clause"], near)
        return None if clause is None else {**condition, "clause": clause}
    if op in _ID_OPS and condition.get("field") == "node_id":
        return {**condition, "field": near}
    return None


class _Rewrite:
    """The working state of one call's rewrite."""

    def __init__(
        self, tool: str, arguments: Mapping[str, Any],
        *, search_methods: Optional[frozenset[str]] = None,
    ) -> None:
        self.tool = tool
        #: The search methods the server serves, when known (a note's words).
        self.search_methods = search_methods
        self.sent: Mapping[str, Any] = arguments
        self.args: dict[str, Any] = dict(arguments)
        self.adjusted: list[dict[str, str]] = []
        self.spellings: list[tuple[str, str]] = []
        self.row_keys: Optional[frozenset[str]] = None
        self.select_retry: Optional[dict[str, Any]] = None
        self.select_retry_mode = "corrected"
        self.sent_select: Optional[tuple[str, ...]] = None

    # -- the one output control --------------------------------------------

    def select_fields(self) -> None:
        """Serve a `select` with the fields this call's rows have."""
        from okg.substrate.mcp.select_vocabulary import plan_select

        plan = plan_select(self.tool, self.args, search_methods=self.search_methods)
        if plan is None:
            return
        if plan.refusal is not None:
            refusal = GrammarRefusal(
                plan.refusal[0], fix_hint=plan.refusal[1], fields=("select",),
            )
            refusal.code = "select_unknown_field"
            refusal.select_data = {
                "unknown_fields": list(plan.refusal[2]),
                "valid_fields": list(plan.refusal[3]),
            }
            raise refusal
        if plan.retry_select is not None:
            # Left for the tool's refusal; this is the call it may offer.
            self.select_retry = {**self.sent, "select": list(plan.retry_select)}
            if plan.retry_method is not None:
                self.select_retry["method"] = plan.retry_method
                self.select_retry_mode = "guess"
        if plan.changed:
            if plan.select is None:
                self.args.pop("select", None)
                self.row_keys = plan.row_keys
            else:
                self.sent_select = tuple(self.args["select"])
                self.args["select"] = list(plan.select)
        self.adjusted += [dict(note) for note in plan.notes]

    # -- conditions -------------------------------------------------------

    def fields(self) -> Fields:
        """The fields of the set this call's conditions are about."""
        if self.tool == "search":
            return SEARCH_FIELDS
        if self.tool == "expand":
            return NODE_FIELDS.on(self.tool)
        scope = self.args.get("input")
        if isinstance(scope, Mapping):
            if scope.get("kind") == "edges":
                # Links carry no named dates on any tool; where dates are
                # compared, a link's date is an attribute.
                return dataclasses.replace(
                    EDGE_FIELDS, tool=self.tool, date_meanings=False,
                    dates=self.tool in DATE_CONDITION_TOOLS,
                )
            ref = scope.get("ref")
            if isinstance(ref, str) and ref.startswith(_LIVE_REF_PREFIX):
                return dataclasses.replace(LIVE_FIELDS, tool=self.tool)
        return NODE_FIELDS.on(self.tool)

    def checks_fields(self) -> bool:
        """Whether every condition of this call is read in one vocabulary.

        `filter`, `map` and `aggregate` over a set of records or links take
        what search's `where` takes in every condition argument, the long
        form included (issue #3666, step 2c): a field is checked by name and
        a record's second names are read as its columns. A saved result's
        conditions are left as sent: its rows are the reference's own. A
        scope that names no set kind (or is no object) is left to the
        tool's own refusal, which names it.
        """
        if self.tool not in _SET_TOOLS:
            return False
        scope = self.args.get("input")
        return (
            isinstance(scope, Mapping) and "ref" not in scope
            and scope.get("kind", "nodes") in ("nodes", "edges")
        )

    def lower(self, condition: Any, argument: str) -> Any:
        fields = self.fields()
        # A top-level `where` on a tool that had none is the new spelling;
        # search's `where` is an argument it already had.
        new_spelling = argument == "where" and self.tool != "search"
        check_fields = self.checks_fields()
        scope = self.args.get("input")
        try:
            lowered, adjusted = lower_condition(
                condition, fields=fields, new_spelling=new_spelling, argument=argument,
                check_fields=check_fields,
                read_words=(
                    self.tool in _SET_TOOLS and isinstance(scope, Mapping) and "ref" in scope
                ),
            )
        except GrammarRefusal as refusal:
            # Every cause the condition has, not only the first one met: the
            # repair walks all of it and says each change, and each name it
            # has no change for.
            notes: list[tuple[str, str]] = []
            # In an argument the tool already had, a second name is repaired
            # to its column: the offered call then needs no note.
            repaired = _text_values_fixed(
                _repaired(
                    condition, fields, notes, new_spelling=new_spelling or check_fields,
                    aliases=new_spelling,
                ),
                notes,
            )
            said = [words for _, words in notes]
            if not refusal.repairable:
                said.insert(0, refusal.fix_hint)
            if said:
                refusal.fix_hint = "; ".join(dict.fromkeys(said))
            if refusal.retry_tool == "filter":
                # A substring test in search's `where` (loose-match-3666):
                # filter's call over the same condition, which lists every
                # match and ranks none, so it is a guess.
                refusal.retry_arguments = (
                    {"where": repaired} if argument == "where"
                    and not any(mode == "unfixable" for mode, _ in notes) else None
                )
                refusal.retry_mode = "guess"
            elif refusal.likes_as_contains:
                # A substring test in search's `where` on a field search's
                # `contains` reads: search's own call, each like written as
                # contains. A guess: contains does not ignore accents.
                contained = _likes_as_contains(repaired)
                refusal.retry_arguments = (
                    _replaced(self.sent, argument, contained)
                    if contained is not None
                    and not any(mode == "unfixable" for mode, _ in notes) else None
                )
                refusal.retry_mode = "guess"
            elif refusal.repairable and not any(mode == "unfixable" for mode, _ in notes):
                # Offered only when every cause has a fix. One guess among
                # the fixes makes the whole call a guess.
                refusal.retry_arguments = _replaced(self.sent, argument, repaired)
                refusal.retry_mode = (
                    "guess" if any(mode == "guess" for mode, _ in notes) else "corrected"
                )
            raise
        self.adjusted += adjusted
        return lowered

    def lower_argument(self, name: str) -> None:
        if isinstance(self.args.get(name), Mapping):
            self.args[name] = self.lower(self.args[name], name)

    def lower_scope_filter(self) -> None:
        scope = self.args.get("input")
        if isinstance(scope, Mapping) and isinstance(scope.get("filter"), Mapping):
            self.args["input"] = {
                **scope, "filter": self.lower(scope["filter"], "input.filter"),
            }

    def take_where(self) -> tuple[bool, Any]:
        """`(given, lowered)` for a top-level `where`, removed from the call."""
        if "where" not in self.args:
            return False, None
        where = self.args.pop("where")
        if where is None:
            return False, None
        return True, self.lower(where, "where")

    def type_into_scope(
        self, scope: Mapping[str, Any], condition: Any,
    ) -> tuple[dict[str, Any], Any]:
        """Move a top-level type condition into a node scope that names none."""
        scope = dict(scope)
        if scope.get("kind", "nodes") != "nodes" or scope.get("subtype"):
            return scope, condition
        subtype, rest = _take_type(condition)
        if subtype is None:
            return scope, condition
        scope["subtype"] = subtype
        return scope, rest

    # -- the arguments placed one level too deep ---------------------------

    def lift(self, outer: str, names: tuple[str, ...]) -> None:
        for name in names:
            holder = self.args.get(outer)
            if not isinstance(holder, Mapping) or name not in holder:
                continue
            nested = holder[name]
            top = self.args.get(name)
            if top is not None and _differ(top, nested):
                # Two different values: left as sent, refused as today with
                # the move the caller has to make.
                continue
            self.args[outer] = {k: v for k, v in holder.items() if k != name}
            self.args[name] = nested
            self.adjusted.append({
                "rule": "argument_lifted",
                "sent": f"{name} inside {outer}",
                "served": f"top-level {name}",
            })

    def default_scope(self, *, predicate: bool = False) -> None:
        """No `where` and no `input`: every record, said in a note.

        The read surface shows `where` and not `input` (#3666, M10), so a
        call that names neither can mean only the whole node set. `filter`
        also gets the empty condition when it sent none.
        """
        if self.args.get("where") is not None or self.args.get("input") is not None:
            return
        if self.args.get("next") is not None:
            # A keyset `next` token (#3666 step 3b) carries the whole read
            # and refuses any read argument beside it, so nothing is
            # defaulted beside it.
            return
        if predicate and self.args.get("predicate") is not None:
            return
        self.args.pop("where", None)
        self.args["input"] = {"kind": "nodes"}
        if predicate:
            self.args["predicate"] = {}
        self.adjusted.append({
            "rule": "scope_defaulted",
            "sent": "no `where` and no `input`",
            "served": 'every record (where={}, input={"kind": "nodes"})',
        })

    # -- per tool ----------------------------------------------------------

    def search(self) -> None:
        self.lower_argument("where")
        query = self.args.get("query")
        if self.args.get("method") == "grep" and isinstance(query, str):
            # grep reads a regex only as re:/<pattern>/ (#3666).
            terms = read_regex_terms(query)
            if terms is not None:
                refusal = _regex_terms_refusal(terms, place="query", fields=("query",))
                if refusal is not None:
                    if refusal.repairable:
                        refusal.retry_arguments = {**self.sent, "query": terms.fixed}
                    raise refusal
                self.args["query"] = terms.served
                self.adjusted.append({
                    "rule": "regex_without_slashes",
                    "sent": f"query {_shown(query)}",
                    "served": f"query {_shown(terms.served)} (a regex is written re:/<pattern>/)",
                })
        # After the query is lowered, so the windows check reads it as served.
        self.windows_view()
        self.select_fields()

    def windows_view(self) -> None:
        """A grep call that names what only the windows read takes, and no
        view, is that read (issue #3666): `context_chars`, `matching="record"`
        or a scope of named records. Served with `view="windows"` and a note,
        only when the windows read takes every argument of the call as sent;
        otherwise left as sent for the tool's own refusal."""
        args = self.args
        if args.get("method") != "grep" or args.get("view") is not None:
            return
        if args.get("completeness") is not None or args.get("continuation_token") is not None:
            # The windows read refuses both; served as that read, the caller
            # would be refused about a view it never sent. Refused as sent.
            return
        from okg.substrate.mcp.search_where import (
            normalize_children_scope,
            normalize_record_scope,
        )

        record_ids = None
        scope = args.get("scope")
        if scope is not None:
            try:
                if normalize_children_scope(scope) is not None:
                    return
                record_ids = normalize_record_scope(scope)
            except Exception:  # noqa: BLE001 - the tool refuses the scope by name.
                return
            if record_ids is None:
                return
        named = [
            name for name, given in (
                ("context_chars", args.get("context_chars") is not None),
                ("matching", args.get("matching") == "record"),
                ("scope", record_ids is not None),
            ) if given
        ]
        if not named:
            return
        from okg.substrate.mcp.record_window_search import validate_window_call

        try:
            validate_window_call(
                view="windows", method="grep", record_ids=record_ids,
                query=args.get("query"), matching=args.get("matching"),
                context_chars=args.get("context_chars"),
                where=args.get("where"), path_prefix=args.get("path_prefix"),
                offset=args.get("offset") or 0, select=args.get("select"),
                rank_by=args.get("rank_by"), profile=args.get("profile"),
                source_family=args.get("source_family"),
                subtype_filter=args.get("subtype_filter"),
                attr_filters=args.get("attr_filters"), filter=args.get("filter"),
                max_per_parent=args.get("max_per_parent"),
            )
        except Exception:  # noqa: BLE001 - not a windows call as sent: left as sent.
            return
        args["view"] = "windows"
        self.adjusted.append({
            "rule": "grep_windows_view",
            "sent": " and ".join(named) + " on grep with no view",
            "served": 'view="windows", the grep read that takes ' + (
                "them" if len(named) > 1 else "it"
            ),
        })

    def inspect(self) -> None:
        self.lines_into_target()
        self.lines_as_ranges()
        self.lift("target", ("select", "max_bytes"))
        self.node_select_second_names()
        self.graph_select_names()

    def graph_select_names(self) -> None:
        """An inspect(graph) `select` names a block by a plain word: read as it.

        Only words with one meaning (`GRAPH_SELECT_NAMES`), and only where the
        word is no block itself; anything else is left for the refusal, which
        lists the blocks (issue #3666, lane RD)."""
        from okg.substrate.mcp.tools import INSPECT_GRAPH_BLOCKS

        target = self.args.get("target")
        select = self.args.get("select")
        if (
            not isinstance(target, Mapping) or target.get("kind") != "graph"
            or not isinstance(select, list)
            or not all(isinstance(name, str) for name in select)
        ):
            return
        out: list[str] = []
        notes: list[dict[str, str]] = []
        for name in select:
            blocks = GRAPH_SELECT_NAMES.get(name) if name not in INSPECT_GRAPH_BLOCKS else None
            if blocks is None:
                if name not in out:
                    out.append(name)
                continue
            out += [block for block in blocks if block not in out and block not in select]
            notes.append({
                "rule": "select_second_name",
                "sent": f"select `{name}`",
                "served": " and ".join(f"`{block}`" for block in blocks),
            })
        if notes:
            self.args["select"] = out
            self.adjusted += notes

    def node_select_second_names(self) -> None:
        """A record read's `select` names `type` for `subtype`: read as it."""
        from okg.substrate.mcp.select_vocabulary import served_second_names
        from okg.substrate.mcp.tools import _GET_NODE_FIELDS

        target = self.args.get("target")
        select = self.args.get("select")
        if (
            not isinstance(target, Mapping) or target.get("kind") not in ("node", "nodes")
            or not isinstance(select, list)
            or not all(isinstance(name, str) for name in select)
        ):
            return
        served, notes = served_second_names(select, frozenset(_GET_NODE_FIELDS))
        if notes:
            self.args["select"] = served
            self.adjusted += notes

    def lines_into_target(self) -> None:
        """A top-level `lines` beside a text target is the target's `lines`.

        Only for `kind: "text"`: a batch (`texts`) has one `lines` per item,
        so a top-level one could mean any of them and is left to the
        unknown-argument refusal. The same value in both places is read
        once; two different values are refused, naming both.
        """
        if "lines" not in self.args:
            return
        target = self.args.get("target")
        if not isinstance(target, Mapping) or target.get("kind") != "text":
            return
        top = self.args["lines"]
        if "lines" in target and _differ(
            _as_ranges(target["lines"]), _as_ranges(top),
        ):
            raise GrammarRefusal(
                "inspect text: `lines` is given beside `target` and inside it, "
                "with different ranges",
                fix_hint=(
                    "send `lines` once, inside target: "
                    '{"kind":"text","node_id":"<record node id>","lines":[[first,last]]}'
                ),
                fields=("lines", "target.lines"),
            )
        self.args.pop("lines")
        self.args["target"] = {**target, "lines": target.get("lines", top)}
        self.adjusted.append({
            "rule": "argument_moved_into_target",
            "sent": "lines beside target",
            "served": "target.lines",
        })

    def lines_as_ranges(self) -> None:
        """A text read's `lines` in another common shape is read in the
        read's form, `[[first, last], ...]`, with a note per change
        (`_normal_lines`); each item of a batch the same.

        The numbers are not echoed in the notes (they may be of any
        length); they are checked as every range is, by the text read.
        """
        target = self.args.get("target")
        if not isinstance(target, Mapping):
            return
        kind = target.get("kind")
        if kind == "text" and "lines" in target:
            lines, notes = _normal_lines(target["lines"], "target.lines")
            if notes:
                self.args["target"] = {**target, "lines": lines}
                self.adjusted += notes
        elif kind == "texts" and isinstance(target.get("items"), list):
            items = list(target["items"])
            notes = []
            for index, item in enumerate(items):
                if not isinstance(item, Mapping) or "lines" not in item:
                    continue
                lines, fixed = _normal_lines(
                    item["lines"], f"target.items[{index}].lines",
                )
                if fixed:
                    items[index] = {**item, "lines": lines}
                    notes += fixed
            if notes:
                self.args["target"] = {**target, "items": items}
                self.adjusted += notes

    def filter(self) -> None:
        self.default_scope(predicate=True)
        self.lift("input", ("select", "max_bytes"))
        self.lower_argument("predicate")
        self.lower_scope_filter()
        given, where = self.take_where()
        if not given:
            if (
                self.args.get("predicate") is None
                and self.args.get("next") is None
                and self.args.get("input") is not None
            ):
                # #3666 step 3b (and M5's rule of the same name): no
                # condition reads every record in the input scope, said.
                self.adjusted.append({
                    "rule": "predicate_missing",
                    "sent": "no predicate",
                    "served": "every record in the input scope",
                })
            return
        predicate = self.args.get("predicate")
        if _given(predicate) and _given(where) and _differ(predicate, where):
            raise _both_spellings("filter", "where", "predicate")
        self.spellings.append(("where", "predicate"))
        if _given(predicate) and not _given(where):
            # An empty `where` adds nothing to the condition already sent.
            if self.args.get("input") is None:
                self.args["input"] = {"kind": "nodes"}
            return
        scope = self.args.get("input")
        if scope is None:
            scope = {"kind": "nodes"}
        if isinstance(scope, Mapping) and "ref" not in scope and "kind" in scope:
            scope, where = self.type_into_scope(scope, where)
        self.args["input"] = scope
        self.args["predicate"] = where

    def _where_into_scope(self, *, kind_optional: bool) -> None:
        """`map` and `aggregate`: a top-level `where` is the scope's `filter`."""
        tool = self.tool
        self.lower_scope_filter()
        given, where = self.take_where()
        if not given:
            return
        scope = self.args.get("input")
        if scope is None:
            scope = {"kind": "nodes"}
        if (
            not isinstance(scope, Mapping)
            or scope.get("node_id") or scope.get("start_node")
            or scope.get("kind") == "node"
        ):
            raise GrammarRefusal(
                f"{tool}: `where` narrows a set of records; `input` here "
                "names one record",
                fix_hint=(
                    "drop `input` and put the whole condition in `where`, or "
                    "drop `where` to read that one record; which of the two "
                    "is meant cannot be told from the call"
                ),
                fields=("where", "input"),
            )
        if "ref" in scope:
            # A saved result is read as its members; nothing on this tool
            # would apply the condition, so serving would drop it.
            raise GrammarRefusal(
                f"{tool}: `where` does not narrow a saved result here",
                fix_hint=(
                    "narrow it with filter first: send filter with this "
                    "`input`, this `where` and retain=true, then pass the "
                    f"reference that reply names to {tool}; that id does not "
                    "exist until filter has run"
                ),
                fields=("where", "input.ref"),
            )
        if _given(scope.get("filter")) and _given(where) and _differ(scope["filter"], where):
            raise _both_spellings(tool, "where", "input.filter")
        self.spellings.append(("where", "input.filter"))
        if _given(scope.get("filter")) and not _given(where):
            # An empty `where` adds nothing to the condition already sent.
            self.args["input"] = dict(scope)
            return
        scope = {k: v for k, v in scope.items() if k != "filter"}
        if "kind" in scope or kind_optional:
            scope, where = self.type_into_scope(scope, where)
        if _given(where):
            scope["filter"] = where
        self.args["input"] = scope

    def map(self) -> None:
        self.default_scope()
        self.lift("input", ("select", "max_bytes"))
        self._where_into_scope(kind_optional=False)
        if "select" not in self.args:
            return
        select = self.args.pop("select")
        if select is None:
            return
        projection = self.args.get("projection")
        if not isinstance(select, list):
            renames = isinstance(select, Mapping) and projection is None
            raise GrammarRefusal(
                "map: `select` is a list of field names",
                fix_hint=(
                    "an object that renames fields goes in `projection`"
                    if renames else
                    "pass the field names as a list in `select`"
                ),
                fields=("select",),
                retry_arguments=(
                    {**{k: v for k, v in self.sent.items() if k != "select"},
                     "projection": dict(select)}
                    if renames and "select" in self.sent else None
                ),
            )
        if projection is not None and _differ(projection, select):
            raise GrammarRefusal(
                "map: `select` and `projection` both name the fields and "
                "they differ; `select` means what `projection` means",
                fix_hint=(
                    "send only `select` or only `projection`; they differ, so "
                    "which one is meant cannot be told from the call"
                ),
                fields=("select", "projection"),
            )
        self.spellings.append(("select", "projection"))
        self.args["projection"] = select

    def aggregate(self) -> None:
        self.default_scope()
        self.lift("input", ("max_bytes",))
        self._where_into_scope(kind_optional=True)
        self.group_keys()

    def group_keys(self) -> None:
        """Refuse a `by` key the set cannot be grouped by, by name.

        Read after the condition, on the set the call names. A key the read
        would refuse anyway is refused here with the keys that set takes,
        and with the corrected call when every key has one fix. A key naming
        the far end of a link gets the call that counts those links.
        """
        by = self.args.get("by")
        kind = _grouped_set(self.args.get("input"), by)
        if kind is None or not isinstance(by, list) or not by:
            return
        if not all(isinstance(key, str) for key in by):
            return
        fields = NODE_FIELDS if kind == "nodes" else EDGE_FIELDS
        unknown: list[tuple[int, str, tuple[str, Optional[str], str]]] = []
        for index, key in enumerate(by):
            verdict = _group_key_verdict(key, kind, fields, frozenset(by) - {key})
            if verdict is not None:
                unknown.append((index, key, verdict))
        links = [item for item in unknown if item[2][0] == "link"]
        try:
            if links:
                raise self.link_refusal(by, links[0][0], links[0][1])
            if unknown:
                raise self.unknown_group_keys(by, kind, unknown)
            _refuse_each_beside(by)
        except GrammarRefusal as refusal:
            refusal.access_rules_hint = _NO_COUNT_UNDER_ACCESS_RULES
            raise

    def unknown_group_keys(
        self, by: list[str], kind: str,
        unknown: list[tuple[int, str, tuple[str, Optional[str], str]]],
    ) -> GrammarRefusal:
        names = [_key_name(key) for _, key, _ in unknown]
        said = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
        what = "a set of records" if kind == "nodes" else "a set of links"
        verb = "is not a group key" if len(names) == 1 else "are not group keys"
        refusal = GrammarRefusal(
            f"aggregate: by {said} {verb} of {what}; {what} is grouped by "
            f"{_GROUP_KEYS[kind]}",
            fix_hint="; ".join(dict.fromkeys(words for _, _, (_, _, words) in unknown)),
            fields=tuple(f"by[{index}]" for index, _, _ in unknown),
            repairable=all(mode != "unfixable" for _, _, (mode, _, _) in unknown),
        )
        if refusal.repairable:
            fixed = list(by)
            for index, _, (_, replacement, _) in unknown:
                fixed[index] = replacement
            if len(set(fixed)) == len(fixed):
                refusal.retry_arguments = {**self.sent, "by": fixed}
                refusal.retry_mode = (
                    "guess" if any(mode == "guess" for _, _, (mode, _, _) in unknown)
                    else "corrected"
                )
        return refusal

    def link_refusal(self, by: list[str], index: int, key: str) -> GrammarRefusal:
        """A record set grouped by the far end of a link: the links' count.

        The call offered counts the links of that type per far end, with the
        record set's conditions moved onto the near end. Offered only when
        that move is exact: no type, no result reference, no other group key,
        and conditions on `node_id` alone (a condition on a link names its
        ends' ids, and no field of the records there yet).
        """
        parsed = link_key(key)
        edge_type, direction = parsed if parsed is not None else ("", "")
        if not edge_type:
            return GrammarRefusal(
                f"aggregate: by {_key_name(key)} names a link; aggregate groups a "
                "set of records by the records' own fields, and counts links "
                "over a set of links",
                fix_hint=(
                    'count the links: aggregate(input={"kind":"edges",'
                    '"edge_type":"<link type>"}, by=["dst"]) groups them by the '
                    'record each points to, by=["src"] by the record each '
                    "starts from"
                ),
                fields=(f"by[{index}]",),
            )
        far, near = ("dst", "src") if direction == "out" else ("src", "dst")
        spelled = _spell({"kind": "edges", "edge_type": edge_type})
        words = (
            f"aggregate(input={spelled}, by=[\"{far}\"]) counts the "
            f"{edge_type} links per record at their {far} end"
        )
        refusal = GrammarRefusal(
            f"aggregate: by {_key_name(key)} groups records by the record at the "
            f"other end of their {edge_type} links; aggregate groups a set of "
            "records by the records' own fields, and counts links over a set "
            "of links",
            fix_hint=words,
            fields=(f"by[{index}]",),
            repairable=True,
        )
        scope = self.args.get("input")
        carried, cannot = _onto_link_end(scope, by, near)
        if carried is None:
            refusal.fix_hint = (
                f"{words}; that call counts every {edge_type} link: a "
                "condition on a link cannot yet name a field of the record at "
                f"its {near} end, so this call's {cannot} cannot be carried "
                "over and no call is offered"
            )
            refusal.repairable = False
            return refusal
        refusal.fix_hint = (
            f"count the {edge_type} links per record at their {far} end "
            "instead; it counts links, not records: a record with no "
            f"{edge_type} link is in no group, and two {edge_type} links "
            "between the same two records count twice"
        )
        link_scope: dict[str, Any] = {"kind": "edges", "edge_type": edge_type}
        if carried:
            link_scope["filter"] = carried
        refusal.retry_arguments = {
            **{k: v for k, v in self.sent.items() if k not in ("input", "where", "by", "metrics")},
            "input": link_scope, "by": [far], "metrics": ["count"],
        }
        refusal.retry_mode = "guess"
        return refusal

    def live_rows_select(self) -> None:
        """`external_live` in a live call's `select` means the live rows.

        They always return in `external_live`, beside the graph rows, so the
        name is taken out of the graph rows' `select`; a `select` left empty
        is dropped, and the graph rows are served whole. Without
        `live` the name is left for the refusal, which says what it is.
        """
        select = self.args.get("select")
        if (
            self.args.get("live") is None or not isinstance(select, list)
            or LIVE_ROWS_KEY not in select
        ):
            return
        rest = [name for name in select if name != LIVE_ROWS_KEY]
        served = "live rows, which always return in `external_live` beside the graph rows"
        if rest:
            self.args["select"] = rest
        else:
            self.args.pop("select")
            served += "; nothing else was selected, so the graph rows are served whole"
        self.adjusted.append({
            "rule": "live_rows_select", "sent": f"select `{LIVE_ROWS_KEY}`", "served": served,
        })

    def expand(self) -> None:
        self.live_rows_select()
        self.lower_argument("filter")
        given, where = self.take_where()
        if not given:
            self.select_fields()
            return
        existing = self.args.get("filter")
        if _given(existing) and _given(where) and _differ(existing, where):
            raise _both_spellings("expand", "where", "filter")
        self.spellings.append(("where", "filter"))
        if not (_given(existing) and not _given(where)):
            # An empty `where` adds nothing to the condition already sent.
            self.args["filter"] = where
        self.select_fields()


#: Plain words agents sent in an inspect(graph) `select` for its blocks
#: (issue #3666, lane RD), each with the blocks it means. A word with more
#: than one reading (`sources`, `dates`) is not here and stays refused.
#: The blocks are `subtypes`, `edge_types`, `embed_lag` and
#: `unresolved_mentions`; the reply keys of the first two (`subtype_counts`,
#: `edge_type_counts`) are read as them, and the schema digest is built
#: whenever both are selected.
GRAPH_SELECT_NAMES: Mapping[str, tuple[str, ...]] = {
    "types": ("subtypes",),
    "subtype_counts": ("subtypes",),
    "relations": ("edge_types",),
    "edge_type_counts": ("edge_types",),
    "counts": ("subtypes", "edge_types"),
    "schema": ("subtypes", "edge_types"),
    "schema_digest": ("subtypes", "edge_types"),
}

#: Arguments an offered call leaves for the caller to repeat.
_REPEATED = ("authority",)

_REWRITES = {
    "search": _Rewrite.search,
    "inspect": _Rewrite.inspect,
    "filter": _Rewrite.filter,
    "map": _Rewrite.map,
    "aggregate": _Rewrite.aggregate,
    "expand": _Rewrite.expand,
}


def _served_as_written(tool: str, arguments: Mapping[str, Any]) -> bool:
    """Whether the rewrite takes `arguments` with no refusal and no note
    that changes the call.

    The link-rows `attrs` note (issue #3666) changes nothing: the call is
    served exactly as sent and the note only says whose `attrs` the rows
    carry, so a corrected link-row select that names `attrs` is still
    offered.
    """
    from okg.substrate.mcp.select_vocabulary import LINK_ROWS_ATTRS_NOTE

    state = _Rewrite(tool, arguments)
    try:
        _REWRITES[tool](state)
    except (GrammarRefusal, RecursionError):
        return False
    return all(
        record.get("rule") == LINK_ROWS_ATTRS_NOTE["rule"] for record in state.adjusted
    )


def rewrite_call(
    tool: str, arguments: Any, *, search_methods: Optional[frozenset[str]] = None,
) -> RewrittenCall:
    """The call the read tools are given for `tool(**arguments)`.

    Pure: the same arguments always give the same call, and nothing but the
    arguments is read. (`search_methods`, the methods the server serves,
    changes only what a `select` note says to call, and whether another
    method's call is offered; never the rewritten arguments.) A tool this module does not know, and arguments that
    are not an object, come back untouched. Raises :class:`GrammarRefusal`
    for a call with two readings, a name the tool cannot evaluate in a new
    spelling, or a call that could only be served by dropping a condition.
    The refusal's `retry_arguments`, when set, is a call this function
    takes as written: it is run through the rewrite before it is offered.
    """
    rewrite = _REWRITES.get(tool)
    if rewrite is None or not isinstance(arguments, Mapping):
        return RewrittenCall(arguments=arguments)
    state = _Rewrite(tool, arguments, search_methods=search_methods)
    try:
        rewrite(state)
    except RecursionError:
        # Deeper than this walk can follow. Nothing was rewritten: the call
        # goes on as sent, to the checks it always met.
        return RewrittenCall(arguments=arguments)
    except GrammarRefusal as refusal:
        candidate = refusal.retry_arguments
        if candidate is not None:
            # The caller's authority is not written into a refusal: the
            # offered call names it as something to repeat.
            repeat = tuple(name for name in _REPEATED if name in candidate)
            candidate = {k: v for k, v in candidate.items() if k not in _REPEATED}
            refusal.retry_arguments, refusal.retry_repeat = candidate, repeat
            if not _served_as_written(refusal.retry_tool or tool, candidate):
                # The repair fixed one mistake and the call has another, or
                # the call is refused for a reason no repair changes.
                refusal.retry_arguments, refusal.retry_repeat = None, ()
        raise
    retry = state.select_retry
    repeat: tuple[str, ...] = ()
    if retry is not None:
        repeat = tuple(name for name in _REPEATED if name in retry)
        retry = {k: v for k, v in retry.items() if k not in _REPEATED}
        if not _served_as_written(tool, retry):
            retry, repeat = None, ()
    return RewrittenCall(
        arguments=state.args,
        adjusted=cap_adjusted(state.adjusted),
        spellings=tuple(state.spellings),
        row_keys=state.row_keys,
        select_retry=retry,
        select_retry_repeat=repeat,
        select_retry_mode=state.select_retry_mode if retry is not None else "corrected",
        sent_select=state.sent_select,
    )
