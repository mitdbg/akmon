"""Search delivery views, independent of ranking and source coverage.

PACT read-model-s4-list (issue #3195, design ``read-model`` section 14,
"Views"). A view decides how the selected records are DELIVERED, never which
records are selected: ``cards`` is the existing search card (and the legacy
behaviour when ``view`` is omitted); ``list`` is a compact page of record
handles with its own row budget; ``metadata`` (#3222 slice 3) delivers no
rows at all, only the selected records' result reference, its exact size and
an optional count summary (:func:`metadata_projection`).

``list`` is served only where search already selects distinct records:
lexical ``rank_by="records"``, and grep, whose list is one compact row per
matching record (#3659). Every other method or rank mode returns
self-ranked rows, passages or alias occurrences, which must not be relabeled
as records, so naming ``list`` there is refused by name before any database
work. :data:`VIEW_MATRIX` is the published method/rank/view matrix.
"""

from __future__ import annotations

from typing import Annotated, Any, Optional

from pydantic import BeforeValidator, Field

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.passage_rollup import validate_rank_by

#: The omitted-``limit`` row budget for cards (and legacy calls), lexical
#: and grep alike. 8 (was 10; PACT read-reply-size-3666): replies were 90%
#: of the Sol graph arm's prompt tokens on ERAG-500, and every reply's first
#: line now names `limit` for more or fewer rows.
CARDS_LIMIT = 8
#: The omitted-``limit`` row budget for a list, which is also its maximum.
LIST_LIMIT = 100
#: The omitted-``limit`` row budget for a grep list (PACT
#: read-names-list-3666): grep's default delivery, one name line per record,
#: about 150-300 bytes each, so 20 rows cost about what 2-3 cards do.
GREP_LIST_LIMIT = 20
#: The largest explicit ``limit`` a list accepts. Larger refuses; never clipped.
LIST_MAX_LIMIT = 100
#: The largest explicit ``limit`` cards accept (``operator_models.MAX_PAGE_LIMIT``).
CARDS_MAX_LIMIT = 1000

def _refuse_bool_limit(value: Any) -> Any:
    # JSON `true` is not 1. Pydantic's lax int mode would coerce it, so a list
    # asked for with `limit: true` would serve one row instead of refusing.
    if isinstance(value, bool):
        raise ValueError("limit must be an integer >= 1, not a boolean")
    return value


#: The published `search.limit`: the same schema as ``operator_models.Limit``
#: (`integer`, 1..1000), but a JSON boolean is refused instead of coerced to
#: 1. Other lax coercions (an integral float or numeric string) are kept, so
#: no legacy card call changes. `Field` comes first: placed after the
#: validator its bounds would publish as non-schema `ge`/`le` keys.
SearchLimit = Annotated[
    int, Field(ge=1, le=CARDS_MAX_LIMIT), BeforeValidator(_refuse_bool_limit),
]

#: ``windows`` is grep's record-windows read (PACT read-record-windows,
#: ``record_window_search``). It is listed so the published `view` enum and
#: the matrix name it; its own read validates it, not
#: :func:`resolve_search_view`. ``metadata`` is a result-reference producer
#: (#3222): ``result_references.producer_refusal`` refuses every form that
#: cannot produce a reference before this resolver runs.
VIEWS = ("cards", "list", "windows", "metadata")
#: Fields a list may select; also its default projection, in this order.
#: `parent` is delivered as its id only (`_list_parent`, issue #3467).
LIST_FIELDS = ("node_id", "subtype", "handle", "parent")
#: What a grep list row delivers, in this order (#3659). The rows are fixed:
#: this resolver takes no `select`; the served tool cuts the finished rows
#: to one (`select_vocabulary`, issue #3666).
GREP_LIST_FIELDS = ("node_id", "subtype", "handle", "matches", "first_match")
#: Every key a served `select` may keep on a grep list row (issue #3666):
#: the fixed fields and a passage row's parent id.
GREP_LIST_SELECTABLE = (*GREP_LIST_FIELDS, "parent")
#: What a cut grep list row always keeps: which record it is, whether it is
#: a passage, and its trust.
GREP_LIST_ALWAYS = (
    "node_id", "subtype", "unit", "provenance", "read_provenance",
    # The id a reader cites (PACT read-names-list-3666): identity, kept.
    "citation_id",
)
#: `citation_id.CITATION_KEY`, restated: this module imports no search code.
_CITATION_KEY = "citation_id"
#: What a list of the query-less lookup (`method="attrs"`) selects when the
#: call names no `select` (issue #3666). Its rows are the lookup's own, so a
#: `select` may name any lookup field.
ATTRS_LIST_FIELDS = ("node_id", "subtype", "handle")
#: Where a list is served, as a refusal states it.
LIST_SERVED_ON = "method=lexical rank_by=records; method=grep; a where with no query"
#: Row keys a list never delivers, even when a producer put them on the row.
LIST_DROPPED_ROW_KEYS = frozenset({
    "score", "distance", "excerpt", "text", "attrs", "parent",
    "matching_passages", "rollup_best_chunk_id", "position",
    # How the evidence kind was decided, not the row's trust: a card and
    # `inspect` carry it. `evidence_kind` itself stays unless it is
    # "unknown" (see `list_projection`).
    "evidence_kind_provenance",
    # A list computes no recency annotation (cards and `inspect` do).
    "recency",
})
#: Page keys a list never delivers: per-row parent summaries and excerpt
#: machinery that describe card content the list does not carry.
LIST_DROPPED_PAGE_KEYS = frozenset({
    "rollup_contributing_chunks", "excerpt_window_basis",
    "rollup_best_chunks", "passage_previews", "recency",
})
#: The row key that marks a list row which is not a record: a passage the
#: records fold kept as its own row (a passage with no readable record, or
#: an alias-promoted passage, design rule 8). Absent on a record row.
LIST_ROW_UNIT_KEY = "unit"

#: method -> rank_by -> views. ``attrs`` and ``identifier_mentions`` have no
#: rank mode (``self`` only); ``passages`` is lexical only, and ``records``
#: is lexical's default and grep's only mode.
VIEW_MATRIX: dict[str, dict[str, tuple[str, ...]]] = {
    "lexical": {
        "records": ("cards", "list", "metadata"), "self": ("cards",),
        "passages": ("cards", "metadata"),
    },
    "semantic": {"self": ("cards",)},
    "alias": {"self": ("cards",)},
    # A `where` with no query lists the records it selects (issue #3666).
    "attrs": {"self": ("cards", "list")},
    "identifier_mentions": {"self": ("cards",)},
    # Grep ranks records only; `windows` reads a record scope, or without
    # one this grep's own top records (#3605); `list` is one compact row
    # per matching record (#3659).
    "grep": {"records": ("cards", "list", "windows")},
}


#: Page keys a metadata page never delivers: the rows, every page-level
#: block keyed by or built from the rows (parent summaries, passage counts
#: and previews, recency, excerpt and preview machinery, hoisted row
#: defaults), and search's own continuation (a metadata page ends
#: ``next: none``, design L7). What is left is counts, coverage, the applied
#: scope and the pin.
METADATA_DROPPED_PAGE_KEYS = frozenset({
    "results", "mentions", "parent_summaries", "rollup_contributing_chunks",
    "rollup_best_chunks", "passage_previews", "recency", "excerpt_window_basis",
    "row_defaults", "search_next", "next_offset_reason", "next_offset_remedy",
    "pagination", "pagination_hint", "prose_preview", "attribute_truncation",
    "exact_matches",
})


class SearchViewUnsupported(SubstrateError):
    """A search ``view`` (or its limit/select) the requested method cannot serve."""

    code = "search_view_unsupported"


def default_search_limit(view: Any, method: Any = None) -> int:
    """The row budget an omitted ``limit`` means for ``view`` (on ``method``,
    the effective method, when known: a grep list has its own)."""
    if view == "list":
        return GREP_LIST_LIMIT if method == "grep" else LIST_LIMIT
    return CARDS_LIMIT


#: The arguments that ask for card content or another grep read; naming
#: any of them keeps a grep call on cards (or on its own read) when it names
#: no view (PACT read-names-list-3666). `select` picks card fields;
#: `matching`, `context_chars` and `scope` belong to the windows read;
#: `retain` and `summary` to result references. `max_chars` is not one:
#: arm A's agent sent it on 1,662 of 1,714 grep calls as a size habit, and
#: a list line is cut to its own 100 characters (the page says the list is
#: the default and names view="cards").
NAMES_DEFAULT_BLOCKERS = (
    "select", "matching", "context_chars", "scope", "retain", "summary",
)


def implied_search_view(
    view: Any, *, method: Any, within: Any = None, arguments: Any = None,
) -> Any:
    """The delivery view a search call means. Pure.

    PACT read-names-list-3666 (issue #3666): a grep call that names no view
    is delivered as the list, one name line per record (id, title, path,
    match count, first matching line), the OKG form of the shell's
    ``rg -l``. ``view="cards"`` keeps the matched lines. The list is implied
    only on grep's unit form (``within`` omitted or ``"unit"``) and only
    when the call names none of :data:`NAMES_DEFAULT_BLOCKERS` (a false
    ``retain`` counts as not named); any other call keeps ``view`` as sent.
    ``method`` is the effective method (a grep page's token sent without
    ``method`` is grep). ``arguments`` maps argument names to the values
    the call bound.
    """
    if view is not None or method != "grep" or within not in (None, "unit"):
        return view
    sent = arguments if isinstance(arguments, dict) else {}
    for name in NAMES_DEFAULT_BLOCKERS:
        value = sent.get(name)
        if value is None or (name == "retain" and value is False):
            continue
        return view
    return "list"


def view_matrix(
    available_methods: Optional[list[str]] = None,
    available_rank_by: Optional[list[str]] = None,
) -> dict[str, dict[str, list[str]]]:
    """The published matrix, narrowed to what this server can run."""
    out: dict[str, dict[str, list[str]]] = {}
    for method, modes in VIEW_MATRIX.items():
        if available_methods is not None and method not in available_methods:
            continue
        cells = {
            rank: list(views) for rank, views in modes.items()
            if available_rank_by is None or rank == "self"
            or rank in available_rank_by
        }
        if cells:
            out[method] = cells
    return out


def _refuse(message: str, *, data: dict[str, Any], fix_hint: str) -> SearchViewUnsupported:
    # Flat strings: refusal data is depth-limited on the wire, so a nested
    # matrix would arrive as placeholders. The full matrix is in
    # `inspect(kind="surface")` capabilities.search.views.
    return SearchViewUnsupported(
        message,
        data={**data, "list_served_on": LIST_SERVED_ON},
        fix_hint=fix_hint,
    )


def _is_attr_name(name: str) -> bool:
    """`attrs.<key>`: one attribute, as `search.ATTR_SELECT_PREFIX` reads it
    (not imported: `search` imports this module)."""
    return name.startswith("attrs.") and len(name) > len("attrs.")


def resolve_search_view(
    view: Any,
    *,
    method: str,
    rank_by: Any,
    max_per_parent_argument: bool,
    limit: Any,
    select: Any,
) -> tuple[int, Any]:
    """Validate a view request and resolve its limit and select. Pure.

    Called before the database is opened. ``method`` is the EFFECTIVE method
    (a query-less lexical call with a predicate is ``attrs``). Returns the
    resolved ``(limit, select)``; ``select`` is unchanged for cards.

    An omitted (None) ``limit`` resolves to the view's default. An explicit
    value is never treated as omission: ``0``, a negative number and a bool
    are refused, and a list ``limit`` above :data:`LIST_MAX_LIMIT` is refused
    rather than clipped.
    """
    if view is not None and view not in VIEWS:
        raise _refuse(
            f"search: view must be one of {list(VIEWS)}; got {view!r}",
            data={"view": view, "supported_views": list(VIEWS)},
            fix_hint="omit view, or pass view='cards', 'list' or 'metadata'",
        )
    if view == "list" and method == "grep":
        if select is not None:
            raise _refuse(
                "search: view='list' on method='grep' has fixed rows "
                f"({', '.join(GREP_LIST_FIELDS)}); this read takes no select",
                data={"view": view, "method": method,
                      "supported_select": list(GREP_LIST_FIELDS)},
                fix_hint=(
                    "omit select here; the served tool cuts the finished rows "
                    "to a select of those row keys"
                ),
            )
    elif view == "list" and method == "attrs":
        # The query-less lookup's own rows, one line each (issue #3666).
        pass
    elif view == "list":
        if method != "lexical":
            raise _refuse(
                f"search: view='list' is served on method='lexical' with "
                f"rank_by='records', on method='grep' and on a where with no "
                f"query, not method={method!r}",
                data={"view": view, "method": method},
                fix_hint=(
                    "use method='lexical' or method='grep' with query text, "
                    "or omit view for cards"
                ),
            )
        resolved_rank = validate_rank_by(
            rank_by, method=method, max_per_parent_argument=max_per_parent_argument,
        )
        if resolved_rank != "records":
            raise _refuse(
                f"search: view='list' needs rank_by='records'; this call "
                f"resolves rank_by={resolved_rank!r}"
                + (" (an explicit max_per_parent selects 'self')"
                   if rank_by is None and max_per_parent_argument else ""),
                data={"view": view, "method": method, "rank_by": resolved_rank},
                fix_hint="drop rank_by and max_per_parent, or omit view for cards",
            )
    if limit is None:
        limit = default_search_limit(view, method)
    elif isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise _refuse(
            f"search: limit must be a positive integer; got {limit!r}",
            data={"view": view or "cards", "limit": limit},
            fix_hint="omit limit for the view's default, or pass an integer >= 1",
        )
    maximum = LIST_MAX_LIMIT if view == "list" else CARDS_MAX_LIMIT
    if limit > maximum:
        raise _refuse(
            f"search: view={view or 'cards'!r} accepts limit at most {maximum}; "
            f"got {limit}",
            data={"view": view or "cards", "limit": limit, "maximum_limit": maximum},
            fix_hint=f"pass limit <= {maximum} and page with offset" + (
                # PACT read-names-list-3666: grep's list is its default, so
                # the cards a larger page used to get are named.
                f', or view="cards" for up to {CARDS_MAX_LIMIT} cards'
                if view == "list" and method == "grep" else ""
            ),
        )
    if view != "list" or method == "grep":
        # A grep list is projected from grep's own default rows.
        return limit, select
    if method == "attrs":
        # The lookup validates a `select` against its own fields.
        return limit, list(ATTRS_LIST_FIELDS) if select is None else select
    if select is None:
        return limit, list(LIST_FIELDS)
    if isinstance(select, (list, tuple)) and not select:
        raise _refuse(
            "search: view='list' needs at least one select field",
            data={"view": view, "select": [], "supported_select": list(LIST_FIELDS)},
            fix_hint="omit select for node_id, subtype, handle and parent",
        )
    # PACT read-passage-rows-3666 (D6): a list row is its head line plus the
    # keys the caller chose: `attrs.<key>` names (one attribute each, as
    # `where` spells them) beside the list's own fields.
    attribute_names = [
        name for name in select if isinstance(name, str) and _is_attr_name(name)
    ] if isinstance(select, (list, tuple)) else []
    if not isinstance(select, (list, tuple)) or any(
        not isinstance(name, str) or (name not in LIST_FIELDS and name not in attribute_names)
        for name in select
    ):
        raise _refuse(
            "search: view='list' selects node_id, subtype, handle, parent and "
            "attrs.<key> only",
            data={"view": view, "select": select, "supported_select": list(LIST_FIELDS)},
            fix_hint=(
                'omit select, select attributes as "attrs.<key>", or omit view '
                "for rows with passages"
            ),
        )
    if len(attribute_names) == len(select):
        # Attributes only: the list's own fields stay (select adds).
        return limit, [*LIST_FIELDS, *attribute_names]
    return limit, list(select)


def refuse_non_record_unit(*, result_unit: Any, subtype: Any) -> None:
    """Refuse a list whose records page would deliver passages. Pure.

    ``rank_by="records"`` with a ``subtype_filter`` naming a passage type
    serves passages (``result_unit="passage"``); a list delivers records, so
    it is refused by name before the ranked read rather than relabeled.
    """
    if result_unit == "record":
        return
    raise _refuse(
        f"search: view='list' delivers records; subtype {subtype!r} is a "
        f"passage type, so this records page would serve passages",
        data={"view": "list", "subtype": subtype, "result_unit": result_unit},
        fix_hint=(
            "filter on the record type that holds these passages, or omit "
            "view for passage cards"
        ),
    )


def list_projection(payload: dict[str, Any]) -> dict[str, Any]:
    """A lexical records page delivered as a list.

    Membership, order, counts, coverage, pins and continuation are the page's
    own; only per-row delivery detail (score, excerpt, text, attrs, parent
    summaries, how the evidence kind was classified) is dropped. Trust
    (`provenance`) stays on every row. `evidence_kind` stays whenever it
    says something; "unknown" is left out, as the text line leaves it out. A
    passage row keeps its :data:`LIST_ROW_UNIT_KEY` label, and a row with a
    resolved parent keeps that parent's id (`_list_parent`).
    """
    result = {k: v for k, v in payload.items() if k not in LIST_DROPPED_PAGE_KEYS}
    if "parent_relation" in result:
        # A list names a passage's parent by id only (`_list_parent`); the
        # relation's page block (scan bounds, counts, withheld notices) stays
        # on cards, as when the list read no parent.
        result["parent_relation"] = {}
    result["view"] = "list"
    result["results"] = [
        _list_row(row) if isinstance(row, dict) else row
        for row in payload.get("results", [])
    ]
    return result


def attrs_list_projection(payload: dict[str, Any], *, selected: Any) -> dict[str, Any]:
    """A query-less lookup page delivered as a list (issue #3666).

    Membership, order, counts and continuation are the lookup's own. A row
    keeps what the call selected, its identity and its trust; how the
    evidence kind was classified is dropped unless selected, and an
    `evidence_kind` of "unknown" is left out, as on every list.
    """
    named = set(selected) if isinstance(selected, (list, tuple)) else set()
    result = dict(payload)
    result["view"] = "list"
    result["results"] = [
        {
            key: value for key, value in row.items()
            if key in named or not (
                key == "evidence_kind_provenance"
                or (key == "evidence_kind" and value == "unknown")
            )
        } if isinstance(row, dict) else row
        for row in payload.get("results", [])
    ]
    return result


#: Page keys a grep list never delivers: blocks that describe card content.
GREP_LIST_DROPPED_PAGE_KEYS = frozenset({
    "matching_passages_basis", "max_chars", "prose_preview",
    "attribute_truncation", "excerpt_window_basis",
})
#: `grep_search.OWN_MATCH_KEY`, restated: this module imports no search code.
_OWN_MATCH_KEY = "own_match"


def grep_list_projection(payload: dict[str, Any]) -> dict[str, Any]:
    """A grep page delivered as a list (#3659): one compact row per record.

    Membership, order, counts, bounds, pins and continuation are the grep
    page's own, and every row already passed grep's read checks; only the
    delivery changes. A row keeps `node_id`, `subtype`, its trust, a
    resolved parent's id, and:

    * `handle`: the record's title, path and date when it has any (PACT
      read-names-list-3666). The title and date are the catalog's, as on a
      card; the path is the catalog's, or, where the type declares none, the
      stored `path` attribute that grep's `path_prefix` and `path:` read
      (`grep_search.GREP_PATH_ATTR`).

    * `matches`: how many of the record's text units match (its own text
      and its passages), grep's unit of a match (`and_scope: text_unit`),
      not occurrences inside a unit. Grep stops scanning once a page is
      full, so on any page but one that read every match the number counts
      the units scanned so far, and the page says `matches_basis: "scanned"`.
    * `first_match`: `{chunk, line, text}` of one matching place: the
      earliest match grep served in the record's own text, or, when only
      passages match, in the first passage it served. `text` is that line,
      cut around the match by grep. With no `line_basis`, `line` is a line
      of the row's record as `inspect` text numbers it, the number that read
      takes back as `lines` (#3666). Otherwise `line_basis` says what the
      line counts: `passage` (lines inside `chunk`) or `own_text` (lines of
      the record's own `text` attribute, which `inspect` text does not
      read). Absent when the query has no text term
      (only `path:`), which leaves no line to show.
    """
    result = {k: v for k, v in payload.items() if k not in GREP_LIST_DROPPED_PAGE_KEYS}
    if "parent_relation" in result:
        result["parent_relation"] = {}
    result["view"] = "list"
    if "matching_passages_basis" in payload:
        result["matches_basis"] = payload["matching_passages_basis"]
    passages = payload.get("result_unit") == "passage"
    result["results"] = [
        _grep_list_row(row, passage=passages) if isinstance(row, dict) else row
        for row in payload.get("results", [])
    ]
    return result


def _grep_list_row(row: dict[str, Any], *, passage: bool) -> dict[str, Any]:
    windows = [w for w in row.get("match_windows") or [] if isinstance(w, dict)]
    own_windows = [w for w in windows if "passage_id" not in w]
    folded = row.get("matching_passages")
    folded = folded if isinstance(folded, int) and not isinstance(folded, bool) else 0
    own = row.get(_OWN_MATCH_KEY)
    if not isinstance(own, bool):
        own = row.get("excerpt") is not None or bool(own_windows)
    out: dict[str, Any] = {"node_id": row.get("node_id"), "subtype": row.get("subtype")}
    if row.get(_CITATION_KEY):
        # The id a reader cites, as the read fenced it (read-names-list-3666).
        out[_CITATION_KEY] = row[_CITATION_KEY]
    handle = row.get("handle")
    if isinstance(handle, dict) and handle:
        # PACT read-names-list-3666: the record's title and path (and date)
        # as the read already fenced them, so a row names what it is.
        out["handle"] = handle
    # A served row matched somewhere, so never fewer than one.
    out["matches"] = max(folded + (1 if own else 0), 1)
    if windows:
        if own_windows:
            first = min(own_windows, key=_window_start)
        else:
            # The first passage grep served, and its earliest match.
            first = min(
                (w for w in windows if w.get("passage_id") == windows[0].get("passage_id")),
                key=_window_start,
            )
        out["first_match"] = {
            "chunk": first.get("passage_id", row.get("node_id")),
            "line": first.get("line"),
            "text": first.get("excerpt"),
        }
        if first.get("line_basis") is not None:
            # The line is not a line of the record's text: it says of what.
            out["first_match"]["line_basis"] = first["line_basis"]
    if passage:
        out[LIST_ROW_UNIT_KEY] = "passage"
    parent = _list_parent(row.get("parent"))
    if parent is not None:
        out["parent"] = parent
    for key in ("provenance", "read_provenance"):
        if key in row:
            out[key] = row[key]
    return out


def _window_start(window: dict[str, Any]) -> int:
    start = window.get("char_start")
    return start if isinstance(start, int) else 0


def _list_row(row: dict[str, Any]) -> dict[str, Any]:
    kept = {
        key: value for key, value in row.items()
        if key not in LIST_DROPPED_ROW_KEYS
        and not (key == "evidence_kind" and value == "unknown")
    }
    parent = _list_parent(row.get("parent"))
    if parent is not None:
        kept["parent"] = parent
    return kept


def _list_parent(parent: Any) -> Optional[dict[str, Any]]:
    """A passage row's resolved parent as ``{"node_id"}``, else None.

    Issue #3467: a list row for a chunk named only the chunk, so an agent
    spent a call per row to learn which document or ticket it came from. The
    id is the one the card prints (the same Nomos-filtered resolution); its
    summary stays on cards. An unresolved state is left to cards and
    `inspect`, as before.
    """
    if (
        isinstance(parent, dict)
        and parent.get("node_id")
        and parent.get("state", "resolved") == "resolved"
    ):
        return {"node_id": parent["node_id"]}
    return None


def metadata_projection(payload: dict[str, Any]) -> dict[str, Any]:
    """A retained lexical page delivered as ``view="metadata"`` (#3222).

    No row and nothing built from the rows: no node id, title, handle or
    excerpt. The page keeps its counts, coverage (whose ``selection`` the
    reference set), applied scope, pin and ``result_reference``; search's
    own continuation is dropped and ``next_offset`` is null, so nothing but
    the reference continues the read.
    """
    result = {
        key: value for key, value in payload.items()
        if key not in METADATA_DROPPED_PAGE_KEYS
    }
    result["view"] = "metadata"
    result["next_offset"] = None
    return result
