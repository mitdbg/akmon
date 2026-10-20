"""Decision-shaped envelope helpers for MCP list-returning tools.

Per `improve-mcp-tool-surface` (`docs/reference/mcp.md`),
every list-returning MCP tool returns the same metadata triple —
`total_count`, `truncated`, `pinned_generation_id` — so the agent
can decide whether to drill, page, or pivot from one return.

This module provides:

  - `SelectUnknownField`  — structured error code for an agent
                            requesting a field that the tool does
                            not recognize. Subclasses `SubstrateError`
                            so the MCP layer surfaces it via the
                            standard error wrapper.

  - `validate_select(select, allowed, *, tool_name)` — raise
                            `SelectUnknownField` if any name in
                            `select` is not in `allowed`. Returns
                            None (validation only).

  - `project_row(row, select, *, default, always)` — return a new
                            dict containing every field in `select`
                            that exists in `row`, plus every field
                            in `always`. When `select` is None,
                            return the projection determined by
                            `default`. The shape is:
                            `(select | always) ∩ row.keys()` when
                            select is given;
                            `(default | always) ∩ row.keys()` when
                            it isn't.

Tools call `project_row` per result row. The envelope fields
(`total_count`, `truncated`, `pinned_generation_id`) are wrapped
around the projected list at the call site.

Conventions (codified per the spec):

  - Always-included for node-shaped rows: `node_id`, `subtype`.
  - `pinned_generation_id` is the canonical envelope name; existing
    tools still carry `generation_id` for backward compat during
    the deprecation window.
  - `total_count` semantics: a tool that emits `total_count` states
    its basis in `total_count_basis` — `exact` (a real COUNT, or a
    result set read whole) or `lower_bound` (a bounded window that
    the tool did not read past). The old "report `limit` when
    truncated" convention is retired: that number described the page,
    not the set, and read as the set's size (issue #1440). A tool that
    cannot count its set emits `returned_count` — the rows in THIS
    payload — and no `total_count` at all; or, where the payload shape
    keeps the key, `total_count: null` with basis `unknown` (filter and
    map past their first page, issue #3190), rendered "count unknown".
"""
from __future__ import annotations

import json
import re
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError


__all__ = [
    "SelectUnknownField",
    "select_retry_from",
    "validate_select",
    "project_row",
    "with_read_coverage",
]


class _LastPageDelivery(dict):
    """`coverage.delivery` proved complete by the producer last-page marker.

    Serializes as the plain mapping. `rows` records how many rows the proof
    covered, so the second (post-fit) pass keeps the proof only for the same
    delivered rows; an exact-size completion is recomputed instead, and a
    copy that drops the subclass loses the proof (completion turns unknown).
    """

    __slots__ = ("rows",)

    def __init__(self, rows: int, **fields: Any) -> None:
        super().__init__(**fields)
        self.rows = rows


def with_read_coverage(payload: dict[str, Any]) -> dict[str, Any]:
    """Qualify list counts without inferring recall from a short output page.

    Called once on producer output, and again after byte fitting. Existing
    retrieval/selection provenance is retained; delivery always reflects rows
    actually emitted. An explicit unknown count supersedes even numeric zero.
    """
    tool = payload.get("operator_name")
    if tool not in {"search", "filter", "map", "aggregate", "expand"}:
        return payload
    if payload.get("error") is not None or payload.get("status") in {"refused", "error"}:
        return payload
    # expand qualifies only its one-hop `neighbors` page (#3201); a path or
    # depth walk has no row list here and is left as it is.
    # A path page lists its rows under `results` too, so expand looks only at
    # `neighbors`.
    keys = ("neighbors",) if tool == "expand" else ("results", "groups", "items")
    key = next((key for key in keys if isinstance(payload.get(key), list)), None)
    # A metadata search (#3222 slice 3) has no row list by design.
    metadata = tool == "search" and payload.get("view") == "metadata"
    if key is None and not metadata:
        return payload
    out = dict(payload)
    basis = out.get("total_count_basis", "unknown")
    if basis == "unknown":
        out["total_count"] = None
        out["total_count_basis"] = "unknown"
    total = out.get("total_count")
    total = total if isinstance(total, int) and not isinstance(total, bool) else None
    method = out.get("method")
    unit = "group" if tool == "aggregate" else "row"
    if tool == "search":
        if out.get("result_unit") == "passage":
            unit = "passage"
        elif method != "identifier_mentions" and (
            out.get("rank_by") in {"records", "passages"} or out.get("total_count_unit")
        ):
            unit = "record"
    # `selection_basis` is a producer input marker, like `retrieval_basis`:
    # recorded once in `coverage.selection` and never repeated on the wire.
    set_basis = out.pop("selection_basis", None) or (
        "retrieved_candidates" if tool == "search" and (
            method in {"semantic", "lexical"} or out.get("diversity_applied")
        ) else "matching_set"
    )
    previous = out.get("coverage")
    previous = previous if isinstance(previous, dict) else {}
    bounds = []
    if out.get("rank_capped") or out.get("candidates_capped"):
        bounds.append("candidate_cap")
    if out.get("rollup_chunk_cap_reached"):
        bounds.append("work_cap")
    if out.get("retrieval_basis") == "result_reference_members":
        # A read over a result reference covers the rows that reference
        # holds, which can be fewer than its search matched: bounded by the
        # reference, never exhaustive of the graph (#3658).
        bounds.append("result_reference")
    if method == "semantic":
        status = "approximate"
    elif bounds or basis == "lower_bound":
        status = "bounded"
    elif out.get("retrieval_exhaustive") is True:
        # Whole matching-space exhaustion is a separate producer fact from
        # cardinality of candidate/input sets. No page-length heuristic.
        status = "exhaustive"
    else:
        status = "unknown"
    # Omission rule (docs/reference/mcp-results.md#counts-and-coverage): an
    # absent coverage field is empty or derivable from another key, never
    # unknown. Unknown stays explicit (`size: null`, status `unknown`).
    fresh_retrieval: dict[str, Any] = {
        "status": status,
        "basis": out.get("retrieval_basis") or ("semantic_ranking" if method == "semantic" else (
            "producer_scan" if out.get("retrieval_exhaustive") is True else "producer_count"
        )),
    }
    if bounds:
        fresh_retrieval["bounds"] = bounds
    retrieval = previous.get("retrieval", fresh_retrieval)
    # The producer proof has been recorded in the public coverage contract;
    # avoid repeating this internal input marker on the wire.
    out.pop("retrieval_exhaustive", None)
    out.pop("retrieval_basis", None)
    exact_size = total if basis == "exact" else None
    fresh_selection: dict[str, Any] = {"unit": unit, "basis": set_basis}
    if exact_size is None:
        # An exact size is `total_count` itself, so it is omitted; a size
        # the producer cannot prove is said, as null.
        fresh_selection["size"] = None
    # A page limit bounds delivery, not membership of the selected set.
    # Existing producers do not freeze top-k membership; do not pretend
    # their page limit is a selection limit. Omitted when there is none.
    if out.get("selection_limit") is not None:
        fresh_selection["limit"] = out["selection_limit"]
    selection = previous.get("selection", fresh_selection)
    if metadata or key is None:
        # The metadata branch (design "Metadata delivery"): no row is
        # delivered and none is owed, so delivery is complete with zero rows
        # while retrieval keeps its own status. A count summary cut short
        # names its bound (`summary_groups`, `output_bytes`) here too.
        delivery: dict[str, Any] = {"complete": True, "view": "metadata", "rows": 0}
        summary = out.get("summary")
        bounds = summary.get("bounds") if isinstance(summary, dict) else None
        if isinstance(bounds, list) and bounds:
            delivery["bounds"] = list(bounds)
        out["coverage"] = {
            "retrieval": retrieval, "selection": selection, "delivery": delivery,
        }
        return out
    size = selection["size"] if "size" in selection else exact_size
    # Delivered rows and a usable continuation are the page's own row list
    # and cursor keys; coverage does not repeat them.
    cursor = out.get("next_offset")
    next_available = (
        isinstance(cursor, int) and not isinstance(cursor, bool) and cursor >= 0
    ) or any(isinstance(out.get(name), str) and bool(out[name].strip())
             for name in ("next", "continuation_token"))
    returned = len(out[key])
    offset = out.get("offset", 0)
    offset = offset if isinstance(offset, int) and not isinstance(offset, bool) else 0
    # `delivery_last_page` is a producer input marker (filter, map and the
    # attrs lookup past their first page). The second pass, after byte
    # fitting, inherits only a completion that marker proved, and only for
    # the same number of delivered rows. Any other earlier `complete: true`
    # (the exact-size rule) is recomputed from this page, never inherited.
    previous_delivery = previous.get("delivery")
    last_page = out.pop("delivery_last_page", None) is True or (
        isinstance(previous_delivery, _LastPageDelivery)
        and previous_delivery.get("complete") is True
        and previous_delivery.rows == returned
    )
    byte_cut = out.get("truncation_reason") == "response_byte_budget" or "response_byte_budget" in out
    complete = False if next_available or byte_cut else None
    if complete is None and isinstance(size, int) and offset + returned >= size:
        # A producer-proven selection can be fully delivered even when
        # retrieval was capped. Its exact size is independent of recall.
        complete = True
    elif complete is None and out.get("truncated"):
        complete = False
    elif complete is None and last_page and not out.get("pagination_capped"):
        # The producer read one row past this page and found none, and no
        # cursor is offered: the page is provably the last one even though
        # an unknown count hides the selected set's size. `truncated: false`
        # alone is not that proof, so only a producer marker establishes it.
        complete = True
    delivery = {"complete": complete}
    if complete is True and last_page and not (
        isinstance(size, int) and offset + returned >= size
    ):
        delivery = _LastPageDelivery(returned, complete=True)
    if byte_cut:
        # Delivery bounds are independent of retrieval bounds.
        delivery["bounds"] = ["output_bytes"]
    out["coverage"] = {
        "retrieval": retrieval,
        "selection": selection,
        "delivery": delivery,
    }
    return out


class SelectHint(str):
    """A `select` hint in parts, so a refusal says each part once.

    The string itself is the whole sentence (what `select_instead` returns
    and older callers read). `fact` goes in the refusal's message, `action`
    in its hint, and `code`, a short fixed word, in `data.instead`.
    """

    fact: str
    action: str
    code: str

    def __new__(cls, *, fact: str, action: str, code: str) -> "SelectHint":
        hint = super().__new__(cls, f"{fact}; {action}")
        hint.fact, hint.action, hint.code = fact, action, code
        return hint


class SelectUnknownField(SubstrateError):
    """The agent requested a field the tool does not produce.

    Per the spec, the response SHALL list the valid field names
    so the agent can correct the call without a separate
    introspection round-trip.
    """
    code = "select_unknown_field"
    #: The ready-retry clause inside `fix_hint`, so :func:`select_retry_from`
    #: can rebuild it from the caller's own `select`.
    retry_hint: Optional[str] = None


#: Names agents send for a field the tools spell differently. Used only to
#: say what to use instead in a refusal (PACT select-field-instructions);
#: never accepted in place of the field.
_CONVENTIONAL_NAMES: dict[str, str] = {
    "id": "node_id", "node": "node_id", "type": "subtype",
    "snippet": "excerpt", "preview": "excerpt", "url": "citation",
}


#: Keys of a reply that agents send in `select` as if they were row fields
#: (issue #3666), the tools whose replies carry each, and what it is. The
#: refusal of such a tool says it; the offered retry drops the name. Only
#: `expand` takes `live` on the read surface.
_REPLY_KEYS_NOT_FIELDS: dict[str, tuple[frozenset[str], str]] = {
    "external_live": (frozenset({"expand"}), (
        "not a select field: it is where a live call's rows come back, beside "
        "the graph rows; pass live={relation, args} to get them"
    )),
}


def _tool_of(tool_name: Optional[str]) -> Optional[str]:
    """The tool a refusal names: `search` for `search(method=x)`."""
    return tool_name.split("(", 1)[0] if tool_name else None


def select_instead(
    unknown: Iterable[str],
    allowed: Iterable[str],
    *,
    elsewhere: Optional[Mapping[str, str]] = None,
    tool_name: Optional[str] = None,
) -> dict[str, str]:
    """For each unknown `select` name, what to use instead, when known.

    In order: the field this tool spells it as (`id` -> `node_id`), where
    the field is served when this call cannot serve it (`handle` -> lexical
    search, `node_id` on expand -> the edge's src/dst), or a close spelling.
    A name with no such answer is left out; the refusal still lists every
    valid field."""
    import difflib

    allowed_list = sorted(set(allowed))
    out: dict[str, str] = {}
    for name in unknown:
        target = _CONVENTIONAL_NAMES.get(name)
        if target is not None and target in allowed_list:
            out[name] = f"use '{target}'"
        elif (
            name in _REPLY_KEYS_NOT_FIELDS and name not in allowed_list
            and _tool_of(tool_name) in _REPLY_KEYS_NOT_FIELDS[name][0]
        ):
            out[name] = _REPLY_KEYS_NOT_FIELDS[name][1]
        elif elsewhere and name in elsewhere:
            out[name] = elsewhere[name]
        elif name.startswith("date.") and len(name) > len("date."):
            # A `where` date field (issue #3666, lane RD): rows carry the
            # stored attribute, not the date meaning.
            out[name] = (
                "a where date field, not a row field; select the stored date "
                "attribute as attrs.<key>"
            )
        else:
            close = difflib.get_close_matches(name, allowed_list, n=1, cutoff=0.8)
            if close:
                out[name] = f"use '{close[0]}'"
    return out


_USE_HINT = re.compile(r"use '([^']+)'")
#: Longest hint `data.instead` repeats for every name that shares it.
_SHARED_HINT_CHARS = 80


def _retry_select(
    select: Sequence[Any],
    unknown: Iterable[str],
    instead: Mapping[str, str],
) -> list[str]:
    """The caller's `select` as a ready retry (PACT select-fields-by-method).

    In the caller's order, without repeats: a refused name whose hint is
    `use '<field>'` (a conventional name or a close spelling) is replaced
    by that field, unless the caller already named it; any other refused
    name is dropped. This is only advice in the refusal: the refused name
    is never accepted, and the validator does not substitute it."""
    unknown_set = set(unknown)
    requested = set(select)
    out: list[str] = []
    for name in select:
        if name in unknown_set:
            match = _USE_HINT.fullmatch(instead.get(name, ""))
            if match is None or match.group(1) in requested:
                continue
            name = match.group(1)
        if name not in out:
            out.append(name)
    return out


def _retry_hint(retry_select: list[str], allowed: Iterable[str]) -> str:
    valid = sorted(set(allowed))
    if retry_select:
        return (
            f"retry with select={retry_select!r}; the tool's selectable "
            f"fields are {valid}"
        )
    return f"drop select or choose from: {valid}"


def align_select_retry(error: Any, retry_call: Mapping[str, Any]) -> None:
    """Make a `select_unknown_field` refusal say what its `retry_call` says.

    The refusal's own advice drops each refused name; the offered call may
    correct one instead (another letter case, a link's far end). Two answers
    in one refusal is one too many: when the call is the same tool and
    method with a corrected `select`, `data.retry_select` and the hint name
    that list; when it is another method's call (a guess), the refusal's own
    list is withdrawn and only the call is offered.
    """
    retry_hint = getattr(error, "retry_hint", None)
    data = getattr(error, "data", None)
    fix_hint = getattr(error, "fix_hint", None)
    if not (retry_hint and isinstance(data, dict) and isinstance(fix_hint, str)):
        return
    arguments = retry_call.get("arguments")
    select = arguments.get("select") if isinstance(arguments, Mapping) else None
    corrected = (
        list(select)
        if retry_call.get("mode") == "corrected" and isinstance(select, (list, tuple))
        else []
    )
    hint = _retry_hint(corrected, data.get("valid_fields") or ())
    error.fix_hint = fix_hint.replace(retry_hint, hint, 1)
    error.retry_hint = hint
    data["retry_select"] = corrected or None


@contextmanager
def select_retry_from(caller_select: Any) -> Iterator[None]:
    """Rebuild a `select_unknown_field` refusal's ready retry from the
    caller's own `select`.

    For a call whose `select` the server rewrote before the operator saw it
    (the `source_lineage` token becomes `attrs`), the refusal raised inside
    names the operator's list. Inside this block it is rewritten in place
    to the caller's list, refused names removed or replaced as
    :func:`_retry_select` says; the refusal itself is re-raised unchanged
    otherwise."""
    try:
        yield
    except SelectUnknownField as exc:
        if isinstance(caller_select, (list, tuple)) and exc.retry_hint and exc.fix_hint:
            retry_select = _retry_select(
                caller_select,
                exc.data.get("unknown_fields") or (),
                exc.data.get("instead") or {},
            )
            hint = _retry_hint(retry_select, exc.data.get("valid_fields") or ())
            exc.fix_hint = exc.fix_hint.replace(exc.retry_hint, hint, 1)
            exc.retry_hint = hint
            exc.data["retry_select"] = retry_select or None
        raise


def validate_select(
    select: Optional[list[str]],
    allowed: Iterable[str],
    *,
    tool_name: str,
    text_node_id: Optional[str] = None,
    elsewhere: Optional[Mapping[str, str]] = None,
) -> None:
    """Raise `SelectUnknownField` if any name in `select` is not
    in `allowed`. No-op when `select` is None.

    `text_node_id` is the node the call targets, when there is one: a
    refused `text`/`attrs.text` then points at the text read for THAT id
    (a record's whole text, or a chunk's own text and its records), so the
    agent never has to guess which id the text read wants.

    `allowed` is the set of field names the tool documents as
    selectable. Always-included names (`node_id`, `subtype`) are
    typically members of `allowed` too, so the agent can request
    them explicitly without surprise.

    `elsewhere` maps a name this call cannot serve to where it is served;
    the refusal then says, per unknown name, what to use instead
    (:func:`select_instead`). Nothing is accepted in its place.
    """
    if select is None:
        return
    allowed_set = set(allowed)
    unknown = [s for s in select if s not in allowed_set]
    if unknown:
        text_target = (
            json.dumps(text_node_id) if text_node_id else "<record or chunk id>"
        )
        instead = select_instead(
            unknown, allowed_set, elsewhere=elsewhere, tool_name=tool_name,
        )
        # One sentence per refusal, not one per field: names that share a
        # hint are listed together before it.
        # A hint in parts (`SelectHint`) puts its fact in the message and its
        # action in the hint, so the reason is stated once in the refusal.
        grouped: dict[str, list[str]] = {}
        facts: dict[str, list[str]] = {}
        actions: list[str] = []
        for name, hint in instead.items():
            if isinstance(hint, SelectHint):
                facts.setdefault(hint.fact, []).append(name)
                if hint.action not in actions:
                    actions.append(hint.action)
            else:
                grouped.setdefault(hint, []).append(name)
                facts.setdefault(hint, []).append(name)
        stated = "; ".join(f"{', '.join(names)}: {fact}" for fact, names in facts.items())
        said = "; ".join(
            [f"{', '.join(names)}: {hint}" for hint, names in grouped.items()] + actions
        )
        # A ready retry (PACT select-fields-by-method): the caller's own
        # `select` with the refused names removed or replaced by their
        # `use '<field>'` hint, in the caller's order. Agents reused a
        # lexical `select` on grep and were refused twice; nothing refused
        # is accepted or substituted here.
        retry_select = _retry_select(select, unknown, instead)
        retry = _retry_hint(retry_select, allowed_set)
        data: dict[str, Any] = {
            "tool": tool_name,
            "unknown_fields": unknown,
            "valid_fields": sorted(allowed_set),
            "retry_select": retry_select or None,
        }
        if instead:
            # A long hint several names share is carried once in `data` too,
            # so the refusal's data stays under its size cap.
            first_with: dict[str, str] = {}
            data["instead"] = {
                name: (
                    hint.code if isinstance(hint, SelectHint)
                    else hint if len(hint) <= _SHARED_HINT_CHARS
                    or first_with.setdefault(hint, name) == name
                    else f"as '{first_with[hint]}'"
                )
                for name, hint in instead.items()
            }
        error = SelectUnknownField(
            f"{tool_name}: unknown field(s) in select: "
            f"{unknown!r}" + (f" ({stated})" if stated else ""),
            data=data,
            fix_hint=(
                (f"{said}; " if said else "")
                + retry
                + (
                    "; to read this text, call "
                    f'inspect(target={{"kind":"text","node_id":{text_target}}})'
                    " (a record returns its whole text in order; a chunk "
                    "returns its own text and the records it belongs to)"
                    if {"attrs.text", "text"} & set(unknown)
                    else ""
                )
            ),
        )
        error.retry_hint = retry
        raise error


def project_row(
    row: dict[str, Any],
    select: Optional[list[str]],
    *,
    default: Iterable[str],
    always: Iterable[str] = ("node_id", "subtype"),
) -> dict[str, Any]:
    """Return a new dict projecting `row` to the requested fields.

    - When `select` is None, the projection is `default | always`.
    - When `select` is given, the projection is `select | always`.

    Fields not present in `row` are silently dropped (the row may
    not carry every documented field — e.g. an unranked search
    result has no `score`). Validation of unknown fields is the
    caller's responsibility via `validate_select`.

    The returned dict preserves `row`'s key ordering for keys
    that survive the projection.
    """
    always_set = set(always)
    if select is None:
        wanted = set(default) | always_set
    else:
        wanted = set(select) | always_set
    return {k: v for k, v in row.items() if k in wanted}
