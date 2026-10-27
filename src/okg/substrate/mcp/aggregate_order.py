"""`aggregate`'s `order_by`: rank every group, then cut the page (PACT
read-aggregate-order-by, lane ri part 2).

One module holds the argument check, the ranking rule and the facts a
ranked reply adds. The stored, result-reference, `match`, date-period, live
and `run` paths all call it, so one set of groups ranks the same wherever it
was counted.

* **The argument.** `"<name>"`, `"<name> asc"`, `"<name> desc"` or
  `"-<name>"`. `<name>` is matched as an opaque string against the call's
  metric labels as the reply prints them, `occurrences` on `match` groups,
  and the call's `by` fields as written. The whole string is matched first
  (a `by` field may hold a space); otherwise the direction is split off by
  :func:`live_reader.split_order_direction`, the live `filter`'s splitter.
  No direction is smallest first.
* **The rule.** Numbers compare as numbers (integers exactly, a sum's float
  part added exactly as a fraction); mixed kinds order numbers, text (by
  code point), booleans, then anything else (NaN and infinity included) by
  its JSON text, in that kind order both ways (only values within a kind
  reverse). A group with no value of the ranked name ranks after every
  group with one, in both directions: a null before a missing key
  (`(absent)`). Ties go by the group key, field by field with the same
  comparison (null and `(absent)` last), then by the key's JSON text, so
  the order is total. One sort, with each group's sort key computed once.
* **The page.** `limit` and `offset` cut the ranked list after the ranking;
  a group's `rank` is its 1-based position across pages.
"""
from __future__ import annotations

import decimal
import difflib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Optional

from okg.substrate.mcp.live_reader import split_order_direction
from okg.substrate.mcp.live_result_sets import ABSENT, ABSENT_VALUE, float_part_label
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

__all__ = [
    "ABSENT_VALUE",
    "OrderBy",
    "Ranked",
    "check_order_by",
    "head_line",
    "live_value",
    "match_key",
    "match_value",
    "metric_value",
    "needs_set_refusal",
    "order_note",
    "order_note_line",
    "page_facts",
    "partial_lines",
    "rank",
    "served_key",
    "stored_key",
    "tie_line",
]

#: The metrics a stored set computes; a live set (and a `run` step over rows)
#: also sums, minimises and maximises a column.
_LIVE_METRIC = re.compile(r"^(sum|min|max)\(.+\)$")
#: The column `match` groups carry beside their record count.
OCCURRENCES = "occurrences"
_SPELLING = 'aggregate: order_by is "<name>", "<name> asc", "<name> desc" or "-<name>"'


@dataclass(frozen=True)
class OrderBy:
    """What one call ranks by: a printed metric label or a `by` field."""

    name: str
    descending: bool
    #: "metric" (a metric label, or `occurrences`) or "group" (a `by` field).
    kind: str

    @property
    def direction(self) -> str:
        return "desc" if self.descending else "asc"

    def payload(self) -> dict[str, Any]:
        return {"name": self.name, "direction": self.direction, "kind": self.kind}

    def argument(self) -> str:
        """The order_by that asks for this order again (the `next` hint)."""
        return f"{self.name} {self.direction}"


def _refuse(message: str, reason: str, fix_hint: str, **data: Any) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        message, data={"argument": "order_by", "reason": reason, **data}, fix_hint=fix_hint,
    )


def needs_set_refusal() -> InvalidOperatorArguments:
    """order_by on the record-anchor or edge-summary form (C12)."""
    return _refuse(
        "aggregate: order_by ranks the groups of a set",
        "order_by_needs_set",
        'rank the groups of a set: input={"kind": "nodes", "subtype": "<type>"} '
        '(or input={"kind": "edges", "edge_type": "<type>"}), by=["<field>"], '
        'metrics=["count"], order_by="count desc"',
    )


def check_order_by(
    order_by: Any,
    *,
    by: Sequence[str],
    metrics: Sequence[str],
    match: bool = False,
    live: bool = False,
    call: Optional[Mapping[str, Any]] = None,
) -> Optional[OrderBy]:
    """The call's order, or None without one. Refuses before anything is
    read: a name that is no metric and no `by` field (`order_by_unknown`),
    a metric the call does not compute (`order_by_not_in_metrics`, with a
    corrected call), or two directions (`order_by_spelling`).

    `metrics` are the printed labels; `live` says the set also computes
    sum/min/max (so naming one that `metrics` lacks is a missing metric,
    not an unknown name); `call` is the call's arguments, for the corrected
    call."""

    if order_by is None:
        return None
    if not isinstance(order_by, str) or not order_by.strip():
        raise _refuse(_SPELLING, "order_by_spelling", 'e.g. order_by="count desc"')
    by_names = [str(field) for field in by]
    metric_names = [str(label) for label in metrics] + ([OCCURRENCES] if match else [])
    names = [*metric_names, *(n for n in by_names if n not in metric_names)]

    def resolved(name: str, descending: bool) -> Optional[OrderBy]:
        if name in metric_names:
            return OrderBy(name, descending, "metric")
        if name in by_names:
            return OrderBy(name, descending, "group")
        return None

    whole = resolved(order_by, False)
    if whole is not None:
        return whole
    name, descending, doubled = split_order_direction(order_by)
    if doubled:
        raise _refuse(_SPELLING, "order_by_spelling", f'e.g. order_by="{name.rsplit(" ", 1)[0]} desc"')
    found = resolved(name, descending)
    if found is not None:
        return found
    # A metric form close to one the call computes is a typo of it, not a
    # metric to add (`max(attrs.static_totl)`): refused as unknown below.
    near_metric = difflib.get_close_matches(name, metric_names, n=1, cutoff=0.85)
    if name == "count" or (live and _LIVE_METRIC.match(name) and not near_metric):
        sent = list(metrics)
        if len(sent) >= 4:
            # No corrected call drops a metric the caller asked for (m4).
            raise _refuse(
                f'aggregate: order_by "{name}" is not in metrics, and metrics takes at '
                f"most 4; replace one with {name}",
                "order_by_not_in_metrics",
                f"send metrics with {name} in place of one of {json.dumps(sent)}",
                metrics=sent,
            )
        corrected = [*sent, name]
        refusal = _refuse(
            f'aggregate: order_by "{name}" is not in metrics',
            "order_by_not_in_metrics",
            f"add it to metrics: metrics={json.dumps(corrected)}",
            metrics=sent,
        )
        if call is not None:
            refusal.retry_call = {  # type: ignore[attr-defined]
                "tool": "aggregate", "mode": "corrected",
                "arguments": {**dict(call), "metrics": corrected},
            }
        raise refusal
    closest = difflib.get_close_matches(name, names, n=1, cutoff=0.5)
    stored_metric = not live and _LIVE_METRIC.match(name) is not None
    raise _refuse(
        f'aggregate: order_by "{name}" is not a metric or a by field of this call; '
        f"it takes one of: {', '.join(names)}"
        + (f" (closest: {closest[0]})" if closest else "")
        + ("; a stored set computes count only" if stored_metric else ""),
        "order_by_unknown",
        f'e.g. order_by="{(closest or names or ["count"])[0]} desc"',
        takes=names, **({"closest": closest[0]} if closest else {}),
    )


# -------------------------------------------------------------------- the rule


def metric_value(computed: Mapping[str, Any], label: str) -> Any:
    """A group's value of metric `label`: a sum that met non-integer values
    is its whole-number part plus its float part, added exactly as a
    fraction (exact over the served values)."""

    value = computed.get(label)
    floats = computed.get(float_part_label(label))
    if floats is None or value is None:
        return value
    return Fraction(value) + Fraction(floats)


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _kind_key(value: Any) -> tuple[int, Any]:
    """(kind, comparable) of a value with a value: numbers, text, booleans,
    then anything else by its JSON text."""

    if isinstance(value, bool):
        return 2, int(value)
    if isinstance(value, (int, Fraction)):
        return 0, value
    if isinstance(value, float):
        return (0, value) if math.isfinite(value) else (3, _json_text(value))
    if isinstance(value, decimal.Decimal):
        return (0, Fraction(value)) if value.is_finite() else (3, _json_text(str(value)))
    if isinstance(value, str):
        return 1, value
    return 3, _json_text(value)


def _value_key(value: Any) -> tuple[int, int, Any]:
    """(class, kind, comparable): class 0 has a value, 1 is null, 2 absent."""

    if value is ABSENT_VALUE:
        return 2, 0, 0
    if value is None:
        return 1, 0, 0
    kind, comparable = _kind_key(value)
    return 0, kind, comparable


def _descending(comparable: Any) -> Any:
    """A comparable that sorts in the opposite order: a negated number, or a
    text as negated code points closed by a marker that sorts after them
    (so a longer text that starts with a shorter one comes first)."""

    if isinstance(comparable, str):
        return tuple(-ord(c) for c in comparable) + (1,)
    return -comparable


def _key_text(key: Sequence[Any]) -> str:
    return _json_text(["\x00absent" if v is ABSENT_VALUE else v for v in key])


@dataclass
class Ranked:
    """The groups in rank order (position = index + 1)."""

    groups: list[Any]
    #: Per group, its value as compared (class, kind, comparable).
    values: list[tuple[int, int, Any]]
    #: Groups with no value of the ranked name (null or absent).
    no_value: int

    def __len__(self) -> int:
        return len(self.groups)


def rank(
    groups: Sequence[Any],
    order: OrderBy,
    *,
    value: Callable[[Any], Any],
    key: Callable[[Any], Sequence[Any]],
) -> Ranked:
    """`groups` ranked by `order`. `value(group)` is the ranked value (None
    for null, :data:`ABSENT_VALUE` for a missing key); `key(group)` is the
    group key, field by field, in the same form (raw text on `match`
    groups)."""

    decorated = []
    for index, group in enumerate(groups):
        compared = _value_key(value(group))
        cls, kind, comparable = compared
        if cls == 0 and order.descending:
            # Kinds keep their order (numbers first) both ways; only the
            # values within a kind reverse (review m1).
            primary: tuple[Any, ...] = (0, kind, _descending(comparable))
        else:
            primary = (cls, kind, comparable)
        raw_key = tuple(key(group))
        tie = tuple(_value_key(v) for v in raw_key)
        decorated.append(((primary, tie, _key_text(raw_key), index), compared, group))
    decorated.sort(key=lambda item: item[0])
    return Ranked(
        groups=[group for _sort, _compared, group in decorated],
        values=[compared for _sort, compared, _group in decorated],
        no_value=sum(1 for _sort, compared, _group in decorated if compared[0] != 0),
    )


def stored_key(item: Sequence[Any], width: int) -> tuple[Any, ...]:
    """A stored group's key in the ranking's form: `item` is `(key, count)`
    or `(key, count, missing)`, `missing` flagging per field a key the record
    does not carry (SQL NULL), which becomes :data:`ABSENT_VALUE`; a stored
    JSON null stays None (B2)."""

    key = item[0]
    missing = item[2] if len(item) > 2 else ()
    return tuple(
        ABSENT_VALUE if i < len(missing) and missing[i] else key[i] for i in range(width)
    )


def live_value(group: Sequence[Any], order: OrderBy, fields: Sequence[str]) -> Any:
    """A live (or held-row) group's ranked value: `group` is `(key,
    computed)` as `live_result_sets.group_report` gives it."""

    key, computed = group
    if order.kind == "metric":
        return metric_value(computed, order.name)
    return key[list(fields).index(order.name)]


def match_value(group: Any, order: OrderBy) -> Any:
    """A `match` group's ranked value: its raw matched text (never the
    fenced text, M4), its record count, or its occurrences."""

    if order.kind == "group":
        return group.raw
    return group.occurrences if order.name == OCCURRENCES else group.records


def match_key(group: Any) -> tuple[Any, ...]:
    """A `match` group's key for ties: the raw matched text."""
    return (group.raw,)


def served_key(value: Any) -> Any:
    """A key value as a ranked reply prints it: a missing key is `(absent)`."""
    return ABSENT if value is ABSENT_VALUE else value


def page_facts(
    ranked: Ranked,
    order: OrderBy,
    *,
    offset: int,
    page: int,
    complete: bool = True,
    tie_value: Optional[Callable[[Any], Any]] = None,
    value: Optional[Callable[[Any], Any]] = None,
) -> dict[str, Any]:
    """The fields a ranked reply adds (beside `rank` on each group).

    `value(group)` is the group's ranked value as served (for the tie);
    `tie_value` serves a key value as the path serves its keys (fenced on a
    live set)."""

    facts: dict[str, Any] = {"order_by": order.payload(), "ranking_complete": bool(complete)}
    if ranked.no_value:
        facts["ranked_last_no_value"] = ranked.no_value
    end = offset + page
    if 0 < end < len(ranked) and end > offset and ranked.values[end - 1] == ranked.values[end]:
        target = ranked.values[end - 1]
        first = end - 1
        while first > 0 and ranked.values[first - 1] == target:
            first -= 1
        last = end
        while last + 1 < len(ranked) and ranked.values[last + 1] == target:
            last += 1
        shown_from = max(first, offset)
        raw = value(ranked.groups[end - 1]) if value is not None else None
        if order.kind == "group":
            raw = served_key(raw)
            if tie_value is not None and raw is not ABSENT:
                raw = tie_value(raw)
        elif isinstance(raw, Fraction):
            raw = float(raw)
        facts["tie_at_page_end"] = {
            "position": first + 1, "value": raw,
            "groups_tied": last - first + 1, "shown": end - shown_from,
        }
    return facts


def order_note(metrics: Sequence[str], *, cut: bool) -> Optional[dict[str, Any]]:
    """The note on an unranked page that `limit` cut while `metrics` holds a
    metric other than count (C7): its groups are not in that metric's order."""

    other = [str(label) for label in metrics if str(label) != "count"]
    if not cut or not other:
        return None
    return {"ordered_by": "count, then key", "rank_with": f'order_by="{other[0]} desc"'}


# -------------------------------------------------------------------- text


def _order_of(payload: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    order = payload.get("order_by")
    if isinstance(order, Mapping) and isinstance(order.get("name"), str):
        return order
    return None


def head_line(payload: Mapping[str, Any]) -> Optional[str]:
    """`ordered by <name>, largest first · all N groups ranked`."""

    order = _order_of(payload)
    if order is None:
        return None
    words = "largest first" if order.get("direction") == "desc" else "smallest first"
    total = payload.get("total_count")
    total = total if isinstance(total, int) and not isinstance(total, bool) else len(
        payload.get("groups") or [],
    )
    noun = "group" if total == 1 else "groups"
    scope = (
        f"all {total:,} {noun} ranked" if payload["ranking_complete"]
        else f"{total:,} {noun} ranked, over the rows read"
    )
    last = payload.get("ranked_last_no_value")
    tail = (
        f" · {last:,} with no {order['name']}, ranked last"
        if isinstance(last, int) and not isinstance(last, bool) and last else ""
    )
    return f"ordered by {order['name']}, {words} · {scope}{tail}"


def tie_line(payload: Mapping[str, Any]) -> Optional[str]:
    """`! positions 2-4 hold the same <name>; 1 shown; ties are ordered by ...`."""

    order = _order_of(payload)
    tie = payload.get("tie_at_page_end")
    if order is None or not isinstance(tie, Mapping):
        return None
    first = int(tie.get("position") or 0)
    last = first + int(tie.get("groups_tied") or 0) - 1
    by = [str(field) for field in payload.get("by") or []]
    return (
        f"! positions {first}-{last} hold the same {order['name']}; "
        f"{int(tie.get('shown') or 0)} shown; ties are ordered by "
        + (", ".join(by) if by else "the group key")
    )


def partial_lines(payload: Mapping[str, Any]) -> list[str]:
    """What a ranking over a partial live read leaves out, one line each."""

    # A ranked payload always carries ranking_complete (ranked_facts sets it
    # with order_by), so a missing key fails loudly instead of reading as done.
    if _order_of(payload) is None or payload["ranking_complete"]:
        return []
    lines: list[str] = []
    not_read = payload.get("not_read")
    unread = int(not_read.get("count") or 0) if isinstance(not_read, Mapping) else 0
    comparable = payload.get("not_comparable")
    undecided = int(comparable.get("values") or 0) if isinstance(comparable, Mapping) else 0
    # A later page of a kept set: its first read's counts (review M2).
    # Where each count is shown: on this page's own block, or, when the count
    # comes only from the first read, in ranking_left_out.
    unread_in = "not_read"
    undecided_in = "not_comparable"
    carried = payload.get("ranking_left_out")
    if isinstance(carried, Mapping):
        if not unread:
            unread = int(carried.get("not_read") or 0)
            unread_in = "ranking_left_out"
        if not undecided:
            undecided = int(carried.get("not_comparable") or 0)
            undecided_in = "ranking_left_out"
    by = [str(field) for field in payload.get("by") or []]
    if unread or undecided:
        if by == ["node_id"]:
            ranked = int(payload.get("total_count") or 0)
            left = [f"{unread:,} not read"] if unread else []
            left += [f"{undecided:,} not comparable"] if undecided else []
            see = " and ".join(
                dict.fromkeys(
                    where for where, on in ((unread_in, unread), (undecided_in, undecided)) if on
                )
            )
            lines.append(
                f"! ranked over {ranked:,} of {ranked + unread + undecided:,} nodes; "
                f"{' and '.join(left)} could rank anywhere (see {see})"
            )
        else:
            parts = [f"{unread:,} {'node' if unread == 1 else 'nodes'} not read"] if unread else []
            parts += [f"{undecided:,} not comparable"] if undecided else []
            lines.append(
                f"! {' and '.join(parts)} {'is' if unread + undecided == 1 else 'are'} "
                "left out of every group; the counts and sums may be low"
            )
    cut = payload.get("ranking_cut")
    if cut:
        detail = "; ".join(str(c) for c in cut) if isinstance(cut, list) else str(cut)
        lines.append(f"! the listing was cut ({detail}); nodes past the cut are not ranked")
    return lines


def order_note_line(payload: Mapping[str, Any]) -> Optional[str]:
    """The text of :func:`order_note`."""

    note = payload.get("order_note")
    if not isinstance(note, Mapping) or not isinstance(note.get("rank_with"), str):
        return None
    rank_with = note["rank_with"]
    name = rank_with.split('"', 2)[1] if rank_with.count('"') >= 2 else rank_with
    if name.endswith(" desc"):
        name = name[: -len(" desc")]
    return (
        f"! groups are ordered by {note.get('ordered_by') or 'count, then key'}, "
        f"not by {name}; pass {rank_with} to rank by it"
    )
