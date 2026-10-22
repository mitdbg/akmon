"""The in-memory reader of a kept live answer, for live typed rows (#3666).

A kept answer holds rows in one of two shapes:

* **raw rows**, as the upstream system returned them. The reader wraps each
  as ``{"attrs": row}``. These keep the behavior they always had.
* **live typed rows**, ``{"node_id", "subtype", "attrs", "live"}`` with
  ``live = {"system", "observed_at", "fields": {<name>: state}}``. The state
  of a field is ``live``, ``absent`` (the call answered without the key),
  ``unavailable`` (the call failed) or ``denied`` (the reader may not see it).

What this module adds to ``filter``, ``map`` and ``aggregate`` over a kept
answer, and to the live-graph resolver's first read of a live type (the
resolver applies its conditions with :func:`select_rows`, so a condition
answers the same rows on the first call and on a later call over the kept
set):

* conditions, ``by`` and metrics on ``node_id`` and ``subtype``;
* a row whose needed field is ``unavailable`` or ``denied`` is never matched,
  never grouped, never put in ``(absent)``; it is counted as "not read" and
  its ids are listed;
* a key that no row carries is refused as a probable typo when the rows are
  the whole listing; on a narrowed set (a push-down, a condition, a cut) it
  is answered with a note naming it; a key the upstream call answered
  without (state `absent`) does not count as carried;
* ``order_by`` and the text operators ``contains``, ``prefix``, ``suffix``;
* one coercion rule for open values: a boolean condition reads upstream
  ``True``/``true``/``False``/``false`` as booleans, a numeric condition reads
  numeric text as a number, otherwise text compares with text. A value that
  cannot be read that way does not match and is counted as "not comparable".
"""
from __future__ import annotations

import difflib
import json
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from okg.substrate.mcp import live_result_sets as _lrs

#: States in which a field's value was not read at all.
NOT_READ_STATES = frozenset({"unavailable", "denied"})
#: Node ids a reply lists for rows not read; the count is always complete.
MAX_NOT_READ_IDS = 100
#: Keys named in a typo refusal.
KEYS_SHOWN = 12

_BOOL_TEXT = {"True": True, "true": True, "False": False, "false": False}


# -- row shape ----------------------------------------------------------------


def is_typed(row: Any) -> bool:
    """Whether a kept row is a live typed row."""

    return (
        isinstance(row, Mapping)
        and "node_id" in row and "subtype" in row and "live" in row
        and isinstance(row.get("attrs", {}), Mapping)
    )


def attrs_of(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """The upstream fields of a row of either shape."""

    return (row.get("attrs") or {}) if is_typed(row) else row


def evaluation_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """The row as the graph condition evaluator reads it."""

    return dict(row) if is_typed(row) else {"attrs": row}


def flat_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """A row as :func:`live_result_sets.group_report` reads it: upstream
    fields by name, record columns behind NUL."""

    if not is_typed(row):
        return row
    return {
        **(row.get("attrs") or {}),
        "\x00node_id": row["node_id"],
        "\x00subtype": row["subtype"],
    }


def field_state(row: Mapping[str, Any], key: str) -> str:
    """`live`, `absent`, `unavailable` or `denied` for one upstream field."""

    if is_typed(row):
        attrs = row.get("attrs") or {}
        states = (row.get("live") or {}).get("fields") or {}
        state = states.get(key)
        if state in NOT_READ_STATES:
            return state
        return "live" if key in attrs else "absent"
    return "live" if key in row else "absent"


def _has_key(row: Mapping[str, Any], key: str) -> bool:
    """Whether a row carries `key`: a value, or a state saying its value was
    not read (`unavailable`, `denied`). A key the call answered without
    (`absent`) is not carried: on every row, that is a probable typo."""

    if is_typed(row):
        if key in (row.get("attrs") or {}):
            return True
        states = (row.get("live") or {}).get("fields") or {}
        return states.get(key) in NOT_READ_STATES
    return key in row


def _carried_keys(row: Mapping[str, Any]) -> set[str]:
    """Every key a row carries, as :func:`_has_key` reads it."""

    keys = set(attrs_of(row))
    if is_typed(row):
        states = (row.get("live") or {}).get("fields") or {}
        keys.update(k for k, state in states.items() if state in NOT_READ_STATES)
    return keys


# -- what a call names ---------------------------------------------------------


def predicate_fields(predicate: Any) -> list[str]:
    """The field names (`attrs.<k>`, `node_id`, `subtype`) a condition tests."""

    out: list[str] = []
    stack = [predicate]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(reversed(node))
            continue
        if not isinstance(node, Mapping):
            continue
        name = node.get("field")
        if isinstance(name, str) and name not in out:
            out.append(name)
        stack.extend(v for k, v in node.items() if k in ("clause", "clauses"))
    return out


def _attr_key(name: str) -> Optional[str]:
    return name.split(".", 1)[1] if name.startswith("attrs.") else None


@dataclass
class Names:
    """What one call reads from the rows, by argument."""

    #: (argument, upstream field) pairs: where a typo would show.
    attrs: list[tuple[str, str]] = field(default_factory=list)
    #: Whether any argument names `node_id` or `subtype`.
    record: list[str] = field(default_factory=list)

    @property
    def keys(self) -> list[str]:
        return list(dict.fromkeys(key for _argument, key in self.attrs))


def names_of(
    predicate: Any = None,
    by: Sequence[str] = (),
    metrics: Sequence[tuple[str, Optional[str]]] = (),
) -> Names:
    names = Names()
    for name in predicate_fields(predicate):
        key = _attr_key(name)
        if key is not None:
            names.attrs.append(("predicate", key))
        elif name in _lrs.RECORD_COLUMNS:
            names.record.append("predicate")
    for name in by:
        key = _attr_key(name)
        if key is not None:
            names.attrs.append(("by", key))
        elif name in _lrs.RECORD_COLUMNS:
            names.record.append("by")
    for _function, key in metrics:
        if key is None:
            continue
        if _lrs.is_record_key(key):
            names.record.append("metrics")
        else:
            names.attrs.append(("metrics", key))
    return names


def projection_names(projection: Any) -> Names:
    """The record columns a `select`/`map` projection names, as `Names`."""

    names = Names()
    if isinstance(projection, Mapping):
        fields = list(projection.values())
    elif isinstance(projection, (list, tuple)):
        fields = list(projection)
    else:
        fields = []
    for name in fields:
        if isinstance(name, str) and name in _lrs.RECORD_COLUMNS:
            names.record.append("projection")
    return names


# -- refusals -------------------------------------------------------------------


def _refuse(argument: str, message: str, fix_hint: str, **data: Any) -> Exception:
    return _lrs.LiveResultPredicateUnsupported(
        message, data={"argument": argument, **data}, fix_hint=fix_hint,
    )


def check_record_columns(rows: Sequence[Mapping[str, Any]], names: Names, operator: str) -> None:
    """`node_id` and `subtype` exist on live typed rows only. A raw row has
    neither, so a clause on one would match nothing and read as "no such
    rows": refuse it by name."""

    if not names.record:
        return
    raw = sum(1 for row in rows if not is_typed(row))
    if raw:
        raise _refuse(
            names.record[0],
            f"{operator}: this kept answer holds {raw:,} raw rows with no "
            "node_id or subtype; those columns belong to live typed rows",
            "name the upstream field as attrs.<field>",
            reason="record_column_on_raw_rows",
        )


def _closest(key: str, ordered: Sequence[str]) -> list[str]:
    return difflib.get_close_matches(key, ordered, n=3, cutoff=0.5) or [
        name for name in ordered if name in key or key in name
    ][:3]


#: The fence source on upstream keys printed in a refusal, hint or note.
KEYS_SOURCE = "external_live:keys"


def served_key_names(keys: Iterable[Any]) -> list[str]:
    """Upstream keys as the agent sees them in a row's ``attrs``: neutralised
    the way :func:`live_result_sets.fence_rows` neutralises keys (no fence
    markers, control characters or line breaks; bounded length), duplicates
    dropped, order kept. Idempotent, so a key already served passes as is."""

    from okg.substrate.external_backed.resolver import _neutralise_key

    out: list[str] = []
    for key in keys:
        safe = _neutralise_key(key)
        if safe not in out:
            out.append(safe)
    return out


def fenced_key_list(keys: Iterable[Any], *, prefix: str = "") -> str:
    """Upstream keys for agent-facing text (a message, a fix hint, a note):
    neutralised by :func:`served_key_names`, joined, and wrapped as untrusted
    text, so a key the system sent reads as data, never as an instruction.
    ``"none"`` when there are none."""

    from okg.substrate.mcp.provenance import _fence_value

    names = [f"{prefix}{name}" for name in served_key_names(keys)]
    if not names:
        return "none"
    return _fence_value(", ".join(names), source=KEYS_SOURCE)


def keys_absent_note(entries: Mapping[str, Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """The reply's note for keys no row of a narrowed set carries.

    `entries`: ``attrs.<key>`` -> ``{"rows": <rows checked>, "closest":
    [<keys seen>]}``. None when there are none.
    """

    if not entries:
        return None
    parts = []
    keys: dict[str, dict[str, Any]] = {}
    for name, entry in entries.items():
        # The closest keys are upstream keys: served as `attrs` serves them,
        # and fenced in the note's text.
        close = served_key_names(entry.get("closest") or [])
        keys[name] = {**dict(entry), "closest": close}
        parts.append(
            f"{name} (none of {int(entry.get('rows') or 0):,} rows"
            + (f"; closest keys seen: {fenced_key_list(close)}" if close else "")
            + ")"
        )
    return {
        "keys": keys,
        "note": (
            "no row in this set carries " + "; ".join(parts) + ". The set is "
            "narrowed (by a condition, a push-down or a cut listing), so a real "
            "key may be absent from every row here: the answer stands as read; "
            "check the spelling"
        ),
    }


def merge_keys_absent(*notes: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """One note from several (the resolver's and the tool body's); the first
    entry for a key wins."""

    entries: dict[str, Mapping[str, Any]] = {}
    for note in notes:
        for name, entry in dict((note or {}).get("keys") or {}).items():
            entries.setdefault(str(name), entry)
    return keys_absent_note(entries)


def check_keys_seen(
    rows: Sequence[Mapping[str, Any]], names: Names, operator: str,
    *, known: Sequence[str] = (), whole: bool = True,
) -> Optional[dict[str, Any]]:
    """A key that no row read carries, on rows that are the whole listing,
    is a probable typo: refused with the closest keys seen, never served as
    an empty page or as `(absent)` for every row.

    `whole` says whether `rows` are the whole listing: no push-down, no
    condition dropped a row, no cut. When they are not, a key none of them
    carries may be a real one (most attributes sit on a few sites), so
    it is not refused: the note returned names it (:func:`keys_absent_note`)
    and the caller serves the answer with it. None when every key is seen.

    `known` are keys the type declares (its per-node fields, listing
    columns and push-down keys): never a typo, so never refused or noted
    here, and offered as close names beside the keys the rows carry.
    """

    if not rows:
        return None
    exempt = set(known)
    missing: dict[str, str] = {}
    for argument, key in names.attrs:
        if key in missing or key in exempt:
            continue
        if not any(_has_key(row, key) for row in rows):
            missing[key] = argument
    if not missing:
        return None
    seen: set[str] = set(exempt)
    for row in rows:
        seen.update(_carried_keys(row))
    ordered = sorted(seen)
    if not whole:
        return keys_absent_note({
            f"attrs.{key}": {"rows": len(rows), "closest": _closest(key, ordered)}
            for key in missing
        })
    key, argument = next(iter(missing.items()))
    close = _closest(key, ordered)
    # Upstream keys: in `data` as `attrs` serves them, in the message fenced,
    # and never in the fix hint (#3666 review M1).
    shown = served_key_names(close or ordered[:KEYS_SHOWN])
    raise _refuse(
        argument,
        f"{operator}: no row read has attrs.{key} "
        f"({len(rows):,} rows read); "
        f"{'closest keys seen' if close else 'keys seen'}: {fenced_key_list(shown)}",
        "name a key the rows carry, as attrs.<key>"
        + ("; data.closest lists the closest, as attrs serves them" if shown else ""),
        reason="key_absent_on_every_row",
        key=key,
        closest=list(shown),
    )


# -- field states ----------------------------------------------------------------


@dataclass
class NotRead:
    """Rows set aside because a field they were needed for was not read."""

    node_ids: list[str] = field(default_factory=list)
    count: int = 0
    #: field -> state -> rows
    fields: dict[str, Counter] = field(default_factory=dict)

    def add(self, row: Mapping[str, Any], problems: Sequence[tuple[str, str]]) -> None:
        self.count += 1
        if len(self.node_ids) < MAX_NOT_READ_IDS:
            self.node_ids.append(str(row.get("node_id")))
        for key, state in problems:
            self.fields.setdefault(key, Counter())[state] += 1

    def absorb(self, other: Optional[Mapping[str, Any]]) -> None:
        """Add the rows another reader set aside, as :meth:`data` gave them:
        the resolver's rows a condition could not be decided on, which never
        reach the tool body."""

        if not other:
            return
        self.count += int(other.get("count") or 0)
        room = MAX_NOT_READ_IDS - len(self.node_ids)
        self.node_ids.extend(str(i) for i in list(other.get("node_ids") or [])[:max(0, room)])
        for key, states in (other.get("fields") or {}).items():
            self.fields.setdefault(str(key), Counter()).update(dict(states))

    def data(self) -> dict[str, Any]:
        """The counts as plain data, for :meth:`absorb`."""

        return {
            "count": self.count,
            "node_ids": list(self.node_ids),
            "fields": {key: dict(states) for key, states in self.fields.items()},
        }

    def report(self) -> Optional[dict[str, Any]]:
        if not self.count:
            return None
        return {
            "count": self.count,
            "node_ids": list(self.node_ids),
            **({"node_ids_cut": True} if self.count > len(self.node_ids) else {}),
            "fields": {
                f"attrs.{key}": dict(states) for key, states in self.fields.items()
            },
            "note": (
                f"{self.count:,} rows were not read: a field this call needs was "
                "unavailable or denied. They are in no group, match no condition "
                "(not even under `not`) and are not counted as (absent)"
            ),
        }


def split_not_read(
    rows: Sequence[Mapping[str, Any]], keys: Sequence[str],
) -> tuple[list[Mapping[str, Any]], NotRead]:
    """Rows readable for every key in `keys`, and the rest as :class:`NotRead`.
    Raw rows are always readable."""

    not_read = NotRead()
    if not keys or not any(is_typed(row) for row in rows):
        return list(rows), not_read
    readable: list[Mapping[str, Any]] = []
    for row in rows:
        problems = (
            [(key, state) for key in keys if (state := field_state(row, key)) in NOT_READ_STATES]
            if is_typed(row) else []
        )
        if problems:
            not_read.add(row, problems)
        else:
            readable.append(row)
    return readable, not_read


# -- the coercion rule ---------------------------------------------------------------


def coerce(value: Any, expected: Any) -> Optional[tuple[Any, Any]]:
    """`(value, expected)` made comparable by the one rule, or None.

    * expected is a boolean: upstream `True`/`true`/`False`/`false` (text or
      boolean) become booleans;
    * expected is a number: numeric text becomes a number;
    * expected is text: text compares with text.
    """

    if isinstance(expected, bool):
        if isinstance(value, bool):
            return value, expected
        if isinstance(value, str) and value in _BOOL_TEXT:
            return _BOOL_TEXT[value], expected
        return None
    if isinstance(expected, (int, float)):
        number = _lrs._number(value)
        return None if number is None else (number, expected)
    if isinstance(expected, str):
        return (value, expected) if isinstance(value, str) else None
    return value, expected


def _order(value: Any, bound: Any) -> Optional[int]:
    """-1, 0, 1, or None when `value` cannot be read as `bound`'s kind."""

    if isinstance(bound, bool) or not isinstance(bound, (int, float, str)):
        return None
    pair = coerce(value, bound)
    if pair is None:
        return None
    left, right = pair
    return -1 if left < right else (1 if left > right else 0)


class Matcher:
    """The condition evaluator for live typed rows.

    One instance per call; :meth:`matches` returns whether a row matches and
    adds to :attr:`not_comparable` the fields whose value could not be read
    as the condition's value (counted once per row and field).
    """

    def __init__(self, predicate: Any) -> None:
        self.predicate = predicate
        self.not_comparable: Counter[str] = Counter()

    def matches(self, row: Mapping[str, Any]) -> bool:
        bad: set[str] = set()
        held = self._node(row, self.predicate, bad)
        self.not_comparable.update(bad)
        return held

    # -- one node -------------------------------------------------------------

    def _value(self, row: Mapping[str, Any], name: str) -> Any:
        if name in _lrs.RECORD_COLUMNS:
            return row.get(name)
        return (row.get("attrs") or {}).get(name.split(".", 1)[1])

    def _node(self, row: Mapping[str, Any], node: Any, bad: set[str]) -> bool:
        if node in (None, True) or node == {}:
            return True
        if not isinstance(node, Mapping):
            raise _refuse("predicate", "a predicate must be an object", "pass a predicate object")
        op = node.get("op")
        if op == "and":
            return all(self._node(row, c, bad) for c in node.get("clauses") or [])
        if op == "or":
            return any(self._node(row, c, bad) for c in node.get("clauses") or [])
        if op == "not":
            return not self._node(row, node.get("clause"), bad)
        name = str(node.get("field") or "")
        value = self._value(row, name)
        if op == "exists":
            if name in _lrs.RECORD_COLUMNS:
                return value is not None
            return name.split(".", 1)[1] in (row.get("attrs") or {})
        if op == "eq":
            return self._same(value, node.get("value"), name, bad)
        if op == "in":
            values = node.get("values")
            if not isinstance(values, list) or not values:
                raise _refuse("predicate", "in needs a non-empty values list", "pass values=[...]")
            if value is None:
                return any(v is None for v in values)
            if any(coerce(value, v) is not None for v in values):
                return any(self._pair_equal(value, v) for v in values)
            bad.add(name)
            return False
        if op in ("lt", "gt"):
            if value is None:
                return False
            order = _order(value, node.get("value"))
            if order is None:
                bad.add(name)
                return False
            return order < 0 if op == "lt" else order > 0
        if op == "between":
            bounds = node.get("values")
            if not isinstance(bounds, list) or len(bounds) != 2:
                raise _refuse("predicate", "between needs values [low, high]", "pass values=[low, high]")
            if value is None:
                return False
            low, high = _order(value, bounds[0]), _order(value, bounds[1])
            if low is None or high is None:
                bad.add(name)
                return False
            return low >= 0 and high <= 0
        if op in ("prefix", "suffix", "contains"):
            return self._text(op, value, node.get("value"), name, bad)
        raise _refuse(
            "predicate", f"a retained live answer does not support the predicate op {op!r}",
            "use and, or, not, eq, in, exists, lt, gt, between, contains, prefix or suffix",
        )

    def _pair_equal(self, value: Any, expected: Any) -> bool:
        pair = coerce(value, expected)
        return pair is not None and pair[0] == pair[1]

    def _same(self, value: Any, expected: Any, name: str, bad: set[str]) -> bool:
        if value is None:
            return expected is None
        if expected is None:
            return False
        pair = coerce(value, expected)
        if pair is None:
            bad.add(name)
            return False
        return pair[0] == pair[1]

    def _text(self, op: str, value: Any, needle: Any, name: str, bad: set[str]) -> bool:
        from okg.substrate.mcp.operator_sql import text_contains, text_operand_problem

        if not isinstance(needle, str) or text_operand_problem(needle) is not None:
            raise _refuse(
                "predicate", f"{op} needs a non-empty text value", f"pass a text value to {op}",
            )
        if value is None:
            return False
        if op == "contains":
            if isinstance(value, (str, list)):
                return text_contains(value, needle)
        elif isinstance(value, str):
            return value.startswith(needle) if op == "prefix" else value.endswith(needle)
        bad.add(name)
        return False


def filter_rows(
    rows: Sequence[Mapping[str, Any]],
    predicate: Any,
    raw_matches: Callable[[Mapping[str, Any]], bool],
) -> tuple[list[Mapping[str, Any]], Counter[str]]:
    """Rows meeting `predicate`, in order, and the fields (with rows) whose
    values could not be read as the condition's. Raw rows go through
    `raw_matches`, the graph evaluator as it always ran."""

    if predicate in (None, True, {}):
        return list(rows), Counter()
    matcher = Matcher(predicate)
    matched = [
        row for row in rows
        if (matcher.matches(row) if is_typed(row) else raw_matches(row))
    ]
    return matched, matcher.not_comparable


def select_rows(
    rows: Sequence[Mapping[str, Any]], predicate: Any, not_read: NotRead,
    not_comparable: Counter[str],
) -> tuple[list[Mapping[str, Any]], list[str]]:
    """Live typed rows meeting `predicate`, by the same rule a later call
    over the kept set applies (:func:`split_not_read`, then
    :class:`Matcher`), and the ids of the rows set aside as not read.

    A row whose field the condition needs was not read is added to
    `not_read` and kept out; values that could not be compared are added to
    `not_comparable`. The live-graph resolver's one evaluator.
    """

    if predicate in (None, True, {}):
        return list(rows), []
    keys = names_of(predicate).keys
    readable: list[Mapping[str, Any]] = []
    unread_ids: list[str] = []
    for row in rows:
        problems = [
            (key, state) for key in keys
            if (state := field_state(row, key)) in NOT_READ_STATES
        ]
        if problems:
            not_read.add(row, problems)
            unread_ids.append(str(row.get("node_id")))
        else:
            readable.append(row)
    matcher = Matcher(predicate)
    matched = [row for row in readable if matcher.matches(row)]
    not_comparable.update(matcher.not_comparable)
    return matched, unread_ids


def not_comparable_report(counter: Mapping[str, int]) -> Optional[dict[str, Any]]:
    total = sum(counter.values())
    if not total:
        return None
    return {
        "values": total,
        "fields": dict(counter),
        "note": (
            f"{total:,} values could not be read as the condition's type "
            "(boolean, number or text): they do not match"
        ),
    }


# -- order_by ----------------------------------------------------------------------


_ORDER_SUFFIX = re.compile(r"^(?P<field>.*\S)\s+(?P<way>asc|desc)$", re.IGNORECASE)


def parse_order_by(order_by: Any) -> tuple[str, bool]:
    """`(key, descending)` of `field`, `-field`, `field desc` or `field asc`
    where field is `attrs.<key>`, `node_id` or `subtype`."""

    if not isinstance(order_by, str) or not order_by.strip():
        raise _refuse("order_by", "order_by is a field name", "e.g. order_by='-attrs.<field>'")
    text, desc = order_by.strip(), False
    if text.startswith("-"):
        text, desc = text[1:].strip(), True
    else:
        suffix = _ORDER_SUFFIX.match(text)
        if suffix:
            text, desc = suffix.group("field"), suffix.group("way").lower() == "desc"
    return _lrs._condition_key(text, argument="order_by"), desc


def _rank(value: Any) -> tuple[int, Any]:
    if isinstance(value, bool):
        return 2, value
    if isinstance(value, (int, float)):
        return 0, value
    if isinstance(value, str):
        return 1, value
    return 3, json.dumps(value, sort_keys=True, default=str)


def order_rows(
    rows: Sequence[Mapping[str, Any]], key: str, desc: bool, operator: str,
) -> list[Mapping[str, Any]]:
    """Rows sorted by one field, equal values in upstream order; a missing,
    null or not-read value sorts last whichever way the sort runs."""

    record = _lrs.is_record_key(key)
    name = key[1:] if record else key
    if record and any(not is_typed(row) for row in rows):
        raise _refuse(
            "order_by",
            f"{operator}: this kept answer holds raw rows with no {name}",
            "name the upstream field as attrs.<field>", reason="record_column_on_raw_rows",
        )
    present: list[tuple[tuple[int, Any], Mapping[str, Any]]] = []
    missing: list[Mapping[str, Any]] = []
    for row in rows:
        if record:
            value = row.get(name)
        elif field_state(row, name) in NOT_READ_STATES:
            value = None
        else:
            value = attrs_of(row).get(name)
        if value is None:
            missing.append(row)
        else:
            present.append((_rank(value), row))
    present.sort(key=lambda item: item[0], reverse=desc)
    return [row for _rank_, row in present] + missing
