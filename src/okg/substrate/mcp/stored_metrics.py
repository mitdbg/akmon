"""Stored totals: sum, min and max over a stored set (PACT read-stored-metrics).

`aggregate` with `sum(attrs.<key>)`, `min(...)` or `max(...)` over a stored
scope (`where`/`input`) or a result reference (`input={"ref": "r1..."}`) of at
most :data:`MAX_ROWS` records. The server reads, in one statement at the
pinned generation, each needed key's value, its JSON type and whether the
record carries it; this module turns those columns into rows and computes the
groups with :func:`live_result_sets.group_report`, so a stored total and a
live total over the same values are the same answer:

* **one number rule** (:func:`live_result_sets._number`): a JSON number, or
  text that is exactly one JSON number;
* **one grouping rule**: numbers group by numeric value, read exactly here
  (a stored number becomes a ``Decimal`` from its SQL text, never a float);
* **nothing dropped silently**: each value left out is counted by class
  (`null`, `absent`, `not_a_number`, `number_text_rejected`) with its first
  ids, and the totals line says what each total covers.

The refusals, in their pinned order (review L4): access rules first (Jason
d388: nothing new is served while access rules are on), then an unknown type
(:func:`unknown_type`), then the declared-type check
(:func:`needs_number`). A set over :data:`MAX_ROWS` is refused after one
bounded count, before any value is read (:func:`too_large`); a call whose
statements do not finish inside one :data:`BUDGET_MS` budget is refused
naming the phase (:func:`over_budget`). Every refusal serves zero values.
"""
from __future__ import annotations

import difflib
import json
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Optional

from okg.substrate.mcp import live_result_sets as _lrs
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

#: The most records one stored sum, min or max reads (Jason d382).
MAX_ROWS = 50_000
#: One SQL budget per call, shared with a date condition's budget.
BUDGET_MS = 5_000
#: Below this much time left, no statement is started.
STATEMENT_FLOOR_MS = 100
#: The declared value types a sum, min or max does not read.
NOT_NUMBER_TYPES = ("text", "bool", "ts")

#: The served words of the access-rule refusal (Jason d388).
ACCESS_RULES_MESSAGE = (
    "aggregate: stored sums are not served while access rules are on "
    "(audit/enforce); this is a later change"
)
ACCESS_RULES_HINT = (
    'Still served: aggregate(metrics=["count"]), filter and search'
)


def refuse(reason: str, message: str, fix_hint: str, **data: Any) -> InvalidOperatorArguments:
    """A stored-total refusal, named by `reason`; zero values are served."""

    return InvalidOperatorArguments(message, data={"reason": reason, **data}, fix_hint=fix_hint)


def refuse_under_access_rules(server: Any) -> None:
    """Refuse a stored sum, min or max while access rules are on (audit or
    enforce) or their state cannot be told, before any graph read (Jason
    d388). Says why, and what is still served."""

    from okg.substrate.mcp.live_access_rules import access_rules_state

    rules = access_rules_state(server)
    if rules is False:
        return
    raise refuse(
        "stored_metric_under_access_rules",
        ACCESS_RULES_MESSAGE
        + ("" if rules else "; whether they are on cannot be told here"),
        ACCESS_RULES_HINT,
        access_rules="on" if rules else "unknown",
    )


def check_metrics(metrics: Sequence[Any]) -> list[tuple[str, Optional[str]]]:
    """The parsed metrics of a stored total: count, and sum, min or max of
    one `attrs.<key>`. A computed column (`calc.`) is a live handle's."""

    parsed = _lrs.check_metrics(list(metrics))
    for function, key in parsed:
        if key is not None and _lrs.is_record_key(key):
            raise refuse(
                "stored_metric_field",
                f"aggregate: {function}({_lrs.column_label(key)}) on a stored set reads "
                "a stored attribute, attrs.<key>",
                f'name a stored attribute, e.g. metrics=["{function}(attrs.<key>)"]',
            )
    return parsed


def check_by(by: Sequence[str]) -> list[str]:
    """`by` of a stored total: nothing (one total), `subtype` or
    `attrs.<key>`. A date period, `match` or `each()` is refused by name,
    offering the forms that work today."""

    from okg.substrate.mcp.match_keys import is_match_key

    fields = [str(field) for field in by]
    for field in fields:
        if field == "subtype" or (
            field.startswith("attrs.") and len(field) > 6 and "(" not in field
        ):
            continue
        kind = (
            "a text match" if is_match_key(field)
            else "a date period" if "(" in field and field.split("(", 1)[0] in (
                "day", "week", "month", "year",
            )
            else "a list element" if field.startswith("each(")
            else "this field"
        )
        raise refuse(
            "stored_metric_by",
            f"aggregate: sum, min and max over a stored set group by subtype or "
            f"attrs.<key>; {field} is {kind}",
            f'count it: by=["{field}"], metrics=["count"]; or sum by '
            'by=["attrs.<key>"] or by=[] (one total)',
            by=fields,
        )
    _lrs.check_by(fields)
    return fields


def unknown_type(
    tool: str, names: Sequence[str], known: Sequence[str], *, noun: str = "record type",
    example: str = 'where={"type": "%s"}',
) -> Optional[InvalidOperatorArguments]:
    """The refusal of a call naming a type that is neither a catalog
    subtype nor a live type (`known`), naming the closest; None when every
    name is known. Never called "a stored set": it is no set at all."""

    known_set = set(known)
    for name in names:
        if name in known_set:
            continue
        ordered = sorted(known_set)
        closest = difflib.get_close_matches(name, ordered, n=3, cutoff=0.5) or (
            difflib.get_close_matches(name, ordered, n=3, cutoff=0.0)
        )
        return refuse(
            "unknown_type",
            f'{tool}: no {noun} is named "{name}"'
            + (f"; closest: {', '.join(closest)}" if closest else ""),
            (f"name a {noun}, e.g. " + example % closest[0] if closest
             else "list the types with inspect(kind=\"graph\")"),
            type=name, closest=list(closest),
        )
    return None


def needs_number(
    parsed: Sequence[tuple[str, Optional[str]]], declared: Mapping[str, str],
) -> Optional[InvalidOperatorArguments]:
    """A sum, min or max of an attribute declared text, bool or ts: refused
    before any read, naming `number()` and what works today."""

    for function, key in parsed:
        if key is None:
            continue
        value_type = declared.get(key)
        if value_type in NOT_NUMBER_TYPES:
            return refuse(
                "stored_metric_needs_number",
                f"aggregate: {function}(attrs.{key}) reads numbers; attrs.{key} is "
                f"declared {value_type}",
                f'count its values: by=["attrs.{key}"], metrics=["count"]; reading text '
                "as numbers with number() on stored sets comes with stored handles",
                field=f"attrs.{key}", declared=value_type,
            )
    return None


def too_large(count: Optional[int], *, call: Mapping[str, Any]) -> InvalidOperatorArguments:
    """A set over :data:`MAX_ROWS`: refused after the bounded count, before
    any value is read. `count` is the exact count, or None when it did not
    finish inside the budget ("more than 50,000")."""

    said = f"{count:,}" if count is not None else f"more than {MAX_ROWS:,}"
    error = refuse(
        "stored_set_too_large",
        f"aggregate: sum, min and max read at most {MAX_ROWS:,} stored records; "
        f"this set holds {said}",
        'narrow where to fewer records, or count it with aggregate(metrics=["count"]), '
        "which has no record cap",
        records=count, limit=MAX_ROWS,
    )
    error.retry_call = {  # type: ignore[attr-defined]
        "tool": "aggregate", "mode": "corrected",
        "arguments": {**dict(call), "metrics": ["count"]},
    }
    return error


class Budget:
    """One SQL budget per call (:data:`BUDGET_MS`). A call that carries a
    date condition takes its time from that budget instead (`date_budget`),
    so the two are one budget, never two."""

    def __init__(self, started_at: float, date_budget: Any = None) -> None:
        self.started_at = started_at
        self.date_budget = date_budget
        #: (phase, statement_timeout ms) per statement sent.
        self.statements: list[tuple[str, int]] = []

    @property
    def total_ms(self) -> int:
        if self.date_budget is not None:
            return int(self.date_budget.total_ms)
        return int(BUDGET_MS)

    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self.started_at) * 1000)

    def remaining_ms(self) -> int:
        if self.date_budget is not None:
            return int(self.date_budget.remaining_ms())
        return int(BUDGET_MS) - self.elapsed_ms()


def over_budget(budget: Budget, phase: str) -> InvalidOperatorArguments:
    """The budget was spent in `phase`: refused with zero values, naming
    the phase and the time, with the narrowing fix."""

    return refuse(
        "stored_read_over_budget",
        f"aggregate: the stored total did not finish inside its {budget.total_ms:,} ms "
        f"SQL budget (phase: {phase}, {budget.elapsed_ms():,} ms elapsed); no value is served",
        'narrow where to fewer records, or count it with aggregate(metrics=["count"])',
        phase=phase, elapsed_ms=budget.elapsed_ms(), budget_ms=budget.total_ms,
    )


def decode(text: Optional[str], json_type: Optional[str]) -> Any:
    """A stored value from its SQL text and JSON type, exactly: a number as
    a ``Decimal`` of its stored text (never a float, review N2), text as
    itself, a list or object as JSON (numbers in them read exactly too)."""

    if json_type == "number":
        return Decimal(str(text))
    if json_type == "string":
        return text
    if json_type == "boolean":
        return text == "true"
    if json_type in ("array", "object"):
        return json.loads(str(text), parse_float=Decimal)
    return None


def rows_from(
    fetched: Sequence[Sequence[Any]], keys: Sequence[str],
) -> list[dict[str, Any]]:
    """Rows as :func:`live_result_sets.group_report` reads them, from the
    column read's `(node_id, subtype, (text, type, present) per key)`: a key
    the record does not carry is left out of its row (`absent`), apart
    from a stored null."""

    rows = []
    # In node-id order, so the first ids a skip names are the same every time
    # (review L4); the read itself has no ORDER BY.
    for record in sorted(fetched, key=lambda record: str(record[0])):
        row: dict[str, Any] = {"\x00node_id": record[0], "\x00subtype": record[1]}
        for index, key in enumerate(keys):
            text, json_type, present = record[2 + 3 * index: 5 + 3 * index]
            if present:
                row[key] = decode(text, json_type)
        rows.append(row)
    return rows


def totals_cover(
    report: _lrs.GroupReport,
    metrics: Sequence[tuple[str, Optional[str]]],
    *,
    generation: Any,
    skipped: Mapping[str, Any],
) -> str:
    """What each stored total covers, exactly (never "at least"):
    `sum(attrs.x) over N of M records at generation G; skipped: ...; Q read
    from text`, one clause per sum, min or max, joined by ` | `."""

    clauses = []
    for metric in metrics:
        function, key = metric
        if key is None:
            continue
        column = _lrs.column_label(key)
        clauses.append(
            f"{_lrs.metric_label(metric)} over {report.counted.get(column, 0):,} of "
            f"{report.rows:,} records at generation {generation}; "
            + _lrs.skipped_clause(skipped.get(column) or {})
        )
    return " | ".join(clauses)


__all__ = [
    "ACCESS_RULES_HINT",
    "ACCESS_RULES_MESSAGE",
    "BUDGET_MS",
    "Budget",
    "MAX_ROWS",
    "check_by",
    "check_metrics",
    "decode",
    "needs_number",
    "over_budget",
    "refuse",
    "refuse_under_access_rules",
    "rows_from",
    "too_large",
    "totals_cover",
    "unknown_type",
]
