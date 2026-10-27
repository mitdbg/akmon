"""One group per list element for ``aggregate`` (issue #3666).

``aggregate(by=["each(attrs.files)"])`` counts rows per element of a list
attribute: a row whose ``files`` is ``["a", "b"]`` counts once in the group
``"a"`` and once in the group ``"b"``. Plain ``by=["attrs.files"]`` keeps its
meaning, one group per whole list.

The rules, which the reply states in fixed fields:

* an element counts once per row, however often the row's list repeats it;
* a row whose value is missing, JSON null, or a list with no element other
  than null is in one group with a null key (``rows_with_none``);
* a row whose value is present and is not a list is in no group
  (``rows_not_a_list``); a call where no selected row holds a list is
  refused, with the plain grouping named;
* ``rows_counted`` is every row the call selected, and ``groups_overlap`` is
  always true: a row with n elements is in n groups, so the group counts do
  not add up to the rows.

The SQL lives in the aggregate group builder (``server``); this module is
the grammar, the reply facts and the refusals. It imports nothing from the
serving layer at import time, so the argument rewrite can read the grammar.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Optional

__all__ = [
    "COUNT_BUDGET_MS",
    "EACH_FORM",
    "EachField",
    "budget_refusal",
    "each_attribute",
    "facts",
    "is_each_shaped",
    "no_list_refusal",
    "parse_each_field",
    "run_within_budget",
]

#: How long one group statement over links or list elements may run. Past
#: it the call is refused, never answered with part of the count.
COUNT_BUDGET_MS = 5_000

#: The one spelling, as refusals name it.
EACH_FORM = "each(attrs.<key>)"

_OPEN, _CLOSE = "each(", ")"
_ATTRS = "attrs."


def each_attribute(text: Any) -> Optional[str]:
    """The attribute ``text`` counts per element, or None.

    None when ``text`` is not shaped ``each(...)`` at all, and when it is
    but does not name one attribute as ``attrs.<key>``. Pure.
    """
    if not isinstance(text, str) or not (text.startswith(_OPEN) and text.endswith(_CLOSE)):
        return None
    inner = text[len(_OPEN):-len(_CLOSE)]
    if inner.startswith(_ATTRS) and len(inner) > len(_ATTRS):
        return inner[len(_ATTRS):]
    return None


def is_each_shaped(text: Any) -> bool:
    """Whether ``text`` is written as a call of ``each``, well formed or not."""
    return isinstance(text, str) and text.startswith(_OPEN)


@dataclass(frozen=True)
class EachField:
    """One ``each(attrs.<key>)`` group field."""

    text: str
    attr: str


def parse_each_field(text: Any) -> Optional[EachField]:
    """The list field ``text`` names, None for any other field, or a refusal.

    Anything written as a call of ``each`` that does not name one attribute
    is refused by name with the form; it is never read as a date period.
    """
    if not is_each_shaped(text):
        return None
    attr = each_attribute(text)
    if attr is not None:
        return EachField(str(text), attr)
    from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

    raise InvalidOperatorArguments(
        f"aggregate: by field {text!r} is not a list field: each() takes one "
        "attribute",
        data={"field": text, "expected": EACH_FORM},
        fix_hint=f"write the field as {EACH_FORM}, e.g. by=['each(attrs.labels)']",
    )


def facts(
    *, rows_counted: int, rows_with_none: int, rows_not_a_list: int,
) -> dict[str, Any]:
    """The reply fields of a count per list element, under fixed names."""
    return {
        "rows_counted": rows_counted,
        "rows_with_none": rows_with_none,
        "rows_not_a_list": rows_not_a_list,
        "groups_overlap": True,
    }


def no_list_refusal(field: EachField, *, rows_not_a_list: int) -> Any:
    """No selected row holds a list in ``field``'s attribute."""
    from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

    return InvalidOperatorArguments(
        f"aggregate: attrs.{field.attr} holds no list in the selected rows "
        f"({rows_not_a_list} hold a single value); each() counts the "
        "elements of a list",
        data={
            "field": field.text,
            "reason": "not_a_list",
            "rows_not_a_list": rows_not_a_list,
        },
        fix_hint=f"group by the value itself: by=['attrs.{field.attr}']",
    )


def run_within_budget(
    conn: Any,
    sql: str,
    params: Mapping[str, Any],
    facts_sql: str = "",
    *,
    links: bool,
    budget_ms: Optional[int] = None,
    prepare: Optional[bool] = None,
) -> tuple[list[Any], Any]:
    """Run a group statement (and its facts statement, when given) under a
    statement timeout of ``budget_ms`` (default :data:`COUNT_BUDGET_MS`).
    ``prepare`` is psycopg's own option for both statements: the graph read
    path (PACT lane-s-storage-redesign-s2) passes False so each is planned
    with its parameters; None keeps psycopg's default.

    Returns the group rows and the facts row (None without one). The
    timeout bounds each statement on its own, so the group statement and the
    facts statement each get up to the budget. A statement cancelled at the
    limit is refused by name (``budget_refusal``) and its savepoint rolled
    back, so the connection stays usable; a cancel for any other reason (a
    client or an administrator) is raised as it came. After a finished read
    the session's timeout is put back as it was.
    """
    import psycopg

    budget = COUNT_BUDGET_MS if budget_ms is None else int(budget_ms)
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('statement_timeout')")
                previous = cur.fetchone()[0]
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)", (f"{budget}ms",),
                )
                cur.execute(sql, dict(params), prepare=prepare)
                fetched = cur.fetchall()
                counted = None
                if facts_sql:
                    cur.execute(facts_sql, dict(params), prepare=prepare)
                    counted = cur.fetchone()
                cur.execute("SELECT set_config('statement_timeout', %s, true)", (previous,))
    except psycopg.errors.QueryCanceled as cancelled:
        if not _timed_out(cancelled):
            raise
        raise budget_refusal(links=links, budget_ms=budget) from None
    return fetched, counted


def _timed_out(cancelled: BaseException) -> bool:
    """Whether PostgreSQL cancelled the statement for its statement_timeout,
    not at a client's or an administrator's request (the same SQLSTATE)."""
    from okg.substrate.mcp.record_text import is_statement_timeout

    if not is_statement_timeout(cancelled):
        return False
    diag = getattr(cancelled, "diag", None)
    primary = getattr(diag, "message_primary", None) or str(cancelled)
    return "statement timeout" in primary


def budget_refusal(*, links: bool, budget_ms: int = COUNT_BUDGET_MS) -> Any:
    """The count did not finish within :data:`COUNT_BUDGET_MS`."""
    from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

    what = "these links" if links else "these list elements"
    return InvalidOperatorArguments(
        f"aggregate: counting {what} did not finish in {budget_ms / 1000:g} s",
        data={"reason": "count_time_budget", "budget_ms": budget_ms},
        fix_hint=(
            "add a condition on the links (edge_type, src, dst or an "
            "attribute) so fewer are counted"
            if links else
            "narrow the set with a type or a condition so fewer rows are "
            "counted"
        ),
    )
