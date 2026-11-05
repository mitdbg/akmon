"""`run`'s steps that hold rows (#3666, lane ri).

A graph step's records stay in the database as SQL; these steps' rows are
held by the run itself, in its own memory, for the run's later steps, and
dropped when the run answers. Nothing here keeps an answer past its run:
no kept-answer id, no reply shaping between steps.

* A live step is ``{"expand": {"input": <record>, "live": {"relation": R,
  "args": {...}, "each": {"<arg>": "$n.<column>"}}}}``. Without ``each`` it
  is one call; with it, one call per distinct non-null value of that column
  of step n's rows (or of a list). It is made in three phases: prepared by
  ``expand``'s own call on a call worker (the principal gate, the session's
  tool surface, the argument model, the trace and the call log), its calls
  made on the wait-only executor (no database), its call sample written on
  a call worker. Each value's outcome is counted; a step without ``each``
  whose one call gave no answer is refused, so a step reading it skips.
* ``{"values": {"<column>": [...]}}`` lists rows.
* ``{"minus" | "intersect": ["$a", "$b", ...], "on": "<column>"}`` keeps
  the rows of the first step whose column value is in none (minus) or all
  (intersect) of the others; the first row per value is kept and the rows
  dropped as duplicates are counted; a row without the column is left out
  and counted.
* ``filter``, ``map`` and ``aggregate`` with ``"input": "$n"`` read step
  n's rows: a condition on ``attrs.<column>`` (or the bare column), and
  ``count``, ``sum``, ``min``, ``max``; sums of integers are exact.
* ``filter``, ``map`` and ``aggregate`` over a kept live answer
  (``"input": {"ref": "l1..."}``, lane re) are sent as their own calls; a
  later step reads the rows of the set the reply's handle names
  (``kept_rows``). A live step's ``each`` may name a kept answer's column,
  ``{"ref": "l1...", "field": "<column>"}``, which expand's own call reads.
"""

from __future__ import annotations

import asyncio
import difflib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import live_result_sets as _lrs
from okg.substrate.mcp import run_steps

__all__ = ["HeldRows", "RunLiveState", "live_step", "rows_step", "set_step", "values_step"]

#: Missing values a reply names: text, and the JSON form.
MISSING_SHOWN = 10
MISSING_SHOWN_JSON = 100
#: Columns a step's head names.
COLUMNS_SHOWN = 12
#: The fence source of a values step's rows.
VALUES_SOURCE = "run_values"
#: How each outcome reads in a reply.
_OUTCOME_WORDS = {
    "ok": "ok", "empty": "empty", "not_found": "not found", "failed": "failed",
    "cut": "cut by the system's own limit", "not_started": "not started",
}


@dataclass
class RunLiveState:
    """What a run's live steps share: its graph version and authority, its
    wall clock, the stop flag every value checks, the upstream calls made,
    and the reader whose one fan-out run this is."""

    generation: Any
    authority: Any
    wall_deadline: float
    stop: threading.Event = field(default_factory=threading.Event)
    upstream_calls: int = 0
    #: (reader, logical session) of this run's one fan-out claim.
    heavy_reader: Optional[tuple[str, Optional[str]]] = None


@dataclass
class HeldRows:
    """One step's rows, held by the run, and what its reply says."""

    rows: list[dict[str, Any]]
    #: The fence source of each row: ``external_live:<relation>`` or
    #: ``run_values``. A row keeps its source through set steps.
    sources: list[str]
    #: The step's head, after ``[step n]``.
    head: str
    #: Notice lines under the head.
    notes: list[str] = field(default_factory=list)
    #: The JSON facts of the step.
    facts: dict[str, Any] = field(default_factory=dict)
    #: Whether some value of this step, or of a step it reads, was not read.
    partial: bool = False
    #: What a total over these rows covers.
    cover: str = ""
    #: An aggregate's groups, as served rows (fenced); None otherwise.
    groups: Optional[list[dict[str, Any]]] = None
    #: The fence source of an aggregate's groups.
    group_source: str = ""
    #: An aggregate's `by` columns as its rows hold them (bare) -> as its
    #: reply prints them (`attrs.site`); None for any other step.
    group_labels: Optional[dict[str, str]] = None
    #: An aggregate's `by` columns named like one of its metrics or `rank`
    #: (review M-B): a later step cannot read these rows; None otherwise.
    collisions: Optional[list[str]] = None

    #: The columns a map with compute prints (its `select`, or the fields
    #: its expressions read and its new columns); None prints every column.
    print_columns: Optional[list[str]] = None

    def columns(self) -> list[str]:
        """Every column any row carries, in first-seen order; a computed
        column as `calc.<name>`."""
        from okg.substrate.mcp import map_compute as _mc

        seen: dict[str, None] = {}
        for row in self.rows:
            for key in row:
                if isinstance(key, str) and key.startswith(_mc.MARK):
                    if key.startswith(_mc.VALUE_PREFIX):
                        seen.setdefault("calc." + key[len(_mc.VALUE_PREFIX):], None)
                    continue
                seen.setdefault(key, None)
        return list(seen)

    def printed(self) -> list[dict[str, Any]]:
        """The rows the step prints: its groups, or its rows."""
        return self.groups if self.groups is not None else self.rows


def _columns_text(columns: list[str]) -> str:
    if not columns:
        return "no columns"
    shown = ", ".join(columns[:COLUMNS_SHOWN])
    more = len(columns) - COLUMNS_SHOWN
    return f"columns {shown}" + (f", +{more} more" if more > 0 else "")


def _plural(n: int, word: str) -> str:
    return f"{n:,} {word}" + ("" if n == 1 else "s")


def _value_text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, default=str)


def _args_text(query: Mapping[str, Any]) -> str:
    return ", ".join(f"{key}={_value_text(value)}" for key, value in query.items())


def _unknown_column(step: run_steps.Step, where: str, column: str, held: HeldRows, n: int) -> SubstrateError:
    columns = held.columns()
    close = difflib.get_close_matches(column, columns, n=1, cutoff=0.5) or [
        name for name in columns if name in column or column in name
    ][:1]
    hint = f"did you mean {close[0]}? " if close else ""
    return run_steps.RunLiveStepRefused(
        f"run: step {step.index} {where}: no row of step {n} has {column!r}; "
        f"its rows carry {', '.join(columns[:COLUMNS_SHOWN]) or 'no column'}",
        fix_hint=f"{hint}name a column step {n}'s rows carry",
        data={"step": step.index, "column": column, "columns": columns[:50]},
    ) if held.rows else run_steps.RunLiveStepRefused(
        f"run: step {step.index} {where}: step {n} has no rows to read {column!r} from",
        fix_hint=f"check step {n}: it holds no rows",
        data={"step": step.index, "column": column, "columns": []},
    )


def _distinct(values: list[Any]) -> list[Any]:
    """Values once each, JSON-equal values the same ("1" apart from 1)."""
    seen: set[Any] = set()
    out: list[Any] = []
    for value in values:
        key = run_steps.canonical(value)
        if key not in seen:
            seen.add(key)
            out.append(value)
    return out


def column_values(held: HeldRows, column: str) -> tuple[list[Any], int]:
    """The distinct non-null values of `column`, in row order, and how many
    rows have none (the column absent, or null)."""
    found: list[Any] = []
    without = 0
    for row in held.rows:
        value = cell(row, column)
        if value is None:
            without += 1
        else:
            found.append(value)
    return _distinct(found), without


# --------------------------------------------------------------------------- #
# values, minus, intersect                                                    #
# --------------------------------------------------------------------------- #


def values_step(step: run_steps.Step) -> HeldRows:
    column = step.arguments["column"]
    values = step.arguments["values"]
    rows = [{column: value} for value in values]
    return HeldRows(
        rows=rows, sources=[VALUES_SOURCE] * len(rows),
        head=f"values {column}: {_plural(len(rows), 'row')}",
        facts={"kind": "values", "rows": len(rows), "columns": [column]},
        cover=f"a values step ({_plural(len(rows), 'value')})",
    )


def set_step(step: run_steps.Step, inputs: list[tuple[int, HeldRows]]) -> HeldRows:
    """minus or intersect of the first input's rows against the others."""
    operator = step.operator
    columns = step.arguments["on"]
    for number, held in inputs:
        check_readable(held, number, step)
    for (number, held), column in zip(inputs, columns):
        if held.rows and all(not has_column(row, column) for row in held.rows):
            raise _unknown_column(step, "on", column, held, number)
    (first_n, first), *others = inputs
    first_column = columns[0]
    other_keys = []
    for (_n, held), column in zip(others, columns[1:]):
        other_keys.append({
            run_steps.canonical(cell(row, column)) for row in held.rows
            if cell(row, column) is not None
        })
    kept_rows: list[dict[str, Any]] = []
    kept_sources: list[str] = []
    seen: set[Any] = set()
    removed = duplicates = without = 0
    for row, source in zip(first.rows, first.sources):
        value = cell(row, first_column)
        if value is None:
            without += 1
            continue
        key = run_steps.canonical(value)
        inside = [key in keys for keys in other_keys]
        keep = not any(inside) if operator == "minus" else all(inside)
        if not keep:
            removed += 1
            continue
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        kept_rows.append(row)
        kept_sources.append(source)
    sign = " − " if operator == "minus" else " ∩ "
    named = sign.join(f"${n}" for n, _held in inputs)
    on = first_column if len(set(columns)) == 1 else ", ".join(columns)
    why = "found in another step" if operator == "minus" else "not in every other step"
    head = (
        f"{operator} {named} on {on}: {_plural(len(kept_rows), 'row')} (from "
        f"{_plural(len(first.rows), 'row')} of step {first_n}: {removed:,} {why}, "
        f"{duplicates:,} duplicates of a kept {first_column} dropped, "
        f"{without:,} without {first_column})"
    )
    partial = any(held.partial for _n, held in inputs)
    notes = []
    if first.partial:
        notes.append(
            f"! partial: step {first_n} did not read every value; its rows of "
            "the values not read are missing here"
        )
    for number, held in others:
        if held.partial:
            # A short later input errs the other way (review M7): a minus
            # keeps rows it would have removed, an intersect drops rows it
            # would have kept.
            notes.append(
                f"! partial: step {number} did not read every value; rows it "
                + ("would have removed may remain" if operator == "minus"
                   else "would have kept may be missing")
            )
    return HeldRows(
        rows=kept_rows, sources=kept_sources, head=head, notes=notes,
        facts={
            "kind": "set", "operator": operator,
            "inputs": [n for n, _held in inputs], "on": columns,
            "rows": len(kept_rows), "from_rows": len(first.rows),
            "removed": removed, "duplicates_dropped": duplicates,
            "without_key": without, "partial": partial,
        },
        partial=partial,
        cover=f"{operator} of steps {', '.join(str(n) for n, _h in inputs)} on {on}",
    )


# --------------------------------------------------------------------------- #
# filter, map, aggregate over held rows                                       #
# --------------------------------------------------------------------------- #

_PREDICATE_KEYS = frozenset({"op", "field", "value", "values", "clause", "clauses"})
#: The input a condition naming a computed column is rewritten against: a
#: live handle's shape (never opened; only the grammar reads it).
LIVE_REF = "l1.run"


def _attrs_field(name: Any) -> Any:
    from okg.substrate.mcp.map_compute import calc_name

    # A computed column (`calc.<name>`) is read as it is named.
    if isinstance(name, str) and name and not name.startswith("attrs.") and calc_name(name) is None:
        return f"attrs.{name}"
    return name


# --------------------------------------------------------------------------- #
# computed columns in held rows (PACT read-run-rows-compute)                  #
# --------------------------------------------------------------------------- #
#
# A held row keeps a computed column as map(compute) keeps it in a live
# handle (map_compute.VALUE_PREFIX and MARK): its value under a NUL key, and
# its state in the row's mark. So a later step's condition, `by` and metric
# read it through the same live readers, and it travels with its row through
# filter, minus and intersect. A row is printed with `calc.<name>` columns.


def cell(row: Mapping[str, Any], column: str) -> Any:
    """A held row's value of `column`: a computed column's value only when
    its state is ok, else None (it is never read as a value)."""
    from okg.substrate.mcp import map_compute as _mc

    name = _mc.calc_name(column)
    if name is None:
        return row.get(column)
    if _mc.MARK not in row or _mc.state_of(row, name) != _mc.OK:
        return None
    return row.get(_mc.value_key(name))


def has_column(row: Mapping[str, Any], column: str) -> bool:
    from okg.substrate.mcp import map_compute as _mc

    name = _mc.calc_name(column)
    if name is None:
        return column in row
    return _mc.MARK in row and _mc.value_key(name) in row


def shown_row(row: Any, columns: Optional[list[str]] = None) -> Any:
    """A held row as a reply prints it: computed columns as `calc.<name>`,
    the mark dropped; only `columns` when given."""
    from okg.substrate.mcp import map_compute as _mc

    if not isinstance(row, Mapping):
        return row
    if _mc.MARK not in row and columns is None:
        return row
    out: dict[str, Any] = {}
    for key, value in row.items():
        if not isinstance(key, str) or not key.startswith(_mc.MARK):
            out[key] = value
        elif key.startswith(_mc.VALUE_PREFIX):
            out["calc." + key[len(_mc.VALUE_PREFIX):]] = value
    if columns is not None:
        out = {c: out[c] for c in columns if c in out}
    return out


def condition_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A held row as a condition reads it: its fields under `attrs`, its
    computed columns as `calc.<name>` beside them."""
    from okg.substrate.mcp import map_compute as _mc

    if _mc.MARK not in row:
        return {"attrs": row}
    return {
        "attrs": _mc.strip(row),
        **{"calc." + k[len(_mc.VALUE_PREFIX):]: v for k, v in row.items()
           if isinstance(k, str) and k.startswith(_mc.VALUE_PREFIX)},
    }


def _attrs_names(node: Any) -> Any:
    """A condition with every column named as ``attrs.<column>``: held rows
    are read as kept answers are (``live_result_sets.check_predicate``)."""
    if isinstance(node, list):
        return [_attrs_names(child) for child in node]
    if not isinstance(node, Mapping):
        return node
    if "field" in node or node.get("op") in ("and", "or", "not"):
        out = dict(node)
        if "field" in out:
            out["field"] = _attrs_field(out["field"])
        if "clauses" in out:
            out["clauses"] = _attrs_names(out["clauses"])
        if "clause" in out:
            out["clause"] = _attrs_names(out["clause"])
        return out
    return {
        (key if key in _PREDICATE_KEYS else _attrs_field(key)): value
        for key, value in node.items()
    }


def _predicate(step: run_steps.Step) -> Any:
    """The step's condition as a predicate over held rows, or None."""
    from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call

    args = step.arguments
    where = args.get("where", args.get("predicate"))
    if "where" in args and "predicate" in args:
        raise run_steps.RunRefusal(f"run: step {step.index}: give where or predicate, not both")
    if where in (None, {}, True):
        return None
    from okg.substrate.mcp.map_compute import mentions_calc

    # A condition naming a computed column is read as one over a live
    # handle's rows, where the grammar takes `calc.<name>` (PACT
    # read-run-rows-compute); any other condition as before.
    ref = LIVE_REF if mentions_calc(where) else "run"
    try:
        rewritten = rewrite_call(
            "filter", {"input": {"ref": ref}, "where": _attrs_names(where)},
        ).arguments
    except GrammarRefusal as exc:
        raise run_steps.RunRefusal(
            f"run: step {step.index} where: {exc}", getattr(exc, "fix_hint", "") or "",
        ) from exc
    predicate = rewritten.get("predicate") if isinstance(rewritten, Mapping) else None
    _lrs.check_predicate(predicate)
    return predicate


#: What a step over held rows takes besides its input and condition.
_ROWS_ARGUMENTS = {
    "filter": frozenset({"input", "where", "predicate", "limit", "select"}),
    "map": frozenset({"input", "where", "predicate", "limit", "select", "projection", "compute"}),
    "aggregate": frozenset({"input", "where", "predicate", "limit", "by", "metrics", "order_by"}),
}


def _metric(item: Any) -> Any:
    """`sum(<column>)` as `sum(attrs.<column>)`; anything else as given."""
    if isinstance(item, str) and "(" in item and item.endswith(")"):
        function, _sep, inner = item[:-1].partition("(")
        return f"{function.strip()}({_attrs_field(inner.strip())})"
    return item


@dataclass
class _Names:
    """A step's arguments with `calc.` names checked against the computed
    columns of the rows it reads, and the columns whose state it needs."""

    predicate: Any
    select: Any
    by: Any
    metrics: Any
    order_by: Any
    needed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _calc_names(step: run_steps.Step, source: HeldRows, predicate: Any) -> _Names:
    """`calc.<name>` in a step over held rows (PACT read-run-rows-compute),
    checked as a later read of a map handle checks it: an unknown name is
    refused calc_column_unknown, a bare name equal to a computed column in
    `by`, `metrics`, `order_by` or `select` reads it (adjusted)."""
    from okg.substrate.mcp import map_compute as _mc

    args = step.arguments
    # A map that computes checks its `select` once its new columns exist.
    select = None if "compute" in args else args.get("select", args.get("projection"))
    out = _Names(
        predicate=predicate, select=select, by=args.get("by"),
        metrics=args.get("metrics"), order_by=args.get("order_by"),
    )
    # A key that starts with NUL and "calc" is a computed column only on a
    # row carrying the computed mark; an upstream row with such a key is
    # refused by name, as map(compute) refuses it (review L1), never read
    # as a computed column that matches nothing.
    taken = sum(
        1 for row in source.rows if _mc.MARK not in row and any(_mc.is_reserved(k) for k in row)
    )
    calc = _mc.columns_of(None, source.rows) if source.rows and _mc.MARK in source.rows[0] else ()
    mentions = _mc.mentions_calc(predicate, select, out.by, out.metrics, out.order_by)
    if taken and (mentions or calc):
        raise _mc.refuse(
            "reserved_key",
            f"run: step {step.index}: {taken:,} rows of the step it reads carry a key "
            "that starts with a NUL and 'calc', which is where computed columns are kept",
            "read those rows without computed columns; they cannot hold any",
            keys=taken,
        )
    if not calc and not mentions:
        return out
    resolved = _mc.resolve_names(
        step.operator, calc, predicate=predicate,
        projection=select if isinstance(select, list) else None,
        order_by=out.order_by, by=out.by, metrics=out.metrics,
        rows_known=bool(source.rows),
    )
    out.predicate = resolved.predicate
    if isinstance(select, list):
        out.select = resolved.projection
    if isinstance(out.by, list):
        out.by = resolved.by
    if isinstance(out.metrics, list):
        out.metrics = resolved.metrics
    if resolved.order_by is not None:
        out.order_by = resolved.order_by
    for argument in ("predicate", "by", "metrics"):
        out.needed += resolved.read.get(argument, [])
    out.notes = [
        f"! adjusted: {note['sent']} read as {note['served']}" for note in resolved.notes
    ]
    return out


def _set_aside(
    pairs: list[tuple[dict[str, Any], str]], needed: list[str],
) -> tuple[list[tuple[dict[str, Any], str]], dict[str, Any]]:
    """Rows with a value in every computed column `needed`, and a report of
    the rest (map_compute.set_aside's rule: never matched, grouped or
    counted, not even under `not`)."""
    from okg.substrate.mcp import map_compute as _mc

    if not needed:
        return pairs, {}
    kept, aside = _mc.set_aside([row for row, _src in pairs], needed)
    if not aside.count:
        return pairs, {}
    keep_ids = {id(row) for row in kept}
    report = aside.report() or {}
    report["note"] = (
        f"{aside.count:,} rows have no value in a computed column this step needs "
        "(not read, or an error): they match no condition (not even under `not`), "
        "are in no group and are not counted"
    )
    return [(row, src) for row, src in pairs if id(row) in keep_ids], report


def _aside_line(report: Mapping[str, Any]) -> str:
    states = "; ".join(
        f"{column}: " + ", ".join(f"{n:,} {state}" for state, n in counts.items())
        for column, counts in (report.get("columns") or {}).items()
    )
    return f"! {report['note']} ({states})"


def _project(row: Mapping[str, Any], columns: list[str]) -> dict[str, Any]:
    """A held row narrowed to `columns`; a computed column keeps its value
    key and the row its mark."""
    from okg.substrate.mcp import map_compute as _mc

    out: dict[str, Any] = {}
    for column in columns:
        name = _mc.calc_name(column)
        key = _mc.value_key(name) if name is not None else column
        if key in row:
            out[key] = row[key]
            if name is not None and _mc.MARK in row:
                out[_mc.MARK] = row[_mc.MARK]
    return out


def rows_step(
    step: run_steps.Step, number: int, source: HeldRows,
    matches: Callable[[dict[str, Any], Any], bool],
    *, deadline: Optional[float] = None,
) -> HeldRows:
    """filter, map or aggregate over step `number`'s held rows; a map with
    `compute` adds computed columns (PACT read-run-rows-compute). `deadline`
    (the run's wall clock, monotonic) bounds the computing."""
    operator = step.operator
    unknown = sorted(set(step.arguments) - _ROWS_ARGUMENTS[operator])
    if unknown:
        raise run_steps.RunRefusal(
            f"run: step {step.index}: {operator} over the rows of step {number} "
            f"does not take {', '.join(unknown)}",
            f"it takes {', '.join(sorted(_ROWS_ARGUMENTS[operator]))}",
        )
    check_readable(source, number, step)
    predicate = _predicate(step)
    names = _calc_names(step, source, predicate)
    predicate = names.predicate
    pairs = list(zip(source.rows, source.sources))
    pairs, aside = _set_aside(pairs, names.needed)
    if predicate is not None:
        pairs = [(row, src) for row, src in pairs if matches(row, predicate)]
    rows = [row for row, _src in pairs]
    sources = [src for _row, src in pairs]
    notes: list[str] = list(names.notes)
    if source.partial:
        notes.append(
            f"! PARTIAL: step {number} did not read every value; this step "
            "covers only the values read"
        )
    if aside:
        notes.append(_aside_line(aside))
    if operator == "aggregate":
        held = _aggregate(step, number, source, rows, sources, notes, names, aside)
        return held
    if operator == "map" and "compute" in step.arguments:
        return _compute_step(
            step, number, source, rows, sources, notes, names, aside,
            narrowed=predicate is not None, deadline=deadline,
        )
    selected = names.select
    if operator == "map" or selected is not None:
        if not (isinstance(selected, list) and selected and all(isinstance(n, str) for n in selected)):
            raise run_steps.RunRefusal(
                f"run: step {step.index}: {operator} over rows takes select, a list of columns",
                'e.g. "select": ["<column>"]',
            )
        columns = [run_steps.row_column(n) for n in selected]
        for column in columns:
            if source.rows and all(not has_column(row, column) for row in source.rows):
                raise _unknown_column(step, "select", column, source, number)
        rows = [_project(row, columns) for row in rows]
    total = len(source.rows)
    head = (
        f"{operator} over ${number}: {len(rows):,} of {_plural(total, 'row')}"
        if predicate is not None else f"{operator} over ${number}: {_plural(len(rows), 'row')}"
    )
    held = HeldRows(
        rows=rows, sources=sources, head=head, notes=notes,
        facts={"kind": "rows", "operator": operator, "input": number,
               "rows": len(rows), "from_rows": total, "partial": source.partial},
        partial=source.partial,
        cover=f"{operator} over step {number} ({source.cover})",
    )
    held.head += " · " + _columns_text(held.columns())
    held.facts["columns"] = held.columns()[:50]
    if aside:
        held.facts["not_computed"] = aside
    return held


def _compute_step(
    step: run_steps.Step, number: int, source: HeldRows,
    rows: list[dict[str, Any]], sources: list[str], notes: list[str],
    names: _Names, aside: Mapping[str, Any], *, narrowed: bool, deadline: Optional[float],
) -> HeldRows:
    """map(compute) over step `number`'s held rows (PACT
    read-run-rows-compute): map_compute's program, refusals, states and
    bounds, over rows the run already holds. No upstream call, no graph
    read, no handle. An aggregate step's groups are read as a groups
    handle's are (a metric by its label, any column as col['...'])."""
    import time as _time

    from okg.substrate.mcp import map_compute as _mc
    from okg.substrate.mcp import map_expressions as _expr

    compute = step.arguments["compute"]
    if not isinstance(compute, Mapping):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: compute names each new column and its expression",
            'e.g. "compute": {"<name>": "<expression>"}',
        )
    select = step.arguments.get("select", step.arguments.get("projection"))
    if select is not None and not (
        isinstance(select, list) and select and all(isinstance(n, str) for n in select)
    ):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: select is a list of the columns to print",
            'e.g. "select": ["<column>", "calc.<name>"]',
        )
    groups_in = source.groups is not None
    payload: Any = None
    group_columns: list[str] = []
    every, evaluated = source.rows, rows
    if groups_in:
        # The groups' raw values under the columns their reply printed
        # (`attrs.site`, `count`, `sum(attrs.size)`, `rank`), as a groups
        # handle holds them outside run.
        labels = dict(source.group_labels or {})
        held_columns = source.columns()
        group_columns = [labels.get(c, c) for c in held_columns]

        def raw(row: Mapping[str, Any], column: str) -> Any:
            # A computed column's value as held, whatever its state: the
            # row's mark, carried below, says whether it is a value (review
            # H1: a state read as an ok null turned an error into a value).
            name = _mc.calc_name(column)
            return row.get(_mc.value_key(name)) if name is not None else row.get(column)

        def labelled(held_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {labels.get(c, c): raw(row, c) for c in held_columns} for row in held_rows
            ]

        payload = _lrs.groups_set(
            labelled(source.rows), columns=group_columns, source=None, from_id=None,
            order="input order",
        )

        def keyed(held_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for row in held_rows:
                group = {payload.key_of(labels.get(c, c)): raw(row, c) for c in held_columns}
                if _mc.MARK in row:
                    # Per-row states, so map_compute reads an error as one.
                    group[_mc.MARK] = row[_mc.MARK]
                out.append(group)
            return out

        every, evaluated = keyed(source.rows), keyed(rows)
    program = _mc.compile_for(compute, payload, every)
    _mc.refuse_operands(program, groups=groups_in, typed=False)
    _mc.refuse_shadowing(compute, _mc.upstream_names(payload, every))
    _mc.check_operands_held(program, payload, every)
    _mc.check_select(select, program)
    seconds = _expr.EVAL_SECONDS
    run_left: Optional[float] = None
    if deadline is not None:
        run_left = max(0.0, deadline - _time.monotonic())
        seconds = min(seconds, run_left)
    try:
        computed = _mc.computed_rows(program, payload, evaluated, seconds=seconds)
    except _expr.ExpressionRefused as exc:
        reason = (exc.data or {}).get("reason")
        if reason == "evaluation_budget" and run_left is not None and run_left < _expr.EVAL_SECONDS:
            # Review L2: the run's wall clock, not the 5 s of computing,
            # cut this step; say which.
            raise _mc.refuse(
                "evaluation_budget",
                f"run: step {step.index}: computing stopped at the run's wall clock "
                f"({run_left:.1f} s were left of it; a compute step takes at most "
                f"{_expr.EVAL_SECONDS:g} s)",
                "send this map in a run with fewer earlier steps, or narrow its input "
                "with where=...",
                rows=len(evaluated), limit_seconds=round(seconds, 3), clock="run",
            ) from exc
        raise
    if groups_in:
        added = [c.label for c in program.columns if c.label not in group_columns]
        out_rows = []
        for row, done in zip(rows, computed.rows):
            new = dict(row)
            for column in program.columns:
                key = payload.key_of(column.label) or (
                    f"{_lrs.GROUP_COLUMN_PREFIX}{len(group_columns) + added.index(column.label)}"
                )
                new[_mc.value_key(column.name)] = done.get(key)
            new[_mc.MARK] = done[_mc.MARK]
            out_rows.append(new)
    else:
        out_rows = computed.rows
    labels = [column.label for column in program.columns]
    calc_now = _mc.columns_of(None, out_rows) if out_rows else tuple(
        dict.fromkeys([*_mc.columns_of(None, source.rows), *(c.name for c in program.columns)])
    )
    if select is not None:
        resolved = _mc.resolve_names("map", calc_now, projection=select)
        notes = [*notes, *(
            f"! adjusted: {note['sent']} read as {note['served']}" for note in resolved.notes
        )]
        printed: Optional[list[str]] = [run_steps.row_column(n) for n in resolved.projection]
    elif groups_in:
        printed = None
    else:
        printed = [run_steps.row_column(n) for n in _mc.default_select(program, typed=False)]
    for line in _mc.computed_line(computed.columns):
        notes.append(f"! {line}")
    for note in computed.notes:
        notes.append("! " + note.replace(
            "replaced in the new handle; the input handle is unchanged",
            f"replaced in this step's rows; step {number}'s rows keep theirs",
        ))
    total = len(source.rows)
    head = (
        f"map over ${number}: {len(out_rows):,} of {_plural(total, 'row')}"
        if narrowed else f"map over ${number}: {_plural(len(out_rows), 'row')}"
    )
    held = HeldRows(
        rows=out_rows, sources=sources, head=head, notes=notes,
        facts={"kind": "rows", "operator": "map", "input": number,
               "rows": len(out_rows), "from_rows": total, "partial": source.partial,
               "computed": computed.columns},
        partial=source.partial,
        cover=f"map over step {number} ({source.cover})",
        print_columns=printed,
    )
    shown = printed if printed is not None else held.columns()
    held.head += f" · computed {', '.join(labels)} · " + _columns_text(shown)
    held.facts["columns"] = held.columns()[:50]
    if aside:
        held.facts["not_computed"] = dict(aside)
    return held


def _aggregate(
    step: run_steps.Step, number: int, source: HeldRows,
    rows: list[dict[str, Any]], sources: list[str], notes: list[str],
    names: Optional[_Names] = None, aside: Optional[Mapping[str, Any]] = None,
) -> HeldRows:
    by_raw = (names.by if names is not None else step.arguments.get("by")) or []
    if not isinstance(by_raw, list):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: by is a list of columns", 'e.g. "by": ["<column>"]',
        )
    by = _lrs.check_by([_attrs_field(item) for item in by_raw])
    metrics_raw = (names.metrics if names is not None else step.arguments.get("metrics")) or ["count"]
    if not isinstance(metrics_raw, list):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: metrics is a list", _lrs.METRICS_HINT,
        )
    metrics = _lrs.check_metrics([_metric(item) for item in metrics_raw])
    named = [run_steps.row_column(field) for field in by]
    named += [
        run_steps.row_column(_lrs.column_label(key)) for _function, key in metrics
        if key is not None
    ]
    for column in named:
        if source.rows and all(not has_column(row, column) for row in source.rows):
            raise _unknown_column(step, "by/metrics", column, source, number)
    labels = [_lrs.metric_label(m) for m in metrics]
    order = _rows_order(
        step, by_raw, by, labels, raw=names.order_by if names is not None else None,
    )
    report = _lrs.group_report(rows, by, metrics)
    one_source = sources[0] if sources and len(set(sources)) == 1 else "run_rows"
    ordered_groups = report.groups
    limit = step.arguments.get("limit")
    limit = limit if isinstance(limit, int) and not isinstance(limit, bool) else None
    ranked_facts: dict[str, Any] = {}
    from okg.substrate.mcp import aggregate_order as _ao

    if order is not None:
        from okg.substrate.external_backed.resolver import _fence_payload

        def value_of(group: Any) -> Any:
            return _ao.live_value(group, order, by)

        ranked = _ao.rank(report.groups, order, value=value_of, key=lambda group: group[0])
        ordered_groups = ranked.groups
        ranked_facts = _ao.page_facts(
            ranked, order, offset=0, page=limit if limit is not None else len(ranked),
            complete=not source.partial, value=value_of,
            # A tie value that is a key is served as the keys are: fenced (M8).
            tie_value=lambda value: _fence_payload(value, source=one_source, fence_keys=True),
        )
    groups = _lrs.fenced_groups(ordered_groups, by, "", source=one_source)
    if order is not None:
        # `rank` is a column of the held groups (C4): a later step filters on it.
        groups = [{"rank": index + 1, **group} for index, group in enumerate(groups)]
    # The groups later steps read hold their raw key values (PACT
    # read-run-rows-compute, review H1): a group key is fenced only where a
    # reply prints it (`groups`), never where a condition, a formula or a
    # live step's `each` reads it. A `by` column is held under its bare
    # name (`attrs.site` as `site`), as a live row holds its fields.
    # A `by` column whose bare name is a metric's label or `rank` (review
    # M-B) is held under its printed name instead, so neither overwrites
    # the other; a later step reading these rows is refused by name
    # (`collisions`), since a condition or formula could not tell them apart.
    metric_names = set(labels) | ({"rank"} if order is not None else set())
    collisions = [
        label for label in by if run_steps.row_column(label) in metric_names
    ]
    # A `by` column that is a computed column (`calc.<name>`) is held as a
    # computed column (its value key and an ok mark), so a later step reads
    # it as `calc.<name>` (each "$n.calc.<name>", a condition, a compute)
    # as over the rows it grouped, and as over a kept groups answer.
    from okg.substrate.mcp import map_compute as _mc

    raw_groups: list[dict[str, Any]] = []
    group_labels: dict[str, str] = {}
    for index, (key, computed) in enumerate(_lrs.served_groups(ordered_groups)):
        raw: dict[str, Any] = {"rank": index + 1} if order is not None else {}
        for position, label in enumerate(by):
            name = label if label in collisions else run_steps.row_column(label)
            group_labels[name] = label
            calc = _mc.calc_name(name) if label not in collisions else None
            if calc is None:
                raw[name] = key[position]
                continue
            raw[_mc.value_key(calc)] = key[position]
            mark = raw.setdefault(_mc.MARK, {"s": {}, "u": []})
            if isinstance(key[position], str):
                mark["u"].append(calc)
        raw.update(computed)
        raw_groups.append(raw)
    cover = f"{_plural(len(rows), 'row')} of step {number} ({source.cover})"
    said_cover = f"{cover}; nothing outside these calls is counted"
    if aside:
        said_cover += f"; {aside['count']:,} not computed"
    if source.partial:
        # A value the system cut, or that failed or never started, left rows
        # out: say how short each metric may be (PACT live-kept-answer-cut).
        said_cover += (
            f"; step {number} did not read every value, so totals count only the "
            "rows read" + _lrs.cut_bounds_text(metrics, grouped=bool(by))
        )
    notes = [*notes, f"! totals cover: {said_cover}"]
    if order is not None:
        said = {"order_by": ranked_facts["order_by"], "total_count": len(groups),
                "ranking_complete": ranked_facts["ranking_complete"],
                "by": by, **({"tie_at_page_end": ranked_facts["tie_at_page_end"]}
                             if "tie_at_page_end" in ranked_facts else {})}
        notes.append("! " + str(_ao.head_line(said)))
        if source.partial:
            notes.append(
                f"! the ranking covers only the values read: step {number} did not "
                "read every value, so a group may be missing or low"
            )
        tie = _ao.tie_line(said)
        if tie is not None:
            notes.append(tie)
    else:
        note = _ao.order_note(labels, cut=limit is not None and len(groups) > limit)
        if note is not None:
            ranked_facts["order_note"] = note
            notes.append(str(_ao.order_note_line({"order_note": note})))
    if report.absent:
        notes.append(f"! {_lrs.absent_sentence(report.absent)}")
    if report.inexact:
        notes.append(f"! {_lrs.inexact_sentence(report.inexact)}")
    # PACT read-stored-metrics: each class of value left out, with its first
    # row keys, and the values read from numeric text; never read as 0.
    skipped = _lrs.skipped_payload(report)
    for sentence in _lrs.skipped_sentences(skipped):
        notes.append(f"! {sentence}; left out of its metrics, never read as 0")
    head = (
        f"aggregate over ${number}: {_plural(len(rows), 'row')} in "
        f"{_plural(len(report.groups), 'group')}"
    )
    return HeldRows(
        rows=raw_groups, sources=[one_source] * len(raw_groups), head=head, notes=notes,
        facts={
            "kind": "rows", "operator": "aggregate", "input": number,
            "rows": len(rows), "groups": len(report.groups), "by": by,
            "metrics": labels, "totals_cover": cover, "partial": source.partial,
            **({"absent_keys": report.absent} if report.absent else {}),
            **({"sum_not_exact": report.inexact} if report.inexact else {}),
            **({"values_skipped": skipped} if skipped else {}),
            **ranked_facts,
            **({"not_computed": dict(aside)} if aside else {}),
        },
        partial=source.partial, cover=cover, groups=groups, group_source=one_source,
        group_labels=group_labels,
        collisions=collisions or None,
    )


def _order_name(name: str) -> str:
    """An order_by name over held rows, as `by` and `metrics` name it: a bare
    `count` is the metric, `max(size)` is `max(attrs.size)`, `size` is
    `attrs.size`."""
    if name == "count":
        return name
    if "(" in name and name.endswith(")"):
        return str(_metric(name))
    return str(_attrs_field(name))


def check_rows_order(step: run_steps.Step) -> None:
    """An aggregate over held rows: its order_by checked from the step's own
    `by` and `metrics` when the steps are classified, before any step runs,
    so a typo costs no upstream call (review m3). A malformed `by` or
    `metrics` is left to the step itself, which refuses it as today."""
    if step.arguments.get("order_by") is None:
        return
    by_raw = step.arguments.get("by") or []
    metrics_raw = step.arguments.get("metrics") or ["count"]
    if not isinstance(by_raw, list) or not isinstance(metrics_raw, list):
        return
    try:
        by = _lrs.check_by([_attrs_field(item) for item in by_raw])
        metrics = _lrs.check_metrics([_metric(item) for item in metrics_raw])
    except SubstrateError:
        return  # the step refuses its own by/metrics when it runs, as today
    _rows_order(step, by_raw, by, [_lrs.metric_label(m) for m in metrics])


def _rows_order(
    step: run_steps.Step, by_raw: list[Any], by: list[str], labels: list[str],
    raw: Any = None,
) -> Any:
    """The order of an aggregate over held rows (PACT read-aggregate-order-by),
    or None: normalised as its `by` and `metrics` are, then checked by the
    shared rule against the printed labels."""
    from okg.substrate.mcp.aggregate_order import check_order_by
    from okg.substrate.mcp.live_reader import split_order_direction

    if raw is None:
        raw = step.arguments.get("order_by")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise run_steps.RunRefusal(
            f"run: step {step.index}: order_by names one metric or by column",
            'e.g. "order_by": "count desc"',
        )
    whole = _order_name(raw.strip())
    if whole in by or whole in labels:
        text, bare = whole, raw.strip()
    else:
        name, descending, doubled = split_order_direction(raw)
        bare = name
        text = raw.strip() if doubled else (
            f"{_order_name(name)} {'desc' if descending else 'asc'}"
        )
    if bare == "count" and "count" in by_raw:
        raise run_steps.RunRefusal(
            f"run: step {step.index}: order_by count is the metric, and by also "
            "names a column count; write it as attrs.count",
            'rank by the column with "order_by": "attrs.count desc"',
        )
    try:
        return check_order_by(text, by=by, metrics=labels, live=True)
    except SubstrateError as exc:
        raise run_steps.RunRefusal(
            f"run: step {step.index}: {exc}", getattr(exc, "fix_hint", "") or "",
        ) from exc


# --------------------------------------------------------------------------- #
# filter, map, aggregate over a kept answer                                   #
# --------------------------------------------------------------------------- #


async def kept_rows(
    server: Any, step: run_steps.Step, payload: Mapping[str, Any], state: RunLiveState,
) -> HeldRows:
    """The rows a later step reads of a step over a kept answer (lane re):
    the set the step's own reply names as its `handle`, read back through
    the server's checked open of a kept answer. A filter or map with
    `select` keeps only those columns, as over a run's own rows."""
    handle = payload.get("handle")
    set_id = handle.get("id") if isinstance(handle, Mapping) else None
    if not isinstance(set_id, str) or not set_id:
        reason = payload.get("handle_reason") or "its reply names no handle"
        raise run_steps.RunLiveStepRefused(
            f"run: step {step.index}: {step.operator} over a kept answer kept no "
            f"handle of its rows ({reason}), so a later step cannot read them",
            fix_hint=(
                "start the run with the live step that listed the values, so "
                "the run holds its rows"
            ),
            data={"step": step.index, "reason": str(reason)},
        )
    read = getattr(server, "_okg_live_kept_rows", None)
    if read is None:
        raise run_steps.RunLiveStepRefused(
            f"run: step {step.index}: this server cannot hold a kept answer's rows in a run",
            fix_hint="start the run with the live step that listed the values",
        )
    context = server.get_context() if hasattr(server, "get_context") else None
    rows, relation, grouped = await server._okg_run_blocking(
        read, step.operator, set_id, state.authority, context,
    )
    # A group's `by` column is printed as `attrs.<key>`; a run names a
    # column of its rows by the bare key, and reads `attrs.<key>` as it.
    # A computed column stays under its internal key with the row's mark
    # (PACT read-run-rows-compute), as a run's own map(compute) holds it.
    group_labels = {
        run_steps.row_column(str(k)): str(k)
        for row in rows[:1] for k in row if not _is_reserved(k)
    } if grouped else None
    rows = [{run_steps.row_column(str(k)): v for k, v in row.items()} for row in rows]
    names = step.arguments.get("select", step.arguments.get("projection"))
    if step.operator != "aggregate" and isinstance(names, list) and names:
        columns = [run_steps.row_column(str(name)) for name in names]
        rows = [_project(row, columns) for row in rows]
    partial = payload.get("total_count_basis") == "lower_bound"
    held = HeldRows(
        rows=list(rows), sources=[f"external_live:{relation}"] * len(rows),
        head=f"{step.operator} over a kept answer: {_plural(len(rows), 'row')} held as {set_id}",
        facts={"kind": "kept", "operator": step.operator, "handle": set_id,
               "rows": len(rows), "partial": partial},
        partial=partial,
        cover=f"{step.operator} over kept answer {set_id}",
    )
    if group_labels is not None:
        # A kept groups answer is read as a run's own aggregate's groups: a
        # compute over it reads a metric by its label (`count`), as over a
        # groups handle.
        from okg.substrate.external_backed.resolver import _fence_payload

        source = f"external_live:{relation}"
        held.group_labels = group_labels
        held.group_source = source
        held.groups = [
            {group_labels.get(k, k): (
                _fence_payload(v, source=source, fence_keys=True) if isinstance(v, str) else v
            ) for k, v in shown_row(row).items()}
            for row in rows
        ]
    held.facts["columns"] = held.columns()[:50]
    return held


def _is_reserved(key: Any) -> bool:
    from okg.substrate.mcp import map_compute as _mc

    return _mc.is_reserved(key)


# --------------------------------------------------------------------------- #
# live steps                                                                  #
# --------------------------------------------------------------------------- #


def _anchor(step: run_steps.Step) -> dict[str, Any]:
    anchor = step.arguments.get("input", step.arguments.get("from"))
    return {"node_id": anchor} if isinstance(anchor, str) else dict(anchor)


def _refused_text(result: Any) -> str:
    from mcp.types import TextContent

    content = getattr(result, "content", None) or []
    return "\n".join(block.text for block in content if isinstance(block, TextContent))


class _PrepareRefused(Exception):
    """`expand`'s own call refused the live step: its reply is the step's."""

    def __init__(self, result: Any) -> None:
        super().__init__("live step refused by its expand call")
        self.result = result


async def live_step(
    server: Any, step: run_steps.Step, held: Mapping[int, HeldRows],
    state: RunLiveState,
) -> HeldRows:
    """One live step: prepare, call, sample (see the module docstring)."""
    from okg.substrate.external_backed import live_dispatch as _ld
    from okg.substrate.mcp.response_bounds import run_sync_in_request_context

    if getattr(server, "_okg_run_live_steps", True) is False:
        raise run_steps.RunLiveStepsOff(
            f"run: step {step.index}: live steps in run are turned off on this server",
            fix_hint="send the expand(live=...) call on its own",
        )
    live = run_steps.live_body(step) or {}
    relation = str(live["relation"])
    source = run_steps.each_source(step)
    each_argument: Optional[str] = None
    values: Optional[list[Any]] = None
    value_source: Optional[str] = None
    without = 0
    from_step: Optional[tuple[int, str]] = None
    #: `{"ref", "field"}`: a column of a kept answer, read by expand's own
    #: call below as a call sent alone reads it (lane re).
    from_kept: Optional[Mapping[str, Any]] = None
    if source is not None:
        each_argument, origin = source
        if isinstance(origin, list):
            values = list(origin)
        elif isinstance(origin, Mapping):
            from_kept = origin
        else:
            number, column = origin
            from_step = (number, column)
            rows = held[number]
            check_readable(rows, number, step)
            if rows.rows and all(not has_column(row, column) for row in rows.rows):
                raise _unknown_column(step, f"live.each.{each_argument}", column, rows, number)
            values, without = column_values(rows, column)
            # Values read from upstream rows are untrusted text: a value a reply
            # names (a call not read) is fenced with its rows' source (review
            # M-A); values an agent listed (a values step) are its own.
            value_source = next((s for s in rows.sources if s != VALUES_SOURCE), None)
            if not values:
                raise run_steps.RunLiveStepRefused(
                    f"run: step {step.index}: step {number} has no value in "
                    f"{column!r} ({_plural(len(rows.rows), 'row')}); nothing to call",
                    fix_hint=f"check step {number}, or name another column",
                    data={"step": step.index, "column": column},
                )
            if len(values) > run_steps.MAX_FAN_OUT_VALUES:
                raise run_steps.RunLiveStepRefused(
                    f"run: step {step.index}: step {number} has {len(values):,} "
                    f"distinct values of {column}; a live step calls with at most "
                    f"{run_steps.MAX_FAN_OUT_VALUES:,}",
                    fix_hint=f"narrow step {number} (a filter over its rows), or split the run",
                    data={"step": step.index, "values": len(values),
                          "cap": run_steps.MAX_FAN_OUT_VALUES},
                )
    calls = len(values) if values is not None else 1
    if from_kept is None:
        _check_upstream_cap(step, state, calls)

    # Phase 1: `expand`'s own call, on a call worker.
    live_call: dict[str, Any] = {"relation": relation}
    if live.get("args") is not None:
        live_call["args"] = live["args"]
    if each_argument is not None and values is not None:
        live_call["each"] = {each_argument: values}
    elif each_argument is not None and from_kept is not None:
        live_call["each"] = {each_argument: dict(from_kept)}
    call: dict[str, Any] = {"input": _anchor(step), "live": live_call}
    if state.generation is not None:
        call["generation_id"] = state.generation
    if state.authority is not None:
        call["authority"] = state.authority
    holder = run_steps.LivePrepare()
    import time as _time

    events = run_steps.BudgetEvents()
    prepare_deadline = min(
        state.wall_deadline, _time.monotonic() + run_steps.STEP_BUDGET_MS / 1000,
    )
    with run_steps.live_prepare_scope(holder), run_steps.deadline_scope(prepare_deadline, events):
        result = await server.call_tool("expand", call)
    prepared = holder.prepared
    if prepared is None or getattr(result, "isError", False):
        raise _PrepareRefused(result)
    if from_kept is not None:
        # The kept answer's values, as expand read them: the same limits as
        # "$n.<column>", before the gate and before any upstream call.
        values = list((prepared.run_each or {}).get(each_argument) or [])
        if len(values) > run_steps.MAX_FAN_OUT_VALUES:
            raise run_steps.RunLiveStepRefused(
                f"run: step {step.index}: the kept answer has {len(values):,} "
                f"distinct values of {from_kept['field']}; a live step calls with "
                f"at most {run_steps.MAX_FAN_OUT_VALUES:,}",
                fix_hint="narrow the kept answer (a filter over it), or split the run",
                data={"step": step.index, "values": len(values),
                      "cap": run_steps.MAX_FAN_OUT_VALUES},
            )
        calls = len(values)
        _check_upstream_cap(step, state, calls)
    bound = prepared.run_bound
    node_id = prepared.run_node_id
    observed_at = datetime.now(timezone.utc)
    if each_argument is not None:
        each_key, per_value = _ld.validate_each(
            bound, prepared.run_args, prepared.run_each, args_model=prepared.run_args_model,
        )
        query = _ld.each_query(each_key, per_value)
    else:
        each_key = None
        per_value = [_ld.validate_live_arguments(
            bound, prepared.run_args, args_model=prepared.run_args_model,
        )]
        query = per_value[0]

    async def sample(outcome: Any, calls_result: Any, refused: Optional[str] = None) -> None:
        kwargs = _ld.run_sample_kwargs(
            bound=bound, node_id=node_id, outcome=outcome, query=query,
            result=calls_result, observed_at=observed_at, refused=refused,
        )
        try:
            await server._okg_run_blocking(prepared.run_write_sample, **kwargs)
        except Exception as exc:  # noqa: BLE001 - re-raised as the step's typed refusal.
            failure = type(exc).__name__
        else:
            return
        raise _ld.LiveSampleWriteFailed(
            f"run: step {step.index}: live relation {relation!r} could not write "
            f"its call sample ({failure}); the step fails: an outside call with "
            "no record is what the sample exists to prevent",
            data={"relation": relation, "node_id": node_id,
                  "adapter_ran": bool(calls_result is not None and calls_result.started)},
            fix_hint="check the call-sample table and the deployment's write connection",
        )

    # One fan-out run at a time per reader and session, and at most
    # HEAVY_RUNS_PER_READER per reader, asked before the gate: a refusal
    # here calls nothing and asks nothing.
    if each_key is not None and state.heavy_reader is None:
        reader = str(prepared.run_reader_key or "")
        session = getattr(prepared, "run_logical_session", None)
        if not _ld.claim_live_heavy_run(reader, session):
            raise run_steps.RunLiveBusy(
                f"run: step {step.index}: another run of this session is calling a "
                "live relation once per value, or this reader has "
                f"{_ld.HEAVY_RUNS_PER_READER} such runs; one per session, "
                f"{_ld.HEAVY_RUNS_PER_READER} per reader",
                fix_hint="send this run again when the other one has answered",
            )
        state.heavy_reader = (reader, session)
    outcome = await server._okg_run_blocking(prepared.run_gate)
    if not outcome.allowed:
        await sample(outcome, None)
        raise run_steps.RunLiveStepRefused(
            f"run: step {step.index}: live relation {relation!r} is not allowed "
            f"here ({outcome.reason})",
            fix_hint="the deployment's live-relation policy decides; no call was made",
            data={"step": step.index, "relation": relation, "reason": outcome.reason},
        )

    # Phase 2: the calls, on the wait-only executor. Nothing here holds a
    # call worker or a connection. Phase 3: the call sample, on a call
    # worker. Both run as one task the client's cancel does not reach
    # (review M1): a cancel sets `stop`, no further value starts, and the
    # calls already made still get their sample.
    state.upstream_calls += calls

    async def calls_then_sample() -> Any:
        try:
            made = await run_sync_in_request_context(
                _ld.run_live_calls,
                bound=bound, adapter=prepared.run_adapter, per_value=per_value,
                each_key=each_key, node_id=node_id, deadline=state.wall_deadline,
                stop=state.stop, max_bytes=run_steps.LIVE_STEP_MAX_BYTES,
                executor=_ld.run_wait_executor(),
            )
        except _ld.LiveConcurrencyExhausted:
            await sample(outcome, None, refused="live_concurrency_exhausted")
            raise
        await sample(outcome, made)
        return made

    task = asyncio.ensure_future(calls_then_sample())
    try:
        calls_result = await asyncio.shield(task)
    except asyncio.CancelledError:
        state.stop.set()
        # Nobody awaits the task now: its failure is logged, not lost.
        task.add_done_callback(_log_unawaited_failure)
        raise
    no_answer = (_ld.VALUE_FAILED, _ld.VALUE_NOT_STARTED)
    if each_key is None and calls_result.outcomes[0] in no_answer:
        # One call, and it gave no answer: the step is refused, so a step
        # that reads it is skipped rather than reading 0 rows as the
        # system's answer (review M7: a failed exclusion listing in a minus
        # kept every row it should have dropped).
        reason = calls_result.reasons[0] or _OUTCOME_WORDS[calls_result.outcomes[0]]
        raise run_steps.RunLiveStepRefused(
            f"run: step {step.index}: the call to live relation {relation!r} "
            f"gave no answer ({_OUTCOME_WORDS[calls_result.outcomes[0]]}: {reason}); "
            "a step that reads it is skipped",
            fix_hint=(
                "send the run again, or the expand(live=...) call on its own "
                "to see the failure"
            ),
            data={"step": step.index, "relation": relation,
                  "reason": f"{calls_result.outcomes[0]}: {reason}"},
        )
    return _live_held(
        step, relation, query, each_key, per_value, from_step, without, calls_result,
        value_source=value_source,
        from_kept=str(from_kept["field"]) if from_kept is not None else None,
    )


def _check_upstream_cap(step: run_steps.Step, state: RunLiveState, calls: int) -> None:
    if state.upstream_calls + calls > run_steps.MAX_RUN_UPSTREAM_CALLS:
        raise run_steps.RunLiveStepRefused(
            f"run: step {step.index} would make {calls:,} upstream calls after "
            f"{state.upstream_calls:,}; a run makes at most "
            f"{run_steps.MAX_RUN_UPSTREAM_CALLS:,}",
            fix_hint="split the run",
            data={"step": step.index, "calls": calls, "made": state.upstream_calls,
                  "cap": run_steps.MAX_RUN_UPSTREAM_CALLS},
        )


def _log_unawaited_failure(task: Any) -> None:
    """A live step's calls-and-sample task that finished after its run was
    cancelled: a failure there (a sample that could not be written) is
    logged, since no reply carries it."""
    if task.cancelled():
        return
    failure = task.exception()
    if failure is not None:
        import logging

        logging.getLogger(__name__).warning(
            "run live step finished after its client went away and failed: %s",
            type(failure).__name__,
        )


def _live_held(
    step: run_steps.Step, relation: str, query: Mapping[str, Any],
    each_key: Optional[str], per_value: list[dict[str, Any]],
    from_step: Optional[tuple[int, str]], without: int, result: Any,
    *, value_source: Optional[str] = None, from_kept: Optional[str] = None,
) -> HeldRows:
    from okg.substrate.external_backed.resolver import _fence_payload

    def served(value: Any) -> Any:
        if value_source is None or not isinstance(value, str):
            return value
        return _fence_payload(value, source=value_source, fence_keys=True)

    counts = result.counts()
    total = len(per_value)
    shared = {k: v for k, v in query.items() if k != each_key}
    name = f"expand live {relation}({_args_text(shared)})"
    if each_key is not None:
        origin = f" <- ${from_step[0]}.{from_step[1]}" if from_step is not None else ""
        if from_kept is not None:
            origin = f" <- kept answer's {from_kept}"
        name += f" each {each_key}{origin}"
    said = ", ".join(
        f"{counts[o]:,} {_OUTCOME_WORDS[o]}" for o in _OUTCOME_WORDS if counts[o]
    )
    calls = f"{_plural(total, 'call')}: {said}" if total > 1 else f"1 call {said.split(' ', 1)[1]}"
    rows = result.rows
    sources = [f"external_live:{relation}"] * len(rows)
    notes: list[str] = []
    if without and from_step is not None:
        notes.append(
            f"! {without:,} rows of step {from_step[0]} have no {from_step[1]}; "
            "they are not called"
        )
    missing = [
        (per_value[i].get(each_key) if each_key is not None else None, outcome, result.reasons[i])
        for i, outcome in enumerate(result.outcomes)
        if outcome not in ("ok", "empty", "not_found")
    ]
    if missing:
        shown = []
        for value, outcome, reason in missing[:MISSING_SHOWN]:
            what = _OUTCOME_WORDS[outcome] + (f": {reason}" if reason and outcome != "not_started" else "")
            shown.append(f"{_value_text(served(value))} ({what})" if each_key is not None else what)
        more = len(missing) - len(shown)
        notes.append(
            f"! partial: {len(missing):,} of {_plural(total, 'value')} not read: "
            + ", ".join(shown) + (f", +{more:,} more" if more > 0 else "")
            + "; totals over this step cover only the values read"
        )
    if result.stopped == "memory":
        notes.append(
            f"! the step held {run_steps.LIVE_STEP_MAX_BYTES // (1024 * 1024)} MiB "
            "of rows and called no further value"
        )
    if result.non_object_rows:
        notes.append(
            f"! {result.non_object_rows:,} rows were not objects; each is held as "
            '{"value": <row>}'
        )
    cover = f"{relation}({_args_text(shared)})"
    if each_key is not None:
        cover += f", one call per {each_key} ({_plural(total, 'value')}"
        cover += f" from step {from_step[0]})" if from_step is not None else ")"
    held = HeldRows(
        rows=rows, sources=sources, head="", notes=notes, partial=bool(missing),
        cover=cover,
    )
    held.head = f"{name}: {calls} · {_plural(len(rows), 'row')} · {_columns_text(held.columns())}"
    held.facts = {
        "kind": "live", "relation": relation, "query": dict(shared),
        **({"each": {
            "argument": each_key, "values": total,
            **({"from_step": from_step[0], "column": from_step[1]} if from_step else {}),
        }} if each_key is not None else {}),
        "calls": counts, "calls_started": result.started, "rows": len(rows),
        "columns": held.columns()[:50], "partial": bool(missing),
        "missing_count": len(missing),
        "missing": [
            {"value": served(value), "outcome": outcome, "reason": reason}
            for value, outcome, reason in missing[:MISSING_SHOWN_JSON]
        ],
        **({"rows_without_value": without} if without else {}),
        **({"stopped": result.stopped} if result.stopped else {}),
        "ms": result.elapsed_ms,
    }
    return held


def check_readable(held: HeldRows, number: int, step: run_steps.Step) -> None:
    """A later step reading the rows of an aggregate step whose `by` column
    has a metric's name or `rank` (PACT read-run-rows-compute, review M-B):
    refused by name, never read as the other column."""
    if not held.collisions:
        return
    names = ", ".join(run_steps.row_column(label) for label in held.collisions)
    raise run_steps.RunRefusal(
        f"run: step {step.index}: step {number} groups by {names}, which is also the "
        "name of one of its metrics (or of rank), so a later step cannot tell the "
        "group key from the metric",
        f"in step {number}, leave that metric (or order_by) out, or read its groups "
        "as printed; nothing was read",
    )
