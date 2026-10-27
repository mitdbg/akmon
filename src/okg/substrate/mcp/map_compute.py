"""``map(compute=...)``: computed columns on live sets (PACT read-map-compute).

``map(input=<live set or handle>, compute={"fill": "attrs.used / attrs.total"})``
adds ``calc.fill`` to every input row: the same rows, in the same order, each
with a value or a state. The formula language is
:mod:`okg.substrate.mcp.map_expressions`; this module is what sits between it
and the live readers:

* the rows a program reads (:func:`evaluation_rows`) and the rows a map keeps
  (:func:`computed_rows`);
* where a computed value lives in a kept row, and how a later ``filter`` or
  ``aggregate`` reads it (:data:`VALUE_PREFIX`, :data:`MARK`);
* the ``calc.`` names a later call writes, a bare name equal to a computed
  column included (:func:`resolve_names`);
* rows whose named computed column has no value (``not read`` or
  ``error:<kind>``): set aside and counted, never matched or grouped
  (:func:`set_aside`);
* the reply block that counts each column's states (:func:`computed_block`).

WHERE A COMPUTED VALUE LIVES
----------------------------
A computed row keeps its value under ``"\\x00calc.<name>"`` and one mark,
``"\\x00calc"`` = ``{"s": {name: state}, "u": [name, ...]}``: the states that
are not ``ok`` and the columns whose text was built from upstream text (served
fenced, 6.8). Both keys start with a NUL, as the record-column keys of
``live_result_sets`` do, so no upstream field can be read as one: a typed
row's upstream fields are under ``attrs``, and a raw row that already carries
a key starting ``"\\x00calc"`` is refused by name before computing. A groups
handle keeps its computed columns as columns (``calc.<name>``), with the same
mark for their states.
"""
from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from okg.substrate.mcp import map_expressions as _expr

#: The row key of a computed value, before the column's name.
VALUE_PREFIX = "\x00calc."
#: The row key of a computed row's states and upstream-text columns.
MARK = "\x00calc"
#: A computed column as a call names it.
CALC_NAME = re.compile(r"calc\.([a-z][a-z0-9_]{0,31})")
#: Node ids a reply names per state, at most.
IDS_SHOWN = 20
#: Rows one map computes over, and computed bytes per call (spec 5.8, 6.6);
#: read when a map runs, so a test can lower them.
MAX_ROWS = _expr.MAX_ROWS
MAX_OUTPUT_BYTES = _expr.MAX_OUTPUT_BYTES
OK = _expr.OK
NOT_READ = _expr.NOT_READ


class MapComputeRefused(_expr.ExpressionRefused):
    """A ``map(compute)`` call refused as a whole, by name, before any
    upstream call or graph read (``data["reason"]``)."""


def refuse(reason: str, message: str, fix_hint: str, **data: Any) -> MapComputeRefused:
    return MapComputeRefused(message, data={"reason": reason, **data}, fix_hint=fix_hint)


# -- names ----------------------------------------------------------------------


def calc_name(value: Any) -> Optional[str]:
    """The column name of ``calc.<name>``, or None."""

    if isinstance(value, str):
        found = CALC_NAME.fullmatch(value)
        if found:
            return found.group(1)
    return None


def value_key(name: str) -> str:
    return VALUE_PREFIX + name


def is_reserved(key: Any) -> bool:
    return isinstance(key, str) and key.startswith(MARK)


def strip(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """A raw row without its computed keys (the row itself when it has none)."""

    if MARK not in row:
        return row
    return {k: v for k, v in row.items() if not is_reserved(k)}


def _is_groups(payload: Any) -> bool:
    from okg.substrate.mcp.live_result_sets import LiveGroupsSet

    return isinstance(payload, LiveGroupsSet)


def columns_of(payload: Any, rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    """The computed columns a set holds, without ``calc.``, in column order."""

    if _is_groups(payload):
        return tuple(n for c in payload.columns if (n := calc_name(c)) is not None)
    names: dict[str, None] = {}
    for row in rows[:1]:
        for key in row:
            if isinstance(key, str) and key.startswith(VALUE_PREFIX):
                names[key[len(VALUE_PREFIX):]] = None
    return tuple(names)


def state_of(row: Mapping[str, Any], name: str) -> str:
    mark = row.get(MARK)
    if not isinstance(mark, Mapping):
        return OK
    return str((mark.get("s") or {}).get(name, OK))


def from_upstream_text(row: Mapping[str, Any], name: str) -> bool:
    mark = row.get(MARK)
    return isinstance(mark, Mapping) and name in (mark.get("u") or ())


def served_value(row: Mapping[str, Any], name: str, relation: str) -> Any:
    """A computed value as a reply serves it: text built from upstream text
    fenced with the input's source (6.8); anything else plain."""

    value = row.get(value_key(name))
    if isinstance(value, str) and from_upstream_text(row, name):
        from okg.substrate.mcp.live_result_sets import fence_rows

        return fence_rows(value, relation)
    return value


# -- the program ------------------------------------------------------------------


def spec_for(payload: Any, rows: Sequence[Mapping[str, Any]]) -> _expr.ColumnSpec:
    calc = columns_of(payload, rows)
    if _is_groups(payload):
        return _expr.ColumnSpec(groups=tuple(payload.columns), calc=calc)
    return _expr.ColumnSpec(calc=calc)


#: Names a computed column may not take: the record columns, the columns a
#: groups handle prints, and the words of the formula language. A later bare
#: `select=["node_id"]` or `order_by="-count"` would read the computed column.
RESERVED_NAMES = frozenset({
    "node_id", "subtype", "count", "rank", "match", "occurrences", "attrs", "live",
    "calc", "date", "col", "text", "true", "false", "null", "and", "or", "not",
    "sum", "min", "max",
})


def _column_names(compute: Any) -> list[str]:
    if not isinstance(compute, Mapping):
        return []
    names = []
    for raw in compute:
        if isinstance(raw, str):
            names.append(raw[len("calc."):] if raw.startswith("calc.") else raw)
    return names


def check_names(compute: Any) -> None:
    """A computed column named like a record column, a groups column or a
    word of the language: refused by name before any work (review L2)."""

    for name in _column_names(compute):
        if name in RESERVED_NAMES:
            raise refuse(
                "reserved_column_name",
                f"map: calc.{name} would shadow `{name}`, a name later reads take as "
                "a column of their own",
                f'name the column otherwise, e.g. compute={{"{name}_x": ...}}',
                column=f"calc.{name}",
            )


def refuse_shadowing(compute: Any, upstream: Any) -> None:
    """A computed column named like an upstream field the input holds or its
    live type declares: refused by name before any work (review M1). A bare
    name equal to a computed column reads the column, so such a field would
    never be read or fetched again."""

    held = {str(k) for k in upstream or ()}
    for name in _column_names(compute):
        if name in held:
            raise refuse(
                "column_shadows_field",
                f"map: calc.{name} has the name of the upstream field attrs.{name}; a "
                "later bare name would read the computed column, not the field",
                f'name the column otherwise, e.g. compute={{"{name}_x": ...}}',
                column=f"calc.{name}", field=f"attrs.{name}",
            )


def upstream_names(payload: Any, rows: Sequence[Mapping[str, Any]]) -> set[str]:
    """The upstream field names a set holds or its live type declares."""

    from okg.substrate.mcp.live_reader import attrs_of, is_typed

    if _is_groups(payload):
        return set()
    names = {str(k) for k in getattr(payload, "declared_keys", ()) or ()}
    for row in rows:
        names.update(str(k) for k in attrs_of(row))
        if is_typed(row):
            names.update(str(k) for k in ((row.get("live") or {}).get("fields") or {}))
    return names


def attrs_operands(program: _expr.Program) -> list[str]:
    keys = []
    for operand in program.operands():
        for prefix in ("attrs.", "date.attrs."):
            if operand.startswith(prefix):
                keys.append(operand[len(prefix):])
    return list(dict.fromkeys(keys))


def check_operands_held(
    program: _expr.Program, payload: Any, rows: Sequence[Mapping[str, Any]],
    *, keys: Optional[Sequence[str]] = None,
) -> None:
    """An `attrs.` operand that no input row holds (no value, no state) and
    that the live type does not declare: refused by name with the closest
    keys held, as `filter`'s typo check refuses it on a whole listing, never
    computed as null with state ok (review H1). `keys` limits the check."""

    from okg.substrate.mcp import live_reader as _reader

    if _is_groups(payload) or not rows:
        return
    wanted = [k for k in (keys if keys is not None else attrs_operands(program))]
    if not wanted:
        return
    _reader.check_keys_seen(
        rows, _reader.Names(attrs=[("compute", k) for k in wanted]), "map",
        known=tuple(getattr(payload, "declared_keys", ()) or ()), whole=True,
    )


def compile_for(compute: Any, payload: Any, rows: Sequence[Mapping[str, Any]]) -> _expr.Program:
    """The program of ``compute`` over this input; every refusal comes before
    any row is read."""

    check_names(compute)
    return _expr.compile(compute, spec_for(payload, rows))


def first_read_program(compute: Any) -> _expr.Program:
    """The program of ``compute`` over a first read of a live type (record
    rows, no computed column yet)."""

    check_names(compute)
    return _expr.compile(compute, _expr.ColumnSpec())


def upstream_keys(compute: Any) -> list[str]:
    """The upstream fields ``compute`` reads (``attrs.<key>`` operands, bare
    names included), for the resolver's ``named_keys``.

    The ``calc.`` names it writes are taken as columns the input has, and a
    program that does not compile names none: the tool's own compile
    (:func:`first_read_program`, :func:`compile_for`) refuses it by name
    before any upstream call. A bare name equal to an input's computed column
    is named here, and is never fetched: the growth of a kept set skips every
    key its rows hold, computed columns included."""

    if not isinstance(compute, Mapping):
        return []
    sources = [s for s in compute.values() if isinstance(s, str)]
    calc = tuple(sorted({m.group(1) for s in sources for m in CALC_NAME.finditer(s)}))
    try:
        program = _expr.compile(compute, _expr.ColumnSpec(calc=calc))
    except _expr.ExpressionRefused:
        return []
    keys: list[str] = []
    for operand in program.operands():
        for prefix in ("attrs.", "date.attrs."):
            if operand.startswith(prefix):
                keys.append(operand[len(prefix):])
    return list(dict.fromkeys(keys))


def record_operands(program: _expr.Program) -> list[str]:
    return [o for o in program.operands() if o in ("node_id", "subtype")]


def refuse_operands(program: _expr.Program, *, groups: bool, typed: bool) -> None:
    """Operands this input cannot give, refused by name before computing,
    never read as null on every row: `node_id`/`subtype` on raw rows (an
    expand answer has neither), and `date.<meaning>` on live rows (a
    stored record's date; a live row has only its own fields)."""

    if groups:
        return
    record = record_operands(program)
    if record and not typed:
        raise refuse(
            "record_column_on_raw_rows",
            f"map: this set holds raw rows with no {record[0]}; that column belongs "
            "to live typed rows",
            "name the upstream field as attrs.<field>",
            operand=record[0],
        )
    dated = [
        o for o in program.operands() if o.startswith("date.") and not o.startswith("date.attrs.")
    ]
    if dated:
        raise refuse(
            "date_on_live_rows",
            f"map: {dated[0]} is a stored record's date; a live row has only its "
            "own fields",
            "read the row's own field as a date: date.attrs.<field>",
            operand=dated[0],
        )


def check_select(select: Any, program: _expr.Program) -> None:
    """A `calc.` name in `select` that neither the input nor this compute
    has: refused by name before any upstream call."""

    if not isinstance(select, list):
        return
    known = set(program.spec.calc) | {c.name for c in program.columns}
    for item in select:
        name = calc_name(item)
        if name is not None and name not in known:
            raise _unknown(name, "select", sorted(known), "map")


def default_select(program: _expr.Program, *, typed: bool) -> list[str]:
    """What a `map` prints without `select`: the record id, the fields the
    expressions read, and the new columns."""

    shown: dict[str, None] = {}
    if typed:
        shown["node_id"] = None
    for operand in program.operands():
        if operand.startswith("date.attrs."):
            operand = "attrs." + operand[len("date.attrs."):]
        if operand.startswith("attrs.") and "." in operand[len("attrs."):]:
            continue
        if operand.startswith(("attrs.", "calc.")) or (typed and operand in ("node_id", "subtype")):
            shown[operand] = None
    for column in program.columns:
        shown[column.label] = None
    return list(shown)


# -- evaluation -------------------------------------------------------------------


def evaluation_rows(
    payload: Any, rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """The rows a program reads, and each row's input states.

    A typed row as it is (``node_id``, ``subtype``, ``attrs``, ``live``); a
    raw row's fields under ``attrs``; a groups row by its printed column
    names. A computed column of the input is ``calc.<name>``, with its state.
    """

    from okg.substrate.mcp.live_reader import is_typed

    groups = payload if _is_groups(payload) else None
    calc = columns_of(payload, rows)
    out: list[dict[str, Any]] = []
    states: list[dict[str, str]] = []
    for row in rows:
        if groups is not None:
            er: dict[str, Any] = {
                name: row.get(groups.key_of(name)) for name in groups.columns
            }
        elif is_typed(row):
            er = {
                "node_id": row.get("node_id"), "subtype": row.get("subtype"),
                "attrs": row.get("attrs") or {}, "live": row.get("live") or {},
            }
            for name in calc:
                er[f"calc.{name}"] = row.get(value_key(name))
        else:
            er = {"node_id": None, "attrs": dict(strip(row))}
            for name in calc:
                er[f"calc.{name}"] = row.get(value_key(name))
        out.append(er)
        states.append({
            f"calc.{name}": state for name in calc
            if (state := state_of(row, name)) != OK
        })
    return out, states


def _operand_text(er: Mapping[str, Any], operand: str, groups: bool) -> bool:
    """Whether one operand read upstream text in this row."""

    if groups:
        return isinstance(er.get(operand), str)
    if operand.startswith("attrs."):
        return isinstance((er.get("attrs") or {}).get(operand[6:]), str)
    if operand.startswith("date.attrs."):
        return isinstance((er.get("attrs") or {}).get(operand[11:]), str)
    if operand == "node_id":
        return True
    return False


@dataclass
class Computed:
    """What one ``map(compute)`` made: the kept rows and the counts."""

    rows: list[dict[str, Any]]
    #: calc.<name> -> {"expression", "states": {state: rows}, "not_computed": [...]}
    columns: dict[str, dict[str, Any]]
    #: Columns replaced in the new handle (the input had them).
    replaced: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _why_not_read(row: Mapping[str, Any], column: _expr.Column, states: Mapping[str, str]) -> str:
    fields = ((row.get("live") or {}).get("fields") or {}) if isinstance(row.get("live"), Mapping) else {}
    for operand in column.operands:
        if operand.startswith("calc."):
            state = states.get(operand)
            if state is not None and state != OK:
                return f"{operand} {state}"
            continue
        key = operand.split(".", 1)[1] if operand.startswith(("attrs.", "date.attrs.")) else operand
        if operand.startswith("date.attrs."):
            key = operand[len("date.attrs."):]
        state = fields.get(key)
        if state in ("unavailable", "denied", "withheld"):
            return f"attrs.{key} {state}"
    return "an input was not read"


#: Seconds apart past which one row's inputs are said to mix read times (6.2).
MIXED_READ_SECONDS = 60


def _stamp(text: Any) -> Any:
    import datetime as _dt

    if not isinstance(text, str):
        return None
    try:
        return _dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _read_spread(row: Mapping[str, Any], column: _expr.Column) -> Optional[tuple[Any, Any]]:
    """`(first, last)` read time of the upstream fields one column read in
    this row, when they are more than a minute apart; None otherwise."""

    live = row.get("live")
    if not isinstance(live, Mapping):
        return None
    stamps = live.get("field_observed_at") if isinstance(live.get("field_observed_at"), Mapping) else {}
    times = []
    for operand in column.operands:
        for prefix in ("attrs.", "date.attrs."):
            if operand.startswith(prefix):
                at = _stamp(stamps.get(operand[len(prefix):]) or live.get("observed_at"))
                if at is not None:
                    times.append(at)
    if len(times) < 2:
        return None
    first, last = min(times), max(times)
    if (last - first).total_seconds() <= MIXED_READ_SECONDS:
        return None
    return first, last


def computed_rows(
    program: _expr.Program, payload: Any, rows: Sequence[Mapping[str, Any]],
    *, served_id: Any = None,
) -> Computed:
    """Every column of every row, kept beside the row's own fields.

    `served_id(row)` gives a row's node id as a reply serves it (fenced when
    it was built from an unchecked upstream name)."""

    from okg.substrate.mcp.live_reader import is_typed

    groups = payload if _is_groups(payload) else None
    if groups is None:
        taken = sorted({
            k for row in rows if not is_typed(row) for k in row
            if is_reserved(k) and MARK not in row
        })
        if taken:
            raise refuse(
                "reserved_key",
                "map: rows of this set carry a key that starts with a NUL and 'calc', "
                "which is where computed columns are kept",
                "read the set with filter; it cannot take computed columns",
                # Counted, never echoed: they are upstream keys.
                keys=len(taken),
            )
    eval_rows, states = evaluation_rows(payload, rows)
    result = program.evaluate(
        eval_rows, states, _expr.Budget(max_rows=MAX_ROWS, max_bytes=MAX_OUTPUT_BYTES),
    )
    existing = set(columns_of(payload, rows))
    replaced = [f"calc.{c.name}" for c in program.columns if c.name in existing]
    by_name = {c.name: c for c in program.columns}
    out: list[dict[str, Any]] = []
    stats: dict[str, dict[str, Any]] = {
        c.label: {"expression": c.source, "states": Counter(), "not_computed": [], "mixed": None}
        for c in program.columns
    }
    new_columns: list[str] = []
    if groups is not None:
        new_columns = [c.label for c in program.columns if c.label not in groups.columns]
    for index, row in enumerate(rows):
        new = dict(row)
        mark = row.get(MARK) if isinstance(row.get(MARK), Mapping) else {}
        row_states = {k: v for k, v in dict(mark.get("s") or {}).items()}
        upstream = set(mark.get("u") or ())
        er = eval_rows[index]
        tainted: dict[str, bool] = {}
        for column in program.columns:
            value = result.columns[column.label].values[index]
            state = result.columns[column.label].states[index]
            tainted[column.name] = bool(
                any(_operand_text(er, o, groups is not None) for o in column.operands)
                or any(
                    o[5:] in upstream for o in column.operands if o.startswith("calc.")
                )
                or any(tainted.get(d) for d in column.depends)
            )
            if groups is not None:
                key = groups.key_of(column.label)
                if key is None:
                    key = (
                        f"c{len(groups.columns) + new_columns.index(column.label)}"
                    )
                new[key] = value
            else:
                new[value_key(column.name)] = value
            if state == OK:
                row_states.pop(column.name, None)
            else:
                row_states[column.name] = state
            if tainted[column.name] and isinstance(value, str):
                upstream.add(column.name)
            else:
                upstream.discard(column.name)
            entry = stats[column.label]
            entry["states"][state] += 1
            spread = _read_spread(row, column) if groups is None and state != NOT_READ else None
            if spread is not None:
                seen = entry["mixed"]
                entry["mixed"] = spread if seen is None else (
                    min(seen[0], spread[0]), max(seen[1], spread[1]),
                )
            if state != OK and len(entry["not_computed"]) < IDS_SHOWN:
                why = (
                    _why_not_read(row, column, states[index]) if state == NOT_READ
                    else state
                )
                node = row.get("node_id") if groups is None and is_typed(row) else None
                entry["not_computed"].append({
                    **({"node_id": served_id(row) if served_id else node}
                       if node is not None else {"row": index + 1}),
                    "state": state, "why": why,
                })
        new[MARK] = {
            "s": row_states,
            "u": sorted(upstream),
        }
        out.append(new)
    columns = {}
    for label, entry in stats.items():
        counts = dict(entry["states"])
        columns[label] = {
            "expression": entry["expression"],
            "rows": len(rows),
            "states": {k: counts[k] for k in sorted(counts, key=lambda s: (s != OK, s))},
            **({"not_computed": entry["not_computed"]} if entry["not_computed"] else {}),
            # 6.2: one row's inputs read more than a minute apart.
            **({"mixes_reads": [
                entry["mixed"][0].strftime("%Y-%m-%dT%H:%M:%SZ"),
                entry["mixed"][1].strftime("%Y-%m-%dT%H:%M:%SZ"),
            ]} if entry["mixed"] is not None else {}),
        }
    notes = list(program.notes)
    for label in replaced:
        notes.append(f"{label} replaced in the new handle; the input handle is unchanged")
    del by_name
    return Computed(rows=out, columns=columns, replaced=replaced, notes=notes)


def computed_line(columns: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """One text line per computed column: its states, and the rows without a
    value, named (`calc.fill: 22 ok · 2 not read (site:A, site:B: attrs.x
    unavailable)`)."""

    lines = []
    for label, entry in columns.items():
        states = dict(entry.get("states") or {})
        parts = [f"{int(n):,} {state}" for state, n in states.items()]
        text = f"{label}: " + " · ".join(parts)
        missing = list(entry.get("not_computed") or [])
        if missing:
            whys: dict[str, list[str]] = {}
            for item in missing:
                name = item.get("node_id") if "node_id" in item else f"row {item.get('row')}"
                whys.setdefault(str(item.get("why")), []).append(str(name))
            shown = "; ".join(f"{', '.join(names)}: {why}" for why, names in whys.items())
            more = sum(int(n) for s, n in states.items() if s != OK) - len(missing)
            text += f" ({shown}{f'; {more:,} more' if more > 0 else ''})"
        mixed = entry.get("mixes_reads")
        if isinstance(mixed, list) and len(mixed) == 2:
            text += f"; {label} mixes values read at {mixed[0][11:16]} and {mixed[1][11:16]}"
        lines.append(text)
    return lines


# -- calc. names in later calls (6.5) ---------------------------------------------


_BARE = re.compile(r"[a-z][a-z0-9_]{0,31}")


def bare_by(by: Any) -> bool:
    """Whether `by` names a bare word that may be a computed column."""

    return isinstance(by, list) and any(
        isinstance(b, str) and _BARE.fullmatch(b) and b not in ("node_id", "subtype")
        for b in by
    )


def mentions_calc(*values: Any) -> bool:
    """Whether any argument names a `calc.` column."""

    import json

    return "calc." in json.dumps(values, default=str)


@dataclass
class Names:
    """A call's names with ``calc.`` resolved over one set."""

    predicate: Any = None
    projection: Any = None
    order_by: Any = None
    by: Any = None
    metrics: Any = None
    #: The ``calc.`` columns the call reads, without the prefix, by argument.
    read: dict[str, list[str]] = field(default_factory=dict)
    notes: list[dict[str, str]] = field(default_factory=list)


def _unknown(name: str, argument: str, calc: Sequence[str], operator: str) -> MapComputeRefused:
    shown = ", ".join(f"calc.{c}" for c in calc) if calc else "none"
    return refuse(
        "calc_column_unknown",
        f"{operator}: this set has no computed column calc.{name}; its computed "
        f"columns: {shown}",
        'name a computed column the map reply printed, or add it with '
        'map(input="<handle>", compute={...})',
        argument=argument, column=f"calc.{name}", columns=[f"calc.{c}" for c in calc],
    )


def resolve_names(
    operator: str, calc: Sequence[str], *, predicate: Any = None, projection: Any = None,
    order_by: Any = None, by: Any = None, metrics: Any = None, rows_known: bool = True,
) -> Names:
    """``calc.<name>`` checked against the set's computed columns, and a bare
    name equal to one read as it, with an ``adjusted`` note (6.5). A bare
    name that is not a computed column is left as it is.

    ``rows_known`` is False for an empty set whose computed columns cannot be
    listed; a ``calc.`` name is then taken as written (there is no row to
    read it from)."""

    known = set(calc)
    out = Names(predicate=predicate, projection=projection, order_by=order_by,
                by=by, metrics=metrics)

    def note(argument: str, bare: str) -> None:
        record = {"rule": "bare_name_is_calc_column", "sent": f"{argument} {bare}",
                  "served": f"calc.{bare}"}
        if record not in out.notes:
            out.notes.append(record)

    def name(text: Any, argument: str) -> Any:
        if not isinstance(text, str):
            return text
        column = calc_name(text)
        if column is not None:
            if rows_known and column not in known:
                raise _unknown(column, argument, calc, operator)
            out.read.setdefault(argument, []).append(column)
            return text
        if text in known and argument != "predicate":
            note(argument, text)
            out.read.setdefault(argument, []).append(text)
            return f"calc.{text}"
        return text

    def leaves(node: Any) -> Any:
        if isinstance(node, list):
            return [leaves(n) for n in node]
        if not isinstance(node, Mapping):
            return node
        new = dict(node)
        if isinstance(new.get("clauses"), list):
            new["clauses"] = [leaves(c) for c in new["clauses"]]
        if isinstance(new.get("clause"), Mapping):
            new["clause"] = leaves(new["clause"])
        if "field" in new:
            new["field"] = name(new["field"], "predicate")
        return new

    out.predicate = leaves(predicate)
    if isinstance(projection, list):
        out.projection = [name(p, "select") for p in projection]
    elif isinstance(projection, Mapping):
        out.projection = {k: name(v, "select") for k, v in projection.items()}
    if isinstance(order_by, str) and order_by.strip():
        from okg.substrate.mcp.live_reader import split_order_direction

        text, desc, _doubled = split_order_direction(order_by)
        resolved = name(text, "order_by")
        if resolved != text:
            out.order_by = ("-" if desc else "") + resolved
    if isinstance(by, list):
        out.by = [name(b, "by") for b in by]
    if isinstance(metrics, list):
        resolved_metrics = []
        for metric in metrics:
            if isinstance(metric, str) and "(" in metric and metric.endswith(")"):
                function, inner = metric.split("(", 1)
                inner = inner[:-1]
                resolved_metrics.append(f"{function}({name(inner, 'metrics')})")
            else:
                resolved_metrics.append(metric)
        out.metrics = resolved_metrics
    return out


@dataclass
class SetAside:
    """Rows set aside because a computed column the call needs has no value."""

    count: int = 0
    columns: dict[str, Counter] = field(default_factory=dict)
    rows: list[Any] = field(default_factory=list)

    def report(self) -> Optional[dict[str, Any]]:
        if not self.count:
            return None
        return {
            "count": self.count,
            "columns": {f"calc.{k}": dict(v) for k, v in self.columns.items()},
            "rows": list(self.rows),
            "note": (
                f"{self.count:,} rows have no value in a computed column this call "
                "needs (not read, or an error): they match no condition (not even "
                "under `not`), are in no group and are not counted"
            ),
        }


def set_aside(
    rows: Sequence[Mapping[str, Any]], names: Sequence[str], *, served_id: Any = None,
) -> tuple[list[Mapping[str, Any]], SetAside]:
    """Rows with a value in every computed column `names`, and the rest."""

    aside = SetAside()
    if not names:
        return list(rows), aside
    keep: list[Mapping[str, Any]] = []
    wanted = list(dict.fromkeys(names))
    for index, row in enumerate(rows):
        problems = [(n, s) for n in wanted if (s := state_of(row, n)) != OK]
        if not problems:
            keep.append(row)
            continue
        aside.count += 1
        for column, state in problems:
            aside.columns.setdefault(column, Counter())[state] += 1
        if len(aside.rows) < IDS_SHOWN:
            node = row.get("node_id")
            aside.rows.append(served_id(row) if served_id and node is not None else (
                node if node is not None else index + 1
            ))
    return keep, aside


def refuse_under_access_rules(server: Any) -> None:
    """``map(compute)`` under access rules (audit or enforce), or when their
    state cannot be told: refused by name before any read (6.10)."""

    from okg.substrate.mcp.live_access_rules import access_rules_state

    rules = access_rules_state(server)
    if rules is False:
        return
    raise refuse(
        "compute_under_access_rules",
        "map: computed columns are not served while access rules are on (audit/enforce)"
        + ("" if rules else ", and whether they are on cannot be told here"),
        "read the stored graph with filter(select=[...]); map(compute) is not served "
        "under access rules",
        access_rules="on" if rules else "unknown",
    )


__all__ = [
    "CALC_NAME",
    "Computed",
    "MARK",
    "MapComputeRefused",
    "Names",
    "SetAside",
    "VALUE_PREFIX",
    "calc_name",
    "columns_of",
    "compile_for",
    "computed_line",
    "computed_rows",
    "evaluation_rows",
    "first_read_program",
    "refuse",
    "refuse_under_access_rules",
    "resolve_names",
    "served_value",
    "set_aside",
    "state_of",
    "strip",
    "upstream_keys",
    "value_key",
]
