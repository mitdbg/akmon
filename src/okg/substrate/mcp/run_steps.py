"""`run`: several read calls in order, over one graph version (issue #3666).

A step is an ordinary call, ``{"<operator>": {...arguments...}}`` with an
optional ``"show"``; by default only the last step prints rows. Steps run in
order, each through the same checks as a call sent alone. A later step of
the same call names an earlier step's records:

* ``"$n"`` used as a condition means "the record is in step n's set". It
  goes in a ``where`` (alone or inside ``and``/``or``/``not``), in a scope's
  ``filter``, in expand's far-end ``where``, and as the ``set`` of an
  ``inspect`` ``nodes`` target.
* ``"$n.<field>"`` in the list position of an ``in`` clause (a short-form
  value, or ``values``) means the distinct values of that field over step
  n's set.

A set is the step's whole set, never the page it printed: a filter, map,
aggregate or one-hop expand step's set is its whole condition, a grep,
lexical or attrs search step's set is its whole match set (at most
``RUN_ROW_CAP`` records, read and counted in full), and a ranked search page
is no set. `limit` and `offset` only bound what a step prints. No set
outlives its run call: ``$r<n>`` and ``retain`` set names are removed.

Nothing is copied from one step into the next. The server keeps each
referenced step's set as SQL (a query of record ids, or of distinct values)
and the later step's own SQL reads it as a subquery.

This module is the part of `run` that reads only the arguments: the step
list, the references, and the rewrite of a reference into a condition the
read tools already accept. The rewrite is an ``in`` leaf on ``node_id`` (or
on the named field) whose one value is a marker naming this run's registry
entry; the effective-view compiler lowers a marker leaf to the subquery
(``lower_marker``) and the row check reads the entry
(``marker_matches``). A marker means something only while its run's
registry is active: outside a run it is an ordinary string that no stored
value holds.
"""

from __future__ import annotations

import copy
import json
import re
import secrets
from decimal import Decimal
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field as dc_field
from typing import Any, Optional

from okg.substrate.errors import SubstrateError

__all__ = [
    "MAX_STEPS",
    "MATERIALIZE_CAP",
    "RUN_ROW_CAP",
    "SET_LASTS_ONE_RUN",
    "STEP_OPERATORS",
    "RunCountNotFinished",
    "RunOverBudget",
    "RunRefusal",
    "RunSets",
    "RunStepTooManyRows",
    "SetNamesRemoved",
    "StepCapture",
    "StepSet",
    "Step",
    "active_capture",
    "bind_step",
    "capture_scope",
    "inlinable_predicate",
    "lower_marker",
    "mark_negated",
    "marker_matches",
    "names_run_values",
    "note_payload",
    "note_set",
    "parse_steps",
    "registry_scope",
    "step_references",
]

#: Most steps one `run` takes.
MAX_STEPS = 8
#: The operators a step may name. `query` has no set to pass on and `run`
#: does not nest. `values`, `minus` and `intersect` (#3666, lane ri) build
#: rows the run holds itself: a literal list, and the rows of earlier
#: steps that hold rows, compared on one column.
STEP_OPERATORS: tuple[str, ...] = (
    "search", "inspect", "expand", "filter", "map", "aggregate",
    "values", "minus", "intersect",
)
#: The steps that compare the rows of earlier steps on one column.
SET_OPERATORS: tuple[str, ...] = ("minus", "intersect")
#: Live steps (#3666, lane ri): most fan-out steps (`each`) one run takes,
#: most values one fan-out calls with, and most upstream calls one run
#: makes in all (a listing is one call, a fan-out one per value).
MAX_FAN_OUT_STEPS = 2
MAX_FAN_OUT_VALUES = 1_500
MAX_RUN_UPSTREAM_CALLS = 1_500
#: Most values one `values` step lists.
MAX_VALUES_STEP = 1_000
#: Inputs a `minus` or `intersect` step takes.
MIN_SET_INPUTS = 2
MAX_SET_INPUTS = 6
#: Most bytes of rows one live step holds (the rows as compact JSON). Past
#: it no further value is called and the step says so: partial.
LIVE_STEP_MAX_BYTES = 8 * 1024 * 1024
#: The whole run's wall clock: a live step stops starting calls here, and
#: the run answers with what it has. The default, and the cap that keeps
#: the answer inside a client's tool timeout (Claude Code: 120 s) with
#: room for the reply.
RUN_WALL_DEFAULT_S = 100.0
CLIENT_TOOL_TIMEOUT_S = 120.0
RUN_WALL_MARGIN_S = 10.0
#: A graph step's own wall clock: its statements are bounded by the
#: database-time budget, and the work around them (text matching in
#: Python) by this.
GRAPH_STEP_WALL_S = 30.0
#: The environment variable an operator sets to change the run's wall
#: clock (seconds), capped at the client's tool timeout less the margin.
RUN_WALL_ENV = "OKG_RUN_WALL_SECONDS"


def run_wall_seconds(environ: Optional[Mapping[str, str]] = None) -> float:
    """The run's wall clock: `RUN_WALL_ENV` when set, else the default,
    never past `CLIENT_TOOL_TIMEOUT_S - RUN_WALL_MARGIN_S`. A value that is
    not a positive number is refused by name at the run, never read as the
    default."""
    import os

    cap = CLIENT_TOOL_TIMEOUT_S - RUN_WALL_MARGIN_S
    raw = (environ if environ is not None else os.environ).get(RUN_WALL_ENV)
    if raw is None or raw == "":
        return min(RUN_WALL_DEFAULT_S, cap)
    try:
        value = float(raw)
    except ValueError:
        value = float("nan")
    if not (value > 0 and value == value):
        raise RunRefusal(
            f"run: {RUN_WALL_ENV}={raw!r} is not a number of seconds above 0",
            f"the operator sets {RUN_WALL_ENV} to seconds, at most {cap:g}",
        )
    return min(value, cap)
#: Most records (or distinct values) of one step's set the row check holds
#: in memory. The SQL side has no cap; a row check over a larger set is
#: refused by name, never answered from part of it.
MATERIALIZE_CAP = 100_000
#: The `inspect` nodes read takes at most this many ids (`MAX_NODE_BATCH`).
INSPECT_SET_CAP = 50
#: Most rows one printed step prints (records, or groups for aggregate);
#: also the most records a search step's whole match set may hold.
RUN_ROW_CAP = 200
#: What a call naming a set kept past its run is told (issue #3666).
SET_LASTS_ONE_RUN = "a set lasts for one run call; put both reads in one run(steps=[...])"

_SET_PREFIX = "okg-run-set:"
_VALUES_PREFIX = "okg-run-values:"
#: ``$n`` (any digits, so ``$0`` is read and refused) and ``$r<n>``.
_REF = re.compile(r"\$(r?[0-9]{1,6})(?:\.([A-Za-z_][A-Za-z0-9_.\-]{0,95}))?\Z")
#: Fields a value list may read: an attribute, the id, the type.
_VALUE_COLUMNS = {"node_id": "target_id", "type": "subtype", "subtype": "subtype"}
_CONDITION_KEYS = ("where", "predicate", "filter")
_LOGICAL = ("and", "or", "not")


class RunOverBudget(SubstrateError):
    """A run step, or a call naming a set, used its time budget."""

    code = "run_over_budget"


class RunStepTooManyRows(SubstrateError):
    """A printed run step would print more than ``RUN_ROW_CAP`` rows, or a
    search step's whole match set holds more than that many records."""

    code = "run_step_too_many_rows"


class RunCountNotFinished(SubstrateError):
    """A run step's exact count did not finish inside the step's budget; it
    is never estimated."""

    code = "run_count_not_finished"


class SetNamesRemoved(SubstrateError):
    """A call named a set kept past its run (``$r<n>``, ``retain=true``)."""

    code = "set_names_removed"


class RunLiveStepsOff(SubstrateError):
    """The operator turned `run`'s live steps off on this server (#3666,
    lane ri); a live call sent on its own is unaffected."""

    code = "run_live_steps_off"


class RunLiveBusy(SubstrateError):
    """This session already has a run whose live step is calling with
    `each`, or its reader has two; one per reader and session, two per
    reader (#3666, lane ri)."""

    code = "run_live_busy"


class RunLiveStepRefused(SubstrateError):
    """A `run` live step refused: before any upstream call, its relation's
    gate denied it, or it would pass a run's limit (values, calls), or the
    column it reads holds no value; or, without `each`, its one call gave
    no answer (#3666, lane ri)."""

    code = "run_live_step_refused"


class RunRefusal(Exception):
    """A step (or the whole run) refused before any call was sent."""

    def __init__(self, message: str, fix_hint: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.fix_hint = fix_hint


@dataclass
class Step:
    index: int
    operator: str
    arguments: dict[str, Any]
    show: bool = True
    #: What the step is (#3666, lane ri): ``graph`` (a read tool over the
    #: graph), ``live`` (an expand that calls a live relation), ``values``,
    #: ``set`` (minus, intersect) or ``rows`` (filter, map or aggregate over
    #: the rows of an earlier step that holds rows). Set by
    #: :func:`classify_steps`.
    kind: str = "graph"

    @property
    def holds_rows(self) -> bool:
        """Whether the run holds this step's rows itself (not as SQL)."""
        return self.kind in ROW_KINDS


#: Step kinds whose rows the run holds in its own memory.
ROW_KINDS = ("live", "values", "set", "rows")


def parse_steps(steps: Any) -> list[Step]:
    """The step list, checked; raises :class:`RunRefusal`. A step with no
    ``show`` prints rows only when it is the last step."""
    example = (
        '[{"filter": {"where": {"type": "<type>"}}}, '
        '{"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}}]'
    )
    if not isinstance(steps, list) or not steps:
        raise RunRefusal("run: steps is a list of calls", "steps=" + example)
    if len(steps) > MAX_STEPS:
        raise RunRefusal(
            f"run: at most {MAX_STEPS} steps; this call has {len(steps)}",
            "split the steps into two run calls",
        )
    parsed: list[Step] = []
    for index, raw in enumerate(steps, start=1):
        if not isinstance(raw, Mapping):
            raise RunRefusal(f"run: step {index} is not an object", "steps=" + example)
        show = raw.get("show", index == len(steps))
        if not isinstance(show, bool):
            raise RunRefusal(f"run: step {index}: show is true or false")
        names = [key for key in raw if key not in ("show", "on")]
        if len(names) != 1 or names[0] not in STEP_OPERATORS:
            raise RunRefusal(
                f"run: step {index} names {', '.join(map(str, names)) or 'no tool'}; "
                f"a step is one of {', '.join(STEP_OPERATORS)} with its arguments",
                'e.g. {"filter": {"where": {"type": "<type>"}}, "show": false}',
            )
        operator = names[0]
        if operator in SET_OPERATORS:
            parsed.append(Step(index, operator, _set_arguments(index, operator, raw), show, "set"))
            continue
        if "on" in raw:
            raise RunRefusal(
                f"run: step {index}: `on` belongs to a minus or intersect step",
                'drop `on`, or write {"minus": ["$1", "$2"], "on": "<column>"}',
            )
        arguments = raw[operator]
        if operator == "values":
            parsed.append(Step(index, operator, _values_arguments(index, arguments), show, "values"))
            continue
        if not isinstance(arguments, Mapping):
            raise RunRefusal(f"run: step {index}: the arguments of {operator} are an object")
        if "next" in arguments:
            raise RunRefusal(
                f"run: step {index}: `next` continues a reply on its own; "
                f"call {operator}(next=<token>) outside run",
            )
        parsed.append(Step(index, operator, dict(arguments), show))
    return parsed


def _set_arguments(index: int, operator: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """``{"inputs": [n, ...], "on": [column per input]}`` of a set step."""
    example = f'{{"{operator}": ["$1", "$2"], "on": "<column>"}}'
    inputs = raw[operator]
    if not isinstance(inputs, list) or not all(isinstance(item, str) for item in inputs):
        raise RunRefusal(
            f"run: step {index}: {operator} takes a list of earlier steps",
            f"e.g. {example}",
        )
    if not MIN_SET_INPUTS <= len(inputs) <= MAX_SET_INPUTS:
        raise RunRefusal(
            f"run: step {index}: {operator} takes {MIN_SET_INPUTS} to "
            f"{MAX_SET_INPUTS} steps; this one names {len(inputs)}",
            f"e.g. {example}",
        )
    numbers: list[int] = []
    for item in inputs:
        ref = _parse_ref(item)
        if ref is None or not is_step_key(ref[0]) or ref[1] is not None:
            raise RunRefusal(
                f"run: step {index}: {operator} names {item!r}; it names whole "
                'steps, as "$n"',
                f"e.g. {example}",
            )
        numbers.append(int(ref[0]))
    on = raw.get("on")
    columns = on if isinstance(on, list) else [on] * len(inputs)
    if (
        on is None or len(columns) != len(inputs)
        or not all(isinstance(c, str) and c for c in columns)
    ):
        raise RunRefusal(
            f"run: step {index}: {operator} compares rows on one column: "
            '`on` is a column name, or a list of one name per input',
            f"e.g. {example}",
        )
    return {"inputs": numbers, "on": [row_column(c) for c in columns]}


def _values_arguments(index: int, arguments: Any) -> dict[str, Any]:
    """``{"column": name, "values": [...]}`` of a values step."""
    example = '{"values": {"<column>": ["a", "b"]}}'
    if not (isinstance(arguments, Mapping) and len(arguments) == 1):
        raise RunRefusal(
            f"run: step {index}: values names one column and its values",
            f"e.g. {example}",
        )
    ((column, values),) = arguments.items()
    if not isinstance(column, str) or not column or not isinstance(values, list):
        raise RunRefusal(
            f"run: step {index}: values names one column and a list of its values",
            f"e.g. {example}",
        )
    if not 1 <= len(values) <= MAX_VALUES_STEP:
        raise RunRefusal(
            f"run: step {index}: a values step lists 1 to {MAX_VALUES_STEP:,} "
            f"values; this one lists {len(values):,}",
            "split the list, or read the values from an earlier step",
        )
    for value in values:
        if value is None or not isinstance(value, (str, int, float, bool)):
            raise RunRefusal(
                f"run: step {index}: a value is a string, a number or true/false; "
                f"{json.dumps(value, default=str)[:60]} is not",
            )
    return {"column": row_column(column), "values": list(values)}


def row_column(name: str) -> str:
    """The column of a row the run holds: `attrs.<column>` and the bare
    name are the same column (live rows are read as `attrs.<field>`
    elsewhere)."""
    return name[len("attrs."):] if name.startswith("attrs.") and len(name) > 6 else name


def live_body(step: Step) -> Optional[Mapping[str, Any]]:
    """The `live` object of an expand step that calls a live relation."""
    if step.operator != "expand":
        return None
    live = step.arguments.get("live")
    return live if isinstance(live, Mapping) else None


#: What a live step takes beside `live`: its anchor, and how many rows it
#: prints. The graph rows of an expand are not read in a run.
_LIVE_STEP_ARGUMENTS = frozenset({
    "input", "from", "live", "generation_id", "authority", "limit", "max_bytes",
})
_LIVE_KEYS = frozenset({"relation", "args", "each"})


def each_source(step: Step) -> Optional[tuple[str, Any]]:
    """``(argument, values)`` of a live step's `each`: values is a list, or
    ``(n, column)`` for ``"$n.<column>"``; None without `each`."""
    live = live_body(step) or {}
    each = live.get("each")
    if each is None:
        return None
    if not (isinstance(each, Mapping) and len(each) == 1):
        raise RunRefusal(
            f"run: step {step.index}: live.each names one argument and its values",
            'e.g. "each": {"<argument>": "$1.<column>"}',
        )
    ((argument, source),) = each.items()
    if isinstance(source, list):
        return str(argument), list(source)
    ref = _parse_ref(source)
    if ref is not None and is_step_key(ref[0]) and ref[1]:
        return str(argument), (int(ref[0]), row_column(ref[1]))
    if isinstance(source, Mapping) and "ref" in source:
        raise RunRefusal(
            f"run: step {step.index}: live.each names a kept answer; in a run, "
            "name the step that lists the values",
            f'e.g. "each": {{"{argument}": "$1.<column>"}}',
        )
    raise RunRefusal(
        f"run: step {step.index}: live.each.{argument} is a list of values or "
        '"$n.<column>", the values of an earlier step\'s column',
        f'e.g. "each": {{"{argument}": "$1.<column>"}}',
    )


def rows_input(step: Step) -> Optional[int]:
    """The step a filter, map or aggregate step reads rows from: a whole
    ``"$n"`` as its `input`; None otherwise."""
    if step.operator not in ("filter", "map", "aggregate"):
        return None
    ref = _parse_ref(step.arguments.get("input"))
    if ref is not None and is_step_key(ref[0]) and ref[1] is None:
        return int(ref[0])
    return None


def classify_steps(steps: list[Step]) -> None:
    """Set each step's `kind` and check what holds rows; raises
    :class:`RunRefusal` before any step runs.

    A live step's `each` and a set step's inputs name earlier steps that
    hold rows; a filter, map or aggregate whose `input` is ``"$n"`` reads
    step n's rows when step n holds rows. The limits (fan-out steps, values
    in a list) are checked here too, so nothing upstream is called for a
    run that cannot run whole."""
    fan_outs = 0
    for step in steps:
        live = live_body(step)
        if live is not None:
            step.kind = "live"
            _check_live_step(step, live)
            source = each_source(step)
            if source is not None:
                fan_outs += 1
                if fan_outs > MAX_FAN_OUT_STEPS:
                    raise RunRefusal(
                        f"run: step {step.index}: a run takes at most "
                        f"{MAX_FAN_OUT_STEPS} steps with live.each",
                        "split the run",
                    )
                argument, values = source
                if isinstance(values, list):
                    if not 1 <= len(values) <= MAX_FAN_OUT_VALUES:
                        raise RunRefusal(
                            f"run: step {step.index}: live.each.{argument} lists "
                            f"{len(values):,} values; a step calls with 1 to "
                            f"{MAX_FAN_OUT_VALUES:,}",
                            "split the values over two runs",
                        )
                else:
                    _check_row_source(steps, step, values[0], f"live.each.{argument}")
            continue
        if step.kind == "set":
            for number in step.arguments["inputs"]:
                _check_row_source(steps, step, number, step.operator)
            continue
        if step.kind == "values":
            continue
        source_step = rows_input(step)
        if source_step is None:
            continue
        _check_earlier(steps, step, source_step, "input")
        if not steps[source_step - 1].holds_rows:
            raise RunRefusal(
                f"run: step {step.index}: input \"${source_step}\" names a step "
                "whose records are a graph set; a graph set is a condition",
                f'write "where": "${source_step}" in place of "input": "${source_step}"',
            )
        step.kind = "rows"
        for key, _field in _walk_refs([step.arguments.get(k) for k in _CONDITION_KEYS]):
            if is_step_key(key):
                raise RunRefusal(
                    f"run: step {step.index}: a condition over the rows of "
                    f"step {source_step} compares their columns; ${key} names "
                    "a set",
                    "compare columns, e.g. {\"attrs.<column>\": \"<value>\"}, "
                    "or compare two steps' rows with minus or intersect",
                )


def _check_earlier(steps: list[Step], step: Step, number: int, where: str) -> None:
    if not 1 <= number < step.index:
        raise RunRefusal(
            f"run: step {step.index} {where} names ${number}; a step names only "
            f"earlier steps of the same call (this call has steps 1 to {len(steps)})",
            "name an earlier step, or move the step after the one it reads",
        )


def _check_row_source(steps: list[Step], step: Step, number: int, where: str) -> None:
    _check_earlier(steps, step, number, where)
    if not steps[number - 1].holds_rows:
        raise RunRefusal(
            f"run: step {step.index} {where} names step {number}, whose records "
            "are a graph set; it reads the rows of a live, values, minus, "
            "intersect, or filter/map/aggregate-over-rows step",
            f'for a graph set\'s values write "${number}.attrs.<key>" in a '
            "condition's in-list",
        )


def _check_live_step(step: Step, live: Mapping[str, Any]) -> None:
    unknown = sorted(set(step.arguments) - _LIVE_STEP_ARGUMENTS)
    if unknown:
        raise RunRefusal(
            f"run: step {step.index}: a live step reads only its live relation; "
            f"it does not take {', '.join(unknown)}",
            "drop " + ", ".join(unknown) + "; the anchor goes in input",
        )
    extra = sorted(set(live) - _LIVE_KEYS)
    if extra:
        hint = (
            "drop retain: a run keeps a live step's rows for its later steps"
            if extra == ["retain"] else "live takes relation, args and each"
        )
        raise RunRefusal(
            f"run: step {step.index}: live does not take {', '.join(extra)} in a run",
            hint,
        )
    if not isinstance(live.get("relation"), str) or not live["relation"]:
        raise RunRefusal(
            f"run: step {step.index}: live names its relation",
            'e.g. "live": {"relation": "<relation>", "args": {...}}',
        )
    anchor = step.arguments.get("input", step.arguments.get("from"))
    if "input" in step.arguments and "from" in step.arguments:
        raise RunRefusal(f"run: step {step.index}: expand takes `from` or `input`, not both")
    if not (
        (isinstance(anchor, str) and anchor and _parse_ref(anchor) is None)
        or (isinstance(anchor, Mapping) and isinstance(anchor.get("node_id"), str))
    ):
        raise RunRefusal(
            f"run: step {step.index}: a live step's input is the record its "
            "relation is declared on, by id",
            'e.g. "input": {"node_id": "<id>"}',
        )


def row_references(step: Step) -> set[int]:
    """The earlier steps whose rows `step` reads."""
    if step.kind == "set":
        return set(step.arguments["inputs"])
    if step.kind == "live":
        source = each_source(step)
        if source is not None and not isinstance(source[1], list):
            return {source[1][0]}
        return set()
    if step.kind == "rows":
        number = rows_input(step)
        return {number} if number is not None else set()
    return set()


def _parse_ref(value: Any) -> Optional[tuple[str, Optional[str]]]:
    """``(key, field)`` for ``"$n"``/``"$n.<field>"`` (key ``"n"``) and for a
    retained set ``"$r7"``/``"$r7.<field>"`` (key ``"r7"``)."""
    if not isinstance(value, str):
        return None
    found = _REF.match(value)
    if found is None:
        return None
    return found.group(1), found.group(2)


def is_step_key(key: str) -> bool:
    return key.isdigit()


def is_retained_key(key: str) -> bool:
    return key.startswith("r")


def _walk_refs(value: Any) -> Iterator[tuple[str, Optional[str]]]:
    ref = _parse_ref(value)
    if ref is not None:
        yield ref
    elif isinstance(value, Mapping):
        for child in value.values():
            yield from _walk_refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_refs(child)


def _reference_places(operator: str, args: Mapping[str, Any]) -> list[Any]:
    """The arguments of a call where a reference can stand."""
    places = [args.get(key) for key in _CONDITION_KEYS]
    scope = args.get("input")
    if isinstance(scope, Mapping):
        places.append(scope.get("filter"))
    if operator == "expand":
        places.append(args.get("from"))
        places.append(args.get("input"))
    if operator == "inspect":
        target = args.get("target")
        if isinstance(target, Mapping):
            places.append(target.get("set"))
    return places


def references(operator: str, args: Mapping[str, Any]) -> dict[str, set[str]]:
    """``{key: fields}``: each set a call names, with the fields whose values
    it reads (empty for a plain set name)."""
    found: dict[str, set[str]] = {}
    for place in _reference_places(operator, args):
        for key, field in _walk_refs(place):
            found.setdefault(key, set())
            if field is not None:
                found[key].add(field)
    return found


def step_references(step: Step) -> dict[int, set[str]]:
    """``{n: fields}``: each step `step` names."""
    return {
        int(key): fields
        for key, fields in references(step.operator, step.arguments).items()
        if is_step_key(key)
    }


def check_references(steps: list[Step]) -> set[int]:
    """The steps a later step names; raises :class:`RunRefusal` before any
    step runs for a reference to step 0, to the step itself, to a later
    step, or past the last one, naming the steps that exist."""
    named: set[int] = set()
    for step in steps:
        for number in sorted(step_references(step)):
            if not 1 <= number < step.index:
                if step.index == 1:
                    exist = "step 1 has no earlier step to name"
                else:
                    exist = f"it may name steps 1 to {step.index - 1}"
                raise RunRefusal(
                    f"run: step {step.index} names ${number}; a step names only "
                    f"earlier steps of the same call, and {exist} (this call "
                    f"has steps 1 to {len(steps)})",
                    "name an earlier step, or move the step after the one it reads",
                )
            named.add(number)
    return named


# --------------------------------------------------------------------------- #
# the registry of one run's sets                                              #
# --------------------------------------------------------------------------- #


@dataclass
class ValueList:
    """The distinct values of one field over a set: ``raw_sql`` selects them
    as JSON. ``held`` is filled the first time a row check needs them."""

    raw_sql: str
    held: Optional[frozenset[Any]] = None


@dataclass
class StepSet:
    """One step's records as SQL: ``raw_sql`` selects their ids. Its
    parameters are bound into the text, so it runs as written and embeds
    into another statement (with ``%`` doubled, ``embed``). ``ids`` is
    filled the first time a row check needs it (a search set has it from
    the start)."""

    raw_sql: str
    ids: Optional[frozenset[str]] = None
    values: dict[str, ValueList] = dc_field(default_factory=dict)
    #: The node condition the set is, when a later step over the same
    #: records may read that condition in place of the subquery
    #: (``inlinable_predicate``, ``bind_condition``); None otherwise.
    predicate: Any = None


def embed(raw_sql: str) -> str:
    """`raw_sql` inside a statement that is executed with parameters."""
    return raw_sql.replace("%", "%%")


class RunSets:
    """The sets of one `run`, by step number (and internal names)."""

    def __init__(self) -> None:
        self.token = secrets.token_hex(8)
        self.sets: dict[str, StepSet] = {}

    def set_marker(self, key: str) -> str:
        return f"{_SET_PREFIX}{self.token}:{key}"

    def values_marker(self, key: str, field: str) -> str:
        return f"{_VALUES_PREFIX}{self.token}:{key}:{field}"


_ACTIVE: ContextVar[Optional[RunSets]] = ContextVar("okg_run_sets", default=None)


def active_registry() -> Optional[RunSets]:
    return _ACTIVE.get()


@contextmanager
def registry_scope(sets: RunSets) -> Iterator[RunSets]:
    token = _ACTIVE.set(sets)
    try:
        yield sets
    finally:
        _ACTIVE.reset(token)


def _marker(node: Any) -> Optional[tuple[StepSet, Optional[str]]]:
    """``(the set, the field read or None)`` when `node` is a marker leaf of
    the active run; None for every other node."""
    if not isinstance(node, Mapping) or node.get("op") != "in":
        return None
    values = node.get("values")
    if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], str):
        return None
    value = values[0]
    sets = _ACTIVE.get()
    if sets is None:
        return None
    if value.startswith(_SET_PREFIX):
        token, _sep, key = value[len(_SET_PREFIX):].partition(":")
        if token != sets.token or key not in sets.sets:
            return None
        return sets.sets[key], None
    if value.startswith(_VALUES_PREFIX):
        token, _sep, rest = value[len(_VALUES_PREFIX):].partition(":")
        key, _sep, field = rest.partition(":")
        if token != sets.token or key not in sets.sets or field not in sets.sets[key].values:
            return None
        return sets.sets[key], field
    return None


def mentions_marker(value: Any) -> bool:
    """Whether `value` holds a marker string of any run, active or not: a
    page continued outside the run it came from holds one."""
    if isinstance(value, str):
        return value.startswith((_SET_PREFIX, _VALUES_PREFIX))
    if isinstance(value, Mapping):
        return any(mentions_marker(child) for child in value.values())
    if isinstance(value, list):
        return any(mentions_marker(child) for child in value)
    return False


def stale_marker_refusal() -> RunRefusal:
    return RunRefusal(
        "this call names the set of a run step that is not open here: "
        + SET_LASTS_ONE_RUN,
        "send the run again; a cut step's reply gives the run call that "
        "prints its next rows",
    )


def unresolved_marker(value: Any) -> bool:
    """Whether `value` names a set that is not open here: a marker of
    another (or a finished) run, or one outside its own leaf."""
    if isinstance(value, str):
        return value.startswith((_SET_PREFIX, _VALUES_PREFIX))
    if isinstance(value, Mapping):
        if names_run_values(value.get("values")) and value.get("op") == "in":
            return _marker(value) is None
        return any(unresolved_marker(child) for child in value.values())
    if isinstance(value, list):
        return any(unresolved_marker(child) for child in value)
    return False


def bind_connection(conn: Any) -> Any:
    return _CONNECTION.set(conn)


def unbind_connection(token: Any) -> None:
    _CONNECTION.reset(token)


def holds_marker(predicate: Any) -> bool:
    """Whether `predicate` holds a marker leaf of the active run."""
    if _ACTIVE.get() is None:
        return False
    if _marker(predicate) is not None:
        return True
    if isinstance(predicate, Mapping):
        return any(holds_marker(child) for child in predicate.values())
    if isinstance(predicate, list):
        return any(holds_marker(child) for child in predicate)
    return False


def _sql_text(value: str) -> str:
    """A SQL string literal, for a statement executed with parameters."""
    if "\x00" in value:
        raise ValueError("a key holding NUL names no stored attribute")
    return "'" + value.replace("'", "''").replace("%", "%%") + "'"


#: Returned by :func:`lower_marker` for a node that is not a marker leaf.
NOT_A_MARKER = object()

#: Set by :func:`mark_negated` on a marker leaf that stands under an odd
#: number of ``not``: its SQL is then read negated.
NEGATED_KEY = "run_negated"


def mark_negated(predicate: Any, negated: bool = False) -> Any:
    """A copy of `predicate` whose marker leaves under an odd number of
    ``not`` carry ``NEGATED_KEY``, so :func:`lower_marker` can choose the
    form that is fast when negated. Every other node is copied unchanged."""
    if isinstance(predicate, list):
        return [mark_negated(child, negated) for child in predicate]
    if not isinstance(predicate, Mapping):
        return predicate
    if _marker(predicate) is not None:
        leaf = {k: v for k, v in predicate.items() if k != NEGATED_KEY}
        if negated:
            leaf[NEGATED_KEY] = True
        return leaf
    flips = predicate.get("op") == "not"
    return {
        key: mark_negated(value, negated != (flips and key == "clause"))
        for key, value in predicate.items()
    }


def _membership(target: str, values_sql: str, *, indexed: bool) -> str:
    """``target`` in the null-free `values_sql`, never null: a null `target`
    is false, so ``not`` reads it as true (as ``COALESCE(.. IN .., FALSE)``
    did). `indexed` (a positive test of an indexed column) probes the index
    with an array; otherwise the test is ``IN``, which the planner hashes."""
    if indexed:
        return f"({target} IS NOT NULL AND {target} = ANY(ARRAY({values_sql})))"
    return f"({target} IS NOT NULL AND {target} IN ({values_sql}))"


def lower_marker(
    node: Any, *, kind: str, columns: Mapping[str, str], relation: str,
    attrs_sql: str, readable: bool,
) -> Any:
    """The SQL of a marker leaf over one relation, None when it cannot be
    lowered exactly (the compile then refuses), or ``NOT_A_MARKER``.

    Every form is ``x IS NOT NULL AND x <in> (<set without nulls>)``, so it
    is never null and reads under ``not`` as ``COALESCE(x IN (<set>),
    FALSE)`` did; `x` is null for an expand's far end that is not in the
    view (a LEFT JOIN). The former COALESCE hid `x` from its index, so every
    statement over a set read every record (issue #3666). A positive test of
    ``target_id`` (the indexed record id) is ``x = ANY(ARRAY(..))``, which
    the planner probes the index with; a negated one (``NEGATED_KEY``,
    :func:`mark_negated`) or one of another field is ``x IN (..)``, which it
    hashes: ``ANY`` over an array is not hashed, so it costs rows times the
    set's size where no index is used."""
    found = _marker(node)
    if found is None:
        return NOT_A_MARKER
    if readable or kind != "nodes":
        return None
    entry, value_field = found
    field = node.get("field")
    negated = node.get(NEGATED_KEY) is True
    rel = f"{relation}." if relation else ""
    if value_field is None:
        if field != "node_id":
            return None
        return _membership(
            f"{rel}target_id",
            f"SELECT run_set.v FROM ({embed(entry.raw_sql)}) AS run_set(v) "
            "WHERE run_set.v IS NOT NULL",
            indexed=not negated,
        )
    source = embed(entry.values[value_field].raw_sql)
    if isinstance(field, str) and field.startswith("attrs.") and field != "attrs.":
        return _membership(
            f"({attrs_sql} -> {_sql_text(field.split('.', 1)[1])})",
            f"SELECT run_values.v FROM ({source}) AS run_values(v) "
            "WHERE run_values.v IS NOT NULL AND jsonb_typeof(run_values.v) <> 'null'",
            indexed=False,
        )
    column = columns.get(field) if isinstance(field, str) else None
    if column is None:
        return None
    return _membership(
        f"{rel}{column}",
        f"SELECT run_values.v #>> '{{}}' FROM ({source}) AS run_values(v) "
        "WHERE jsonb_typeof(run_values.v) = 'string'",
        indexed=column == "target_id" and not negated,
    )


#: Condition keys a plain node condition may hold besides field names.
_PLAIN_KEYS = frozenset({"op", "field", "value", "values", "clause", "clauses"})
#: Tests that are not a plain comparison of a stored field.
_NOT_PLAIN_OPS = frozenset({"matches"})


def _plain_field(name: Any) -> bool:
    return isinstance(name, str) and (
        name in ("node_id", "type", "subtype")
        or (name.startswith("attrs.") and len(name) > len("attrs."))
    )


def _plain_condition(node: Any, depth: int = 0) -> bool:
    """Whether `node` compares only stored fields (the id, the type, an
    attribute): no text clause, no date meaning, nothing a read tool
    rewrites before it compiles a condition."""
    if depth > 32:
        return False
    if isinstance(node, list):
        return all(_plain_condition(child, depth + 1) for child in node)
    if not isinstance(node, Mapping):
        return not isinstance(node, (set, tuple))
    for key, value in node.items():
        if key == "op":
            if not isinstance(value, str) or value in _NOT_PLAIN_OPS:
                return False
        elif key == "field":
            if not _plain_field(value):
                return False
        elif key in ("clause", "clauses"):
            if not _plain_condition(value, depth + 1):
                return False
        elif key in ("value", "values"):
            continue
        elif _plain_field(key):
            # The short form: {"attrs.k": "v"} or {"attrs.k": {"op": ...}}.
            if isinstance(value, Mapping) and (
                "field" in value or not _plain_condition(value, depth + 1)
            ):
                return False
        else:
            return False
    return True


def inlinable_predicate(definition: Any) -> Any:
    """The condition of a node set a later step may read in place of the
    set's subquery, else None.

    Only a set of nodes whose whole condition compares stored fields: the
    later step's own compiler then reads exactly the records the subquery
    would list (one view of one graph version for the whole run), at the
    cost of the condition instead of the condition plus a scan of every
    record for membership (issue #3666)."""
    if not (isinstance(definition, tuple) and len(definition) == 3):
        return None
    tag, kind, predicate = definition
    if tag != "records" or kind != "nodes":
        return None
    if not isinstance(predicate, Mapping) or not predicate:
        return None
    return predicate if _plain_condition(predicate) else None


#: The tools whose condition reads a record set of one kind (`input`).
_SET_CONDITION_TOOLS = ("filter", "map", "aggregate")


def _reads_nodes(operator: str, args: Mapping[str, Any]) -> bool:
    """Whether a call's condition is read over every node record: `filter`,
    `map` or `aggregate` with no `input` (nodes is the default) or an
    `input` of kind nodes that names no single record or saved result."""
    if operator not in _SET_CONDITION_TOOLS:
        return False
    scope = args.get("input")
    if scope is None:
        return True
    return (
        isinstance(scope, Mapping) and scope.get("kind") == "nodes"
        and not any(key in scope for key in ("ref", "node_id", "start_node"))
    )


_CONNECTION: ContextVar[Any] = ContextVar("okg_run_connection", default=None)


@contextmanager
def connection_scope(conn: Any) -> Iterator[None]:
    """The connection a read call holds: a row check reads a set's ids or
    values on it, the first time it needs them."""
    token = _CONNECTION.set(conn)
    try:
        yield
    finally:
        _CONNECTION.reset(token)


def _fetch(raw_sql: str, *, values: bool) -> list[Any]:
    conn = _CONNECTION.get()
    if conn is None:
        raise RunRefusal(
            "a row check needs a set and no read connection is open",
            "send the call again",
        )
    keep = (
        " WHERE run_set.v IS NOT NULL AND jsonb_typeof(run_set.v) <> 'null'"
        if values else ""
    )
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT run_set.v FROM ({raw_sql}) AS run_set(v){keep} "
            f"LIMIT {int(MATERIALIZE_CAP) + 1}"
        )
        rows = [row[0] for row in cur.fetchall()]
    if len(rows) > MATERIALIZE_CAP:
        raise RunRefusal(_too_large_message(), _too_large_hint())
    return rows


def canonical(value: Any) -> Any:
    """A JSON value as jsonb equality compares it: a number by its value
    (1 equals 1.0), true apart from 1, a string apart from a number."""
    if value is None:
        return None
    return _canonical_json(value)


def _canonical_json(value: Any) -> Any:
    """Hashable, and equal exactly when jsonb equality holds, at any depth."""
    if value is None:
        return ("z",)
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, (int, float)):
        return ("n", Decimal(str(value)).normalize())
    if isinstance(value, str):
        return ("s", value)
    if isinstance(value, Mapping):
        return ("o", tuple(sorted((str(k), _canonical_json(v)) for k, v in value.items())))
    if isinstance(value, (list, tuple)):
        return ("a", tuple(_canonical_json(v) for v in value))
    return ("s", json.dumps(value, sort_keys=True, default=str))


def marker_matches(
    row: Mapping[str, Any], node: Any, field_value: Callable[[Mapping[str, Any], str], Any],
) -> Any:
    """The row check of a marker leaf, or ``NOT_A_MARKER``. It agrees with
    the SQL ``lower_marker`` writes. The set's ids or values are read once,
    on the call's own connection, when a row check first needs them; a set
    of more than ``MATERIALIZE_CAP`` is refused by name."""
    found = _marker(node)
    if found is None:
        return NOT_A_MARKER
    entry, value_field = found
    if value_field is None:
        if entry.ids is None:
            entry.ids = frozenset(str(v) for v in _fetch(entry.raw_sql, values=False))
        return str(row.get("node_id")) in entry.ids
    held = entry.values[value_field]
    if held.held is None:
        held.held = frozenset(canonical(v) for v in _fetch(held.raw_sql, values=True))
    field = str(node.get("field") or "")
    value = field_value(row, field)
    if value is None:
        return False
    if not field.startswith("attrs."):
        # A stored column compares with the string values only.
        return isinstance(value, str) and ("s", value) in held.held
    return canonical(value) in held.held


def _too_large_message() -> str:
    return (
        f"a set this call checks row by row holds more than "
        f"{MATERIALIZE_CAP:,} records or values"
    )


def _too_large_hint() -> str:
    return "narrow the earlier step, or count the set with aggregate instead"


def value_column(field: str) -> Optional[str]:
    """The stored column a value list reads for `field`, or None for an
    attribute (``attrs.<key>``); raises for a field it cannot read."""
    if field.startswith("attrs.") and len(field) > len("attrs."):
        return None
    if field in _VALUE_COLUMNS:
        return _VALUE_COLUMNS[field]
    raise RunRefusal(
        f"run: $n.{field} names a field a value list cannot read",
        "use $n.attrs.<key>, $n.node_id or $n.type",
    )


#: Most time one run step (or one call naming a set) may take, and one
#: whole run. Every statement opened under a budget gets what is left as its
#: statement_timeout.
STEP_BUDGET_MS = 5_000
RUN_BUDGET_MS = 15_000

_DEADLINE: ContextVar[Optional[float]] = ContextVar("okg_run_deadline", default=None)


class BudgetEvents:
    """What happened to one budget: whether a statement was cut by the
    budget (its timeout or its deadline watch), and whether the deadline
    watch sent a cancel."""

    def __init__(self) -> None:
        self.cancelled = False
        self.fired = False
        #: The budget's deadline (``time.monotonic``), set by ``deadline_scope``.
        self.deadline: Optional[float] = None


_EVENTS: ContextVar[Optional[BudgetEvents]] = ContextVar("okg_run_budget_events", default=None)


@contextmanager
def deadline_scope(deadline: float, events: Optional[BudgetEvents] = None) -> Iterator[None]:
    """Statements opened inside run until `deadline` (``time.monotonic``)."""
    token = _DEADLINE.set(deadline)
    if events is not None:
        events.deadline = deadline
    events_token = _EVENTS.set(events) if events is not None else None
    try:
        yield
    finally:
        if events_token is not None:
            _EVENTS.reset(events_token)
        _DEADLINE.reset(token)


def run_time_left_ms() -> Optional[int]:
    """What is left of the active run budget, in ms (never below 0); None
    outside one. Code that sets its own statement timeout caps it here, so
    no statement inside a run outlives the run's budget."""
    deadline = _DEADLINE.get()
    if deadline is None:
        return None
    import time

    return max(0, int((deadline - time.monotonic()) * 1000))


#: Once the deadline has passed, the watch sends a cancel again this often
#: (seconds) until it is stopped, so a statement started after the deadline
#: is cut too.
WATCH_REARM_S = 0.1


class DeadlineWatch:
    """Cancels what one connection runs from the run budget's deadline on.

    A budget sets each statement's timeout to what was left when its read
    opened, so a read that runs several statements could run past the
    deadline. From the deadline until ``stop``, the watch sends a cancel
    every ``WATCH_REARM_S`` seconds, so no statement on the connection runs
    more than about that long past the deadline (plus the cancel's own
    round trip); a statement cut so is reported as QueryCanceled.

    ``stop`` and a cancel take one lock: once ``stop`` returns, no cancel is
    sent. ``stop`` returns whether any cancel was sent; the connection's
    owner then closes it rather than return it to the pool, because a
    cancel the server has not yet acted on could cut whatever runs on that
    connection next."""

    def __init__(self, conn: Any, deadline: float, events: Optional[BudgetEvents]) -> None:
        import threading

        self._conn = conn
        self._deadline = deadline
        self._events = events
        self._lock = threading.Lock()
        self._stopped = False
        self.fired = False
        self._timer: Any = None
        self._arm()

    def _arm(self) -> None:
        import threading
        import time

        delay = max(0.0, self._deadline - time.monotonic())
        if self.fired:
            delay = WATCH_REARM_S
        timer = threading.Timer(delay, self._fire)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def _fire(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self.fired = True
            if self._events is not None:
                self._events.fired = True
            try:
                self._conn.cancel()
            except Exception:  # noqa: BLE001 - a connection already closed or broken has nothing running.
                pass
            self._arm()

    def stop(self) -> bool:
        """Stop the watch; whether it sent any cancel."""
        with self._lock:
            self._stopped = True
            if self._timer is not None:
                self._timer.cancel()
            return self.fired


def watch_deadline(conn: Any) -> Optional[DeadlineWatch]:
    """A ``DeadlineWatch`` on `conn` for the active run budget (None
    outside a run)."""
    deadline = _DEADLINE.get()
    if deadline is None:
        return None
    return DeadlineWatch(conn, deadline, _EVENTS.get())


#: A statement cancelled this close to the budget's deadline (seconds) was
#: cut by the budget: the statement timeout a budget sets is what was left
#: when the read opened, so it expires at the deadline.
BUDGET_CUT_SLACK_S = 0.05


def budget_cut(exc: BaseException, events: Optional[BudgetEvents] = None) -> bool:
    """Whether `exc` is the run budget cutting a read: the budget refused
    to start one (RunOverBudget), or a statement was cancelled (QueryCanceled)
    after the budget's deadline watch sent a cancel or once the deadline had
    come (`events`, by default the active budget's). Decided by those facts,
    never by the server's message text. A statement cancelled earlier, such
    as by a tool's own lower timeout, is not the budget's and keeps its own
    error."""
    if events is None:
        events = _EVENTS.get()
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RunOverBudget):
            return True
        if type(current).__name__ == "QueryCanceled":
            if events is None:
                return False
            if events.fired:
                return True
            import time

            return (
                events.deadline is not None
                and time.monotonic() >= events.deadline - BUDGET_CUT_SLACK_S
            )
        current = current.__cause__ or current.__context__
    return False


def note_cancelled(exc: BaseException) -> None:
    """Record that `exc` is a statement the run budget cut (``budget_cut``)."""
    events = _EVENTS.get()
    if events is None or _DEADLINE.get() is None:
        return
    if budget_cut(exc, events):
        events.cancelled = True


def restore_timeout(conn: Any, previous: str) -> bool:
    """Put a connection's statement_timeout back; on any failure close the
    connection (a pool discards a closed connection) and return False."""
    try:
        status = conn.info.transaction_status
        if getattr(status, "name", "IDLE") != "IDLE":
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (previous,))
        return True
    except Exception:  # noqa: BLE001 - a connection whose timeout cannot be restored is never reused.
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - already broken.
            pass
        return False


_COUNTING: ContextVar[bool] = ContextVar("okg_run_counting", default=False)


@contextmanager
def counting_scope() -> Iterator[None]:
    """A step's exact count is being read: it runs under the same budget as
    the step's call (tests name it to cut only the count)."""
    token = _COUNTING.set(True)
    try:
        yield
    finally:
        _COUNTING.reset(token)


def counting() -> bool:
    return _COUNTING.get()


def statement_budget_ms() -> Optional[int]:
    """What is left of the active run budget, in ms; None outside one.
    Spent, it is refused by name."""
    deadline = _DEADLINE.get()
    if deadline is None:
        return None
    import time

    left = int((deadline - time.monotonic()) * 1000)
    if left < 50:
        raise RunOverBudget(
            "this run step used its time budget "
            f"({STEP_BUDGET_MS:,} ms a step, {RUN_BUDGET_MS:,} ms a run)",
            fix_hint="narrow the step's condition, or split the run",
        )
    return left


# --------------------------------------------------------------------------- #
# what a step leaves for the steps after it                                   #
# --------------------------------------------------------------------------- #


@dataclass
class StepCapture:
    """Filled while one step runs: the definition of its set (by the read
    tool that served it) and its raw reply."""

    definition: Optional[tuple[Any, ...]] = None
    payload: Optional[dict[str, Any]] = None
    #: The set's condition holds the ids a text clause matched in place of
    #: that clause (``note_set(text=True)``): never inlined.
    text: bool = False


_CAPTURE: ContextVar[Optional[StepCapture]] = ContextVar("okg_run_capture", default=None)


@contextmanager
def capture_scope(capture: StepCapture) -> Iterator[StepCapture]:
    token = _CAPTURE.set(capture)
    try:
        yield capture
    finally:
        _CAPTURE.reset(token)


def active_capture() -> Optional[StepCapture]:
    return _CAPTURE.get()


def note_set(*definition: Any, text: bool = False) -> None:
    """A read tool states the set it read: ``("records", kind, predicate)``
    or ``("expand", node_id, edge_type, direction, far_end_filter)``. Only
    the outermost call of a step is recorded. `text`: a text clause was
    replaced by the ids it matched, so a later step reads the set as a
    membership subquery, not as that condition (``inlinable_predicate``)."""
    capture = _CAPTURE.get()
    if capture is not None and capture.definition is None:
        capture.definition = definition
        capture.text = text


def note_payload(payload: Any) -> None:
    capture = _CAPTURE.get()
    if capture is not None and isinstance(payload, dict) and capture.payload is None:
        capture.payload = payload


@dataclass
class LivePrepare:
    """A run's live step, prepared by `expand`'s own call (#3666, lane ri).

    The step is sent as ``expand(input=<anchor>, live=...)`` through the
    server's ``call_tool``, so it meets the principal gate, the session's
    tool surface, the argument model, the trace and the call log a call sent
    alone meets. While this holder is active, the byte-bound wrapper keeps
    the prepared live call here and returns before the graph step and
    before any adapter runs; the run then makes the calls itself."""

    prepared: Any = None


_LIVE_PREPARE: ContextVar[Optional[LivePrepare]] = ContextVar("okg_run_live_prepare", default=None)


@contextmanager
def live_prepare_scope(holder: LivePrepare) -> Iterator[LivePrepare]:
    token = _LIVE_PREPARE.set(holder)
    try:
        yield holder
    finally:
        _LIVE_PREPARE.reset(token)


def live_prepare_active() -> bool:
    """Whether the call being served is a run's live step being prepared."""
    holder = _LIVE_PREPARE.get()
    return holder is not None and holder.prepared is None


def keep_live_prepared(prepared: Any) -> bool:
    """Keep `prepared` for the run whose live step this call prepares;
    False outside one (the call then runs as sent)."""
    holder = _LIVE_PREPARE.get()
    if holder is None or holder.prepared is not None:
        return False
    holder.prepared = prepared
    return True


def payload_ids(payload: Mapping[str, Any]) -> list[str]:
    """The record ids a search or inspect reply returned, in order."""
    ids: list[str] = []
    for key in ("results", "items", "nodes"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, Mapping) and isinstance(row.get("node_id"), str):
                if row["node_id"] not in ids:
                    ids.append(row["node_id"])
    node = payload.get("node")
    if isinstance(node, Mapping) and isinstance(node.get("node_id"), str):
        ids.append(node["node_id"])
    return ids


# --------------------------------------------------------------------------- #
# a step with its references written as conditions the tools accept           #
# --------------------------------------------------------------------------- #


def _set_leaf(sets: RunSets, key: str, inline: bool = False) -> dict[str, Any]:
    """The condition a whole ``$key`` is read as: its set's own condition
    when `inline` and the set has one (``inlinable_predicate``), else the
    marker leaf ``lower_marker`` reads as a membership subquery.

    Both select the same records: a call that reads node records (`inline`)
    reads them through the same view, at the same graph version, as the
    set's own step did. The marker leaf lowers two-valued
    (``x IS NOT NULL AND x <in> (..)``, never null) and so does a plain condition, so the
    two agree under ``not`` too. The condition costs what the step's own
    condition costs; the subquery costs the set's own query plus one index
    probe per id (issue #3666)."""
    entry = sets.sets.get(key)
    if inline and entry is not None and entry.predicate is not None:
        return copy.deepcopy(entry.predicate)
    return {"op": "in", "field": "node_id", "values": [sets.set_marker(key)]}


def _values_list(
    sets: RunSets, value: Any, where: str, binds: Callable[[str], bool],
) -> Optional[list[Any]]:
    """`[marker]` for a ``$n.<field>`` value (alone or as a one-item list),
    None when `value` holds no reference this binding reads."""
    items = value if isinstance(value, list) else [value]
    refs = [_parse_ref(item) for item in items]
    if not any(ref is not None and binds(ref[0]) for ref in refs):
        return None
    if len(items) != 1:
        raise RunRefusal(
            f"{where}: a value list from a set stands alone in its list",
            'e.g. "attrs.key": "$1.attrs.key"',
        )
    key, field = refs[0]  # type: ignore[misc]
    if field is None:
        raise RunRefusal(
            f"{where}: ${key} is a set; as a field's values write "
            f"${key}.<field>, or use ${key} as a whole condition",
            f'e.g. {{"op": "and", "clauses": ["${key}", {{"type": "<type>"}}]}}',
        )
    return [sets.values_marker(key, field)]


def expand_from_set_sentence(key: str) -> str:
    """The one wording for a set written as expand's start in a form the
    link join does not read (a field's values, a set inside an object)."""
    return (
        f"expand from a set takes ${key} itself as from, with one link type "
        "and direction in or out, inside a run call"
    )


def join_source(step: Step) -> Optional[int]:
    """The step an expand step joins from: a whole ``"$n"`` as its `from`
    (or `input`), else None."""
    if step.operator != "expand":
        return None
    for key in ("from", "input"):
        ref = _parse_ref(step.arguments.get(key))
        if ref is not None and is_step_key(ref[0]) and ref[1] is None:
            return int(ref[0])
    return None


def unread_reference(where: str, key: str) -> RunRefusal:
    """The refusal for ``$<key>`` written where no set is read: a set
    stands only in the places this module binds."""
    return RunRefusal(
        f"{where}: ${key} stands where no set is read",
        f'a set is a whole condition ("where": "${key}"), the values of a '
        f'field ({{"attrs.<key>": "${key}.attrs.<key>"}}), or inspect\'s '
        f'{{"kind": "nodes", "set": "${key}"}}',
    )


#: The short-form tests (`{"<field>": {"in": ...}}`) a value list may fill.
_VALUE_LIST_OPS = ("in", "eq")


def _bound_operator_object(
    sets: RunSets, key: str, value: Mapping[str, Any], where: str,
    binds: Callable[[str], bool], notes: Optional[list[dict[str, str]]],
) -> Optional[dict[str, Any]]:
    """`{"in": "$n.<field>"}` under a short-form key, bound; None when the
    object holds no reference this binding reads.

    The test is written with its operator as the key, as the read grammar
    reads it only once it has an `op`. It can mean one thing, so it is
    served as `{"op": "in", "values": [marker]}` and the reply says so.
    """
    if len(value) != 1:
        return None
    (op, operand), = value.items()
    if not any(binds(ref) for ref, _field in _walk_refs(operand)):
        return None
    if op not in _VALUE_LIST_OPS:
        raise RunRefusal(
            f"{where}: a value list goes in `in` (or `eq`), not `{op}`",
            f'e.g. {{"{key}": {{"op": "in", "values": ["$<n>.<field>"]}}}}',
        )
    marker = _values_list(sets, operand, where, binds)
    if marker is None:
        return None
    if notes is not None:
        notes.append({
            "rule": "operator_as_key",
            "sent": f'{{"{key}": {{"{op}": ...}}}}',
            "served": f'{{"{key}": {{"op": "in", "values": ...}}}}',
        })
    return {"op": "in", "values": marker}


def bind_condition(
    sets: RunSets, node: Any, where: str, binds: Callable[[str], bool] = is_step_key,
    notes: Optional[list[dict[str, str]]] = None, *, inline: bool = False,
) -> Any:
    """`node` with each reference `binds` accepts written as a marker leaf
    (or, with `inline`, as its set's plain condition: ``_set_leaf``).
    A test that `notes` must disclose (read as another spelling) adds an
    ``adjusted`` record there."""
    ref = _parse_ref(node)
    if ref is not None:
        key, field = ref
        if not binds(key):
            return node
        if field is not None:
            raise RunRefusal(
                f"{where}: ${key}.{field} is a list of values; it goes "
                "where a field's value goes",
                f'e.g. {{"attrs.<key>": "${key}.{field}"}}',
            )
        return _set_leaf(sets, key, inline)
    if not isinstance(node, Mapping):
        return node
    op = node.get("op")
    if op in ("and", "or"):
        clauses = node.get("clauses")
        if isinstance(clauses, list):
            return {**node, "clauses": [
                bind_condition(sets, c, where, binds, notes, inline=inline) for c in clauses
            ]}
        return dict(node)
    if op == "not":
        return {**node, "clause": bind_condition(
            sets, node.get("clause"), where, binds, notes, inline=inline,
        )}
    if op is not None or "field" in node:
        out = dict(node)
        if "values" in node:
            marker = _values_list(sets, node["values"], where, binds)
            if marker is not None:
                out["values"] = marker
        if "value" in node:
            marker = _values_list(sets, node["value"], where, binds)
            if marker is not None:
                if op not in (None, "eq"):
                    raise RunRefusal(
                        f"{where}: a value list goes in `in` (or `eq`), not `{op}`"
                    )
                out.pop("value")
                out["op"] = "in"
                out["values"] = marker
        return out
    out = {}
    for key, value in node.items():
        if isinstance(value, Mapping):
            if "op" in value:
                inner = bind_condition(sets, {"field": key, **value}, where, binds, notes)
                value = {k: v for k, v in inner.items() if k != "field"}
            else:
                bound = _bound_operator_object(sets, key, value, where, binds, notes)
                if bound is not None:
                    value = bound
            out[key] = value
            continue
        marker = _values_list(sets, value, where, binds)
        out[key] = value if marker is None else marker
    return out


def bind_call(
    operator: str, arguments: Mapping[str, Any], sets: RunSets, *,
    where: str, inspect_ids: Callable[[str], list[str]],
    binds: Callable[[str], bool] = is_step_key,
    notes: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    """The arguments a call is sent with, its references (those `binds`
    accepts) written as conditions; raises :class:`RunRefusal`.

    A reference `binds` accepts that is still there once the call is bound
    stands where no set is read; it is refused by name here, never sent on
    to be read as text."""
    args = _bind_call(
        operator, arguments, sets, where=where, inspect_ids=inspect_ids,
        binds=binds, notes=notes,
    )
    for key in sorted(references(operator, args)):
        if binds(key):
            raise unread_reference(where, key)
    return args


def _bind_call(
    operator: str, arguments: Mapping[str, Any], sets: RunSets, *,
    where: str, inspect_ids: Callable[[str], list[str]],
    binds: Callable[[str], bool], notes: Optional[list[dict[str, str]]],
) -> dict[str, Any]:
    args = dict(arguments)
    if operator == "search":
        if any(
            binds(key) for place in (args.get(k) for k in _CONDITION_KEYS)
            for key, _field in _walk_refs(place)
        ):
            raise RunRefusal(
                f"{where}: search ranks words and does not take a set as "
                "a condition",
                "list the set's records with filter or map instead",
            )
        return args
    if operator == "inspect":
        target = args.get("target")
        if isinstance(target, Mapping) and "set" in target:
            ref = _parse_ref(target.get("set"))
            if ref is not None and not binds(ref[0]):
                return args
            if ref is None or ref[1] is not None or target.get("kind") != "nodes":
                raise RunRefusal(
                    f"{where}: inspect reads a set as "
                    '{"kind": "nodes", "set": "$n"}',
                )
            rest = {k: v for k, v in target.items() if k != "set"}
            args["target"] = {**rest, "node_ids": inspect_ids(ref[0])}
        return args
    if operator == "expand":
        start = args.get("from")
        ref = _parse_ref(start)
        # expand starts from one record; a set in `input` ("$1", "$1.node_id"
        # or inside an input object) is the same request as a set in `from`.
        in_input = any(binds(key) for key, _field in _walk_refs(args.get("input")))
        if (ref is not None and binds(ref[0])) or in_input:
            # A whole "$n" as `from` (or `input`) is the link join, which
            # run serves before binding; any other spelling of a set here
            # is refused with the one that works.
            key = ref[0] if ref is not None and binds(ref[0]) else next(
                k for k, _f in _walk_refs(args.get("input")) if binds(k)
            )
            raise RunRefusal(
                f"{where}: {expand_from_set_sentence(key)}",
                f'e.g. {{"expand": {{"from": "${key}", "edge_type": "<link>", '
                '"direction": "out"}}',
            )
        if start is not None and ref is None:
            if "input" in args:
                raise RunRefusal(f"{where}: expand takes `from` or `input`, not both")
            args.pop("from")
            args["input"] = start
    inline = _reads_nodes(operator, args)
    for key in _CONDITION_KEYS:
        if key in args:
            args[key] = bind_condition(
                sets, args[key], f"{where} {key}", binds, notes, inline=inline,
            )
    scope = args.get("input")
    if isinstance(scope, Mapping) and "filter" in scope:
        args["input"] = {
            **scope,
            "filter": bind_condition(
                sets, scope["filter"], f"{where} input.filter", binds, notes,
                inline=inline,
            ),
        }
    return args


def bind_step(
    step: Step, sets: RunSets, *, inspect_ids: Callable[[int], list[str]],
    notes: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    """A run step's arguments with its references to earlier steps bound;
    a removed set name (``$r7``) is left for the step's own call, which
    refuses it by name."""
    return bind_call(
        step.operator, step.arguments, sets, where=f"run: step {step.index}",
        inspect_ids=lambda key: inspect_ids(int(key)), notes=notes,
    )


def names_run_values(values: Any) -> bool:
    """Whether an ``in`` clause's `values` is a run's value-list marker: a
    candidate index must not read it as a literal."""
    return (
        isinstance(values, list) and len(values) == 1
        and isinstance(values[0], str)
        and values[0].startswith((_SET_PREFIX, _VALUES_PREFIX))
    )
