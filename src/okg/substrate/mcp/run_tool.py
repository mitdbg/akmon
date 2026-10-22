"""The `run` tool: several read calls in one, composed (issue #3666).

``serve_run`` is installed on the OKG server as ``_okg_run_handler`` and is
reached from ``_RawResultFastMCP.call_tool`` after the `run` call itself
passed the principal gate, the session's tool surface and its argument
model. Each step is then sent through the server's own ``call_tool``, so it
meets every check, rewrite, byte bound and access check a call sent alone
meets. What `run` adds is in ``run_steps``: references to earlier steps of
the same call, written as conditions the read tools take, and kept as SQL
by ``RunBackend`` between steps.

The rule: a step reads its whole set or is refused with the exact count.

* A filter, map, aggregate or one-hop expand step's set is its whole
  condition; its reply begins ``[step n] <tool>: N records`` (``N records
  in G groups`` for aggregate), N from one COUNT over the set's SQL inside
  the step's time budget (refused ``run_count_not_finished`` past it).
* A grep, lexical or attrs search step that a later step names reads its
  whole match set (at most ``RUN_ROW_CAP`` records, counted); a ranked
  search a later step names is refused by name; one no later step names
  prints its page with a not-a-total line.
* A printed step prints at most ``RUN_ROW_CAP`` rows (records, or groups),
  else it is refused with its count and two calls that are served. `limit`
  and `offset` bound only what a step prints.

Every step reads the graph version the run pinned before its first step.
By default only the last step prints rows. Printed steps share
``max_bytes``: a step whose rows do not fit its share prints the rows that
fit and one cut line whose next call is the same run at the pinned
generation with that step's offset moved past the rows shown. A refused
step prints its refusal; a step that names it is skipped and says so; the
others run. Any failure inside a step is that step's refusal.

``guard_call`` is the server's hook for every read call: no set name
outlives its run call, so ``$r<n>`` and ``retain=true`` set names are
refused by name, and ``$n`` outside a run says where it works.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
import uuid
from collections.abc import Mapping
from typing import Any, Optional, Protocol

from mcp.types import CallToolResult, TextContent

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import run_steps
from okg.substrate.mcp.response_bounds import (
    DEFAULT_RESPONSE_BYTES,
    MIN_RESPONSE_BYTES,
    InvalidOperatorArguments,
    resolve_budget,
    served_call_tool_result,
)
from okg.substrate.mcp.result_text import (
    compact_json,
    current_result_context,
    cut_line,
    page_rows_key,
    render,
)

__all__ = ["RunBackend", "guard_call", "serve_run"]

_log = logging.getLogger(__name__)

#: The tools whose step set is their whole condition (``run_steps.note_set``).
_CONDITION_TOOLS = ("filter", "map", "aggregate", "expand")
#: Search methods whose whole match set one call reads and counts
#: (``completeness="exact"``).
WHOLE_SET_SEARCH_METHODS = ("lexical", "grep", "attrs")
#: Bytes kept for the run's first line and each step's own lines.
_RESERVE = 200
_RESERVE_PER_STEP = 160
#: The smallest share a printed step may get; below it the run is refused
#: with the max_bytes that gives every printed step this much.
MIN_STEP_SHARE = 512
#: Keys of a step's page that the run states itself: every step is read at
#: the generation the run pinned, so a step's own `pin_scope`
#: (`explicit_call`, from the generation the run passes it) would say the
#: caller pinned it (lane ri, item 8). The JSON reply names the generation.
_RUN_PINNED_KEYS = ("pin_scope",)
#: The text line a step's own reply prints for that key.
_PIN_SCOPE_LINE = "! pin scope:"

#: Keys of a step's page that continue it outside the run. A step page is
#: continued by the run's own next call, so they are not printed.
_CONTINUATION_KEYS = (
    "next", "next_offset", "next_offset_reason", "next_offset_remedy",
    "continuation_token", "continuation_withheld", "search_next",
    "truncation_reason", "truncation_remedy", "response_byte_budget",
    "truncated", "pagination", "pagination_hint", "result_reference",
    "not_a_total",
)


class RunBackend(Protocol):
    """The server's side of `run`: each method opens its own read at the
    run's graph version and is called off the event loop."""

    def refused_reason(self) -> Optional[str]: ...

    def current_generation(self, authority: Any) -> Any: ...

    def step_set(
        self, definition: tuple[Any, ...], sets: run_steps.RunSets, key: str,
        generation: Any, authority: Any,
    ) -> str: ...

    def ids_set(self, ids: list[str], generation: Any, authority: Any) -> str: ...

    def values_sql(
        self, sets: run_steps.RunSets, key: str, field: str,
        generation: Any, authority: Any,
    ) -> str: ...

    def materialize(
        self, raw_sql: str, generation: Any, authority: Any, cap: int,
    ) -> Optional[list[Any]]: ...

    def count(self, raw_sql: str, generation: Any, authority: Any) -> int: ...

    def join_far(
        self, start_sql: str, edge_type: str, direction: str, generation: Any, authority: Any,
    ) -> str: ...

    def join_facts(
        self, start_sql: str, edge_type: str, direction: str, ids_shown: int,
        generation: Any, authority: Any,
    ) -> tuple[int, int, list[str]]: ...

    def row_matches(self, row: dict[str, Any], predicate: Any) -> bool:
        """Whether a row the run holds meets `predicate` (a condition on
        `attrs.<column>`), as a kept live answer's rows are checked. Pure:
        no database."""
        ...


def _refusal(
    code: str, message: str, fix_hint: str = "", *, tool: str = "run",
    data: Optional[dict[str, Any]] = None,
) -> CallToolResult:
    payload: dict[str, Any] = {
        "status": "refused",
        "operator_name": tool,
        "error": {"code": code, "message": message},
    }
    if fix_hint:
        payload["error"]["fix_hint"] = fix_hint
    if data:
        payload["error"]["data"] = data
    return served_call_tool_result(payload, is_error=True, tool=tool)


def _alone_call(step: Optional[run_steps.Step], generation: Any) -> Optional[dict[str, Any]]:
    """The step as its own call, outside run's budget and at the run's graph
    version; None when it names an earlier step's set (it cannot stand
    alone) or there is no step."""
    if step is None or run_steps.step_references(step):
        return None
    arguments = dict(step.arguments)
    if generation is not None:
        arguments["generation_id"] = generation
    return {"tool": step.operator, "arguments": arguments}


def _over_budget(
    tool: str, step: Optional[run_steps.Step] = None, generation: Any = None,
) -> CallToolResult:
    """The refusal of a step whose read its time budget cut. A step that
    names no earlier set is offered as its own call, which runs under the
    deployment's own statement timeout instead of the step's."""
    alone = _alone_call(step, generation)
    if alone is None:
        hint = (
            "narrow the step's condition, or split the run; no call is "
            "offered: the step reads an earlier step's set"
            if step is not None else "narrow the step's condition, or split the run"
        )
        data = None
    else:
        hint = (
            "narrow the step's condition, or send the step on its own: "
            f"{call_text(alone)}"
        )
        data = {"retry_calls": [alone]}
    return _refusal(
        run_steps.RunOverBudget.code,
        f"{tool}: this step used its time budget "
        f"({run_steps.STEP_BUDGET_MS:,} ms a step, "
        f"{run_steps.RUN_BUDGET_MS:,} ms a run)",
        hint,
        tool=tool,
        data=data,
    )


def _failure(
    tool: str, exc: BaseException, events: Optional[run_steps.BudgetEvents] = None,
    step: Optional[run_steps.Step] = None, generation: Any = None,
) -> CallToolResult:
    """An unexpected failure inside one call, as that call's refusal."""
    if isinstance(exc, run_steps.RunRefusal):
        return _refusal("invalid_operator_arguments", exc.message, exc.fix_hint, tool=tool)
    if isinstance(exc, run_steps.RunOverBudget):
        return _over_budget(tool, step, generation)
    if isinstance(exc, SubstrateError):
        return _refusal(exc.code, exc.message, exc.fix_hint or "", tool=tool, data=exc.data)
    if run_steps.budget_cut(exc, events):
        # A read run makes for the step (its set, values, a join) cut by
        # the step's own budget: the budget refusal, as for the step's call.
        # A statement cancelled for any other reason keeps its own error.
        return _over_budget(tool, step, generation)
    error_id = uuid.uuid4().hex[:12]
    _log.error(
        "%s: a step of run failed error_id=%s", tool, error_id, exc_info=exc,
        extra={"okg_error_id": error_id, "okg_operator": tool},
    )
    return _refusal(
        "operator_execution_failed",
        f"{tool}: this step failed on the server (error id {error_id})",
        "send the step on its own to see its own refusal",
        tool=tool,
    )


def _text(result: Any) -> str:
    content = getattr(result, "content", None) or []
    return "\n".join(
        block.text for block in content if isinstance(block, TextContent)
    )


def _served(server: Any, name: str) -> bool:
    """Whether this session can call `name` now."""
    visible = getattr(server, "okg_visible_tools", None)
    seen = visible() if callable(visible) else None
    if seen is not None:
        return name in seen
    manager = getattr(server, "_tool_manager", None)
    return manager is not None and manager.get_tool(name) is not None


def _plural(n: int, word: str) -> str:
    return f"{n:,} {word}" + ("" if n == 1 else "s")


def _int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class _StepOutcome:
    def __init__(self, step: run_steps.Step) -> None:
        self.step = step
        self.status = "skipped"
        #: The served text of a refusal, or of a step printed as served.
        self.text = ""
        self.needs: Optional[int] = None
        self.capture = run_steps.StepCapture()
        #: The step's page as the tool returned it (bounded), when served.
        self.payload: Optional[dict[str, Any]] = None
        #: The step's exact count: records (`count`) and, for aggregate,
        #: groups. None for a step whose set is not exact.
        self.count: Optional[int] = None
        self.groups: Optional[int] = None
        #: What a printed exact step's rows count: groups, links or records.
        self.units: Optional[int] = None
        self.offset = 0
        self.limit: Optional[int] = None
        #: A search page no later step names: printed with a not-a-total line.
        self.ranked_page = False
        #: `adjusted` records of the binding (a reference read as another
        #: spelling), printed with the step's reply.
        self.adjusted: list[dict[str, str]] = []
        #: The tool the step is sent to: its own, or filter for a link join.
        self.tool = step.operator
        #: A link join's start and facts (``_join_call``), else None.
        self.join: Optional[dict[str, Any]] = None
        #: The rows the run holds for this step (``run_live.HeldRows``):
        #: a live, values, set, or filter/map/aggregate-over-rows step.
        self.held: Any = None

    @property
    def exact(self) -> bool:
        return self.count is not None

    def count_phrase(self) -> str:
        assert self.count is not None
        if self.groups is not None:
            return f"{_plural(self.count, 'record')} in {_plural(self.groups, 'group')}"
        return _plural(self.count, "record")

    def rows_expected(self) -> int:
        """R: the rows a printed exact step prints, before any byte cut."""
        assert self.units is not None
        rows = max(0, self.units - self.offset)
        return rows if self.limit is None else min(rows, self.limit)


def _unknown_arguments(server: Any, tool: str, arguments: Mapping[str, Any]) -> list[str]:
    """The arguments `tool` does not declare, as its own call refuses them:
    checked after the grammar rewrite, as a dispatched call is, so a
    spelling the rewrite reads (``where``) is never named. A call the
    rewrite refuses names none."""
    from okg.substrate.mcp.read_grammar import rewrite_call

    refuse = getattr(server, "_refuse_unknown_operator_arguments", None)
    if not callable(refuse):
        return []
    try:
        rewritten = rewrite_call(tool, dict(arguments)).arguments
    except Exception:  # noqa: BLE001 - a refused rewrite names no argument.
        return []
    if not isinstance(rewritten, Mapping):
        return []
    try:
        refuse(tool, dict(rewritten))
    except SubstrateError as exc:
        unknown = (exc.data or {}).get("unknown_arguments")
        if isinstance(unknown, list):
            return [str(key) for key in unknown]
    return []


def _first_step_hint(server: Any, raw_steps: Any) -> str:
    """The run's first step as its own call, for a run refused whole.

    The call is offered only when the server takes it as written
    (``_okg_takes_as_written``) and it names no earlier step: a hint never
    carries a call that would itself be refused. A step with an argument its
    tool does not declare names that argument instead."""
    general = "send each step as its own call, which the access rules check"
    if not (isinstance(raw_steps, list) and raw_steps and isinstance(raw_steps[0], Mapping)):
        return general
    names = [key for key in raw_steps[0] if key != "show"]
    if len(names) != 1 or not isinstance(raw_steps[0][names[0]], Mapping):
        return general
    tool, arguments = names[0], dict(raw_steps[0][names[0]])
    if tool not in run_steps.STEP_OPERATORS or not _served(server, tool):
        return general
    try:
        (step,) = run_steps.parse_steps([raw_steps[0]])
        alone = not run_steps.step_references(step)
    except run_steps.RunRefusal:
        alone = False
    takes = getattr(server, "_okg_takes_as_written", None)
    if alone and callable(takes) and takes(tool, arguments):
        call = call_text({"tool": tool, "arguments": arguments})
        return f"send the first step as its own call, which the access rules check: {call}"
    unknown = _unknown_arguments(server, tool, arguments)
    if unknown:
        return (
            f"{tool} does not accept {', '.join(repr(key) for key in unknown)}; "
            "send each step as its own call without it, which the access rules check"
        )
    return general


async def serve_run(server: Any, arguments: dict[str, Any], backend: RunBackend) -> Any:
    """Serve one `run` call: its steps in order, one reply."""

    raw_steps = arguments.get("steps")
    reason = await server._okg_run_blocking(backend.refused_reason)
    if reason is not None:
        # Before any step and any composed statement: under access rules a
        # set or a value list would hold records built outside the rules.
        return _refusal(
            "run_refused_under_access_rules", reason, _first_step_hint(server, raw_steps),
        )
    try:
        steps = run_steps.parse_steps(raw_steps)
        named = run_steps.check_references(steps)
        run_steps.classify_steps(steps)
        wall_seconds = run_steps.run_wall_seconds()
    except run_steps.RunRefusal as exc:
        return _refusal("invalid_operator_arguments", exc.message, exc.fix_hint)
    budget, budget_refusal = resolve_budget(
        "run", arguments,
        deployment_default_bytes=getattr(server, "response_default_bytes", None),
        deployment_ceiling_bytes=getattr(server, "response_ceiling_bytes", None),
    )
    if budget_refusal is not None:
        return served_call_tool_result(budget_refusal, is_error=True, tool="run")
    shown = [step for step in steps if step.show]
    fixed = _RESERVE + _RESERVE_PER_STEP * len(steps)
    share = (budget - fixed) // max(1, len(shown))
    if shown and share < MIN_STEP_SHARE:
        need = fixed + MIN_STEP_SHARE * len(shown)
        return _refusal(
            "invalid_response_byte_budget",
            f"run: max_bytes {budget} cannot hold {len(shown)} printed steps "
            f"of at least {MIN_STEP_SHARE} bytes each",
            f"pass max_bytes={need}, or set \"show\": false on a step",
        )
    authority = next(
        (s.arguments["authority"] for s in steps if isinstance(s.arguments.get("authority"), dict)),
        None,
    )
    # One graph version for every step: the run's own, else the first
    # step's, else the session's pin now, before any step runs.
    generation = arguments.get("generation_id")
    first = steps[0].arguments.get("generation_id")
    if generation is not None and first is not None and str(first) != str(generation):
        return _refusal(
            "invalid_operator_arguments",
            f"run: step 1 names generation {first}; this run reads {generation}",
            "drop generation_id from the step",
        )
    if generation is None:
        generation = first
    # Two clocks (#3666, lane ri, review finding 2). The run's wall clock
    # bounds everything, live calls included; graph steps also share a
    # database-time budget, measured over the graph steps alone, so a
    # minute-long live step does not use up the graph steps' time.
    started_at = time.monotonic()
    wall_deadline = started_at + wall_seconds
    db_used_ms = 0
    if generation is None:
        try:
            with run_steps.deadline_scope(
                min(wall_deadline, started_at + run_steps.RUN_BUDGET_MS / 1000),
            ):
                generation = await server._okg_run_blocking(
                    backend.current_generation, authority,
                )
        except Exception as exc:  # noqa: BLE001 - served as the run's refusal.
            return _failure("run", exc)
        db_used_ms += int((time.monotonic() - started_at) * 1000)

    from okg.substrate.mcp import run_live

    sets = run_steps.RunSets()
    outcomes: list[_StepOutcome] = []
    state = run_live.RunLiveState(
        generation=generation, authority=authority, wall_deadline=wall_deadline,
    )
    held: dict[int, run_live.HeldRows] = {}
    try:
        with run_steps.registry_scope(sets):
            for step in steps:
                outcome = _StepOutcome(step)
                outcomes.append(outcome)
                if step.holds_rows:
                    await _held_step(server, backend, outcomes, outcome, held, state, generation)
                    continue
                started = time.monotonic()
                db_left = run_steps.RUN_BUDGET_MS - db_used_ms
                wall_left = wall_deadline - started
                events = run_steps.BudgetEvents()
                if db_left < 50 or wall_left <= 0.05:
                    outcome.status = "refused"
                    outcome.text = _text(_over_budget(step.operator, step, generation))
                    continue
                deadline = min(
                    wall_deadline, started + min(run_steps.STEP_BUDGET_MS, db_left) / 1000,
                )
                try:
                    with run_steps.deadline_scope(deadline, events):
                        blocked = await asyncio.wait_for(
                            _run_step(
                                server, backend, sets, outcomes, outcome,
                                generation=generation, authority=authority,
                                whole_bytes=max(MIN_RESPONSE_BYTES, budget if step.show else 0),
                                share_bytes=max(MIN_RESPONSE_BYTES, share if step.show else 0),
                                named_later=step.index in named, raw_steps=raw_steps,
                                arguments=arguments,
                            ),
                            # The step's wall bound: its statements end at
                            # its database-time deadline, and the work
                            # around them (text matching in Python) here.
                            timeout=min(run_steps.GRAPH_STEP_WALL_S, wall_left),
                        )
                    if blocked is not None:
                        outcome.needs = blocked
                        outcome.text = f"skipped: needs step {blocked}"
                        continue
                except asyncio.TimeoutError:
                    outcome.status = "refused"
                    outcome.text = _text(_over_budget(step.operator, step, generation))
                    continue
                except _Served as served:
                    outcome.status = "refused"
                    outcome.text = _text(served.result)
                    continue
                except Exception as exc:  # noqa: BLE001 - one step's failure is its refusal.
                    outcome.status = "refused"
                    outcome.text = _text(_failure(step.operator, exc, events, step, generation))
                    continue
                finally:
                    db_used_ms += int((time.monotonic() - started) * 1000)
                if outcome.status == "refused" and events.cancelled:
                    # Only a statement the step's own budget cut.
                    outcome.text = _text(_over_budget(step.operator, step, generation))
    except asyncio.CancelledError:
        # The client went away: no live value starts from here on.
        state.stop.set()
        raise
    finally:
        if state.heavy_reader is not None:
            from okg.substrate.external_backed.live_dispatch import release_live_heavy_run

            release_live_heavy_run(*state.heavy_reader)
    return _serve(outcomes, budget, arguments, generation)


async def _held_step(
    server: Any, backend: RunBackend, outcomes: list[_StepOutcome],
    outcome: _StepOutcome, held: dict[int, Any], state: Any, generation: Any,
) -> None:
    """A step whose rows the run holds (``run_live``): live, values, minus,
    intersect, or filter/map/aggregate over an earlier step's rows. Its
    failure is its refusal; a step it reads that did not run skips it."""
    from okg.substrate.mcp import run_live

    step = outcome.step
    for number in sorted(run_steps.row_references(step)):
        if outcomes[number - 1].status != "ok":
            outcome.needs = number
            outcome.text = f"skipped: needs step {number}"
            return
    try:
        if step.kind == "live":
            rows = await run_live.live_step(server, step, held, state)
        elif step.kind == "values":
            rows = run_live.values_step(step)
        elif step.kind == "set":
            rows = run_live.set_step(
                step, [(number, held[number]) for number in step.arguments["inputs"]],
            )
        else:
            number = run_steps.rows_input(step)
            assert number is not None
            rows = run_live.rows_step(step, number, held[number], backend.row_matches)
    except run_live._PrepareRefused as refused:  # noqa: SLF001 - this module's own seam.
        outcome.status = "refused"
        outcome.text = _text(refused.result)
        return
    except Exception as exc:  # noqa: BLE001 - one step's failure is its refusal.
        outcome.status = "refused"
        outcome.text = _text(_failure(step.operator, exc, None, step, generation))
        return
    held[step.index] = rows
    outcome.held = rows
    outcome.status = "ok"


async def _run_step(
    server: Any, backend: RunBackend, sets: run_steps.RunSets,
    outcomes: list[_StepOutcome], outcome: _StepOutcome, *,
    generation: Any, authority: Any, whole_bytes: int, share_bytes: int,
    named_later: bool, raw_steps: list[Any], arguments: Mapping[str, Any],
) -> Optional[int]:
    """Send one step; the earlier step it needs and lacks, else None.

    A step whose rows the run fits itself (an exact step) is read with the
    whole budget and cut to its share when the reply is laid out; any other
    step is read with its share and printed as served."""
    step = outcome.step
    refs = run_steps.step_references(step)
    for number in sorted(refs):
        if outcomes[number - 1].status != "ok":
            return number
    for number, fields in sorted(refs.items()):
        key = str(number)
        if key not in sets.sets:
            producer = outcomes[number - 1]
            if producer.step.holds_rows:
                raise run_steps.RunRefusal(
                    f"run: step {step.index} names ${number} as a set of graph "
                    f"records; step {number} holds rows the run read itself",
                    f'read its rows with "input": "${number}" (filter, map or '
                    "aggregate), or compare two steps' rows with minus or intersect",
                )
            raise run_steps.RunRefusal(
                f"run: step {number} ({producer.step.operator}) did not select "
                "a whole set of records (links, a saved or live result, a path "
                "read, or an inspect kind other than nodes)",
                "name the set of a filter, map, aggregate, one-hop expand, or a "
                "grep, lexical or attrs search step",
            )
        await hold_set(
            server, backend, sets, key, fields=fields, generation=generation,
            authority=authority, for_inspect=step.operator == "inspect",
        )
    notes: list[dict[str, str]] = []
    join_from = run_steps.join_source(step)
    if join_from is not None:
        # The link join: the far records of every record of step n's set,
        # read as a filter over them, so the step meets every rule a set
        # step meets.
        call = await _join_call(
            server, backend, sets, outcome, join_from, generation, authority, notes,
        )
        outcome.tool = "filter"
    else:
        call = run_steps.bind_step(
            step, sets, inspect_ids=lambda number: _inspect_ids(sets, number),
            notes=notes,
        )
    given = call.get("generation_id")
    if given is not None and str(given) != str(generation):
        raise run_steps.RunRefusal(
            f"run: step {step.index} names generation {given}; "
            f"a run reads one version of the graph ({generation})",
            "drop generation_id from the step",
        )
    # Run's own pin, marked as run's, so a step that turns out to read a live
    # type (which has no generations) is served without it; a generation_id
    # the step itself named is not marked and is still refused there.
    injected = generation if given is None else None
    if generation is not None:
        call["generation_id"] = generation
    limit = _int(call.get("limit"))
    outcome.limit = limit
    outcome.offset = _int(call.get("offset")) or 0
    operator = outcome.tool
    label = f"run: step {step.index}"
    call["max_bytes"] = (
        whole_bytes if operator in _CONDITION_TOOLS or (operator == "search" and named_later)
        else share_bytes
    )

    if operator in _CONDITION_TOOLS:
        # Every row R may print, at most the cap; a step not printed reads
        # one row (its set is its condition, not the page).
        if step.show:
            call["limit"] = run_steps.RUN_ROW_CAP if limit is None else min(limit, run_steps.RUN_ROW_CAP)
        else:
            call["limit"] = 1
            call["offset"] = 0
    elif operator == "search" and named_later:
        await _search_set(
            server, backend, sets, outcome, call, generation, authority,
            raw_steps, arguments,
        )
        if not step.show:
            outcome.status = "ok"
            return None
        call["limit"] = max(1, outcome.rows_expected())
    elif operator == "search":
        outcome.ranked_page = True

    outcome.adjusted = notes
    from okg.substrate.mcp import live_resolver

    with run_steps.capture_scope(outcome.capture), live_resolver.run_generation_scope(
        operator, injected,
    ):
        result = await server.call_tool(operator, call)
    outcome.text = _text(result)
    if getattr(result, "isError", False):
        outcome.status = "refused"
        return None
    outcome.status = "ok"
    payload = outcome.capture.payload
    outcome.payload = dict(payload) if isinstance(payload, dict) else None
    if outcome.payload is not None and notes:
        outcome.payload["adjusted"] = [*(outcome.payload.get("adjusted") or []), *notes]

    if operator in _CONDITION_TOOLS:
        definition = outcome.capture.definition
        if definition is None or (definition[0] == "records" and definition[1] != "nodes"):
            # Links, a saved or live result, a path read: no set, no count.
            return None
        key = str(step.index)
        raw = await server._okg_run_blocking(
            backend.step_set, definition, sets, key, generation, authority,
        )
        sets.sets[key] = run_steps.StepSet(
            raw_sql=raw,
            # A text step's set is its matched ids: read as the set, never
            # inlined as the condition it was recorded as.
            predicate=(
                None if outcome.capture.text
                else run_steps.inlinable_predicate(definition)
            ),
        )
        outcome.count = await _count(server, backend, raw, generation, authority, label)
        page = outcome.payload or {}
        if operator == "aggregate":
            groups = _int(page.get("total_count"))
            if groups is None:
                raise run_steps.RunCountNotFinished(
                    f"{label}: aggregate did not count its groups exactly",
                    fix_hint="narrow the step's condition, or group by fewer keys",
                )
            outcome.groups = groups
            outcome.units = groups
        elif operator == "expand":
            # Its rows are the links to those records.
            outcome.units = _int(page.get("total_count"))
            if outcome.units is None:
                outcome.units = outcome.count
        else:
            outcome.units = outcome.count
        if outcome.join is not None:
            await _join_facts(server, backend, sets, outcome, generation, authority, label)
        if step.show and outcome.rows_expected() > run_steps.RUN_ROW_CAP:
            raise _too_many_rows(server, outcome, raw_steps, arguments)
    elif operator == "inspect" and named_later:
        ids = _inspect_target_ids(call.get("target"))
        if ids is None:
            return None
        raw = await server._okg_run_blocking(backend.ids_set, ids, generation, authority)
        sets.sets[str(step.index)] = run_steps.StepSet(raw_sql=raw, ids=frozenset(ids))
    return None


#: What a link-join step takes besides its start; anything else is refused
#: by name rather than dropped.
_JOIN_ARGUMENTS = frozenset({
    "from", "input", "edge_type", "direction", "where", "filter", "select",
    "limit", "offset", "order_by", "generation_id", "authority", "max_bytes",
})
#: Most ids of starting records with no link a join names.
JOIN_IDS_SHOWN = 10


async def _join_call(
    server: Any, backend: RunBackend, sets: run_steps.RunSets,
    outcome: _StepOutcome, join_from: int, generation: Any, authority: Any,
    notes: list[dict[str, str]],
) -> dict[str, Any]:
    """The filter call a link-join step is read as, with its far-end set
    kept as SQL under the step's own key."""
    step = outcome.step
    args = step.arguments
    edge_type = args.get("edge_type")
    direction = args.get("direction") or "out"
    if not (isinstance(edge_type, str) and edge_type and edge_type != "*") or direction not in ("in", "out"):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: a join from ${join_from} names one link "
            "type (edge_type) and direction in or out",
            f'e.g. {{"expand": {{"from": "${join_from}", "edge_type": "<link>", '
            '"direction": "out"}}',
        )
    unknown = sorted(set(args) - _JOIN_ARGUMENTS)
    if unknown:
        raise run_steps.RunRefusal(
            f"run: step {step.index}: a join from ${join_from} does not take "
            + ", ".join(unknown),
            "a join takes edge_type, direction, where, select, limit, offset and order_by",
        )
    if "from" in args and "input" in args:
        raise run_steps.RunRefusal(f"run: step {step.index}: expand takes `from` or `input`, not both")
    start = sets.sets[str(join_from)].raw_sql
    key = f"join{step.index}"
    sets.sets[key] = run_steps.StepSet(raw_sql=await server._okg_run_blocking(
        backend.join_far, start, edge_type, direction, generation, authority,
    ))
    leaf: dict[str, Any] = {"op": "in", "field": "node_id", "values": [sets.set_marker(key)]}
    far = args.get("where", args.get("filter"))
    where: Any = leaf
    if far:
        bound = run_steps.bind_condition(
            sets, far, f"run: step {step.index} where", notes=notes,
        )
        where = {"op": "and", "clauses": [leaf, bound]}
    call: dict[str, Any] = {"where": where}
    for name in ("select", "limit", "offset", "order_by", "authority", "generation_id"):
        if name in args:
            call[name] = args[name]
    outcome.join = {
        "from_step": join_from, "edge_type": edge_type, "direction": direction,
        "start": start,
    }
    return call


async def _join_facts(
    server: Any, backend: RunBackend, sets: run_steps.RunSets,
    outcome: _StepOutcome, generation: Any, authority: Any, label: str,
) -> None:
    """What a join reached and what it did not: exact, never estimated."""
    join = outcome.join
    assert join is not None
    try:
        with run_steps.counting_scope():
            starts, without, ids = await server._okg_run_blocking(
                backend.join_facts, join["start"], join["edge_type"], join["direction"],
                JOIN_IDS_SHOWN, generation, authority,
            )
    except Exception as exc:
        if run_steps.budget_cut(exc):
            raise run_steps.RunCountNotFinished(
                f"{label}: the join's counts did not finish inside the step's "
                f"time budget ({run_steps.STEP_BUDGET_MS:,} ms a step); they are "
                "never estimated",
                fix_hint="narrow the starting step, or split the run",
            ) from exc
        raise
    join.update(starts=int(starts), starts_without_link=int(without), without_link_ids=list(ids))


def _join_lines(outcome: _StepOutcome) -> list[str]:
    join = outcome.join
    if join is None or "starts" not in join:
        return []
    link = f"{join['edge_type']} {join['direction']}"
    line = (
        f"! join from step {join['from_step']}: {join['starts_without_link']:,} of "
        f"{_plural(join['starts'], 'starting record')} have no {link} link"
    )
    if join["without_link_ids"]:
        shown = ", ".join(join["without_link_ids"])
        more = join["starts_without_link"] - len(join["without_link_ids"])
        line += f" ({shown}" + (f", +{more:,} more)" if more > 0 else ")")
    return [
        line + f"; {_plural(outcome.count or 0, 'record')} reached",
        "! join: " + JOIN_NOT_COUNTED,
    ]


#: Said on every join (issue #3666): a link whose far record was never
#: loaded is not stored, so nothing here can count it.
JOIN_NOT_COUNTED = "cannot count links to records not in the graph"


def _join_block(outcome: _StepOutcome) -> Optional[dict[str, Any]]:
    join = outcome.join
    if join is None or "starts" not in join:
        return None
    return {
        "from_step": join["from_step"], "edge_type": join["edge_type"],
        "direction": join["direction"], "starts": join["starts"],
        "starts_without_link": join["starts_without_link"],
        "without_link_ids": join["without_link_ids"], "reached": outcome.count,
        "complete": False, "not_counted": JOIN_NOT_COUNTED,
    }


def _inspect_target_ids(target: Any) -> Optional[list[str]]:
    """The records an inspect nodes (or node) step names, or None."""
    if not isinstance(target, Mapping):
        return None
    ids = target.get("node_ids")
    if isinstance(ids, list) and all(isinstance(i, str) for i in ids):
        return list(dict.fromkeys(ids))
    one = target.get("node_id")
    if isinstance(one, str):
        return [one]
    return None


async def _count(
    server: Any, backend: RunBackend, raw: str, generation: Any, authority: Any,
    label: str,
) -> int:
    """One COUNT over a step's set, inside the step's budget; never estimated."""
    try:
        with run_steps.counting_scope():
            return int(await server._okg_run_blocking(
                backend.count, raw, generation, authority,
            ))
    except Exception as exc:
        if run_steps.budget_cut(exc):
            raise run_steps.RunCountNotFinished(
                f"{label}: the exact count of this step's set did not finish "
                f"inside the step's time budget ({run_steps.STEP_BUDGET_MS:,} ms "
                "a step); it is never estimated",
                fix_hint="narrow the step's condition, or split the run",
            ) from exc
        raise


async def _search_set(
    server: Any, backend: RunBackend, sets: run_steps.RunSets,
    outcome: _StepOutcome, call: dict[str, Any], generation: Any, authority: Any,
    raw_steps: list[Any], arguments: Mapping[str, Any],
) -> None:
    """A search step a later step names: its whole match set, read and
    counted in one exact call, or a refusal by name."""
    step = outcome.step
    method = call.get("method") or "lexical"
    if method not in WHOLE_SET_SEARCH_METHODS:
        message = (
            f"run: step {step.index} is a {method} search: a ranked page of "
            "the best matches, not a whole set, so a later step cannot read it"
        )
        offered = _text_matches_call(server, step, raw_steps, arguments)
        if offered is None:
            why = (
                "filter is not served here" if _served(server, "filter") is False
                and isinstance(step.arguments.get("query"), str)
                and step.arguments["query"].strip()
                else "the step has no query words to match"
            )
            raise run_steps.RunRefusal(
                message,
                "search with method grep, lexical or attrs, which read and "
                f"count every match; no filter call is offered: {why}",
            )
        raise InvalidOperatorArguments(
            message,
            fix_hint=(
                "list every record whose text has those words, as a whole "
                f"set: {call_text(offered)}"
            ),
            data={"retry_calls": [offered]},
        )
    set_call = {
        key: value for key, value in call.items()
        if key not in (
            "limit", "offset", "next", "continuation_token", "view", "summary",
            "select", "completeness", "max_chars", "context_chars",
        )
    }
    # The whole set is read once, never printed: at the largest reply the
    # deployment serves, so a byte bound does not cut it.
    set_call.update(
        limit=run_steps.RUN_ROW_CAP, offset=0, completeness="exact", select=["node_id"],
        max_bytes=getattr(server, "response_ceiling_bytes", None) or DEFAULT_RESPONSE_BYTES,
    )
    capture = run_steps.StepCapture()
    with run_steps.capture_scope(capture):
        result = await server.call_tool("search", set_call)
    if getattr(result, "isError", False):
        outcome.status = "refused"
        outcome.text = _text(result)
        raise _Served(result)
    page = capture.payload if isinstance(capture.payload, dict) else {}
    total = _int(page.get("total_count"))
    ids = run_steps.payload_ids(page)
    if total is not None and total > run_steps.RUN_ROW_CAP:
        raise run_steps.RunStepTooManyRows(
            f"run: step {step.index}: search matches {_plural(total, 'record')}; "
            f"a search step's set holds at most {run_steps.RUN_ROW_CAP}",
            fix_hint="narrow the search (a type in `where`, more words), or "
            "name a filter step's set instead",
            data={"count": total, "cap": run_steps.RUN_ROW_CAP},
        )
    if page.get("completeness") != "exact" or total is None or total != len(ids):
        raise run_steps.RunRefusal(
            f"run: step {step.index}: this search's whole match set could not be "
            "read in one call (it stopped at a scan, time or byte bound, or did "
            "not count every match), so a later step cannot read it",
            "narrow the search, or name a filter step's set instead",
        )
    raw = await server._okg_run_blocking(backend.ids_set, ids, generation, authority)
    sets.sets[str(step.index)] = run_steps.StepSet(raw_sql=raw, ids=frozenset(ids))
    outcome.count = total
    outcome.units = total


def _text_matches_call(
    server: Any, step: run_steps.Step, raw_steps: list[Any], arguments: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """The same run with a ranked search step replaced by a filter on its
    words (M7's text condition, every word must occur), or None when filter
    is not served or the step has no query words. The step's type, when it
    names one, is kept."""
    query = step.arguments.get("query")
    if not (isinstance(query, str) and query.strip()) or not _served(server, "filter"):
        return None
    where: dict[str, Any] = {}
    named = step.arguments.get("where")
    if isinstance(named, Mapping) and isinstance(named.get("type"), (str, list)):
        where["type"] = named["type"]
    elif isinstance(step.arguments.get("subtype_filter"), str):
        where["type"] = step.arguments["subtype_filter"]
    where["text"] = {"op": "matches", "value": query}
    replaced = _steps_copy(raw_steps)
    show = replaced[step.index - 1].get("show")
    replaced[step.index - 1] = {"filter": {"where": where}}
    if show is not None:
        replaced[step.index - 1]["show"] = show
    return _run_call(arguments, replaced)


class _Served(Exception):
    """A step's own call was refused: its served refusal is the reply."""

    def __init__(self, result: Any) -> None:
        super().__init__("served refusal")
        self.result = result


def _steps_copy(raw_steps: list[Any]) -> list[Any]:
    return copy.deepcopy(list(raw_steps))


def _step_body(raw: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    (name,) = [key for key in raw if key != "show"]
    return name, raw[name]


def _run_call(arguments: Mapping[str, Any], steps: list[Any], **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"steps": steps}
    for key in ("generation_id", "max_bytes"):
        if arguments.get(key) is not None:
            out[key] = arguments[key]
    out.update(extra)
    return {"tool": "run", "arguments": out}


def call_text(call: Mapping[str, Any]) -> str:
    """A call as one line: ``run(steps=[...], generation_id=...)``."""
    arguments = call["arguments"]
    return f"{call['tool']}(" + ", ".join(
        f"{key}={compact_json(value)}" for key, value in arguments.items()
    ) + ")"


def _too_many_rows(
    server: Any, outcome: _StepOutcome, raw_steps: list[Any], arguments: Mapping[str, Any],
) -> run_steps.RunStepTooManyRows:
    """The refusal of a printed step past the row cap, with two calls."""
    step = outcome.step
    index = step.index - 1
    hidden = _steps_copy(raw_steps)
    hidden[index]["show"] = False
    calls = [_run_call(arguments, hidden)]
    name, body = _step_body(raw_steps[index])
    counted: Optional[str] = None
    condition = {k: v for k, v in body.items() if k in ("where", "input", "generation_id", "authority")}
    if name == "expand":
        counted = "an expand step's links are not counted by aggregate here"
    elif "predicate" in body:
        counted = "the step writes its condition as `predicate`, which aggregate does not take"
    elif not _served(server, "aggregate"):
        counted = "aggregate is not served here"
    else:
        replaced = _steps_copy(raw_steps)
        replaced[index] = {
            "aggregate": {**condition, "by": ["subtype"], "metrics": ["count"]},
            "show": True,
        }
        calls.append(_run_call(arguments, replaced))
    what = outcome.count_phrase()
    unit = "groups" if step.operator == "aggregate" else "rows"
    hint = f"pass the set on without its rows: {call_text(calls[0])}"
    if len(calls) > 1:
        hint += f"; or count it: {call_text(calls[1])}"
    else:
        hint += f" (no count call is offered: {counted})"
    return run_steps.RunStepTooManyRows(
        f"run: step {step.index} {step.operator}: {what}; a printed step "
        f"prints at most {run_steps.RUN_ROW_CAP} {unit}, and this one would "
        f"print {outcome.rows_expected():,}",
        fix_hint=hint,
        data={
            "count": outcome.count, "groups": outcome.groups,
            "cap": run_steps.RUN_ROW_CAP, "retry_calls": calls,
        },
    )


def _inspect_ids(sets: run_steps.RunSets, key: Any) -> list[str]:
    entry = sets.sets[str(key)]
    if entry.ids is None or len(entry.ids) > run_steps.INSPECT_SET_CAP:
        raise run_steps.RunRefusal(
            f"inspect reads at most {run_steps.INSPECT_SET_CAP} records; "
            f"${key} has more",
            f"read the set's fields with map(where=\"${key}\", select=[...])",
        )
    return sorted(entry.ids)


async def hold_set(
    server: Any, backend: RunBackend, sets: run_steps.RunSets, key: str, *,
    fields: set[str], generation: Any, authority: Any, for_inspect: bool,
) -> None:
    """What a call reading set `key` needs of it besides its SQL: the ids for
    an inspect read (at most 50), and each value list named. A row check
    reads ids and values itself, when it first needs them."""
    entry = sets.sets[key]
    if for_inspect and entry.ids is None:
        held = await server._okg_run_blocking(
            backend.materialize, entry.raw_sql, generation, authority,
            run_steps.INSPECT_SET_CAP,
        )
        if held is None:
            raise run_steps.RunRefusal(
                f"inspect reads at most {run_steps.INSPECT_SET_CAP} "
                f"records; ${key} has more",
                f"read the set's fields with map(where=\"${key}\", select=[...])",
            )
        entry.ids = frozenset(str(value) for value in held)
    for field in sorted(fields):
        run_steps.value_column(field)
        if field not in entry.values:
            raw = await server._okg_run_blocking(
                backend.values_sql, sets, key, field, generation, authority,
            )
            entry.values[field] = run_steps.ValueList(raw_sql=raw)


# --------------------------------------------------------------------------- #
# the one reply                                                               #
# --------------------------------------------------------------------------- #


def _first_line(text: str) -> str:
    return text.split("\n", 1)[0]


def _select_names(arguments: Mapping[str, Any]) -> Optional[tuple[str, ...]]:
    select = arguments.get("select")
    if isinstance(select, list) and all(isinstance(s, str) for s in select):
        return tuple(select)
    return None


def _inspect_kind(step: run_steps.Step) -> Optional[str]:
    if step.operator != "inspect":
        return None
    target = step.arguments.get("target")
    if isinstance(target, Mapping) and isinstance(target.get("kind"), str):
        return target["kind"]
    return target if isinstance(target, str) else None


class _Block:
    """One step's part of the reply, fitted to a share of max_bytes."""

    def __init__(
        self, outcome: _StepOutcome, fmt: str, arguments: Mapping[str, Any],
        generation: Any, budget: int,
    ) -> None:
        self.outcome = outcome
        self.fmt = fmt
        self.arguments = arguments
        self.generation = generation
        self.budget = budget
        step = outcome.step
        self.flexible = bool(
            outcome.status == "ok" and step.show and outcome.exact
            and outcome.payload is not None
        )
        self.rows: list[Any] = []
        self.rows_key: Optional[str] = None
        self.page: dict[str, Any] = {}
        if self.flexible:
            page = {
                k: v for k, v in (outcome.payload or {}).items()
                if k not in _CONTINUATION_KEYS and k not in _RUN_PINNED_KEYS
            }
            self.rows_key = page_rows_key(page, outcome.tool)
            if self.rows_key is not None:
                self.rows = list(page[self.rows_key])
            self.page = page
        self.shown = len(self.rows)
        #: The share the rows were cut to, or None when they all fit.
        self.share: Optional[int] = None

    def next_call(self, shown: int) -> dict[str, Any]:
        outcome = self.outcome
        steps = _steps_copy(self.arguments["steps"])
        name, body = _step_body(steps[outcome.step.index - 1])
        body["offset"] = outcome.offset + shown
        out = _run_call(self.arguments, steps)
        out["arguments"]["generation_id"] = self.generation
        return out

    def _cut(self, shown: int, share: Optional[int]) -> Optional[tuple[str, dict[str, Any], int]]:
        """(reason, next call, rows left) when `shown` rows leave some."""
        outcome = self.outcome
        expected = outcome.rows_expected()
        left_total = max(0, (outcome.units or 0) - outcome.offset - shown)
        if shown < expected:
            reason = f"cut to fit {share if share is not None else self.budget:,} bytes"
        elif outcome.limit is not None and left_total > 0 and shown >= outcome.limit:
            reason = f"cut at limit {outcome.limit}"
        else:
            return None
        return reason, self.next_call(shown), left_total

    def render(self, shown: int, share: Optional[int]) -> Any:
        """Text lines, or the JSON step row, with `shown` rows."""
        if self.fmt == "json":
            return self._json(shown, share)
        return [
            line for line in self._lines(shown, share)
            if not line.startswith(_PIN_SCOPE_LINE)
        ]

    def _lines(self, shown: int, share: Optional[int]) -> list[str]:
        outcome = self.outcome
        step = outcome.step
        label = f"[step {step.index}]"
        if outcome.status == "skipped":
            return [f"{label} {step.operator} {outcome.text}"]
        if outcome.status == "refused":
            return [f"{label} {outcome.text}"]
        if outcome.exact:
            head = [f"{label} {step.operator}: {outcome.count_phrase()}", *_join_lines(outcome)]
            if not step.show:
                return head
            if not self.flexible:
                return [*head, *outcome.text.split("\n")[1:]]
            page = dict(self.page)
            if self.rows_key is not None:
                page[self.rows_key] = self.rows[:shown]
            body = render(
                page, tool=outcome.tool, kind=_inspect_kind(step),
                select=_select_names(step.arguments),
            ).split("\n")[1:]
            lines = [*head, *body]
            cut = self._cut(shown, share)
            if cut is not None:
                reason, call, left = cut
                lines.append(cut_line(reason, shown, left, call_text(call)))
            return lines
        if not step.show:
            return [f"{label} {_first_line(outcome.text)}"]
        first, _sep, rest = outcome.text.partition("\n")
        lines = [f"{label} {first}"]
        if outcome.ranked_page:
            lines.append(
                "! not a total: a search page holds the best matches, and no "
                "later step reads it"
            )
        if outcome.adjusted:
            lines.append("! adjusted: " + "; ".join(
                f"{record['sent']} read as {record['served']}" for record in outcome.adjusted
            ))
        if rest:
            lines.append(rest)
        return lines

    def _json(self, shown: int, share: Optional[int]) -> dict[str, Any]:
        outcome = self.outcome
        step = outcome.step
        row: dict[str, Any] = {
            "step": step.index, "tool": step.operator, "status": outcome.status,
        }
        if outcome.status == "skipped":
            row["needs_step"] = outcome.needs
            return row
        if outcome.status == "refused":
            try:
                row["result"] = json.loads(outcome.text)
            except ValueError:
                row["result"] = outcome.text
            return row
        if outcome.exact:
            row["count"] = outcome.count
            if outcome.groups is not None:
                row["groups"] = outcome.groups
        join = _join_block(outcome)
        if join is not None:
            row["join"] = join
        if not step.show:
            row["rows_shown"] = 0
            if not outcome.exact:
                row["result"] = {
                    k: v for k, v in (outcome.payload or {}).items()
                    if k not in ("items", "results", "groups", "neighbors", "rows", "nodes")
                }
            return row
        if self.flexible:
            page = dict(self.page)
            if self.rows_key is not None:
                page[self.rows_key] = self.rows[:shown]
            row["result"] = page
            row["rows_shown"] = shown
            cut = self._cut(shown, share)
            row["rows_left"] = max(0, (outcome.units or 0) - outcome.offset - shown)
            if cut is not None:
                row["next_call"] = cut[1]
            return row
        payload = outcome.payload
        if payload is None:
            try:
                payload = json.loads(outcome.text)
            except ValueError:
                payload = outcome.text
        row["result"] = payload
        if outcome.ranked_page:
            row["not_a_total"] = "a search page holds the best matches, and no later step reads it"
        return row

    def size(self, shown: int, share: Optional[int]) -> int:
        rendered = self.render(shown, share)
        if self.fmt == "json":
            return len(compact_json(rendered).encode("utf-8")) + 1
        return len("\n".join(rendered).encode("utf-8")) + 1

    def fit(self, share: int) -> None:
        """The most rows whose block fits `share` bytes (at least none)."""
        if not self.flexible or self.size(len(self.rows), None) <= share:
            self.shown = len(self.rows)
            self.share = None
            return
        low, high = 0, len(self.rows)
        while low < high:
            middle = (low + high + 1) // 2
            if self.size(middle, share) <= share:
                low = middle
            else:
                high = middle - 1
        self.shown = low
        self.share = share

    def final(self) -> Any:
        return self.render(self.shown, self.share)


def _fenced_held(held: Any, count: int) -> tuple[list[Any], list[str]]:
    """The first `count` printed rows of a held step, wrapped as untrusted
    text of their own source, and those sources."""
    from okg.substrate.external_backed.resolver import _fence_payload

    if held.groups is not None:
        rows = held.groups[:count]
        return list(rows), [held.group_source or "run_rows"] * len(rows)
    rows = held.rows[:count]
    sources = held.sources[:count]
    out: list[Any] = []
    start = 0
    while start < len(rows):
        end = start
        while end < len(rows) and sources[end] == sources[start]:
            end += 1
        out.extend(_fence_payload(rows[start:end], source=sources[start], fence_keys=True))
        start = end
    return out, list(sources)


class _HeldBlock:
    """A held step's part of the reply (``run_live``): its head, its notes,
    and as many of its rows as fit its share. Its rows are not continued by
    another call: a later step of the same run reads all of them."""

    def __init__(self, outcome: _StepOutcome, fmt: str, budget: int) -> None:
        self.outcome = outcome
        self.fmt = fmt
        self.budget = budget
        held = outcome.held
        step = outcome.step
        self.limit = _int(step.arguments.get("limit")) if step.kind != "set" else None
        cap = run_steps.RUN_ROW_CAP if self.limit is None else min(self.limit, run_steps.RUN_ROW_CAP)
        self.total = len(held.printed())
        self.flexible = bool(step.show and self.total)
        self.rows, self.sources = _fenced_held(held, cap) if self.flexible else ([], [])
        self.shown = len(self.rows)
        self.share: Optional[int] = None
        self.unit = "groups" if held.groups is not None else "rows"

    def _cut_reason(self, shown: int, share: Optional[int]) -> Optional[str]:
        if shown >= self.total:
            return None
        if shown < len(self.rows):
            return f"cut to fit {share if share is not None else self.budget:,} bytes"
        if self.limit is not None and shown >= self.limit:
            return f"cut at limit {self.limit}"
        return f"at most {run_steps.RUN_ROW_CAP} a step"

    def render(self, shown: int, share: Optional[int]) -> Any:
        from okg.substrate.mcp.result_text import held_rows_lines

        held = self.outcome.held
        step = self.outcome.step
        reason = self._cut_reason(shown, share) if self.flexible else None
        if self.fmt == "json":
            row: dict[str, Any] = {
                "step": step.index, "tool": step.operator, "status": "ok",
                **held.facts, "notes": [line.lstrip("! ") for line in held.notes],
                "rows_shown": shown if self.flexible else 0,
            }
            if self.flexible:
                row["result"] = {self.unit: self.rows[:shown]}
                row["rows_left"] = self.total - shown
                if reason is not None:
                    row["cut"] = reason
            return row
        lines = [f"[step {step.index}] {held.head}", *held.notes]
        if self.flexible and held.groups is not None:
            from okg.substrate.mcp.result_text import held_groups_lines

            lines += held_groups_lines(
                self.rows[:shown], list(held.facts.get("by") or []),
                list(held.facts.get("metrics") or []), held.group_source or "run_rows",
            ) if shown else []
        elif self.flexible:
            lines += held_rows_lines(self.rows[:shown], self.sources[:shown])
            if reason is not None:
                lines.append(
                    f"! {reason}: {shown:,} of {self.total:,} {self.unit} printed; "
                    f'a later step of this run reads them all (e.g. "input": "${step.index}")'
                )
        return lines

    def size(self, shown: int, share: Optional[int]) -> int:
        rendered = self.render(shown, share)
        if self.fmt == "json":
            return len(compact_json(rendered).encode("utf-8")) + 1
        return len("\n".join(rendered).encode("utf-8")) + 1

    def fit(self, share: int) -> None:
        if not self.flexible or self.size(len(self.rows), None) <= share:
            self.shown = len(self.rows)
            self.share = None
            return
        low, high = 0, len(self.rows)
        while low < high:
            middle = (low + high + 1) // 2
            if self.size(middle, share) <= share:
                low = middle
            else:
                high = middle - 1
        self.shown = low
        self.share = share

    def final(self) -> Any:
        return self.render(self.shown, self.share)


def _serve(
    outcomes: list[_StepOutcome], budget: int, arguments: Mapping[str, Any],
    generation: Any,
) -> CallToolResult:
    fmt = "json" if current_result_context().format == "json" else "text"
    refused = sum(1 for o in outcomes if o.status == "refused")
    skipped = sum(1 for o in outcomes if o.status == "skipped")
    blocks: list[Any] = [
        _HeldBlock(o, fmt, budget) if o.held is not None
        else _Block(o, fmt, arguments, generation, budget)
        for o in outcomes
    ]
    # Partial (#3666, lane ri): a live step did not read every value. The
    # head says so first; each live step names what it did not read.
    missing = sum(
        int(o.held.facts.get("missing_count") or 0)
        for o in outcomes if o.held is not None and o.held.facts.get("kind") == "live"
    )
    partial = any(o.held is not None and o.held.partial for o in outcomes)
    head = f"run {'partial' if partial else 'ok'} · {len(outcomes)} steps"
    if missing:
        head += f" · {missing:,} live values not read"
    if refused:
        head += f" · {refused} refused"
    if skipped:
        head += f" · {skipped} skipped"
    # Every block that is not cut takes its own size; the printed exact
    # steps share what is left, the smallest first, so room one does not
    # use goes to the others.
    room = budget - len(head.encode("utf-8")) - 64
    flexible = [b for b in blocks if b.flexible]
    for block in blocks:
        if not block.flexible:
            room -= block.size(0, None)
    for remaining, block in enumerate(
        sorted(flexible, key=lambda b: b.size(len(b.rows), None)),
    ):
        share = max(0, room) // (len(flexible) - remaining)
        block.fit(share)
        room -= block.size(block.shown, block.share)
    if fmt == "json":
        payload = {
            "status": "ok", "operator_name": "run", "generation_id": generation,
            "steps": [block.final() for block in blocks],
            "refused_steps": refused, "skipped_steps": skipped,
            **({"partial": True, "live_values_not_read": missing} if partial else {}),
        }
        return served_call_tool_result(payload, tool="run")
    lines = [head]
    for block in blocks:
        lines.extend(block.final())
    text = "\n".join(lines)
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)


# --------------------------------------------------------------------------- #
# no set name outlives its run call                                           #
# --------------------------------------------------------------------------- #


def retain_named_a_set(name: str, arguments: Mapping[str, Any]) -> bool:
    """Whether `retain=true` on this call asked for a set name, which only
    the removed set names gave. A lexical search and a filter over a saved
    result keep their result reference, as before."""
    if arguments.get("retain") is not True:
        return False
    if name in ("map", "aggregate", "expand"):
        return True
    if name == "filter":
        scope = arguments.get("input")
        return not (isinstance(scope, Mapping) and "ref" in scope)
    if name == "search":
        return (arguments.get("method") or "lexical") != "lexical"
    return False


def _set_sentence(server: Any) -> str:
    if server is not None and _served(server, "run"):
        return run_steps.SET_LASTS_ONE_RUN
    return "a set lasts for one run call, and this server does not serve run"


async def guard_call(
    server: Any, name: str, arguments: Any, plain: Any, backend: RunBackend,
) -> Any:
    """One read call: a set name kept past its run (``$r<n>``, or
    ``retain=true`` where only a set name answered it) and a step name
    (``$n``) outside its run are refused by name before any SQL is built.
    Every other call goes to `plain` unchanged."""
    if not isinstance(arguments, dict):
        return await plain(name, arguments)
    named = run_steps.references(name, arguments)
    removed = sorted(k for k in named if run_steps.is_retained_key(k))
    steps = sorted((k for k in named if run_steps.is_step_key(k)), key=int)
    if removed:
        return _refusal(
            run_steps.SetNamesRemoved.code,
            f"{name}: ${removed[0]} names a set kept from an earlier call; "
            + _set_sentence(server),
            "write the set's condition into this call, or send both reads in one run",
            tool=name,
        )
    if steps:
        if run_steps.active_registry() is not None:
            # Inside a run step: the steps were sent together, and this
            # reference is one the step's binding did not read.
            unread = run_steps.unread_reference(name, steps[0])
            return _refusal(
                "invalid_operator_arguments", unread.message, unread.fix_hint, tool=name,
            )
        return _refusal(
            "invalid_operator_arguments",
            f"{name}: ${steps[0]} names a step of a run call; " + _set_sentence(server),
            "write the set's condition into this call",
            tool=name,
        )
    if retain_named_a_set(name, arguments):
        reason = await server._okg_run_blocking(backend.refused_reason)
        if reason is None:
            return _refusal(
                run_steps.SetNamesRemoved.code,
                f"{name}: retain=true names no set; " + _set_sentence(server),
                "drop retain",
                tool=name,
            )
        # Under access rules `retain` is refused as it always was.
    return await plain(name, arguments)
