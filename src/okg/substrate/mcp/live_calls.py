"""The upstream side of a live read: plans, calls, samples (#3666).

Shared by the set-read resolver (:mod:`okg.substrate.mcp.live_resolver`)
and the one-node reads (:mod:`okg.substrate.mcp.live_nodes`):

* the refusal type every live read raises before any upstream call;
* the context variables that carry a resolved read to the tool body
  (:data:`HANDOFF`) and hold it for the rest of one MCP call
  (:data:`HOLDER`);
* the per-node call plan (:func:`plan_groups`) and its fetch
  (:func:`fetch_groups`), which fill each row's fields and their states;
* :class:`Reader`, which makes the calls in three phases: prepare on a
  call worker, upstream calls on the wait-only executor, one call sample per
  upstream call on a call worker;
* the first line of every live reply (:func:`live_line`).

Nothing here touches the stored graph or keeps a node.
"""
from __future__ import annotations

import asyncio
import contextvars
import difflib
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError

log = logging.getLogger(__name__)

#: Upstream calls one tool call may make.
MAX_UPSTREAM_CALLS = 1_500
#: The whole read's deadline, prepare, waits and samples included.
DEADLINE_SECONDS = 100.0
#: Rows held per relation answer, as compact JSON.
MAX_ANSWER_BYTES = 8 * 1024 * 1024
#: The field states a live row carries.
FIELD_STATES = ("live", "unavailable", "denied", "absent")
#: Fields every row has without a call.
RECORD_FIELDS = frozenset({"node_id", "subtype"})
#: The id kept sets of this kind carry before their random part.
REF_PREFIX = "l1.lg"
#: Names of upstream rows a reply lists when they fail the type's
#: ``name_pattern``; the count is always complete.
MAX_NAMES_SHOWN = 20


class LiveReadRefused(SubstrateError):
    """A read of a live type refused before any upstream call."""

    code = "live_read_refused"


def refused(message: str, reason: str, fix_hint: str, **data: Any) -> LiveReadRefused:
    return LiveReadRefused(message, data={"reason": reason, **data}, fix_hint=fix_hint)


# -- the empty-expression answer ----------------------------------------------


def is_empty_expression_error(exc: BaseException, markers: Sequence[str]) -> bool:
    """Whether an adapter failure is the system saying "nothing matched".

    `markers` are the substrings the live type's `push_down.empty_error`
    declares. With none declared nothing is read as empty: the failure stays
    a failure.
    """

    excerpt = getattr(exc, "excerpt", None)
    if not isinstance(excerpt, str) or not markers:
        return False
    folded = excerpt.casefold()
    return any(marker.casefold() in folded for marker in markers)


def empty_answer_adapter(
    adapter: Callable[..., Any], markers: Sequence[str] = (),
) -> Callable[..., Any]:
    """The adapter, with a declared empty-expression error read as zero rows."""

    from okg.substrate.external_backed.live_dispatch import LiveAdapterError

    def call(**kwargs: Any) -> Any:
        try:
            return adapter(**kwargs)
        except LiveAdapterError as exc:
            if exc.failure != "upstream_empty_result" and is_empty_expression_error(
                exc, markers,
            ):
                raise LiveAdapterError(
                    "upstream_empty_result", excerpt=exc.excerpt, status=exc.status,
                ) from None
            raise

    return call


# -- one coercion rule ----------------------------------------------------------

#: An upstream value that cannot be compared with the condition's value.
NOT_COMPARABLE = object()


def coerce_for(condition_value: Any, upstream: Any) -> Any:
    """``upstream`` in the condition value's kind, or :data:`NOT_COMPARABLE`,
    by the kept-set reader's one rule (:func:`live_reader.coerce`)."""

    from okg.substrate.mcp.live_reader import coerce

    if upstream is None:
        return NOT_COMPARABLE
    pair = coerce(upstream, condition_value)
    return NOT_COMPARABLE if pair is None else pair[0]


def typed_value(value: Any, kind: str) -> Any:
    """A declared field's value in its declared type, when it converts."""

    probe: Any = {"integer": 0, "number": 0.0, "boolean": True}.get(kind, "")
    coerced = coerce_for(probe, value)
    if coerced is NOT_COMPARABLE:
        return value
    if kind == "integer" and isinstance(coerced, float) and coerced.is_integer():
        return int(coerced)
    return coerced


# -- small shared helpers -------------------------------------------------------


def as_mapping(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    return value


def canonical(value: Any) -> str:
    return json.dumps(as_mapping(value), sort_keys=True, default=str, separators=(",", ":"))


def now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def system_word(system: Optional[str]) -> str:
    if not system:
        return "the live system"
    return system[:1].upper() + system[1:]


def with_tail(line: str, tail: str) -> str:
    """The live line with its last part (kept / not kept) replaced."""

    head, sep, _old = line.rpartition(" · ")
    return f"{head}{sep}{tail}" if sep else tail


_BARE = frozenset({"attrs", "live", *RECORD_FIELDS})


def field_key(name: Any) -> Optional[str]:
    """`attrs.<key>` -> key; a bare name (the bare-select retry) -> key; else
    None."""

    if not isinstance(name, str):
        return None
    name = name.lstrip("-+")
    if name.startswith("attrs."):
        return name[len("attrs."):] or None
    if name in _BARE or "." in name or "(" in name:
        return None
    return name or None


def name_checked(live_type: Any, name: str) -> bool:
    """Whether an upstream name may become a node of `live_type`: it must
    match the type's anchored ``name_pattern`` when the type declares one."""

    from okg.substrate.external_backed.live_types import name_matches

    # The same rule as a node id a caller passes (`parse_node_id`).
    return name_matches(getattr(live_type, "name_pattern", None), name)


# -- context variables: the handoff and the whole-call holder -----------------


@dataclass
class Resolved:
    """One resolved live read, as handed to the tool body."""

    tool: str
    ref: str
    rows: list[dict[str, Any]]
    #: Fields fetched for these rows (`attrs.<key>` names and record fields).
    fields: frozenset[str]
    state: dict[str, Any]
    calls_made: int


#: Set by the resolver around the tool body; read by the byte-bound wrapper.
HANDOFF: contextvars.ContextVar[Optional[Resolved]] = contextvars.ContextVar(
    "okg_live_graph_handoff", default=None,
)
#: Set for one whole MCP call (`_okg_call_tool_plain`): key -> Resolved.
HOLDER: contextvars.ContextVar[Optional[dict[str, Resolved]]] = contextvars.ContextVar(
    "okg_live_graph_holder", default=None,
)


# -- the plan of calls ------------------------------------------------------------


@dataclass
class CallGroup:
    """One relation called once per node, with fixed arguments shared."""

    label: str
    relation: str
    fixed: dict[str, Any]
    #: The argument that takes the node's name.
    self_arg: str
    #: field name -> (column, type) for declared fields; None for open fields.
    fields: Optional[dict[str, tuple[str, str]]]
    #: The field keys this group's call fills.
    keys: set[str] = field(default_factory=set)


def call_args(call: Any, tool: str) -> tuple[dict[str, Any], list[str]]:
    """(fixed arguments, self-argument names) of one declared call."""

    fixed: dict[str, Any] = {}
    selfs: list[str] = []
    for arg in getattr(call, "args", ()) or ():
        if arg.kind == "fixed":
            fixed[arg.name] = arg.value
        elif arg.kind == "self":
            selfs.append(arg.name)
        elif arg.required:
            raise refused(
                f"{tool}: relation {call.relation!r} needs the caller argument "
                f"{arg.value!r}, which a set read does not take",
                "caller_argument_missing",
                "follow the edge with expand(..., args={...}) instead",
                relation=call.relation, argument=arg.value,
            )
    return fixed, selfs


def group_for(tool: str, call: Any, label: str, fields: Optional[dict[str, tuple[str, str]]]) -> CallGroup:
    fixed, selfs = call_args(call, tool)
    if len(selfs) != 1:
        raise refused(
            f"{tool}: relation {call.relation!r} takes {len(selfs)} name parts "
            "of a node; this read takes relations that take one",
            "unsupported_relation_shape",
            "read this type with expand for now",
            relation=call.relation,
        )
    return CallGroup(label=label, relation=call.relation, fixed=fixed,
                     self_arg=selfs[0], fields=fields)


def open_group_key(call: Any, tool: str) -> tuple[str, str]:
    fixed, _ = call_args(call, tool)
    return call.relation, canonical(fixed) + "#open"


def plan_groups(
    name: str, live_type: Any, keys: set[str], listing_keys: set[str],
) -> dict[tuple[str, str], CallGroup]:
    """The per-node calls `keys` need: one group per relation and fixed
    arguments. A key that is neither a declared field, a listing column nor
    an open field is refused with the closest names."""

    declared = dict(live_type.fields or {})
    groups: dict[tuple[str, str], CallGroup] = {}
    for key in sorted(keys):
        if key in declared:
            fld = declared[key]
            fixed, _ = call_args(fld.call, name)
            group_key = (fld.call.relation, canonical(fixed))
            group = groups.get(group_key)
            if group is None:
                group = group_for(name, fld.call, fld.call.relation, {})
                groups[group_key] = group
            assert group.fields is not None
            group.fields[key] = (fld.column, fld.type)
            group.keys.add(key)
        elif key in listing_keys:
            continue
        elif live_type.open_fields is not None:
            call = live_type.open_fields
            group_key = open_group_key(call, name)
            group = groups.get(group_key)
            if group is None:
                group = group_for(name, call, "attributes", None)
                groups[group_key] = group
            group.keys.add(key)
        else:
            from okg.substrate.mcp import live_reader

            known = sorted({*declared, *listing_keys})
            # Listing columns may be the upstream listing's own keys: served
            # as `attrs` serves them, and fenced in the text (#3666 review M1).
            close = live_reader.served_key_names(difflib.get_close_matches(key, known, n=3))
            raise refused(
                f"{name}: {live_type.subtype} has no field {key!r}"
                + (f"; did you mean {live_reader.fenced_key_list(close, prefix='attrs.')}?"
                   if close else ""),
                "unknown_field",
                f"its fields: {live_reader.fenced_key_list(known[:20], prefix='attrs.')}",
                field=f"attrs.{key}", closest=close,
            )
    return groups


async def fetch_groups(
    reader: "Reader", groups: Mapping[tuple[str, str], CallGroup],
    rows_out: list[dict[str, Any]], system_node: str, declared: Mapping[str, Any],
    *, observed_at: Optional[str] = None,
) -> None:
    """Every group's call once per row, filling each row's fields and their
    states in place: `live`, `absent` (answered without it), `unavailable`
    (the call failed) or `denied` (the gate said no).

    With `observed_at`, every field a group fills records that time in the
    row's ``live.field_observed_at`` (a set grown after its first read).
    """

    from okg.substrate.external_backed import live_dispatch as _ld

    def stamp(row: dict[str, Any], key: str) -> None:
        if observed_at is not None:
            row["live"].setdefault("field_observed_at", {})[key] = observed_at

    for group in groups.values():
        outcome = await reader.gate(group.relation, system_node)
        if not rows_out:
            continue
        if not outcome.allowed:
            for row in rows_out:
                for key in group.keys:
                    row["live"]["fields"][key] = "denied"
                    stamp(row, key)
            continue
        names = [row["node_id"].split(":", 1)[1] for row in rows_out]
        per_value = [{**group.fixed, group.self_arg: n} for n in names]
        made = await reader.calls(
            group.relation, per_value, group.self_arg, [row["node_id"] for row in rows_out],
        )
        reader.fetch.per_node[group.label] = reader.fetch.per_node.get(group.label, 0) + made.started
        answers: dict[str, list[dict[str, Any]]] = {}
        for answer_row in made.rows:
            answers.setdefault(str(answer_row.get(group.self_arg)), []).append(answer_row)
        for index, row in enumerate(rows_out):
            result = made.outcomes[index]
            got = answers.get(names[index], [])
            whole = result in (_ld.VALUE_OK, _ld.VALUE_EMPTY, _ld.VALUE_NOT_FOUND)
            answered = whole or result == _ld.VALUE_CUT
            fields_state = row["live"]["fields"]
            if group.fields is None:
                merged: dict[str, Any] = {}
                for answer_row in got:
                    merged.update({k: v for k, v in answer_row.items() if k != group.self_arg})
                if answered:
                    for key, value in merged.items():
                        # A value already held (a listing column) is kept.
                        if key not in declared and key not in row["attrs"]:
                            row["attrs"][key] = value
                            fields_state[key] = "live"
                            stamp(row, key)
                for key in group.keys:
                    if not answered:
                        fields_state[key] = "unavailable"
                        row["attrs"].pop(key, None)
                    elif key in merged:
                        row["attrs"][key] = merged[key]
                        fields_state[key] = "live"
                    else:
                        fields_state[key] = "absent" if whole else "unavailable"
                    stamp(row, key)
            else:
                first = got[0] if got else {}
                for key, (column, kind) in group.fields.items():
                    if not answered:
                        fields_state[key] = "unavailable"
                    elif column in first:
                        row["attrs"][key] = typed_value(first[column], kind)
                        fields_state[key] = "live"
                    else:
                        fields_state[key] = "absent" if whole else "unavailable"
                    stamp(row, key)


# -- the calls ----------------------------------------------------------------------


@dataclass
class Fetch:
    """What the calls of one read came to, for the reply and the samples."""

    started_at: float = field(default_factory=time.monotonic)
    calls: int = 0
    listing_calls: int = 0
    per_node: dict[str, int] = field(default_factory=dict)
    waited_seconds: float = 0.0
    cut: list[str] = field(default_factory=list)
    outcomes: dict[str, dict[str, int]] = field(default_factory=dict)


class Reader:
    """The upstream side of one read: gates, calls in three phases, samples.

    A heavy read's claim (:meth:`claim_heavy`) is released by :meth:`close`
    once no call task of this read is still running: a client that cancels
    leaves the calls in flight running to their end (their samples are
    written), and the claim is held until they stop.
    """

    def __init__(self, server: Any, tool: str, prepared: Any, deadline: float) -> None:
        self.server = server
        self.tool = tool
        self.prepared = prepared
        self.deadline = deadline
        self.stop = threading.Event()
        self.fetch = Fetch()
        self.gates: dict[str, Any] = {}
        self.bound: dict[str, Any] = {}
        self._heavy: Optional[tuple[str, Optional[str]]] = None
        self._running: set[Any] = set()
        self._closing = False

    def claim_heavy(self) -> None:
        """Take one of the heavy live reads this reader and okg logical
        session may run at once, before any upstream call; on a stateless
        transport the reader alone is the key. A third is refused by name,
        never queued."""

        from okg.substrate.external_backed import live_dispatch as _ld

        reader = str(getattr(self.prepared, "reader_key", "") or "")
        stateless = bool(getattr(self.prepared, "stateless", False))
        session = None if stateless else getattr(self.prepared, "logical_session", None)
        if not _ld.claim_live_type_read(reader, session):
            per = (
                "per reader (stateless transport)" if session is None
                else "per reader and session"
            )
            raise refused(
                f"{self.tool}: {_ld.LIVE_TYPE_READS_PER_KEY} live reads that call "
                f"once per node are already running {per}; this one is refused "
                "rather than queued, and no upstream call was made",
                "heavy_reads_busy",
                "send this call again when one of the others has answered, or "
                "narrow it to fields the listing itself carries",
                running=_ld.LIVE_TYPE_READS_PER_KEY, key="reader" if session is None
                else "reader_and_session",
            )
        self._heavy = (reader, session)

    def _release(self) -> None:
        if self._heavy is not None:
            from okg.substrate.external_backed import live_dispatch as _ld

            _ld.release_live_type_read(*self._heavy)
            self._heavy = None

    def close(self) -> None:
        """The read is over: release its heavy claim now, or once its last
        running call task ends."""

        self._closing = True
        if not self._running:
            self._release()

    def _task_done(self, task: Any) -> None:
        self._running.discard(task)
        if self._closing and not self._running:
            self._release()

    @property
    def heavy_held(self) -> bool:
        return self._heavy is not None

    async def gate(self, relation: str, node_id: str) -> Any:
        if relation not in self.gates:
            self.bound[relation] = self.prepared.bound(relation)
            self.gates[relation] = await self.server._okg_run_blocking(
                self.prepared.gate, self.bound[relation], node_id,
            )
        return self.gates[relation]

    async def calls(
        self, relation: str, per_value: list[dict[str, Any]], each_key: Optional[str],
        node_ids: list[str], empty_error: Sequence[str] = (),
    ) -> Any:
        """Phase 2 then phase 3: the calls on the wait-only executor, then
        one call sample per upstream call made, on a call worker. A client
        cancel sets `stop`; the samples of the calls made are still written."""

        from okg.substrate.external_backed import live_dispatch as _ld
        from okg.substrate.mcp.response_bounds import run_sync_in_request_context

        if time.monotonic() >= self.deadline:
            raise refused(
                f"{self.tool}: the live read reached its {DEADLINE_SECONDS:.0f} s "
                "deadline before its calls to "
                f"{relation!r}",
                "deadline", "narrow the read with a condition the system applies",
            )
        bound = self.bound[relation]
        outcome = self.gates[relation]
        adapter = empty_answer_adapter(self.prepared.adapter(relation), empty_error)
        validated = [self.prepared.validate(relation, args) for args in per_value]
        observed_at = datetime.now(timezone.utc)
        submitted = time.monotonic()
        entered: list[float] = []

        def calls_body() -> Any:
            entered.append(time.monotonic())
            return _ld.run_live_calls(
                bound=bound, adapter=adapter, per_value=validated, each_key=each_key,
                node_id=node_ids[0] if len(node_ids) == 1 else node_ids[0].split(":", 1)[0] + ":*",
                deadline=self.deadline, stop=self.stop, max_bytes=MAX_ANSWER_BYTES,
            )

        async def calls_then_samples() -> Any:
            made = None
            try:
                made = await run_sync_in_request_context(
                    calls_body, executor=_ld.run_wait_executor(),
                )
            finally:
                samples = _samples(
                    bound, outcome, validated, each_key, node_ids, made, observed_at,
                )
                if samples:
                    await self.server._okg_run_blocking(self.prepared.write_samples, samples)
            return made

        task = asyncio.ensure_future(calls_then_samples())
        self._running.add(task)
        task.add_done_callback(self._task_done)
        try:
            made = await asyncio.shield(task)
        except asyncio.CancelledError:
            self.stop.set()
            task.add_done_callback(_log_unawaited_failure)
            raise
        except _ld.LiveConcurrencyExhausted as exc:
            # Raised before any call was submitted. The run step's wording
            # ("send the run again") does not fit a read tool.
            raise _ld.LiveConcurrencyExhausted(
                f"{self.tool}: {_ld.MAX_CONCURRENT_LIVE_CALLS} live calls are "
                "already in flight in this process; this read is refused rather "
                "than queued, and no upstream call was made",
                data=dict(getattr(exc, "data", None) or {}),
                fix_hint="send this call again once the other live calls have returned",
            ) from None
        if entered:
            self.fetch.waited_seconds += max(0.0, entered[0] - submitted)
        self.fetch.calls += made.started
        counts = made.counts()
        bucket = self.fetch.outcomes.setdefault(relation, {})
        for key, value in counts.items():
            if value:
                bucket[key] = bucket.get(key, 0) + value
        return made


def _log_unawaited_failure(task: Any) -> None:
    if task.cancelled():
        return
    failure = task.exception()
    if failure is not None:
        log.warning(
            "a live read's calls finished after its client went away and failed: %s",
            type(failure).__name__,
        )


def _samples(
    bound: Any, outcome: Any, per_value: list[dict[str, Any]], each_key: Optional[str],
    node_ids: list[str], made: Any, observed_at: datetime,
) -> list[dict[str, Any]]:
    """One call sample per upstream call made: one database row per
    upstream call proves the call happened."""

    from okg.substrate.external_backed import live_dispatch as _ld

    if made is None:
        return []
    out: list[dict[str, Any]] = []
    by_value: dict[str, list[dict[str, Any]]] = {}
    if each_key is not None:
        for row in made.rows:
            by_value.setdefault(str(row.get(each_key)), []).append(row)
    for index, value_outcome in enumerate(made.outcomes):
        if value_outcome == _ld.VALUE_NOT_STARTED:
            continue
        query = per_value[index]
        if each_key is not None:
            payload: Any = by_value.get(str(query.get(each_key)), [])
        else:
            payload = made.rows
        reason = made.reasons[index]
        if value_outcome in (_ld.VALUE_OK, _ld.VALUE_EMPTY, _ld.VALUE_NOT_FOUND, _ld.VALUE_CUT):
            status = "ok"
        elif reason and "deadline" in str(reason):
            status = "timeout"
        else:
            status = "error"
        out.append({
            "relation": bound.relation.name,
            "node_id": node_ids[index] if index < len(node_ids) else node_ids[0],
            "principal": outcome.principal,
            "decision": "allow",
            "status": status,
            "payload": payload if status == "ok" else None,
            # A `digest` relation (the per-node ones) keeps a hash and this
            # pointer, never the payload; the writer refuses a digest with no
            # way to fetch it again.
            "refetch": _ld._refetch_pointer(
                relation=bound.relation.name, source=str(bound.binding.kind),
                query=query, args_fingerprint=_ld.canonical_args_fingerprint(query),
            ),
            "store_inline": True,
            "observed_at": observed_at,
            "attrs": {
                "args_fingerprint": _ld.canonical_args_fingerprint(query),
                "action": outcome.action,
                "adapter_kind": bound.binding.kind,
                "reason_code": outcome.reason,
                "adapter_ran": True,
                "served_to": "live_read",
                "value_outcome": value_outcome,
                **({"failure": str(reason)} if reason and status != "ok" else {}),
                **(
                    {"policy_fingerprint": outcome.policy_fingerprint}
                    if getattr(outcome, "policy_fingerprint", None) else {}
                ),
            },
        })
    return out


def _limit_of(args: Mapping[str, Any]) -> Optional[int]:
    from okg.substrate.external_backed.live_dispatch import UPSTREAM_LIMIT_ARGUMENT

    value = args.get(UPSTREAM_LIMIT_ARGUMENT)
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


async def listing(
    reader: Reader, relation: str, args: dict[str, Any], system_node: str, label: str,
    system: Optional[str] = None, empty_error: Sequence[str] = (),
) -> tuple[list[dict[str, Any]], bool]:
    """One listing call: (rows, cut). A listing that gives no answer (a
    failure, a timeout) is refused: a set read never serves 0 rows for it."""

    from okg.substrate.external_backed import live_dispatch as _ld

    outcome = await reader.gate(relation, system_node)
    if not outcome.allowed:
        raise refused(
            f"{reader.tool}: the {label} listing is not allowed here",
            "listing_denied",
            "the deployment's live-relation policy decides; no call was made",
            relation=relation,
        )
    made = await reader.calls(
        relation, [args], None, [system_node], empty_error=empty_error,
    )
    reader.fetch.listing_calls += made.started
    result = made.outcomes[0]
    if result in (_ld.VALUE_FAILED, _ld.VALUE_NOT_STARTED):
        raise refused(
            f"{reader.tool}: the {label} listing gave no answer "
            f"({made.reasons[0] or result}); "
            f"{system_word(system or system_node.split(':', 1)[-1])} unavailable",
            "listing_failed",
            "send the call again later",
            relation=relation, failure=made.reasons[0],
        )
    rows = list(made.rows)
    limit = _limit_of(reader.prepared.validate(relation, args))
    cut = result == _ld.VALUE_CUT or (limit is not None and len(rows) >= limit)
    if cut:
        reader.fetch.cut.append(f"{label} listing cut at {limit if limit else len(rows)}")
    return rows, cut


# -- the reply's first line ------------------------------------------------------------


def live_line(
    *, system: Optional[str], started: str, finished: str, label: str,
    coverage: Mapping[str, Any],
) -> str:
    """The first line of a live reply: the system, the read window, what was
    read, the calls made, what the system applied, cuts, and whether the
    rows were kept."""

    window = f"read {started[11:19]}-{finished[11:19]}Z"
    parts = [
        "live", system_word(system), window,
        f"{coverage['read']} of {coverage['listed']} {label} read",
    ]
    per_node = coverage.get("per_node_calls") or {}
    listing_calls = int(coverage.get("listing_calls") or 0)
    detail = ([f"{listing_calls} list"] if listing_calls or not per_node else []) + [
        f"{count} {name}" for name, count in per_node.items()
    ]
    calls = int(coverage.get("calls") or 0)
    parts.append(f"{calls} call{'s' if calls != 1 else ''} ({' + '.join(detail)})")
    applied = coverage.get("applied_by_system") or []
    if applied:
        parts.append(f"applied by {system_word(system)}: {'; '.join(applied)}")
    parts.extend(coverage.get("cuts") or [])
    names_refused = int(coverage.get("names_refused") or 0)
    if names_refused:
        parts.append(
            f"{names_refused} listed name{'s' if names_refused != 1 else ''} "
            "failed the type's name pattern and were left out"
        )
    if coverage.get("grown_from"):
        window_from = coverage.get("grown_from_window")
        parts.append(
            f"grown from {coverage['grown_from']}"
            + (f" (its fields read {window_from})" if window_from else "")
        )
    if coverage.get("not_comparable"):
        parts.append(f"{coverage['not_comparable']} values not comparable")
    waited = float(coverage.get("waited_seconds") or 0.0)
    if waited >= 0.5:
        parts.append(f"waited {waited:.0f} s for {system_word(system)} slots")
    parts.append("kept" if coverage.get("kept") else "not kept")
    return " · ".join(parts)


__all__ = [
    "DEADLINE_SECONDS",
    "HANDOFF",
    "HOLDER",
    "LiveReadRefused",
    "MAX_UPSTREAM_CALLS",
    "NOT_COMPARABLE",
    "REF_PREFIX",
    "Reader",
    "Resolved",
    "as_mapping",
    "call_args",
    "canonical",
    "coerce_for",
    "fetch_groups",
    "field_key",
    "group_for",
    "listing",
    "live_line",
    "name_checked",
    "now_text",
    "plan_groups",
    "refused",
    "system_word",
    "with_tail",
]
