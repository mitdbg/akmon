"""The live-graph resolver: a read of a live type, turned into upstream calls.

Issue #3666, PACT ``live-graph-rucio-sites``.

WHERE IT RUNS
-------------
At the asynchronous call layer (``_RawResultFastMCP.call_tool``), beside
``run``'s handler hook. Everything above it has already run: the argument
rewrite, the principal gate for the tool called, the session's tool surface
and the argument validation. Nothing below it holds a call worker or a pool
connection while an upstream system answers.

WHAT IT DOES, IN ORDER
----------------------
1. Finds the type the read reaches: ``input.subtype``, a ``subtype eq`` clause
   of the top-level ``and`` or of a nested ``and``, or a node-id prefix. A
   type under ``or``/``not``, live and stored types in one set, an anchored
   set input on a live type, and a ``search`` that names a live type are
   refused by name. No live type: nothing happens, and the call goes on
   exactly as before.
2. Checks the caller: the dispatch gate for ``expand`` where one is installed
   (multi-user servers); ``expand`` in the session's surface and the type in
   ``agent_surface.live_types``; access rules off.
3. Runs the tool body's own argument refusals (the shared pre-check), the
   per-tool identity-channel check, and the condition's own shape check.
4. Plans: pushes down only conditions on keys the system edge's
   ``push_down.keys`` names (and ``node_id``), works out every field the call
   needs, and refuses a planned overrun of :data:`MAX_UPSTREAM_CALLS`.
5. Fetches in ``run``'s three phases (:class:`live_calls.Reader`). A client
   cancel stops further calls; the calls already made still get their
   samples, and a heavy read's claim is held until they stop.
6. Refuses a key that no row read carries (a probable typo) when the rows
   are the whole listing, or notes it on a narrowed read, then applies
   every remaining condition with the kept-set reader's own evaluator
   (:func:`live_reader.select_rows`), so the first call and a later call
   over the kept set answer the same rows.
7. Hands the rows to the tool body through a context variable
   (:data:`live_calls.HANDOFF`), which the byte-bound wrapper reads inside
   its own draft scope (:func:`seed_call_memo`); the tool body then reads
   them as a kept set, ``input={"ref": <id>}``.
8. Holds the rows for the whole MCP call (:func:`holder_scope`), so a second
   entry within that call makes no new upstream call.

Nothing about a live node is stored.
"""
from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import hashlib
import json
import re
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError
import okg.substrate.mcp.live_calls as _calls
import okg.substrate.mcp.live_access_rules as _access_rules
from okg.substrate.mcp.live_calls import (
    DEADLINE_SECONDS,
    HANDOFF,
    HOLDER,
    MAX_NAMES_SHOWN,
    MAX_UPSTREAM_CALLS,
    NOT_COMPARABLE,
    RECORD_FIELDS,
    REF_PREFIX,
    LiveReadRefused,
    Reader,
    Resolved,
    as_mapping,
    call_args,
    canonical,
    coerce_for,
    fetch_groups,
    field_key,
    is_empty_expression_error,
    listing,
    live_line,
    name_checked,
    now_text,
    plan_groups,
    refused,
    system_word,
    with_tail,
)

#: The read tools a live type is served through, by this resolver.
LIVE_TOOLS = frozenset({"filter", "map", "aggregate"})
#: Leaf operators the in-memory evaluator answers.
_LEAF_OPS = frozenset({
    "eq", "in", "exists", "lt", "gt", "between", "prefix", "suffix", "contains",
})

#: `(tool, generation)` that `run` itself put on one step's call: run pins
#: every step to one graph version. A live type has no generations, so the
#: resolver drops exactly this value from a live read (and only once the read
#: is found to be live), while a generation_id the agent named is still
#: refused. None outside such a call (#3666 review M3).
_RUN_GENERATION: contextvars.ContextVar[Optional[tuple[str, str]]] = contextvars.ContextVar(
    "okg_live_run_generation", default=None,
)


@contextlib.contextmanager
def run_generation_scope(tool: str, generation: Any) -> Iterator[None]:
    """Mark `generation` as run's own on the one step call made inside."""

    token = _RUN_GENERATION.set(None if generation is None else (str(tool), str(generation)))
    try:
        yield
    finally:
        _RUN_GENERATION.reset(token)


def without_run_generation(name: str, arguments: Any) -> Any:
    """`arguments` without the generation_id `run` put on this live read
    (see :data:`_RUN_GENERATION`); unchanged otherwise, so an agent's own
    generation_id still reaches :func:`check_arguments` and is refused."""

    marked = _RUN_GENERATION.get()
    given = arguments.get("generation_id")
    if marked is None or given is None or marked != (str(name), str(given)):
        return arguments
    return {k: v for k, v in arguments.items() if k != "generation_id"}


#: The tools whose `input` may be a string (`Union[str, ...]`): a node id, or
#: a JSON object FastMCP decodes inside `Tool.run` (`pre_parse_json`). That is
#: after the grammar rewrite and the resolver have read the call, so both
#: must see the decoded form. Every other argument is refused as a string by
#: argument validation, loudly, before either runs.
STRING_INPUT_TOOLS = frozenset({"aggregate", "expand", "filter", "map"})
#: The tools whose `input` may name a handle as a string (PACT
#: read-live-handles, N7): `"<id>"`, `'"<id>"'` and `'{"ref": "<id>"}'` all
#: become `{"ref": "<id>"}`.
HANDLE_INPUT_TOOLS = frozenset({"aggregate", "filter", "map"})

#: The string-input tools whose `input` is required: it has no `None` form, so
#: a string that FastMCP would decode to `None` fails validation inside
#: `Tool.run`, as an untyped error (#4022). `expand`'s `input` is optional
#: (`None` is the absent input, which `next` continues), so it is not listed.
REQUIRED_INPUT_TOOLS = frozenset({"aggregate"})


class JsonStringRefused(ValueError):
    """A string `input` that starts like JSON but is not an object."""

    def __init__(self, tool: str, problem: str) -> None:
        self.tool, self.problem = tool, problem
        self.fix_hint = (
            "send `input` as an object, or as a JSON string that decodes to one; "
            "a node id is sent as a plain string; leave `input` out to count every record"
        )
        super().__init__(f"{tool}: the string `input` {problem}")


def decode_json_arguments(tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
    """`arguments` with a JSON-object string `input` decoded, as
    `pre_parse_json` decodes it later (#3989). The same mapping when nothing
    was decoded.

    A handle id, bare or JSON-encoded (`result_references.string_input_shape`),
    becomes `{"ref": id}` on `filter`, `map` and `aggregate`. A string that
    decodes to JSON null is refused on a tool whose `input` is required
    (#4022). A string that does not start with `{` or `[`, or starts with `[`
    and is not JSON, is left alone: it is a node id. One that starts with `{`
    and is not valid JSON, or that decodes to a list, is refused
    (:class:`JsonStringRefused`). FastMCP keeps every other decoded scalar (a
    number, a boolean, a quoted string) as the string sent, so those stay
    node ids.
    """

    value = arguments.get("input") if tool in STRING_INPUT_TOOLS else None
    if not isinstance(value, str):
        return arguments
    from okg.substrate.mcp.result_references import string_input_shape

    # One rule for the shape of a string input, shared with the grammar's
    # handle rewrite (PACT read-live-handles, N7), so the two never disagree.
    shape, named = string_input_shape(value)
    if shape == "handle" and tool in HANDLE_INPUT_TOOLS:
        return {**arguments, "input": {"ref": named}}
    head = value.lstrip()[:1]
    if head == "n" and tool in REQUIRED_INPUT_TOOLS:
        try:
            decoded_null = json.loads(value) is None
        except ValueError:
            decoded_null = False
        if decoded_null:
            raise JsonStringRefused(
                tool,
                "decodes to null; send an object or a node id, "
                "or leave `input` out to count every record",
            )
        return arguments
    if shape == "text" or head not in ("{", "["):
        return arguments
    try:
        decoded = json.loads(value)
    except RecursionError:
        raise JsonStringRefused(tool, "is nested too deeply to read") from None
    except ValueError:
        if head == "[":
            return arguments
        raise JsonStringRefused(tool, "starts like a JSON object but is not valid JSON") from None
    if not isinstance(decoded, dict):
        raise JsonStringRefused(tool, "decodes to a list; send an object") from None
    return {**arguments, "input": decoded}


# Kept under their earlier names for callers and tests.
_HANDOFF = HANDOFF
_HOLDER = HOLDER
_empty_answer_adapter = _calls.empty_answer_adapter
_with_tail = with_tail


# -- the kept-set kind -------------------------------------------------------


@dataclass(frozen=True)
class LiveTypedSet:
    """A live read of one live type: the kept-set kind for live types.

    Bound to the live type, a digest of its declaration, the reader and okg's
    logical session (None on stateless HTTP: the reader alone), with the gate
    outcome and the coverage per relation, instead of a stored anchor node
    and one relation. The attributes after ``coverage`` keep the reader that
    reads a kept expand answer working on this kind too.
    """

    subtype: str
    system: Optional[str]
    declaration_digest: str
    reader_key: str
    logical_session: Optional[str]
    stateless: bool
    #: relation name -> GateOutcome.
    gate_outcomes: Mapping[str, Any]
    #: relation name -> BoundLiveRelation, for a later read's gate.
    bound: Mapping[str, Any]
    coverage: Mapping[str, Any]
    observed_at: str
    rows_json: bytes
    #: The listing relation and its arguments (the read's first call).
    relation_name: str
    query: Mapping[str, Any]
    #: The system node the set starts at (``system:example``).
    node_id: str
    row_count: int
    digest: str
    live_line: str
    each: Optional[str] = None
    each_from: Optional[Mapping[str, Any]] = None
    #: The kept set this one grew from, or None for a first read.
    grown_from: Optional[str] = None
    #: The deployment a later read's call sample is written for.
    deployment_name: str = ""
    #: Whether every node id was built from a name that passed the type's
    #: anchored ``name_pattern``; ids that did not are served as untrusted.
    ids_checked: bool = False
    #: Keys the type declares (per-node fields, listing columns, name
    #: columns): never refused as a probable typo.
    declared_keys: tuple[str, ...] = ()
    #: Whether the rows are the whole listing: nothing pushed down, no
    #: condition dropped a row, no cut, no name refused. Only then is a key
    #: no row carries refused as a probable typo; on a narrowed set it is
    #: answered with a note naming it.
    whole_listing: bool = True
    #: A handle derived from another (a filter that narrowed or ordered it,
    #: PACT read-live-handles): the handle it came from, and the row count
    #: of the call that made the original set, for `original_call`.
    derived_from: Optional[str] = None
    original_rows: Optional[int] = None

    @property
    def refetch(self) -> Any:
        """The listing call's re-fetch pointer, for a later read's sample."""

        from okg.substrate.external_backed import live_dispatch as _ld

        return _ld._refetch_pointer(
            relation=self.relation_name, source=self.source, query=dict(self.query),
            args_fingerprint=self.args_fingerprint,
        )

    @property
    def relation(self) -> Any:
        return self.bound[self.relation_name].relation

    @property
    def subject(self) -> Optional[str]:
        outcome = self.gate_outcomes.get(self.relation_name)
        return getattr(outcome, "principal", None)

    @property
    def args_fingerprint(self) -> str:
        from okg.substrate.external_backed.live_dispatch import canonical_args_fingerprint

        return canonical_args_fingerprint(self.query)

    @property
    def source(self) -> str:
        return str(self.bound[self.relation_name].binding.kind)


def declaration_digest(live_type: Any) -> str:
    """A stable digest of one live type's declaration."""

    def plain(value: Any) -> Any:
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return {f.name: plain(getattr(value, f.name)) for f in dataclasses.fields(value)}
        if isinstance(value, Mapping):
            return {str(k): plain(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
        if isinstance(value, (set, frozenset)):
            return sorted(plain(v) for v in value)
        if isinstance(value, (list, tuple)):
            return [plain(v) for v in value]
        return value

    text = json.dumps(plain(live_type), sort_keys=True, default=str, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def declared_keys(live_type: Any, edge: Any = None) -> tuple[str, ...]:
    """Keys `live_type` declares: its per-node fields, its name columns, and
    the listing columns and push-down keys of the edge a set of it is read
    through. None of them is ever refused as a probable typo."""

    keys = set(live_type.fields or {}) | set(live_type.id_from or ())
    if edge is not None:
        keys |= set(getattr(edge, "member_columns", ()) or ())
        push = getattr(edge, "push_down", None)
        if push is not None:
            keys |= set(getattr(push, "member_columns", ()) or ())
            keys |= set(getattr(push, "keys", ()) or ())
    return tuple(sorted(keys))


# -- context variables: the handoff and the whole-call holder -----------------


@contextlib.contextmanager
def holder_scope() -> Iterator[None]:
    """Hold resolved live rows for the rest of one MCP call."""

    token = HOLDER.set({})
    try:
        yield
    finally:
        HOLDER.reset(token)


@contextlib.contextmanager
def handoff_scope(resolved: Resolved) -> Iterator[None]:
    token = HANDOFF.set(resolved)
    try:
        yield
    finally:
        HANDOFF.reset(token)


def has_handoff(operator_name: str) -> bool:
    """Whether this call carries a resolved live read for `operator_name`:
    checked before anything else is done for it, so a stored read pays
    nothing."""

    resolved = HANDOFF.get()
    return resolved is not None and resolved.tool == operator_name


# -- the keep rule -------------------------------------------------------------

#: `filter`, `map` and `aggregate` keep every live read (PACT
#: read-live-handles, 4.1.1). The far end of a live node's edge
#: (`expand(input=<live node>, edge_type=...)`) is not one of the set reads
#: 3a-i names: it keeps today's rule, kept when it made MORE than this many
#: upstream calls or its reply is cut.
EXPAND_KEEP_OVER_CALLS = 20
#: The page size a read tool serves when the call names none.
DEFAULT_PAGE = 50
#: Tools whose reply is a page of rows (`limit` + `offset`).
_PAGED_TOOLS = frozenset({"filter", "map", "expand"})


def _reply_cut(operator_name: str, state: Mapping[str, Any], call: Mapping[str, Any]) -> bool:
    """Whether the reply will not show every row: the listing was cut, or
    the page the call asks for ends before the rows do."""

    coverage = state.get("coverage") or {}
    if coverage.get("cut"):
        return True
    if operator_name not in _PAGED_TOOLS:
        return False
    limit = call.get("limit")
    offset = call.get("offset")
    page = int(limit) if isinstance(limit, int) and not isinstance(limit, bool) else DEFAULT_PAGE
    start = int(offset) if isinstance(offset, int) and not isinstance(offset, bool) else 0
    rows = state.get("rows") or []
    return start + max(1, page) < len(rows)


def _lifetime_minutes() -> int:
    from okg.substrate.mcp import live_result_sets as _lrs

    return max(1, _lrs.LIFETIME_SECONDS // 60)


def _keep(operator_name: str, state: dict[str, Any], *, cut: bool) -> None:
    """Mint the kept set inside this call's draft scope, or say why not.

    At capacity the mint raises before any reply is built: an uncut read is
    served unkept ("not kept: capacity"); a cut read is refused by name, with
    the conditions that would narrow it and the upstream calls already made,
    because page 2 could not read rows that were never kept.
    """

    from okg.substrate.mcp import live_reader
    from okg.substrate.mcp import live_result_sets as _lrs
    from okg.substrate.mcp import result_references as _rr

    store = _lrs.live_store()
    try:
        draft = _mint_kept(state)
    except _rr.ResultReferenceCapacity as exc:
        reason = (exc.data or {}).get("reason")
        why = "too_large" if reason in {"reference_members", "reference_bytes"} else "capacity"
        if cut:
            calls = int(state.get("calls_made") or 0)
            narrowing = list(state.get("narrowing") or [])
            raise refused(
                f"{operator_name}: this live read's reply would be cut, and its "
                + ("rows are too many to keep" if why == "too_large"
                   else "kept-set store is full")
                + f" ({reason}); page 2 could not read rows that were never "
                f"kept, so the read is refused. {calls} upstream "
                f"call{'s were' if calls != 1 else ' was'} already made",
                "store_full" if why == "capacity" else "kept_set_too_large",
                "store full: narrow the read"
                + (" with an eq or in on "
                   + live_reader.fenced_key_list(narrowing[:4]) + " or on node_id"
                   if narrowing else "")
                + f", or retry within {_lifetime_minutes()} min",
                calls_made=calls, narrowing=narrowing, store_reason=reason,
            ) from None
        state["not_kept"] = why
        state["live_line"] = with_tail(str(state.get("live_line") or ""), f"not kept: {why}")
        return
    _rr.adopt_draft(store, draft)
    _record_kept(state, draft)


def _mint_kept(state: Mapping[str, Any]) -> Any:
    """The kept set's draft in the live store (its reader, its logical
    session's quota); raises the store's capacity refusal."""

    from okg.substrate.mcp import handles as _handles
    from okg.substrate.mcp import live_result_sets as _lrs

    kept_set: LiveTypedSet = state["set"]
    mint = state.get("mint") or {}
    coverage = {**dict(kept_set.coverage), "kept": True}
    payload = dataclasses.replace(
        kept_set, coverage=coverage,
        live_line=with_tail(kept_set.live_line, "kept"),
        grown_from=state.get("grow_from"),
    )
    return _lrs.live_store().mint(
        reader_key=kept_set.reader_key, binding=dict(mint.get("binding") or {}),
        members=(), header_claim=mint.get("header_claim"),
        multi_user=bool(mint.get("multi_user")), payload=payload,
        payload_bytes=len(kept_set.rows_json), payload_rows=kept_set.row_count,
        # The caps count per reader and logical session (M1).
        quota_key=_handles.quota_key(
            kept_set.reader_key, kept_set.logical_session, bool(kept_set.stateless),
        ),
    )


def _record_kept(state: dict[str, Any], draft: Any) -> None:
    """What a kept set's state says once its draft is held or committed."""

    from okg.substrate.mcp import handles as _handles
    from okg.substrate.mcp import live_result_sets as _lrs

    kept_set: LiveTypedSet = state["set"]
    stateless = bool(kept_set.stateless)
    state["kept_id"] = draft.id
    call = state.get("call")
    if isinstance(call, Mapping):
        # The calls that rebuild it, for a dead handle's retry hint (3.6);
        # a grown set's chain starts with the one its parent was made by.
        _handles.remember(
            draft.id, kept_set.reader_key, call, parent=state.get("grow_from"),
        )
    state["result_reference"] = _lrs.reference_block(
        set_id=draft.id, expires_at=draft.expires_at, rows=kept_set.row_count,
        digest=kept_set.digest, observed_at=kept_set.observed_at,
    )
    tail = f"kept as {draft.id} for {_lifetime_minutes()} min"
    if stateless:
        tail += " · kept per reader (stateless transport)"
    state["live_line"] = with_tail(str(state.get("live_line") or ""), tail)


def keep_grown_now(state: dict[str, Any]) -> None:
    """Commit a grown set now, before the tool body computes over it (#4181).

    `map(compute)` over a handle that lacked fields fetched them (up to
    1,500 upstream calls); a refusal found after the fetch (the output
    bounds, the evaluation time, a field absent from every row) used to
    release the grown set with the refused reply, so a narrower retry paid
    the same calls again. Committed here, it counts against its reader's
    and logical session's quota like any kept set, lives its own 600 s, and
    every refusal of this call names it (:func:`name_kept_growth`). So a
    refused call now takes one handle slot: at the caller's own cap the
    commit evicts the caller's oldest unpinned handle, never another
    quota key's (review M3). A served reply names it as its handle's
    `from`, as before. A full store
    leaves the set to the keep rule of the tool body, which says why it was
    not kept; the refusal then says it was not kept and why.
    """

    from okg.substrate.mcp import live_result_sets as _lrs
    from okg.substrate.mcp import result_references as _rr

    if state.get("set") is None or state.get("kept_id"):
        return
    store = _lrs.live_store()
    try:
        draft = _mint_kept(state)
    except _rr.ResultReferenceCapacity as exc:
        state["kept_now_failed"] = str((exc.data or {}).get("reason") or "capacity")
        return
    try:
        store.commit(draft)
    except _rr.ResultReferenceCapacity as exc:
        state["kept_now_failed"] = str((exc.data or {}).get("reason") or "capacity")
        return
    finally:
        # A committed draft holds no reservation, so this is a no-op then.
        store.release(draft.id)
    _record_kept(state, draft)
    state["kept_now"] = True


#: Canonical bytes of a kept-handle block's `template`, at most (review L6):
#: past it the template is left out and the block keeps the id and counts.
MAX_KEPT_TEMPLATE_BYTES = 1024
#: The holder key a call's kept grown set is registered under (review L1).
_KEPT_GROWTH = "kept-growth:"


def kept_growth_block(
    state: Optional[Mapping[str, Any]], operator: str, arguments: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """`error.kept_handle` of a refusal after growth committed its set, or
    None when this call grew nothing (#4181)."""

    if not isinstance(state, Mapping) or not state.get("grow_from"):
        return None
    if not (state.get("kept_now") or state.get("kept_now_failed")):
        return None
    from okg.substrate.mcp import handles as _handles

    calls = int(state.get("calls_made") or 0)
    said = f"{calls:,} upstream call{'s' if calls != 1 else ''}"
    if not state.get("kept_now"):
        return {
            "id": None, "reason": str(state["kept_now_failed"]),
            "upstream_calls": calls,
            "note": (
                f"the fields this call fetched ({said}) could not be kept: the "
                "handle store refused them; a retry fetches them again"
            ),
        }
    kept = str(state["kept_id"])
    kept_set = state.get("set")
    template_arguments: dict[str, Any] = {"input": {"ref": kept}}
    for public, internal in (("compute", "compute"), ("select", "projection"),
                             ("where", "predicate")):
        value = arguments.get(internal)
        if value not in (None, True, {}):
            template_arguments[public] = value
    block: dict[str, Any] = {
        "id": kept,
        "rows": int(getattr(kept_set, "row_count", 0) or 0),
        "fields": list(state.get("fetched") or ()),
        **({"fields_on_no_row": list(state["fetched_none"])}
           if state.get("fetched_none") else {}),
        "upstream_calls": calls,
        "expires_at": _handles.expires_of(kept),
        "template": {"tool": operator, "arguments": template_arguments},
        "note": (
            f"the fields this call fetched ({said}) are kept as {kept}, one of "
            "your handles until it expires; template is the refused call on "
            "it and is refused again if sent unchanged: change it as the retry "
            "line says (a where, fewer or shorter columns, a field name); over "
            "the kept handle it makes no upstream call"
        ),
    }
    if len(json.dumps(block["template"], default=str)) > MAX_KEPT_TEMPLATE_BYTES:
        del block["template"]
        block["template_omitted"] = "too_large"
    return block


def _retarget_retry(retry: Any, kept: Optional[str]) -> None:
    """A refusal's corrected call on the kept set, not the input it grew
    from, which would fetch again (review L5)."""

    if not kept or not isinstance(retry, dict):
        return
    arguments = retry.get("arguments")
    if isinstance(arguments, dict) and "input" in arguments:
        arguments["input"] = {"ref": kept}


def name_kept_growth(
    state: Optional[Mapping[str, Any]], operator: str, arguments: Mapping[str, Any],
    refusal: Any,
) -> None:
    """Name the kept grown set on `refusal` (a raised substrate error, the
    tool error wrapping one, or a refusal payload); a no-op when this call
    kept no grown set or the refusal already names it."""

    block = kept_growth_block(state, operator, arguments)
    if block is None:
        return
    kept = block.get("id")
    prefix = f"{operator} over the kept handle {kept} (no upstream call): " if kept else ""
    if isinstance(refusal, dict):
        if refusal.get("status") != "refused" or not isinstance(refusal.get("error"), dict):
            return
        error = refusal["error"]
        if "kept_handle" in error:
            return
        error["kept_handle"] = block
        if prefix:
            error["fix_hint"] = prefix + str(error.get("fix_hint") or "send the call on it")
        if isinstance(error.get("data"), dict):
            error["data"]["kept_handle"] = kept
        _retarget_retry(error.get("retry_call"), kept)
        return
    error = refusal if isinstance(refusal, SubstrateError) else getattr(
        refusal, "__cause__", None,
    )
    if not isinstance(error, SubstrateError) or getattr(error, "kept_handle", None):
        return
    error.kept_handle = block  # type: ignore[attr-defined]
    if prefix:
        error.fix_hint = prefix + (error.fix_hint or "send the call on it")
    if isinstance(error.data, dict):
        error.data["kept_handle"] = kept
    _retarget_retry(getattr(error, "retry_call", None), kept)


def name_kept_growth_served(operator: str, arguments: Mapping[str, Any], payload: Any) -> None:
    """Name this MCP call's kept grown set on the refusal it serves, however
    the refusal arose: the tool body's, a commit-time capacity refusal, a
    server fault (review L1). Reads the call's holder; a no-op outside one."""

    holder = HOLDER.get()
    if not holder or operator != "map":
        return
    for key, held in list(holder.items()):
        if isinstance(key, str) and key.startswith(_KEPT_GROWTH):
            name_kept_growth(getattr(held, "state", None), operator, arguments, payload)
            return


def seed_call_memo(
    operator_name: str, call: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """Inside the tool's own draft scope: seed ``memo["live-read:" + ref]``.

    Called by the byte-bound wrapper on its outermost entry, with the call's
    bound arguments, only when :func:`has_handoff` says this call carries a
    resolved read. Every live set read is kept (PACT read-live-handles,
    4.1.1): the set is minted and adopted here, in this draft scope; a set
    over the per-handle cap is served with no handle when its reply is not
    cut, and refused when it is. Returns the live line for the reply, or None when this
    call has no resolved live read. Every byte-fitting rerun runs inside the
    same scope and reads the same memo, so it makes no upstream call and
    mints nothing.
    """

    resolved = HANDOFF.get()
    if resolved is None or resolved.tool != operator_name:
        return None
    from okg.substrate.mcp import result_references

    memo = result_references.call_memo()
    if memo is None:
        return None
    # One copy per draft scope: a draft a failed attempt released (a
    # bare-select retry runs a new scope) is never named by the next one.
    state = dict(resolved.state)
    state["fresh"] = True
    state["calls_made"] = resolved.calls_made
    memo["live-read:" + resolved.ref] = state
    if (
        state.get("set") is not None and state.get("keepable", True)
        # A grown set committed by its growth (#4181) is kept already.
        and not state.get("kept_id")
    ):
        cut = _reply_cut(operator_name, state, call or {})
        if (
            operator_name in LIVE_TOOLS or cut
            or resolved.calls_made > EXPAND_KEEP_OVER_CALLS
        ):
            _keep(operator_name, state, cut=cut)
    return str(state.get("live_line") or "") or None


def finish_reply(operator_name: str, result: Any) -> Any:
    """The tool body's reply, after it is built (inside the draft scope).

    A reply the byte budget cut that was not kept yet is kept now (every cut
    reply is kept), or refused when the store is full. Then the reply starts
    with the live line, unless the body put it there itself.
    """

    resolved = HANDOFF.get()
    if resolved is None or resolved.tool != operator_name or not isinstance(result, dict):
        return result
    from okg.substrate.mcp import result_references

    memo = result_references.call_memo()
    state = (memo or {}).get("live-read:" + resolved.ref)
    if not isinstance(state, dict) or result.get("status") == "refused":
        return result
    if (
        result.get("truncated") and state.get("set") is not None
        and state.get("keepable", True) and not state.get("kept_id")
    ):
        _keep(operator_name, state, cut=True)
        if state.get("kept_id"):
            result["result_reference"] = dict(state["result_reference"])
            if operator_name != "expand":
                result["input"] = {"ref": state["kept_id"]}
                if operator_name in ("filter", "map") and result.get("handle") is None:
                    # The reply names its handle (PACT read-live-handles 3.7).
                    from okg.substrate.mcp import handles as _handles

                    kept = state["set"]
                    result.pop("handle_reason", None)
                    result.pop("kept", None)
                    result["handle"] = _handles.handle_block(
                        set_id=str(state["kept_id"]), rows=kept.row_count,
                        columns=_handles.columns_of(kept, state.get("rows") or []),
                        order="input order",
                        expires_at=_handles.expires_of(str(state["kept_id"])),
                        stateless=bool(kept.stateless),
                    )
    line = str(state.get("live_line") or "")
    if line:
        result = {"live_line": line, **{k: v for k, v in result.items() if k != "live_line"}}
    return result


def names_refused_block(state: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """Listed names that failed the type's ``name_pattern``: counted, and
    named as untrusted text (they are upstream values)."""

    if not isinstance(state, Mapping):
        return None
    names = list(state.get("names_refused") or [])
    count = int(state.get("names_refused_count") or len(names))
    if not count:
        return None
    from okg.substrate.mcp import live_result_sets as _lrs

    kept_set = state.get("set")
    relation = str(getattr(kept_set, "relation_name", None) or "live")
    return {
        "count": count,
        "names": _lrs.fence_rows(names, relation),
        **({"names_cut": True} if count > len(names) else {}),
        "note": (
            f"{count:,} listed name{'s' if count != 1 else ''} did not match the "
            "type's name pattern; no node was made of "
            + ("it" if count == 1 else "them")
        ),
    }


def fresh_served_fields(state: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """What a reply built from a resolved live read says about its set.

    None for a read of a set kept earlier. Kept now: the set's real id as the
    reply's ``input`` and its ``result_reference``. Not kept: no id at all,
    since the resolver's own handoff id names nothing a later call can read.
    """

    if not isinstance(state, Mapping) or not state.get("fresh"):
        return None
    out: dict[str, Any] = {}
    if state.get("kept_id"):
        out["input"] = {"ref": state["kept_id"]}
        out["result_reference"] = dict(state["result_reference"])
    else:
        # filter, map and aggregate keep every read (PACT read-live-handles
        # 4.1.1): a reply with no id says why (too_large, capacity). Only a
        # live node's edge read can still be "not_needed".
        why = str(state.get("not_kept") or "not_needed")
        out["input"] = None
        out["kept"] = {"kept": False, "reason": why}
        out["handle_reason"] = why
    if state.get("grow_from"):
        out["grown_from"] = state["grow_from"]
    names = names_refused_block(state)
    if names is not None:
        out["names_refused"] = names
    return out


def with_live_line(payload: dict[str, Any], state: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """`payload` starting with the live line of a read resolved in this call
    (inside the payload, so a byte-fitting rerun counts its bytes)."""

    if not isinstance(state, Mapping) or not state.get("fresh") or not state.get("live_line"):
        return payload
    return {"live_line": str(state["live_line"]), **payload}


# -- condition helpers ----------------------------------------------------------


def _clauses(node: Mapping[str, Any]) -> list[Any]:
    clauses = node.get("clauses")
    return list(clauses) if isinstance(clauses, list) else []


def _is_empty(condition: Any) -> bool:
    return condition in (None, True, {}, [])


def _and(conditions: Sequence[Any]) -> Any:
    kept = [c for c in conditions if not _is_empty(c)]
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return {"op": "and", "clauses": kept}


def _leaf_values(node: Mapping[str, Any]) -> list[Any]:
    if node.get("op") == "in":
        values = node.get("values")
        return list(values) if isinstance(values, (list, tuple)) else []
    return [node.get("value")]


def _walk(condition: Any, under_or_not: bool = False) -> Iterator[tuple[Mapping[str, Any], bool]]:
    """Every leaf, with whether it sits under an `or` or a `not`."""

    if not isinstance(condition, Mapping):
        return
    op = condition.get("op")
    if op == "and":
        for clause in _clauses(condition):
            yield from _walk(clause, under_or_not)
    elif op == "or":
        for clause in _clauses(condition):
            yield from _walk(clause, True)
    elif op == "not":
        yield from _walk(condition.get("clause"), True)
    elif op is not None:
        yield condition, under_or_not


def _strip_types(condition: Any, live: str) -> Any:
    """The condition without its and-level clauses that name only `live`."""

    if not isinstance(condition, Mapping):
        return condition
    if condition.get("field") == "subtype" and condition.get("op") in ("eq", "in"):
        if set(map(str, _leaf_values(condition))) == {live}:
            return None
        return condition
    if condition.get("op") == "and":
        return _and([_strip_types(c, live) for c in _clauses(condition)])
    return condition


@dataclass
class Detection:
    live_type: Any
    condition: Any


def _subtype_values(value: Any) -> list[str]:
    if isinstance(value, str) and value:
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if isinstance(v, str)]
    return []


def _split_calls(tool: str, live: list[str], stored: list[str]) -> str:
    calls = [f'{tool}(where={{"type": "{name}", ...}})' for name in [*live, *stored]]
    return "send one call per type: " + "; ".join(calls)


def tool_condition(tool: str, arguments: Mapping[str, Any]) -> Any:
    """The whole condition of a rewritten read call."""

    scope = as_mapping(arguments.get("input"))
    scoped = scope.get("filter") if isinstance(scope, Mapping) else None
    return _and([as_mapping(scoped), as_mapping(arguments.get("predicate"))])


#: Set-input keys that anchor a read at one node.
_ANCHOR_KEYS = ("node_id", "start_node")


def _refuse_anchored(tool: str, registry: Any, scope: Any) -> None:
    """An anchored set input on a live type is refused by name: the anchor
    would be dropped and every node of the type read."""

    anchor = None
    named: list[str] = []
    if isinstance(scope, str) and ":" in scope:
        anchor = scope
    elif isinstance(scope, Mapping):
        anchor = next((scope.get(k) for k in _ANCHOR_KEYS if scope.get(k)), None)
        named = _subtype_values(scope.get("subtype"))
        if anchor is None and scope.get("kind") == "node":
            anchor = ""
    if anchor is None:
        return
    live = [t for t in named if registry.by_subtype(t) is not None]
    owner = (
        registry.by_prefix(anchor.split(":", 1)[0])
        if isinstance(anchor, str) and ":" in anchor else None
    )
    if owner is not None:
        live.append(str(owner.subtype))
    if not live:
        return
    raise refused(
        f"{tool}: an input anchored at one node does not read a set of the live "
        f"type {live[0]!r}; the anchor would be dropped and every "
        f"{live[0]} read",
        "anchored_live_set",
        f'read one node with inspect(target={{"kind": "node", "node_id": "<{live[0]} id>"}}), '
        f'or the set with filter(where={{"type": "{live[0]}", ...}})',
        type=live[0],
    )


def detect(registry: Any, tool: str, arguments: Mapping[str, Any]) -> Optional[Detection]:
    """The live type a read reaches, or None.

    Raises :class:`LiveReadRefused` for a type under `or`/`not`, for live
    and stored types in one set, and for an anchored input on a live type,
    before anything else runs.
    """

    if registry is None or tool not in LIVE_TOOLS or not getattr(registry, "types", None):
        return None
    scope = as_mapping(arguments.get("input"))
    if isinstance(scope, Mapping) and "ref" in scope:
        return None
    _refuse_anchored(tool, registry, scope)
    if not isinstance(scope, Mapping):
        return None
    condition = tool_condition(tool, arguments)
    set_types: list[str] = list(_subtype_values(scope.get("subtype")))
    nested_types: list[str] = []
    id_types: list[str] = []
    any_live_id = False
    for leaf, nested in _walk(condition):
        fld = leaf.get("field")
        if fld == "subtype" and leaf.get("op") in ("eq", "in"):
            values = [str(v) for v in _leaf_values(leaf) if isinstance(v, str)]
            (nested_types if nested else set_types).extend(values)
        elif fld == "node_id" and leaf.get("op") in ("eq", "in"):
            for value in _leaf_values(leaf):
                if not isinstance(value, str) or ":" not in value:
                    continue
                owner = registry.by_prefix(value.split(":", 1)[0])
                if owner is None:
                    continue
                any_live_id = True
                if not nested:
                    id_types.append(owner.subtype)
    if not set_types and not nested_types and not any_live_id:
        return None
    live_set = {t for t in set_types if registry.by_subtype(t) is not None} | set(id_types)
    stored_set = {t for t in set_types if registry.by_subtype(t) is None}
    live_nested = [t for t in nested_types if registry.by_subtype(t) is not None]
    if live_nested or (live_set and nested_types):
        # Otherwise the call would read the stored graph and answer 0 rows
        # with no warning, or keep a clause no live row can meet.
        named = sorted(set(live_nested) or set(nested_types))
        raise refused(
            f"{tool}: the type {named[0]!r} is named under `or` or `not`; "
            "a set of a live type is named once, as a condition every record "
            "meets",
            "type_under_or_not",
            _split_calls(tool, sorted(set(live_nested) | live_set), sorted(stored_set)),
            types=sorted(set(nested_types)),
        )
    if not live_set:
        return None
    live = sorted(live_set)
    stored = sorted(stored_set)
    if stored or len(live) > 1:
        raise refused(
            f"{tool}: one set mixes the live type(s) {live} with "
            + (f"the stored type(s) {stored}" if stored else "other live types")
            + "; a live type is read from its system, a stored type from the graph",
            "mixed_set",
            _split_calls(tool, live, stored),
            live_types=live, stored_types=stored,
        )
    live_type = registry.by_subtype(live[0])
    return Detection(live_type=live_type, condition=_strip_types(condition, live[0]))


def _search_types(value: Any) -> Iterator[str]:
    """Every type name a `search` call's `where`, `filter` or `scope` names."""

    value = as_mapping(value)
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in ("type", "subtype", "subtypes", "subtype_filter"):
                yield from _subtype_values(item)
            elif key == "field" and item in ("subtype", "type"):
                for name in ("value", "values"):
                    yield from _subtype_values(value.get(name))
            else:
                yield from _search_types(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _search_types(item)


def refuse_live_search(registry: Any, arguments: Mapping[str, Any]) -> None:
    """`search` reads the stored graph: one that names a live type would
    answer 0 rows for it. Refused by name, with the read that works."""

    names = set(_subtype_values(arguments.get("subtype_filter")))
    for key in ("where", "filter", "scope"):
        names.update(_search_types(arguments.get(key)))
    live = sorted(t for t in names if registry.by_subtype(t) is not None)
    if not live:
        return
    raise refused(
        f"search: the type {live[0]!r} is live; search reads the stored graph, "
        "where no node of a live type is kept",
        "search_of_live_type",
        f'read it with filter(where={{"type": "{live[0]}", ...}}), or one node '
        f'with inspect(target={{"kind": "node", "node_id": "<{live[0]} id>"}})',
        type=live[0],
    )


# -- fields the call needs ------------------------------------------------------


def _strings(value: Any) -> Iterator[str]:
    value = as_mapping(value)
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def condition_keys(condition: Any) -> set[str]:
    keys: set[str] = set()
    for leaf, _nested in _walk(condition):
        fld = leaf.get("field")
        if isinstance(fld, str) and fld.startswith("attrs."):
            keys.add(fld[len("attrs."):])
    return keys


def named_keys(tool: str, arguments: Mapping[str, Any]) -> list[tuple[str, str]]:
    """(argument, key) for every key the reply reads: select / projection,
    by, metrics, order_by."""

    pairs: list[tuple[str, str]] = []
    # aggregate's order_by names one of its own metrics or by fields, which
    # are read below (PACT read-aggregate-order-by): never an upstream key.
    names = ("select", "projection", "by") + (("order_by",) if tool != "aggregate" else ())
    from okg.substrate.mcp.live_reader import split_order_direction

    for name in names:
        for text in _strings(arguments.get(name)):
            if name == "order_by":
                # The direction is split off as the body's parse_order_by
                # splits it ("x desc", "x asc", "-x"); read whole, "x desc" was
                # fetched as a field of its own (review R3-1, #4054).
                text = split_order_direction(text)[0]
            key = field_key(text)
            if key is not None:
                pairs.append((name, key))
    for text in _strings(arguments.get("metrics")):
        if "(" in text and text.endswith(")"):
            key = field_key(text[text.index("(") + 1:-1])
            if key is not None:
                pairs.append(("metrics", key))
    if tool == "map" and arguments.get("compute") is not None:
        # PACT read-map-compute (6.2): the upstream fields the computed
        # columns read are fetched as a select names them: with the listing
        # on a first read, by growth on a kept set that lacks them.
        from okg.substrate.mcp.map_compute import upstream_keys

        pairs.extend(("compute", key) for key in upstream_keys(arguments.get("compute")))
    return pairs


def output_keys(tool: str, arguments: Mapping[str, Any]) -> set[str]:
    """Keys the reply reads: select / projection, by, metrics, order_by."""

    return {key for _argument, key in named_keys(tool, arguments)}


def refuse_unsupported_ops(tool: str, condition: Any) -> None:
    """Every leaf of a live read's condition is one the in-memory evaluator
    answers, on a field it reads, in the shape the kept-set reader takes:
    refused by name before any call."""

    from okg.substrate.mcp import live_result_sets as _lrs

    for leaf, _nested in _walk(condition):
        op = leaf.get("op")
        fld = leaf.get("field")
        if op not in _LEAF_OPS:
            raise refused(
                f"{tool}: the condition op {op!r} is not answered over a live type",
                "unsupported_op",
                "use eq, in, exists, lt, gt, between, prefix, suffix or contains",
                op=op,
            )
        if not (isinstance(fld, str) and (fld in RECORD_FIELDS or fld.startswith("attrs."))):
            raise refused(
                f"{tool}: a live type's condition reads node_id, subtype or "
                f"attrs.<field>, not {fld!r}",
                "unsupported_field",
                'write the field as "attrs.<field>"',
                field=fld,
            )
    # The kept-set reader's own shape check (`in` takes `values`, `between`
    # two bounds, the text operators text): one rule for both paths.
    _lrs.check_predicate(condition)


def _refuse_whole_attrs(tool: str, arguments: Mapping[str, Any], live_type: Any, edge: Any) -> None:
    """`select=["attrs"]` on a set read of a live type would show only the
    listing's columns; the fields cost one call per node each, so they are
    named, not fetched wholesale."""

    for name in ("select", "projection"):
        if "attrs" in set(_strings(arguments.get(name))):
            known = sorted(set(declared_keys(live_type, edge)))
            open_note = (
                ", or any other attrs.<key> (one call per node for all of them)"
                if live_type.open_fields is not None else ""
            )
            raise refused(
                f"{tool}: {name}=['attrs'] on the live type {live_type.subtype!r} "
                "does not fetch its fields; name the fields the reply needs",
                "whole_attrs_on_live_set",
                f"name fields such as {', '.join('attrs.' + k for k in known[:12])}"
                + open_note,
                argument=name, type=str(live_type.subtype),
            )


# -- push-down ------------------------------------------------------------------


@dataclass
class PushPlan:
    expression: Optional[str]
    #: The pushed conditions, as the system was told them.
    applied: list[str]
    #: What is left for memory.
    rest: Any


def _pushed_text(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, str) and value:
        return value
    return None


class _Pusher:
    def __init__(self, tool: str, push: Any, live_type: Any) -> None:
        from okg.substrate.external_backed.live_types import PUSH_VALUE_CHARS

        self.tool = tool
        self.push = push
        self.live_type = live_type
        join = dict(getattr(push, "join", None) or {})
        # No default operators: a push-down that does not declare `and` and
        # `or` is refused at composition (`live_types._parse_push_down`).
        self.j_and = str(join["and"])
        self.j_or = str(join["or"])
        self.j_not = join.get("and_not")
        self.atoms = dict(getattr(push, "atoms", None) or {})
        # Only keys the declaration says the system's expression tests, and
        # never a declared per-node field: anything else is applied in memory.
        self.keys = frozenset(getattr(push, "keys", ()) or ()) - frozenset(
            getattr(live_type, "fields", None) or {}
        )
        self.allowed = PUSH_VALUE_CHARS

    def _sendable(self, text: str) -> bool:
        """Whether `text` may go into the system's expression: only letters,
        digits, `_`, `.` and `-`, so no operator can change which nodes are
        read. A clause whose text may not is not pushed: it stays in memory,
        applied after the fetch to the fields the call reads."""

        return self.allowed.fullmatch(text) is not None

    def _eq(self, key: str, value: Any) -> Optional[str]:
        template = self.atoms.get("eq")
        text = _pushed_text(value)
        if not isinstance(template, str) or text is None:
            return None
        if not (self._sendable(key) and self._sendable(text)):
            return None
        return template.replace("{key}", key).replace("{value}", text)

    def leaf(self, node: Mapping[str, Any]) -> Optional[str]:
        op, fld = node.get("op"), node.get("field")
        if fld == "node_id" and op == "eq":
            template = self.atoms.get("node_id_eq")
            value = node.get("value")
            if not isinstance(template, str) or not isinstance(value, str) or ":" not in value:
                return None
            prefix, name = value.split(":", 1)
            if prefix != self.live_type.id_prefix or not name or not self._sendable(name):
                return None
            return template.replace("{name}", name)
        if not (isinstance(fld, str) and fld.startswith("attrs.")):
            return None
        key = fld[len("attrs."):]
        if key not in self.keys:
            return None
        if op == "eq":
            return self._eq(key, node.get("value"))
        if op == "in" and "in" in self.atoms:
            parts = [self._eq(key, v) for v in _leaf_values(node)]
            if not parts or any(p is None for p in parts):
                return None
            return parts[0] if len(parts) == 1 else "(" + self.j_or.join(parts) + ")"  # type: ignore[arg-type]
        return None

    def render(self, node: Any, nested: bool) -> Optional[str]:
        if not isinstance(node, Mapping):
            return None
        op = node.get("op")
        if op == "and":
            positives: list[str] = []
            negatives: list[str] = []
            for clause in _clauses(node):
                if isinstance(clause, Mapping) and clause.get("op") == "not":
                    inner = clause.get("clause")
                    if not isinstance(self.j_not, str):
                        return None
                    group = (
                        _clauses(inner)
                        if isinstance(inner, Mapping) and inner.get("op") == "or"
                        else [inner]
                    )
                    for part in group:
                        text = self.render(part, True)
                        if text is None:
                            return None
                        negatives.append(text)
                else:
                    text = self.render(clause, True)
                    if text is None:
                        return None
                    positives.append(text)
            if not positives:
                return None
            body = self.j_and.join(positives) + "".join(
                str(self.j_not) + n for n in negatives
            )
            compound = len(positives) > 1 or bool(negatives)
            return f"({body})" if nested and compound else body
        if op == "or":
            parts = [self.render(c, True) for c in _clauses(node)]
            if not parts or any(p is None for p in parts):
                return None
            body = self.j_or.join(parts)  # type: ignore[arg-type]
            return f"({body})" if nested and len(parts) > 1 else body
        if op == "not":
            return None
        return self.leaf(node)


def plan_push(tool: str, edge: Any, live_type: Any, condition: Any) -> PushPlan:
    push = getattr(edge, "push_down", None)
    if _is_empty(condition):
        return PushPlan(None, [], None)
    if push is None:
        return PushPlan(None, [], condition)
    pusher = _Pusher(tool, push, live_type)
    whole = pusher.render(condition, False)
    if whole is not None:
        return PushPlan(whole, [whole], None)
    if isinstance(condition, Mapping) and condition.get("op") == "and":
        pushed, rest = [], []
        for clause in _clauses(condition):
            if isinstance(clause, Mapping) and clause.get("op") == "not":
                inner = clause.get("clause")
                group = (
                    _clauses(inner)
                    if isinstance(inner, Mapping) and inner.get("op") == "or" else [inner]
                )
                takes = isinstance(pusher.j_not, str) and all(
                    pusher.render(part, True) is not None for part in group
                )
            else:
                takes = pusher.render(clause, True) is not None
            (pushed if takes else rest).append(clause)
        positives = [c for c in pushed if not (isinstance(c, Mapping) and c.get("op") == "not")]
        if positives:
            expression = pusher.render({"op": "and", "clauses": pushed}, False)
            if expression is not None:
                return PushPlan(expression, [expression], _and(rest))
    return PushPlan(None, [], condition)


# -- the typo check over the rows read ----------------------------------------------


def refuse_typos(
    tool: str, rows: Sequence[Mapping[str, Any]], pairs: Sequence[tuple[str, str]],
    known: Sequence[str], *, whole: bool,
) -> Optional[dict[str, Any]]:
    """A key no row read carries, after the fetch and before any remaining
    condition is applied: a key the per-node call answered without is
    `absent`, which does not count as carried. Keys the type declares are
    never refused.

    Refused as a probable typo, with the closest keys seen, only when
    `whole`: the rows are the whole listing (nothing pushed, no condition
    dropped a row, no cut, no name refused). On a narrowed read the key may
    be real and absent from every row read, so the note naming it is
    returned for the reply instead."""

    from okg.substrate.mcp import live_reader

    names = live_reader.Names(attrs=list(pairs))
    return live_reader.check_keys_seen(rows, names, tool, known=known, whole=whole)


# -- the resolver -----------------------------------------------------------------

#: The tools the resolver sits before: set reads, reads of one live node
#: (`inspect`) or of a live edge (`expand`), in `live_nodes`, and `search`,
#: which is refused when it names a live type.
RESOLVED_TOOLS = LIVE_TOOLS | frozenset({"inspect", "expand", "search"})


async def resolve(server: Any, name: str, arguments: dict[str, Any]) -> Any:
    """Resolve a read of a live type; None when the call reaches none.

    Returns ``(arguments, Resolved)`` for the tool body, or a refusal payload
    (a dict) the tool body's own pre-check returned. Raises
    :class:`LiveReadRefused` (and the gates' own errors) before any call.
    """

    registry = getattr(server, "_okg_live_types", None)
    if registry is None or not getattr(registry, "types", None):
        return None
    try:
        # The server decodes before its rewrite; this is for a direct caller.
        arguments = decode_json_arguments(name, arguments)  # type: ignore[assignment]
    except JsonStringRefused as exc:
        raise refused(str(exc), "input_string_not_object", exc.fix_hint) from None
    if name == "search":
        refuse_live_search(registry, arguments)
        return None
    if name not in LIVE_TOOLS:
        if name in RESOLVED_TOOLS:
            from okg.substrate.mcp import live_nodes

            return await live_nodes.resolve_node(server, name, arguments)
        return None
    grown = await _resolve_growth(server, registry, name, arguments)
    if grown is not None:
        return grown
    detection = detect(registry, name, arguments)
    if detection is None:
        return None
    live_type = detection.live_type
    condition = detection.condition
    subtype = str(live_type.subtype)
    arguments = without_run_generation(name, arguments)

    await check_caller(server, name, subtype)
    refusal = await check_arguments(server, name, subtype, arguments)
    if refusal is not None:
        return refusal

    # Step 4: the plan that needs no call.
    refuse_unsupported_ops(name, condition)
    edge_set = registry.edge_set(subtype)
    if edge_set is None:
        raise refused(
            f"{name}: live type {subtype!r} has no system edge to read a set from",
            "no_system_edge", "inspect one node by id instead", type=subtype,
        )
    system_type, edge = edge_set
    _refuse_whole_attrs(name, arguments, live_type, edge)
    if name == "map" and arguments.get("compute") is not None:
        # PACT read-map-compute (review M1): a computed column named like a
        # field the type declares, refused before the listing call.
        from okg.substrate.mcp import map_compute as _mc

        _mc.refuse_shadowing(arguments.get("compute"), declared_keys(live_type, edge))
    push = plan_push(name, edge, live_type, condition)
    keys = condition_keys(push.rest) | output_keys(name, arguments)
    holder = HOLDER.get()
    holder_key = canonical({
        "type": subtype, "edge": getattr(edge, "name", None),
        "condition": condition, "tool": name,
    })
    if holder is not None and holder_key in holder:
        held = holder[holder_key]
        if {f"attrs.{k}" for k in keys} <= held.fields:
            # A second entry within this MCP call (a byte-fit rerun, a
            # bare-select retry): the rows already hold every field, so no
            # new upstream call is made.
            return _rewritten(name, arguments, held.ref), dataclasses.replace(held, tool=name)

    # Step 5: prepare on a call worker.
    prepared = await prepare_read(
        server, name, arguments, sorted({subtype, str(system_type.subtype)}),
    )
    reader = Reader(server, name, prepared, time.monotonic() + DEADLINE_SECONDS)
    try:
        if _may_fan_out(live_type, edge, push, keys):
            reader.claim_heavy()
        resolved = await _read(
            reader, name, arguments, live_type, system_type, edge, push, keys, condition,
        )
    finally:
        reader.close()
    _note_call(resolved, name, arguments)
    if holder is not None:
        holder[holder_key] = resolved
    return _rewritten(name, arguments, resolved.ref), resolved


def _note_call(resolved: Resolved, name: str, arguments: Mapping[str, Any]) -> None:
    """The producing call a kept set is remembered with (3.6), `authority`
    removed, with the upstream calls it made."""

    from okg.substrate.mcp import handles as _handles

    resolved.state["call"] = _handles.producing_call(
        name, arguments, upstream_calls=resolved.calls_made,
    )


def _held_keys(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    """Every key the rows hold: their upstream fields, the fields with a
    state, and their computed columns' names (PACT read-map-compute, 6.5),
    so a bare name equal to a computed column is never fetched upstream."""

    from okg.substrate.mcp.map_compute import VALUE_PREFIX

    held: set[str] = set()
    for row in rows:
        held.update((row.get("attrs") or {}).keys())
        held.update(((row.get("live") or {}).get("fields") or {}).keys())
        held.update(
            key[len(VALUE_PREFIX):] for key in row
            if isinstance(key, str) and key.startswith(VALUE_PREFIX)
        )
    return held


_WINDOW = re.compile(r"read (\d\d:\d\d:\d\d-\d\d:\d\d:\d\dZ)")


async def _resolve_growth(
    server: Any, registry: Any, name: str, arguments: Mapping[str, Any],
) -> Any:
    """A later call on a kept live-type set that needs a field the set lacks:
    only that field is fetched, for the nodes the set holds, and the rows go
    to the tool body as a NEW set.

    The new set is minted without `parent`, so it lives 600 s from its own
    mint; it records where it came from (`grown_from`) and when its older
    fields were read; every field fetched now carries its own time in
    ``live.field_observed_at``. The old set stays readable until its own
    expiry: a handle is an immutable name (PACT read-live-handles, N4). None when the call needs no new
    field: the body reads the set as it is, and refuses a key no row carries
    (a probable typo) itself when the set is the whole listing, or notes it
    when the set is narrowed.
    """

    from okg.substrate.mcp import live_result_sets as _lrs

    scope = as_mapping(arguments.get("input"))
    ref = scope.get("ref") if isinstance(scope, Mapping) else None
    if not _lrs.is_live_id(ref):
        return None
    predicate = as_mapping(arguments.get("predicate"))
    keys = condition_keys(predicate) | output_keys(name, arguments)
    if not keys:
        return None
    lookup = getattr(server, "_okg_live_kept_set", None)
    if lookup is None:
        return None
    # The call's MCP context, so the lookup sees the caller's logical session:
    # the server's own context variable is set only inside the tool body, and
    # this runs before it (#4050: the owner read as another session, growth
    # was skipped and the column came back null with no call and no note).
    context = server.get_context() if hasattr(server, "get_context") else None
    retained = await server._okg_run_blocking(
        lookup, name, str(ref), arguments.get("authority"), context,
    )
    if not isinstance(retained, LiveTypedSet):
        return None
    live_type = registry.by_subtype(retained.subtype)
    if live_type is None:
        return None
    arguments = without_run_generation(name, arguments)
    # The rows decoded here count against the decode budget like any read of
    # a handle (PACT read-live-handles 4.1.5, review F5). This runs before any
    # draft scope, so the reservation is given back here.
    from okg.substrate.mcp import handles as _handles

    # The caller first (review R3-2): a caller who lost access hears that
    # refusal, never a decode-capacity one.
    await check_caller(server, name, str(live_type.subtype))
    token = _handles.decode_budget.reserve(len(retained.rows_json))
    try:
        return await _grow_kept(
            server, registry, name, arguments, retained, live_type, str(ref), keys, predicate,
        )
    finally:
        _handles.decode_budget.release(token)


async def _grow_kept(
    server: Any, registry: Any, name: str, arguments: Mapping[str, Any],
    retained: "LiveTypedSet", live_type: Any, ref: str, keys: set[str], predicate: Any,
) -> Any:
    """:func:`_resolve_growth` once the kept set is found and its decode is
    reserved: the fields it lacks, fetched into a new set."""

    from okg.substrate.mcp import live_result_sets as _lrs

    rows = _lrs.decode_rows(retained.rows_json)
    if name == "map" and arguments.get("compute") is not None:
        # PACT read-map-compute: the expressions are compiled against this
        # set's own columns before any upstream call, so a refused one makes
        # no call (the tool body compiles them again over the grown rows).
        from okg.substrate.mcp import map_compute as _mc

        program = _mc.compile_for(arguments.get("compute"), retained, rows)
        _mc.check_select(arguments.get("projection"), program)
        # Every refusal the tool body would make over these rows, before
        # growth fetches anything (review M1, M2).
        _mc.refuse_operands(program, groups=False, typed=True)
        _mc.refuse_shadowing(
            arguments.get("compute"),
            _mc.upstream_names(retained, rows) | set(declared_keys(live_type)),
        )
    missing = keys - _held_keys(rows)
    if not missing:
        return None
    declared = dict(live_type.fields or {})
    edge_set = registry.edge_set(live_type.subtype)
    set_edge = edge_set[1] if edge_set is not None else None
    from_listing, per_node = growth_fields(registry, live_type, retained, missing)
    if name == "map" and arguments.get("compute") is not None:
        # An expression field no row holds that growth cannot fetch either:
        # refused before any call, never computed as null (review H1).
        unfetchable = sorted(
            (set(_mc.attrs_operands(program)) & missing) - from_listing - per_node,
        )
        _mc.check_operands_held(program, retained, rows, keys=unfetchable)
    if not per_node and not from_listing:
        return None

    holder = HOLDER.get()
    holder_key = canonical({"grow": str(ref), "tool": name})
    if holder is not None and holder_key in holder:
        held = holder[holder_key]
        if {f"attrs.{k}" for k in keys} <= held.fields:
            return _regrown(arguments, held.ref), dataclasses.replace(held, tool=name)

    subtype = str(live_type.subtype)
    # The caller check ran in :func:`_resolve_growth`, before the decode.
    refusal = await check_arguments(server, name, subtype, arguments)
    if refusal is not None:
        return refusal
    subtypes = {subtype}
    if from_listing and edge_set is not None:
        subtypes.add(str(edge_set[0].subtype))
    planned = len(rows) * len(plan_groups(name, live_type, per_node, set()))
    if planned + (1 if from_listing else 0) > MAX_UPSTREAM_CALLS:
        raise refused(
            f"{name}: growing {ref} by {sorted(missing)} would make {planned:,} "
            f"upstream calls; a call makes at most {MAX_UPSTREAM_CALLS:,}",
            "planned_overrun", "narrow the set first, then ask for the field",
            planned=planned, made=0, cap=MAX_UPSTREAM_CALLS,
        )
    prepared = await prepare_read(server, name, arguments, sorted(subtypes))
    reader = Reader(server, name, prepared, time.monotonic() + DEADLINE_SECONDS)
    started = now_text()
    try:
        groups = plan_groups(name, live_type, per_node, set())
        if groups and rows:
            reader.claim_heavy()
        if from_listing and set_edge is not None and live_type.id_from:
            id_column = live_type.id_from[0]
            full_args, _ = call_args(set_edge.call, name)
            label = str(set_edge.name)
            listed, cut = await listing(
                reader, set_edge.call.relation, full_args, retained.node_id, label,
            )
            by_name = {
                str(r.get(id_column)): r for r in listed if r.get(id_column) is not None
            }
            for row in rows:
                found = by_name.get(str(row["node_id"]).split(":", 1)[-1])
                states = row["live"]["fields"]
                stamps = row["live"].setdefault("field_observed_at", {})
                for key in from_listing:
                    if found is not None and key in found:
                        row["attrs"][key] = found[key]
                        states[key] = "live"
                    else:
                        # A node missing from a cut listing was not read; one
                        # missing from a whole listing has no such column.
                        states[key] = "unavailable" if (found is None and cut) else "absent"
                    stamps[key] = started
        await fetch_groups(
            reader, groups, rows, retained.node_id, declared, observed_at=started,
        )
    finally:
        reader.close()
    finished = now_text()
    unread = sorted(
        row["node_id"] for row in rows
        if any(row["live"]["fields"].get(k) in ("unavailable", "denied") for k in missing)
    )
    old = dict(retained.coverage)
    window = _WINDOW.search(retained.live_line or "")
    coverage = {
        "listed": len(rows),
        "read": len(rows) - len(unread),
        "not_read": unread,
        "not_comparable": 0,
        "cut": bool(old.get("cut")),
        "cuts": list(old.get("cuts") or []) + [
            c for c in reader.fetch.cut if c not in (old.get("cuts") or [])
        ],
        "applied_by_system": list(old.get("applied_by_system") or []),
        "calls": reader.fetch.calls,
        "listing_calls": reader.fetch.listing_calls,
        "per_node_calls": dict(reader.fetch.per_node),
        "outcomes": {k: dict(v) for k, v in reader.fetch.outcomes.items()},
        "waited_seconds": round(reader.fetch.waited_seconds, 1),
        "grown_from": str(ref),
        **({"grown_from_window": window.group(1)} if window else {}),
        "kept": False,
    }
    label = str(getattr(set_edge, "name", None) or f"{subtype} nodes")
    line = live_line(
        system=retained.system, started=started, finished=finished, label=label,
        coverage=coverage,
    )
    # The new set answers for every relation either read used.
    reader.gates = {**dict(retained.gate_outcomes), **reader.gates}
    reader.bound = {**dict(retained.bound), **reader.bound}
    resolved = resolved_read(
        reader, name=name, live_type=live_type, system=retained.system, rows=rows,
        coverage=coverage, line=line,
        # Most of its values were read with the set it grew from: the set's
        # time is that older read's; each new field carries its own.
        finished=retained.observed_at,
        listing_relation=retained.relation_name, query=retained.query,
        node_id=retained.node_id,
        fields=frozenset(f"attrs.{k}" for k in _held_keys(rows)),
        narrowing=[], grown_from=str(ref),
        ids_checked=retained.ids_checked, known=retained.declared_keys,
        whole=retained.whole_listing,
    )
    _note_call(resolved, name, arguments)
    if name == "map" and arguments.get("compute") is not None:
        # #4181: the grown set is committed before anything after the fetch
        # can refuse the call, so each such refusal names it and a narrower
        # map over it makes no upstream call.
        resolved.state["calls_made"] = resolved.calls_made
        # The fields read on at least one row, and those no row answered
        # with (a misspelled open field: review L3).
        asked = sorted(from_listing | per_node)
        read = {
            k for k in asked
            if any(row["live"]["fields"].get(k) == "live" for row in rows)
        }
        resolved.state["fetched"] = [f"attrs.{k}" for k in asked if k in read]
        resolved.state["fetched_none"] = [f"attrs.{k}" for k in asked if k not in read]
        # The store's lock and eviction plan run off the event loop, as the
        # kept-set lookup above does (review L2).
        await server._okg_run_blocking(keep_grown_now, resolved.state)
        if holder is not None:
            # Every refusal this MCP call serves names it (review L1).
            holder[_KEPT_GROWTH + resolved.ref] = resolved
    known = retained.declared_keys or declared_keys(live_type, set_edge)
    try:
        # The same rule as a first read: refused as a probable typo only
        # when the set's rows are the whole listing, otherwise noted.
        keys_absent = refuse_typos(
            name, rows,
            [("predicate", k) for k in sorted(condition_keys(predicate) & missing)]
            + [(arg, k) for arg, k in named_keys(name, arguments) if k in missing],
            known, whole=retained.whole_listing,
        )
    except SubstrateError as exc:
        name_kept_growth(resolved.state, name, arguments, exc)
        raise
    if keys_absent:
        resolved.state["keys_absent"] = dict(keys_absent)
    if holder is not None:
        holder[holder_key] = resolved
    return _regrown(arguments, resolved.ref), resolved


def growth_fields(
    registry: Any, live_type: Any, retained: "LiveTypedSet", missing: set[str],
) -> tuple[set[str], set[str]]:
    """`(from_listing, per_node)`: the keys a growth of `retained` fetches,
    of the `missing` ones (a listing column it lacks, a declared field, or
    an open field when the open-fields call was not made)."""

    declared = dict(live_type.fields or {})
    open_read = "attributes" in (retained.coverage.get("per_node_calls") or {})
    edge_set = registry.edge_set(live_type.subtype)
    set_edge = edge_set[1] if edge_set is not None else None
    listing_columns = set(getattr(set_edge, "member_columns", ()) or ())
    from_listing = {k for k in missing if k not in declared and k in listing_columns}
    per_node = {
        k for k in missing - from_listing
        if k in declared or (live_type.open_fields is not None and not open_read)
    }
    return from_listing, per_node


def refuse_unfetched(
    registry: Any, operator: str, retained: Any, rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str], ref: str,
) -> None:
    """A read of a kept live-type set that names a field growth fetches and
    no row holds (no value, no field state): refused by name (#4050).

    Growth fetches such a field before the tool body runs; reaching the body
    without it means growth did not run, and serving the rows would read the
    column as null on every row with no upstream call and no note."""

    if not isinstance(retained, LiveTypedSet) or registry is None or not keys or not rows:
        return
    live_type = registry.by_subtype(retained.subtype)
    if live_type is None:
        return
    missing = set(keys) - _held_keys(rows)
    if not missing:
        return
    from_listing, per_node = growth_fields(registry, live_type, retained, missing)
    unfetched = sorted(from_listing | per_node)
    if not unfetched:
        return
    shown = ", ".join(f"attrs.{k}" for k in unfetched)
    raise refused(
        f"{operator}: the kept set's rows hold no {shown}, and the read that "
        f"fetches {'it' if len(unfetched) == 1 else 'them'} did not run; "
        "the rows are not served, since the column would read null on every row",
        "field_not_fetched",
        "read the set again with the field named (the read that made the set, "
        "with this field in its condition, select or metrics)",
        fields=[f"attrs.{k}" for k in unfetched], ref=str(ref),
    )

def _regrown(arguments: Mapping[str, Any], ref: str) -> dict[str, Any]:
    """A grown set's call: the same call on the new rows, its own conditions
    kept (the tool body applies them, as on any kept set)."""

    out = dict(arguments)
    out["input"] = {"ref": ref}
    return out


async def prepare_read(
    server: Any, name: str, arguments: Mapping[str, Any], subtypes: Sequence[str],
) -> Any:
    """Phase 1 of a live read, on a call worker (the server's hook)."""

    prepare = getattr(server, "_okg_live_graph_prepare", None)
    if prepare is None:
        raise refused(
            f"{name}: this server cannot read live types", "prepare_missing", "report this",
        )
    return await server._okg_run_blocking(
        prepare, name, authority=arguments.get("authority"), subtypes=list(subtypes),
        context=server.get_context() if hasattr(server, "get_context") else None,
    )


def _may_fan_out(live_type: Any, edge: Any, push: PushPlan, keys: set[str]) -> bool:
    """Whether a set read may call once per node: a heavy read.

    Decided before any call, from the declaration: a declared field needs a
    per-node call; a key that is not a declared listing column is an open
    field, read per node. Undeclared listing columns count as heavy.
    """

    declared = set(live_type.fields or {})
    if keys & declared:
        return True
    columns = set(getattr(edge, "member_columns", ()) or ())
    if live_type.id_from:
        columns.update(live_type.id_from)
    rest = keys - declared
    if not rest:
        return False
    if not getattr(edge, "member_columns", ()):
        return live_type.open_fields is not None
    return any(key not in columns for key in rest) and live_type.open_fields is not None


def ask_expand_gate(server: Any, name: str, subtype: Optional[str] = None) -> None:
    """The dispatch gate's decision on expand; the refusal is the gate's own."""

    gate = getattr(server, "_okg_dispatch_principal_gate", None)
    if gate is None:
        return
    try:
        gate("expand")
    except SubstrateError as exc:
        data = getattr(exc, "data", None)
        if isinstance(data, dict):
            data.setdefault("tool_called", name)
            data.setdefault("note", "live read: expand permission")
        _calls.log.info(
            "live read refused by the expand decision: tool=%s type=%s", name, subtype,
        )
        raise


def check_expand_served(server: Any, name: str, subtype: Optional[str] = None) -> None:
    """Expand is served to this session. A kept raw answer has no live type
    (`subtype` None): expand minted it, so expand's own check is the one."""

    visible = None
    reader = getattr(server, "okg_visible_tools", None)
    if callable(reader):
        visible = reader()
    if visible is None:
        manager = getattr(server, "_tool_manager", None)
        expand_served = manager is not None and manager.get_tool("expand") is not None
    else:
        expand_served = "expand" in visible
    if not expand_served:
        what = f"type {subtype!r} is live" if subtype is not None else "a kept live answer"
        raise refused(
            f"{name}: {what}; a live read needs expand, which "
            "this session is not served",
            "expand_not_served",
            "ask for expand on this session's surface, or read a stored type",
            tool_called=name, **({"type": subtype} if subtype is not None else {}),
        )


def check_served(server: Any, name: str, subtype: str) -> None:
    """Expand and the live type are served to this session."""

    check_expand_served(server, name, subtype)
    surface = getattr(server, "_okg_agent_surface", None)
    served_types = getattr(surface, "live_types", None) if surface is not None else None
    try:
        type_served = served_types is not None and subtype in served_types
    except TypeError:
        type_served = False
    if not type_served:
        raise refused(
            f"{name}: type {subtype!r} is live and is not served here "
            "(agent_surface.live_types does not name it)",
            "live_type_not_served",
            "read a stored type, or ask the operator to serve this live type",
            type=subtype,
        )


def check_caller_sync(server: Any, name: str, subtype: str) -> None:
    """:func:`check_caller` for a caller already on a call worker, without the
    access-rule check (the kept-set read makes that one itself). A read of a
    kept set asks it again, so a caller that lost expand, the type, or the
    dispatch gate's allowance since the first read is refused as a first
    read is."""

    ask_expand_gate(server, name, subtype)
    check_served(server, name, subtype)


async def check_caller(server: Any, name: str, subtype: str) -> None:
    """The caller, before any upstream call."""

    if getattr(server, "_okg_dispatch_principal_gate", None) is not None:
        await server._okg_run_blocking(ask_expand_gate, server, name, subtype)
    check_served(server, name, subtype)
    # Issue #3923: the one access-rule check of every live call (refused
    # while the rules are on, audit or enforce, or cannot be told), off the
    # event loop because the state reads the deployment's Nomos mode.
    subject = f"{name}: type {subtype!r} is live"
    if callable(getattr(server, "_okg_access_rules_state", None)):
        await server._okg_run_blocking(
            _access_rules.refuse_live_under_access_rules, server, subject,
            type=subtype, tool_called=name,
        )
    else:
        _access_rules.refuse_live_under_access_rules(
            server, subject, type=subtype, tool_called=name,
        )


async def check_arguments(
    server: Any, name: str, subtype: str, arguments: Mapping[str, Any],
) -> Any:
    """The tool's own argument refusals, one rule in one place, before any
    upstream call. Returns the body's refusal payload, or None."""

    identity = getattr(server, "_okg_identity_precheck", None)
    if identity is not None:
        await server._okg_run_blocking(identity, name, arguments.get("authority"))
    if arguments.get("generation_id") is not None:
        raise refused(
            f"{name}: type {subtype!r} is live; it has no generations, so "
            "generation_id does not apply",
            "generation_id_on_live_type",
            "drop generation_id",
            type=subtype,
        )
    precheck = getattr(server, "_okg_live_argument_precheck", None)
    if precheck is None:
        raise refused(
            f"{name}: this server has no argument check for live reads",
            "precheck_missing", "report this",
        )
    return await server._okg_run_blocking(precheck, name, arguments)


def _rewritten(name: str, arguments: Mapping[str, Any], ref: str) -> dict[str, Any]:
    """The call the tool body runs: the kept-set path, every condition gone
    (the resolver applied them)."""

    out = {k: v for k, v in arguments.items() if k not in ("predicate", "generation_id")}
    out["input"] = {"ref": ref}
    return out


async def _read(
    reader: Reader, name: str, arguments: Mapping[str, Any], live_type: Any,
    system_type: Any, edge: Any, push: PushPlan, keys: set[str], condition: Any,
) -> Resolved:
    from okg.substrate.mcp import live_reader

    started = now_text()
    system = getattr(edge, "of", None) or live_type.system
    system_node = f"{system_type.id_prefix}:{system}" if system else str(system_type.id_prefix)
    label = str(getattr(edge, "name", None) or "members")
    id_column = live_type.id_from[0] if live_type.id_from else None
    if id_column is None:
        raise refused(
            f"{name}: live type {live_type.subtype!r} has no name column",
            "no_name_column", "report this",
        )
    declared = dict(live_type.fields or {})
    known = declared_keys(live_type, edge)
    # The columns each listing's rows carry, as declared (a pushed
    # listing may give the member name only); None when the declaration does not say.
    member_columns = tuple(getattr(edge, "member_columns", ()) or ()) or None
    push_down = getattr(edge, "push_down", None)
    pushed_columns = (
        set(getattr(push_down, "member_columns", ()) or ()) | {id_column}
        if push_down is not None else {id_column}
    )

    full_args, _selfs = call_args(edge.call, name)
    listed: dict[str, dict[str, Any]] = {}
    names_refused: list[str] = []
    any_cut = False
    query: dict[str, Any]
    listing_relation: str

    def take(rows: Sequence[Mapping[str, Any]]) -> None:
        # A name that fails the type's name pattern is never made a node: it
        # is counted, and named (as untrusted text) in the reply.
        for row in rows:
            node_name = row.get(id_column)
            if node_name is None or node_name == "":
                continue
            if not isinstance(node_name, str) or not name_checked(live_type, node_name):
                names_refused.append(str(node_name))
                continue
            listed.setdefault(node_name, dict(row))

    # A pushed-down read joins the full listing (one call) only when it needs
    # a listing field the pushed rows do not carry.
    needs_full = push.expression is not None and any(
        key not in declared and key not in pushed_columns
        and (member_columns is None or key in member_columns)
        for key in keys
    )
    full_cut = False
    unjoined: set[str] = set()
    if push.expression is not None:
        pd = edge.push_down
        pushed_args = {pd.arg: push.expression}
        declared_args = {a.name for a in reader.prepared.bound(pd.relation).relation.args}
        pushed_args.update({k: v for k, v in full_args.items() if k in declared_args})
        rows, cut = await listing(
            reader, pd.relation, pushed_args, system_node, label,
            empty_error=pd.empty_error,
        )
        any_cut = any_cut or cut
        take(rows)
        listing_relation, query = pd.relation, pushed_args
        if needs_full and listed:
            full_rows, full_cut = await listing(
                reader, edge.call.relation, full_args, system_node, label,
            )
            any_cut = any_cut or full_cut
            joined: set[str] = set()
            for row in full_rows:
                node_name = row.get(id_column)
                if node_name in listed:
                    listed[node_name] = dict(row)
                    joined.add(node_name)
            unjoined = set(listed) - joined
    else:
        rows, cut = await listing(reader, edge.call.relation, full_args, system_node, label)
        any_cut = any_cut or cut
        take(rows)
        listing_relation, query = edge.call.relation, full_args

    seen_columns: set[str] = set()
    for row in listed.values():
        seen_columns.update(row)
    if push.expression is not None and not needs_full:
        listing_keys = pushed_columns | seen_columns
    elif member_columns is not None:
        listing_keys = set(member_columns) | {id_column}
    else:
        listing_keys = seen_columns | {id_column}
    groups = plan_groups(name, live_type, keys, listing_keys)

    prefix = live_type.id_prefix
    rows_out: list[dict[str, Any]] = []
    for node_name, row in listed.items():
        states = {key: "live" for key in row}
        rows_out.append({
            "node_id": f"{prefix}:{node_name}",
            "subtype": str(live_type.subtype),
            "attrs": dict(row),
            "live": {"system": system, "observed_at": started, "fields": states},
        })
    for key in keys:
        if key in listing_keys and key not in declared:
            for row in rows_out:
                node_name = row["node_id"].split(":", 1)[1]
                # A pushed node the cut full listing did not reach was not
                # read; one a whole listing lacks has no such column.
                not_reached = full_cut and node_name in unjoined and key not in pushed_columns
                row["live"]["fields"].setdefault(key, "unavailable" if not_reached else "absent")

    # Conditions on listing fields first: a node they drop needs no call.
    not_comparable: Counter[str] = Counter()
    condition_not_read = live_reader.NotRead()
    rest = push.rest
    early, late = [], []
    if isinstance(rest, Mapping) and rest.get("op") == "and":
        parts = _clauses(rest)
    else:
        parts = [rest] if not _is_empty(rest) else []
    for part in parts:
        needed = condition_keys(part)
        (early if needed <= (listing_keys - set(declared)) else late).append(part)
    early_condition = _and(early)
    not_read: list[str] = []
    made_nodes = len(rows_out)
    if early_condition is not None:
        rows_out, unread_ids = live_reader.select_rows(  # type: ignore[assignment]
            rows_out, early_condition, condition_not_read, not_comparable,
        )
        not_read.extend(unread_ids)
    # Whether the rows the typo check reads are the whole listing: nothing
    # pushed down, no cut, no listed name refused, no early condition
    # dropped a row. Only then is a key no row carries a probable typo.
    whole = (
        push.expression is None and not any_cut and not names_refused
        and len(rows_out) == made_nodes
    )

    planned = reader.fetch.calls + sum(len(rows_out) for _ in groups)
    if planned > MAX_UPSTREAM_CALLS:
        # The conditions that would narrow it: an eq or in the system edge
        # pushes down, on a key the declaration names; with no such key,
        # node_id or a listing column, applied before any per-node call.
        pushable = sorted(
            k for k in (getattr(push_down, "keys", ()) or ())
            if k != id_column and k not in declared
        )
        # Listing columns may be the upstream listing's own keys (no
        # `member_columns` declared): served as `attrs` serves them, and
        # fenced in the hint (#3666 review M1).
        columns = live_reader.served_key_names(sorted(
            k for k in listing_keys if k != id_column and k not in declared
        ))
        if pushable:
            hint = (
                f"narrow the set with a condition {system_word(system)} applies "
                "itself: an eq or in on "
                + ", ".join(f"attrs.{k}" for k in pushable[:3])
                + ", or on node_id"
            )
        else:
            hint = "narrow the set with an eq on node_id" + (
                ", or a condition on a listing column (applied before any "
                "per-node call): "
                + live_reader.fenced_key_list(columns[:3], prefix="attrs.")
                if columns else ""
            )
        raise refused(
            f"{name}: this read would make {planned:,} upstream calls "
            f"({reader.fetch.calls} made, {planned - reader.fetch.calls:,} planned); "
            f"a call makes at most {MAX_UPSTREAM_CALLS:,}",
            "planned_overrun", hint,
            planned=planned, made=reader.fetch.calls, cap=MAX_UPSTREAM_CALLS,
            push_down_fields=[f"attrs.{k}" for k in pushable],
            **({} if pushable else {"listing_fields": [f"attrs.{k}" for k in columns]}),
        )

    await fetch_groups(reader, groups, rows_out, system_node, declared)

    # Before any remaining condition: a key no node read carries. On the
    # whole listing it is a probable typo, refused with the closest keys,
    # never "0 rows" or "all absent"; on a narrowed read it may be a real,
    # sparse key, so the answer is served with a note naming it (the
    # resolver applies the conditions, so the tool body never sees them).
    keys_absent = refuse_typos(
        name, rows_out,
        [("predicate", k) for k in sorted(condition_keys(rest))]
        + named_keys(name, arguments),
        known, whole=whole,
    )

    late_condition = _and(late)
    if late_condition is not None:
        checked = len(rows_out)
        rows_out, unread_ids = live_reader.select_rows(  # type: ignore[assignment]
            rows_out, late_condition, condition_not_read, not_comparable,
        )
        not_read.extend(unread_ids)
        whole = whole and len(rows_out) == checked
    finished = now_text()

    unread = sorted({
        row["node_id"] for row in rows_out
        if any(row["live"]["fields"].get(k) in ("unavailable", "denied") for k in keys)
    } | set(not_read))
    listed_count = len(listed)
    coverage = {
        "listed": listed_count,
        "read": listed_count - len(unread),
        "not_read": unread,
        "not_comparable": sum(not_comparable.values()),
        "cut": any_cut,
        "cuts": list(reader.fetch.cut),
        "applied_by_system": list(push.applied),
        "calls": reader.fetch.calls,
        "listing_calls": reader.fetch.listing_calls,
        "per_node_calls": dict(reader.fetch.per_node),
        "outcomes": {k: dict(v) for k, v in reader.fetch.outcomes.items()},
        "waited_seconds": round(reader.fetch.waited_seconds, 1),
        "names_refused": len(names_refused),
        "kept": False,
    }
    line = live_line(
        system=system, started=started, finished=finished, label=label,
        coverage=coverage,
    )
    fetched = frozenset({f"attrs.{k}" for k in keys} | {f"attrs.{k}" for k in listing_keys})
    resolved = resolved_read(
        reader, name=name, live_type=live_type, system=system, rows=rows_out,
        coverage=coverage, line=line, finished=finished,
        listing_relation=listing_relation, query=query, node_id=system_node,
        fields=fetched,
        # Upstream keys, as `attrs` serves them; fenced where a hint prints them.
        narrowing=[
            f"attrs.{k}" for k in live_reader.served_key_names(
                sorted(k for k in listing_keys if k != id_column),
            )
        ],
        ids_checked=live_type.name_pattern is not None, known=known,
        whole=whole, keys_absent=keys_absent,
    )
    note_conditions(resolved.state, condition_not_read, not_comparable, names_refused)
    return resolved


def note_conditions(
    state: dict[str, Any], condition_not_read: Any, not_comparable: Counter[str],
    names_refused: Sequence[str],
) -> None:
    """What the reply's own notes need from the resolver: the rows a
    condition could not be decided on (they never reach the tool body),
    the values that could not be compared, and the names refused."""

    if condition_not_read.count:
        state["condition_not_read"] = condition_not_read.data()
    if not_comparable:
        state["condition_not_comparable"] = dict(not_comparable)
    if names_refused:
        state["names_refused"] = list(names_refused[:MAX_NAMES_SHOWN])
        state["names_refused_count"] = len(names_refused)


def resolved_read(
    reader: Reader, *, name: str, live_type: Any, system: Optional[str],
    rows: list[dict[str, Any]], coverage: Mapping[str, Any], line: str, finished: str,
    listing_relation: str, query: Mapping[str, Any], node_id: str,
    fields: frozenset[str], narrowing: Sequence[str], grown_from: Optional[str] = None,
    ids_checked: bool = False, known: Sequence[str] = (), whole: bool = True,
    keys_absent: Optional[Mapping[str, Any]] = None,
) -> Resolved:
    """The rows of one live read as the tool body receives them: a
    :class:`LiveTypedSet` (minted only if the keep rule says so, in the
    tool's own draft scope) and the state its kept-set reader reads."""

    from okg.substrate.external_backed.live_dispatch import GateOutcome
    from okg.substrate.mcp import live_result_sets as _lrs

    rows_json = _lrs.encode_rows(rows)
    prepared = reader.prepared
    ref = f"{REF_PREFIX}{uuid.uuid4().hex[:20]}"
    kept_set = LiveTypedSet(
        subtype=str(live_type.subtype), system=system,
        declaration_digest=declaration_digest(live_type),
        reader_key=str(prepared.reader_key), logical_session=prepared.logical_session,
        stateless=bool(prepared.stateless),
        gate_outcomes=dict(reader.gates), bound=dict(reader.bound), coverage=dict(coverage),
        observed_at=finished, rows_json=rows_json,
        relation_name=listing_relation, query=dict(query), node_id=node_id,
        row_count=len(rows),
        digest="sha256:" + hashlib.sha256(rows_json).hexdigest(),
        live_line=line, grown_from=grown_from,
        deployment_name=str(getattr(prepared, "deployment", "") or ""),
        ids_checked=bool(ids_checked), declared_keys=tuple(known),
        whole_listing=bool(whole),
    )
    listing_gate = reader.gates.get(listing_relation)
    state = {
        "set": kept_set,
        "rows": rows,
        "generation": prepared.generation,
        "session_id": prepared.session_id,
        "outcome": listing_gate if listing_gate is not None else GateOutcome(
            allowed=True, reason="allow", principal="unresolved", action="",
        ),
        "gates": dict(reader.gates),
        "coverage": dict(coverage),
        "live_line": line,
        # What the keep rule mints with: the store's binding and the
        # principal the per-principal cap counts.
        "mint": {
            "binding": dict(getattr(prepared, "binding", None) or {}),
            "header_claim": getattr(prepared, "header_claim", None),
            "multi_user": bool(getattr(prepared, "multi_user", False)),
        },
        # The conditions a refusal offers to narrow the read.
        "narrowing": list(narrowing),
        **({"grow_from": grown_from} if grown_from else {}),
        # Keys no row of a narrowed read carries: the reply's note, since
        # the tool body never sees the conditions the resolver applied.
        **({"keys_absent": dict(keys_absent)} if keys_absent else {}),
    }
    return Resolved(
        tool=name, ref=ref, rows=rows, fields=fields, state=state,
        calls_made=reader.fetch.calls,
    )


__all__ = [
    "DEADLINE_SECONDS",
    "LIVE_TOOLS",
    "LiveReadRefused",
    "LiveTypedSet",
    "MAX_UPSTREAM_CALLS",
    "NOT_COMPARABLE",
    "Resolved",
    "coerce_for",
    "declaration_digest",
    "detect",
    "handoff_scope",
    "has_handoff",
    "holder_scope",
    "is_empty_expression_error",
    "live_line",
    "plan_push",
    "resolve",
    "seed_call_memo",
]
