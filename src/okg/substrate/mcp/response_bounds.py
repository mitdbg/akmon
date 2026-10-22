"""Public MCP response byte bounds and the one served result form.

Every okg MCP tool result is served ONCE, as one text block in ``content``
(issue #3059): no ``structuredContent`` and no published ``outputSchema``.
The block is the plain-text rendering of the tool's dictionary
(``result_text.render``), or its compact JSON when the call asked for JSON
through the request ``_meta`` key ``okg/result_format``. A returned payload
with ``status: "refused"`` is served with ``isError: true``.

The byte limit is measured at the SDK boundary an agent receives:
``CallToolResult.model_dump_json(by_alias=True, exclude_none=True)`` of the
result exactly as it is served, in the call's format. One builder,
:func:`served_call_tool_result`, makes both the served result and every
measurement, so a page that fits the measure is the page that is served.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import copy
import contextvars
import functools
import inspect
import json
import logging
import re
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional, Protocol

import pydantic
import pydantic_core
from pydantic import StrictInt
from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, ImageContent, TextContent

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.call_read_cache import call_read_cache
from okg.substrate.mcp.excerpt_window import UNIT_WHOLE_CHARS
import okg.substrate.mcp.result_references as result_references
import okg.substrate.mcp.live_resolver as _live_resolver
from okg.substrate.mcp.operator_models import as_mapping, refuse_substring_contains
from okg.substrate.mcp.run_steps import note_payload as _note_run_payload
from okg.substrate.mcp.run_steps import STEP_OPERATORS as _SET_TOOLS
from okg.substrate.mcp.read_grammar import GrammarRefusal, render_call, rewrite_call
from okg.substrate.mcp.result_text import (
    RESULT_FORMAT_META_KEY,
    RESULT_FORMATS,
    ResultContext,
    compact_json,
    current_result_context,
    default_result_format,
    is_block,
    render,
    result_context,
)
from okg.substrate.mcp.session_tool_surface import SessionScopedToolsMixin

_log = logging.getLogger(__name__)


class ToolResultShapeChanged(SubstrateError):
    """The SDK stopped handing back the raw tool result we serve from."""

    code = "tool_result_shape_changed"


class InvalidResultFormat(SubstrateError):
    """A call named a result format the server does not serve."""

    code = "invalid_result_format"


class UnknownOperatorArgument(SubstrateError):
    """A caller named an argument the operator does not declare.

    Typed so the refusal keeps the offending key in `error.data` instead of
    being flattened into the generic execution failure.
    """

    code = "unknown_operator_argument"


class InvalidOperatorArguments(SubstrateError, ValueError):
    """A graph-read tool's declared input model refused the call.

    Also raised where an operator checks a caller argument by hand, so the
    caller reads the message and fixes the call instead of getting the
    generic `operator_execution_failed` (issue #2809). It stays a
    `ValueError` for the internal callers that already catch one.
    """

    code = "invalid_operator_arguments"

    def __init__(
        self,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        fix_hint: str | None = None,
    ) -> None:
        # A hand check names its field in the message, not by path: it
        # carries the registry's data keys with `argument_check` as its kind,
        # and the serving layer adds `tool`.
        super().__init__(
            message,
            data=data if data is not None else {
                "invalid_fields": [], "validation_kinds": ["argument_check"],
            },
            fix_hint=fix_hint or "correct the argument the message names, then retry",
        )


class DateCallOverBudget(SubstrateError):
    """A read's date work did not finish inside its time budget (issue
    #3666). The caller's arguments are valid; the call reads too many
    records for a date condition no index serves. The message and
    ``fix_hint`` say what to add; no retry call is named."""

    code = "date_call_over_budget"


class DateCallClock:
    """The clock of one MCP call that may carry a date condition (issue
    #3666).

    The byte-budget wrapper runs a tool body again, at a smaller limit, when
    a page does not fit (``_fit_to_budget``). The time budget of the call's
    date work belongs to the call, not to one run of the body: it counts
    from ``started_at``, set once where the wrapper first dispatches the
    call, and every run takes from it. ``budget`` is the call's one
    ``DateBudget`` (the tool body creates it on its first date work and
    reuses it on a rerun); ``stored_values`` keeps what the read of stored
    date values returned, so a rerun does not issue it again; ``entries``
    counts the runs of the body. Nothing here outlives the call.
    """

    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self.entries = 0
        self.budget: Any = None
        self.stored_values: dict[Any, Any] = {}
        #: Whether the last run of the body reached its reply.
        self.served = False
        #: Milliseconds the current run of the body waited for a pooled
        #: connection and spent opening its session, until the budget
        #: takes them as phases.
        self.pool_wait_ms = 0.0
        self.session_open_ms = 0.0
        #: What the call's runs read, for its reruns: a rerun to fit
        #: max_bytes differs in its limit alone, so it is cut from these
        #: and issues no statement. `dated` is what a read with a date
        #: condition settled on (the predicate it read with, its notes);
        #: `pages` the rows of each page statement and each group count;
        #: `totals` each bounded total. Dropped with the call.
        self.dated: dict[Any, Any] = {}
        self.pages: dict[Any, Any] = {}
        self.totals: dict[Any, Any] = {}


_DATE_CALL: ContextVar[Optional[DateCallClock]] = ContextVar("okg_date_call", default=None)


def current_date_call() -> Optional[DateCallClock]:
    """The clock of the MCP call in flight, or None outside the wrapper."""
    return _DATE_CALL.get()


@contextlib.contextmanager
def date_call():
    """One MCP call's :class:`DateCallClock`: created here, reused by a read
    tool called from inside another, and dropped when the call's reply has
    been produced or it was refused, whichever way it ended. The call's
    budget, if it started one, is reported once on the way out."""
    existing = _DATE_CALL.get()
    if existing is not None:
        yield existing
        return
    call = DateCallClock()
    token = _DATE_CALL.set(call)
    try:
        yield call
    finally:
        _DATE_CALL.reset(token)
        if call.budget is not None:
            call.budget.finish("served" if call.served else "refused")


class QueryOffUnderAccessRules(SubstrateError):
    """`query` was called on a deployment whose access rules are on.

    Jason, via orchestrator chat 2026-10-13, query-by-default: serve `query`
    by default without access rules (issue #3666). Under `audit` or `enforce` it is not
    served: arbitrary SQL cannot be filtered by the per-record rules, so the
    server neither lists nor runs it, and a call says so.
    """

    code = "query_off_under_access_rules"


class SchemaBehindReadOnly(SubstrateError):
    """A non-read tool was called on a server that is read-only by skew.

    PACT unknown-s8-mcp-readonly (#2035). The release verdict let this
    server start only because every pending migration is safe to read
    behind; nothing may write through it until the database is migrated.
    """

    code = "schema_behind_read_only"


#: Serialized ceiling for one error's `data` object.
#:
#: Producer metadata is the only unbounded part of a refusal. The deployment
#: release-skew error, for example, lists every pending migration with its
#: checksum — about 3 KB — when what a caller can act on is one line and a
#: fix hint. The rest is not lost, it is fetched: a capped envelope carries
#: the byte count and, when a trace is active, the trace id that finds the
#: full record.
ERROR_DATA_CAP_BYTES = 1024

DEFAULT_RESPONSE_BYTES = 64 * 1024

#: The type every read tool declares for `max_bytes`. STRICT on purpose: a
#: lax `int` would coerce `"8192"`, `16384.0` and `True` into integers for the
#: operator while the raised-error path in `ByteBoundedFastMCP.call_tool`,
#: which only sees the RAW arguments, would reject the same value and report
#: a budget the caller never sent. Strict, the input model refuses anything
#: but an integer or null by name before either path runs, so both resolve
#: the same budget. It still publishes as a plain `{"type":"integer"}`.
MaxBytes = Optional[StrictInt]
MAX_RESPONSE_BYTES = 256 * 1024
MIN_RESPONSE_BYTES = 4 * 1024

#: Issue #3153: a caller's integer `max_bytes` under `MIN_RESPONSE_BYTES` is
#: raised to it, not refused, and the served page says so in
#: `max_bytes_raised` ({"requested", "applied"}). Set by the byte-bound
#: wrapper for the call in flight; read once the page is final.
_MAX_BYTES_RAISED: "contextvars.ContextVar[dict[str, int] | None]" = contextvars.ContextVar(
    "okg_max_bytes_raised", default=None,
)


@dataclass(frozen=True)
class _CallRewrite:
    """What the grammar rewrite did to the call in flight (issue #3666)."""

    #: `{"rule", "sent", "served"}` for each rewrite the reply discloses.
    adjusted: tuple[dict[str, str], ...] = ()
    #: `(sent, read as)` for each accepted spelling read as an older argument.
    spellings: tuple[tuple[str, str], ...] = ()
    #: The rewrite's own refusal, raised where argument validation runs.
    refusal: Optional[SubstrateError] = None
    #: For a view with fixed rows served with a `select` (step 3a): the keys
    #: each row of the finished page keeps.
    row_keys: Optional[frozenset[str]] = None
    #: The `error.retry_call` a `select_unknown_field` refusal of this call
    #: carries: the same call with the `select` the caller's input
    #: determines, already proven to be served as written.
    select_retry: Optional[dict[str, Any]] = None
    #: The caller's own `select` when the tool was given a narrower one.
    sent_select: Optional[tuple[str, ...]] = None
    #: The call's arguments as the tool is given them (after the rewrite),
    #: so a refusal the tool body raises can offer the corrected call.
    arguments: Optional[Mapping[str, Any]] = None
    #: Records the tool body adds while it runs (step 2b): a rewrite that
    #: needs the deployment's own names. One list for the whole call, so a
    #: byte-fitting rerun of the tool adds nothing twice.
    body_notes: list[dict[str, str]] = dataclasses.field(default_factory=list)
    #: `{field, op, operand, folded}` for each like and like_exact test of
    #: the call (loose-match-3666): the reply's `matched_loosely`.
    matched_loosely: tuple[dict[str, str], ...] = ()


#: Set by `ByteBoundedFastMCP.call_tool` for the call in flight, before
#: authorization and validation. A direct Python call of a tool function
#: has none: it is not rewritten.
_CALL_REWRITE: "contextvars.ContextVar[_CallRewrite | None]" = contextvars.ContextVar(
    "okg_call_rewrite", default=None,
)


#: How many byte-bound read tools the call in flight is inside. One read
#: tool that calls another is one call with one reply: only the outermost
#: stamps the `adjusted` note.
_READ_TOOL_DEPTH: "contextvars.ContextVar[int]" = contextvars.ContextVar(
    "okg_read_tool_depth", default=0,
)


def call_adjusted() -> list[dict[str, str]]:
    """The `adjusted` records the reply to the call in flight must carry."""

    state = _CALL_REWRITE.get()
    if state is None:
        return []
    return [dict(record) for record in (*state.body_notes, *state.adjusted)]


def note_adjusted(records: Sequence[Mapping[str, str]]) -> None:
    """Add `adjusted` records to the reply of the call in flight.

    For a rewrite the tool body makes because it needs what only the
    deployment knows (step 2b). A record already noted is not added again:
    the byte-bound wrapper may run the tool several times for one reply.
    Outside a read tool call there is no reply to note it on.
    """

    state = _CALL_REWRITE.get()
    if state is None:
        return
    for record in records:
        record = dict(record)
        if record not in state.body_notes:
            state.body_notes.append(record)


def forget_adjusted(records: Sequence[Mapping[str, str]]) -> None:
    """Take back records :func:`note_adjusted` added: the call they describe
    is being refused for the very reading they state."""

    state = _CALL_REWRITE.get()
    if state is None:
        return
    for record in records:
        record = dict(record)
        if record in state.body_notes:
            state.body_notes.remove(record)


def call_arguments() -> Optional[dict[str, Any]]:
    """The arguments of the call in flight as the tool was given them, or
    None outside a dispatched call."""

    state = _CALL_REWRITE.get()
    if state is None or state.arguments is None:
        return None
    return dict(state.arguments)


def offer_retry_call(
    server: Any, tool: str, error: SubstrateError, arguments: Optional[Mapping[str, Any]],
    *, mode: str = "corrected",
) -> bool:
    """Attach `tool(**arguments)` to `error` as its `retry_call`, when proven.

    The call is offered only when the server takes it as written
    (`_okg_takes_as_written`): the rewrite with no note, the declared
    arguments, the argument model and the checks the tool body makes.
    Returns whether it was attached. The caller's authority is named as
    something to repeat, never echoed.
    """

    takes = getattr(server, "_okg_takes_as_written", None)
    if arguments is None or not callable(takes):
        return False
    repeat = [name for name in ("authority",) if name in arguments]
    candidate = {k: v for k, v in arguments.items() if k not in ("authority",)}
    if not takes(tool, candidate):
        return False
    call = {
        "tool": tool, "mode": mode, "arguments": candidate,
        **({"repeat": repeat} if repeat else {}),
    }
    if len(json.dumps(call, default=str)) > _RETRY_CALL_BYTES:
        return False
    error.retry_call = call
    return True


def call_sent_select() -> Optional[list[str]]:
    """The caller's own `select`, when the rewrite gave the tool a narrower
    one (step 3a); None otherwise. A path read signs this list into its
    token, so the page `next` continues is planned as the first one was."""

    state = _CALL_REWRITE.get()
    if state is None or state.sent_select is None:
        return None
    return list(state.sent_select)


def _next_select_notes(token: Any) -> tuple[dict[str, str], ...]:
    """The `select` note of the path read an `expand(next=...)` continues.

    `next` carries no arguments, so the rewrite saw no `select`; the token
    records the caller's own, and the note is rebuilt from it. Nothing here
    decides whether the token is good: one that does not verify has no note.
    """

    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        open_continuation,
    )
    from okg.substrate.mcp.select_vocabulary import path_select

    try:
        binding = open_continuation(token)["binding"]
    except (ContinuationTokenInvalid, SubstrateError, ValueError, TypeError):
        return ()
    projection = binding.get("projection")
    if binding.get("kind") != "expand_path" or not isinstance(projection, dict):
        return ()
    return path_select(projection.get("select"))[1]


@contextlib.contextmanager
def adjusted_scope(adjusted: Sequence[Mapping[str, str]]):
    """Run a block as a call served with these `adjusted` records.

    `ByteBoundedFastMCP.call_tool` sets them from the grammar rewrite; this
    is the same state for a caller that drives a read tool function itself.
    """

    token = _CALL_REWRITE.set(
        _CallRewrite(adjusted=tuple(dict(record) for record in adjusted)),
    )
    try:
        yield
    finally:
        _CALL_REWRITE.reset(token)


def adjusted_reserve_bytes() -> int:
    """Bytes the `adjusted` field adds to the served reply, for a read that
    cuts its own page against the budget before the field is stamped."""

    adjusted = call_adjusted()
    state = _CALL_REWRITE.get()
    loose = list(state.matched_loosely) if state is not None else []
    if not adjusted and not loose:
        return 0
    # The JSON form inside the served text block, where every quote is
    # escaped: the larger of the two served forms of these records. The
    # `matched_loosely` records are stamped beside them (loose-match-3666).
    fields: dict[str, Any] = {}
    if adjusted:
        fields["adjusted"] = adjusted
    if loose:
        fields["matched_loosely"] = loose
    inner = json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
    return len(json.dumps(inner, ensure_ascii=False).encode("utf-8"))


def _matched_loosely(
    operator_name: str, signature: inspect.Signature, args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> list[dict[str, str]]:
    """The like and like_exact tests of a set read's conditions, as the
    reply's `matched_loosely` names them (loose-match-3666)."""
    if operator_name not in ("filter", "map", "aggregate", "expand"):
        return []
    from okg.substrate.mcp.operator_models import as_mapping
    from okg.substrate.mcp.text_fold import like_leaves

    try:
        bound = signature.bind_partial(*args, **kwargs).arguments
    except TypeError:
        return []
    return like_leaves(*(
        as_mapping(bound.get(name)) for name in ("predicate", "input", "filter")
    ))


BYTE_TRUNCATION_REASON = "response_byte_budget"
_SENSITIVE_ERROR_KEY = re.compile(
    r"(?:^|_)(?:subject|identity_claim|access_context|token|secret|password|credential)(?:$|_)",
    re.IGNORECASE,
)


# ── image content (#3046) ──────────────────────────────────────────────
#
# `inspect(kind="image")` is the one read that returns bytes a node points
# at. They do not travel in the payload: the byte bound measures the
# payload and the call trace records it, and an image is neither a
# response the 64 KiB contract was written for nor something to store in
# a trace table. The read hands them to `attach_images`; `call_tool` opened
# a slot for them before the tool ran and attaches them to the result
# after it succeeded. Every other call's result is untouched.

_PENDING_IMAGES: ContextVar[Optional[list[tuple[bytes, str]]]] = ContextVar(
    "okg_mcp_pending_images", default=None,
)

#: The one tool whose successful result may carry image content.
IMAGE_CONTENT_TOOLS: frozenset[str] = frozenset({"inspect"})


def attach_images(images: Sequence[tuple[bytes, str]]) -> bool:
    """Queue ``(bytes, mime_type)`` images for the current call's result.

    Returns False when no call slot is open (a direct in-process call, not
    through `ByteBoundedFastMCP.call_tool`), so the caller can say so
    rather than silently send nothing."""
    pending = _PENDING_IMAGES.get()
    if pending is None:
        return False
    pending.extend(images)
    return True


def _with_images(result: CallToolResult, images: list[tuple[bytes, str]]) -> CallToolResult:
    """The served result with the call's images appended.

    The result already carries the payload as the server renders it (its
    text block, and the structured copy where a client asked for one); the
    images follow it, so the caption and every panel's status read first.
    Only image calls pay for the extra blocks.
    """
    import base64

    blocks: list[Any] = list(result.content)
    blocks += [
        ImageContent(type="image", data=base64.b64encode(data).decode("ascii"), mimeType=mime)
        for data, mime in images
    ]
    return CallToolResult(
        content=blocks, structuredContent=result.structuredContent, isError=False,
    )


def payload_text(payload: Any) -> str:
    """The compact JSON of a payload: the JSON result format's one text."""

    return compact_json(payload)


def served_text(
    payload: Any,
    *,
    tool: Optional[str] = None,
    raised: bool = False,
    session_state: Optional[dict[str, Any]] = None,
    result_format: Optional[str] = None,
    kind: Optional[str] = None,
) -> str:
    """The one text block a result carries, in the call's format.

    ``tool`` and ``kind`` default to the current call's (set by
    ``ByteBoundedFastMCP.call_tool``), then to the payload's
    ``operator_name``. ``session_state`` is passed only when serving: a
    measurement renders the stale-source notice as if it will print, so a
    fitted page never grows when it is served (#3076 DESIGN 1.2.3).
    """

    context = current_result_context()
    fmt = result_format or context.format
    name = tool or context.tool
    if name is None and isinstance(payload, dict):
        name = payload.get("operator_name")
    if fmt == "json":
        # PACT read-reply-size-3666: the same size facts the text's status
        # line says, as `reply_size`, its `bytes` the served JSON's own.
        from okg.substrate.mcp.reply_size import with_size_field
        from okg.substrate.mcp.result_text import size_facts_for

        facts_for = size_facts_for(
            payload, tool=str(name or "result"),
            kind=kind if kind is not None else context.kind, raised=raised,
        )
        if facts_for is None:
            return compact_json(payload)
        return with_size_field(payload, compact_json, facts_for)
    return render(
        payload,
        tool=str(name or "result"),
        kind=kind if kind is not None else context.kind,
        session_state=session_state,
        raised=raised,
    )


def served_call_tool_result(
    payload: Any,
    *,
    is_error: bool = False,
    tool: Optional[str] = None,
    raised: bool = False,
    session_state: Optional[dict[str, Any]] = None,
    result_format: Optional[str] = None,
    kind: Optional[str] = None,
) -> CallToolResult:
    """The result the server serves: ONE text block, no structured copy.

    Issue #3059. ``isError`` is set for an error and for any payload whose
    top-level ``status`` is ``"refused"``, returned or raised, so a client
    that reads only ``content`` can tell a refusal from an answer. The error
    form carries the whole bounded error payload in its one block; nothing
    is sent twice.

    An ``error.data`` object over ``ERROR_DATA_CAP_BYTES`` is replaced with
    a marker naming its size and trace, so an unbounded producer cannot spend
    a caller's whole budget on metadata nobody asked for.
    """

    payload = bound_error_data(payload)
    refused = isinstance(payload, dict) and payload.get("status") == "refused"
    return CallToolResult(
        content=[
            TextContent(
                type="text",
                text=served_text(
                    payload,
                    tool=tool,
                    raised=raised,
                    session_state=session_state,
                    result_format=result_format,
                    kind=kind,
                ),
            )
        ],
        isError=bool(is_error or refused),
    )


def public_call_tool_result(
    payload: Any,
    *,
    is_error: bool = False,
) -> CallToolResult:
    """The served result for ``payload`` in the current call's format."""

    return served_call_tool_result(payload, is_error=is_error)


def public_call_tool_result_bytes(
    payload: Any,
    *,
    is_error: bool = False,
    tool: Optional[str] = None,
    kind: Optional[str] = None,
) -> int:
    """Return exact UTF-8 bytes of the served SDK result.

    Measures what is served: the same builder, in the current call's format
    (text by default, compact JSON when the call asked for it), the
    stale-source notice rendered as if it will print. Every byte measurer --
    the pager, the error fitter, the ``inspect`` text and subtype pagers,
    the stamp probe and the tracer -- goes through here, so none measures a
    form the server does not send.
    """

    encoded = served_call_tool_result(
        payload, is_error=is_error, tool=tool, raised=is_error, kind=kind,
    ).model_dump_json(
        by_alias=True,
        exclude_none=True,
    )
    return len(encoded.encode("utf-8"))


def _refusal(
    operator_name: str,
    *,
    code: str,
    message: str,
    max_bytes: Any,
    remedy: str,
) -> dict[str, Any]:
    return {
        "status": "refused",
        "operator_name": operator_name,
        "error": {
            "code": code,
            "message": message,
            "max_bytes": max_bytes,
            "remedy": remedy,
        },
    }


def _signed_path_budget(token: Any) -> int | None:
    """The budget an ``expand_path`` token's first page was served under.

    None for any token that does not verify, is not a path token, or was
    signed before path bindings recorded a budget: the operator then
    compares the whole binding and refuses by name, so nothing here decides
    whether the token is good.
    """

    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        open_continuation,
    )

    try:
        binding = open_continuation(token)["binding"]
    except ContinuationTokenInvalid:
        return None
    projection = binding.get("projection")
    if binding.get("kind") != "expand_path" or not isinstance(projection, dict):
        return None
    value = projection.get("max_bytes")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _signed_next_budget(operator_name: str, token: Any) -> int | None:
    """The page size a ``next`` token resumes under: the budget `next`
    alone restores (`_fit_to_budget`), or None when the token does not
    verify or signs none."""

    from okg.substrate.mcp.continuation import ContinuationTokenInvalid, route_next

    if operator_name == "expand":
        return _signed_path_budget(token)
    try:
        binding = route_next(token, tool=operator_name)
    except (ContinuationTokenInvalid, SubstrateError, ValueError):
        return None
    projection = binding.get("projection")
    if binding.get("kind") not in _SIGNED_BUDGET_KINDS or not isinstance(projection, dict):
        return None
    value = projection.get("max_bytes")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


#: `next` token kinds whose binding signs the page size `next` resumes at.
_SIGNED_BUDGET_KINDS = frozenset({
    "inspect_text", "inspect_subtype", "inspect_text_batch", "inspect_node_batch",
})


def _valid_page_size(value: Any, ceiling: int | None = None) -> bool:
    upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
    return (
        isinstance(value, int) and not isinstance(value, bool)
        and MIN_RESPONSE_BYTES <= value <= upper
    )


def _resized_next(token: str, max_bytes: int) -> str:
    """`token` re-signed for pages of `max_bytes` from the same cursor
    (#3634, `nextresize A`). Only a token this server verifies is re-signed,
    and only its signed page size changes: the operator still compares the
    whole binding with the read it rebuilds, now at the new size, and the
    deployment ceiling still applies to the restored size."""
    from okg.substrate.mcp.continuation import issue_continuation, open_continuation

    body = open_continuation(token)
    binding = dict(body["binding"])
    binding["projection"] = {**binding["projection"], "max_bytes": max_bytes}
    return issue_continuation(binding, cursor=body.get("cursor") or {})


def _next_budget_refusal(
    operator_name: str, token: Any, *, given: Any = None, ceiling: int | None = None,
) -> dict[str, Any]:
    """Refuse an explicit ``max_bytes`` beside ``next`` that cannot be
    served, as operators refuse: on expand any, on inspect an invalid size
    (#3605 `nextbytes A`, #3634 `nextresize A` accept a valid one). Names
    the read's own page size and the ways on.

    Same envelope as the server's operator refusals (``operator_name``,
    ``status``, ``error``), with the token's generation when it verifies.
    """

    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        open_continuation,
    )

    # inspect takes a valid size beside `next` (`nextbytes A`, `nextresize
    # A`), so its refusal is only for an invalid one; expand keeps refusing
    # any max_bytes beside `next`, with the general hint.
    signed = _signed_next_budget(operator_name, token) if operator_name == "inspect" else None
    upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
    if signed is not None:
        where = " (this deployment's response ceiling)" if ceiling is not None else ""
        message = (
            f"{operator_name}: max_bytes beside `next` must be an integer from "
            f"{MIN_RESPONSE_BYTES} to {upper}{where}, got {given!r}; "
            f"this read's pages are max_bytes={signed}"
        )
        fix_hint = (
            f"call {operator_name}(next=<token>) alone for pages of {signed} bytes, "
            f"or with a max_bytes from {MIN_RESPONSE_BYTES} to {upper} "
            "to go on at that size"
        )
    else:
        message = (
            f"{operator_name}: `next` carries the whole read and cannot "
            "be combined with max_bytes"
        )
        fix_hint = (
            f"call {operator_name}(next=<token>) without max_bytes; it "
            "resumes under the signed budget. To change the page size, "
            "restart the read from its target with the new max_bytes"
        )
    payload: dict[str, Any] = {
        "operator_name": operator_name,
        "status": "refused",
        "error": {
            "code": "next_with_read_arguments",
            "message": message,
            "fix_hint": fix_hint,
        },
    }
    if signed is not None:
        payload["error"]["signed_max_bytes"] = signed
    try:
        generation = open_continuation(token)["binding"].get("generation")
    except ContinuationTokenInvalid:
        generation = None
    if generation is not None:
        payload["generation_id"] = generation
        payload["pinned_generation_id"] = generation
    return payload


def _token_retry(token: Any) -> dict[str, Any] | None:
    """The call that reads ``token``'s page under a new budget, or None."""

    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        budget_retry_call,
        open_continuation,
    )

    try:
        binding = open_continuation(token)["binding"]
    except ContinuationTokenInvalid:
        return None
    return budget_retry_call(binding)


def _restored_budget_refusal(
    invalid: dict[str, Any],
    token: Any,
    *,
    restored: Any,
    ceiling: int | None,
) -> dict[str, Any]:
    """A signed budget the current ceiling refuses, with a retry that works.

    The deployment ceiling can drop after a token was issued. Since #3634
    (`nextresize A`) an inspect text, texts or subtype token goes on at a new
    max_bytes, so its retry is ``next`` with a size under the ceiling. Other
    tokens get the call that reads the page
    (:func:`okg.substrate.mcp.continuation.budget_retry_call`).
    """

    from okg.substrate.mcp.continuation import TOKEN_PLACEHOLDER

    upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
    error = invalid["error"]
    message = (
        f"this continuation resumes under its signed max_bytes {restored}, "
        f"which this deployment's response ceiling ({upper}) now refuses"
    )
    if _signed_next_budget("inspect", token) is not None:
        error["message"] = message
        error["fix_hint"] = (
            f"continue with inspect(next=<this token>, max_bytes=<{MIN_RESPONSE_BYTES}..{upper}>) "
            "from the same place at that size"
        )
        error["remedy"] = error["fix_hint"]
        error["retry_call"] = {
            "tool": "inspect", "mode": "continue",
            "arguments": {"next": TOKEN_PLACEHOLDER},
            "max_bytes": {"min": MIN_RESPONSE_BYTES, "max": upper},
        }
        return invalid
    call = _token_retry(token)
    if call is None:
        return invalid
    error["message"] = message
    _name_budget_retry(error, call, low=MIN_RESPONSE_BYTES, high=upper)
    return invalid


def _name_budget_retry(
    error: dict[str, Any],
    call: dict[str, Any],
    *,
    low: int,
    high: int,
    otherwise: str | None = None,
) -> None:
    """Point ``error`` at ``call`` with ``max_bytes`` in ``low..high``.

    ``fix_hint`` is the one-line ``retry:`` the text status line prints
    (it must stay under the header's value length); ``remedy`` spells the
    whole call, and ``retry_call`` carries it for a program.
    """

    from okg.substrate.mcp.continuation import TOKEN_PLACEHOLDER, render_retry_call

    span = f"{low}..{high}"
    spelled = render_retry_call(call, f"<{span}>")
    if call["mode"] == "restart" and call["arguments"]["target"].get("kind") == "texts":
        hint = (
            f"restart this batch without a token, with the same items and "
            f"generation_id, max_bytes {span}; its token binds the budget"
        )
        remedy = (
            f"restart the batch with {spelled}, passing the same items and the "
            "same authority the first page passed, if any; a batch token binds "
            "its budget, so every item is read again"
        )
    elif call["mode"] == "restart":
        hint = (
            f"restart this subtype read without a token, with max_bytes {span}; "
            "its token binds the budget"
        )
        remedy = f"restart the read with {spelled}; a subtype token binds its budget"
    else:
        # The token pins its generation and keeps only a digest of the
        # reader's authority: name the pin, and ask for the same authority.
        def continue_hint(generation: str) -> str:
            if call["tool"] == "expand":
                return (
                    "continue with expand(<this read's input, path, "
                    f"target_subtype, limit, select, authority>, "
                    f"generation_id={generation}, "
                    f"continuation_token={TOKEN_PLACEHOLDER}, max_bytes {span})"
                )
            return (
                f"continue with inspect(target.continuation_token={TOKEN_PLACEHOLDER}, "
                f"generation_id={generation}, <this read's authority>, "
                f"max_bytes {span}); `next` cannot change the budget"
            )

        hint = continue_hint(json.dumps(call["arguments"].get("generation_id")))
        if is_block(hint):
            # A long generation id would push the hint off the status line;
            # retry_call and remedy spell it.
            hint = continue_hint("<pinned; see retry_call>")
        remedy = (
            f"continue with {spelled}, passing the same token and the same "
            "authority the first page passed, if any; `next` cannot change "
            "the budget"
        )
    if otherwise:
        remedy = f"{remedy}; or {otherwise}"
    error["fix_hint"] = hint
    error["remedy"] = remedy
    error["retry_call"] = {**call, "max_bytes": {"min": low, "max": high}}


def _point_to_budget_retry(
    error: dict[str, Any],
    token: Any,
    *,
    budget: int,
    ceiling: int | None,
    default_budget: int | None = None,
) -> None:
    """Rewrite an oversize path continuation's remedy to calls that work.

    The budgets offered stop at the default budget (R9, PACT
    read-passage-rows-3666): past it the remedy names the restart only."""

    call = _token_retry(token)
    if call is None:
        return
    ceiling_bytes = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
    upper = min(ceiling_bytes, default_budget) if default_budget is not None else ceiling_bytes
    restart = (
        "restart the path read without a token, with a smaller limit or a "
        "narrower select"
    )
    if budget < upper:
        _name_budget_retry(error, call, low=budget + 1, high=upper, otherwise=restart)
    elif upper < ceiling_bytes:
        error["fix_hint"] = (
            f"no larger max_bytes is offered past the {upper}-byte default; {restart}"
        )
        error["remedy"] = error["fix_hint"]
    else:
        error["fix_hint"] = (
            f"no larger max_bytes is allowed here (ceiling {upper}); {restart}"
        )
        error["remedy"] = error["fix_hint"]


def _validated_budget(
    operator_name: str,
    value: Any,
    *,
    ceiling: int | None = None,
) -> tuple[int | None, dict[str, Any] | None]:
    """Validate a caller's `max_bytes`, against the deployment ceiling if any.

    `ceiling` is `agent_surface.response_bytes.ceiling` (PACT
    `effective-surface-declaration`). It may only LOWER the substrate
    maximum; the manifest parser refuses a higher one at load, so by the time
    a call arrives the ceiling is known to be inside the substrate bounds.

    Over the ceiling is REFUSED, not clamped, for the same reason an
    out-of-range value already was: a clamp answers a question the caller did
    not ask and gives no signal that the budget it reasoned about was not the
    budget applied.
    """
    upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
    if isinstance(value, bool) or not isinstance(value, int):
        return None, _refusal(
            operator_name,
            code="invalid_response_byte_budget",
            message="max_bytes must be an integer",
            max_bytes=None,
            remedy=(
                f"pass max_bytes between {MIN_RESPONSE_BYTES} and {upper}, "
                "or omit it for the deployment default"
            ),
        )
    if value < MIN_RESPONSE_BYTES or value > upper:
        return None, _refusal(
            operator_name,
            code="invalid_response_byte_budget",
            message=(
                f"max_bytes must be between {MIN_RESPONSE_BYTES} and "
                f"{upper}"
                + (
                    ", which this deployment declares in "
                    "agent_surface.response_bytes.ceiling"
                    if ceiling is not None else ""
                )
                + "; it is refused rather than silently clamped"
            ),
            max_bytes=value,
            remedy=(
                f"pass max_bytes between {MIN_RESPONSE_BYTES} and {upper}"
            ),
        )
    return value, None


def _raise_small_budget(
    arguments: dict[str, Any], *, restored_from: Any,
) -> dict[str, int] | None:
    """Raise a caller's integer `max_bytes` under the floor to the floor.

    Issue #3153: agents told to keep outputs small pass values under 4 KiB
    and every such call was refused. The value is raised in `arguments` and
    the change is returned so the served page can say so. A budget restored
    from a token is the server's own and is left alone; a non-integer and a
    value over the ceiling are still refused by `_validated_budget`.
    """
    value = arguments.get("max_bytes")
    if (
        restored_from is not None
        or isinstance(value, bool)
        or not isinstance(value, int)
        or value >= MIN_RESPONSE_BYTES
    ):
        return None
    arguments["max_bytes"] = MIN_RESPONSE_BYTES
    return {"requested": value, "applied": MIN_RESPONSE_BYTES}


def resolve_budget(
    operator_name: str,
    arguments: Mapping[str, Any],
    *,
    deployment_default_bytes: int | None = None,
    deployment_ceiling_bytes: int | None = None,
) -> tuple[int | None, dict[str, Any] | None]:
    """Work out the ONE byte budget a read call gets, before any work runs.

    Every consumer of the budget takes this number and never recomputes it:
    the byte-bound wrapper, the raised-error refusal, and any operator that
    pages against it (issue #2156).

    The caller's `max_bytes` counts as SET only when it is present and not
    None. That is the whole fix for #2156: the MCP SDK hands the tool every
    argument, schema defaults included, so the old test "is `max_bytes` among
    the bound arguments" was always true over MCP and the deployment default
    was never applied. With `max_bytes: Optional[int] = None` in the
    signature, an omitted value and an explicit null both arrive as None.

    Set: used as-is, and a value outside 4 KiB..ceiling is refused, never
    clamped (the byte-bound wrapper raises a caller's value under 4 KiB to
    4 KiB first and discloses it, #3153). Unset: the deployment's effective default (already lowered to
    its ceiling by `AgentSurface.effective_response_default`), or 64 KiB when
    the deployment declares nothing.
    """
    requested = arguments.get("max_bytes")
    if requested is None:
        requested = (
            deployment_default_bytes
            if deployment_default_bytes is not None
            else DEFAULT_RESPONSE_BYTES
        )
    return _validated_budget(
        operator_name,
        requested,
        ceiling=deployment_ceiling_bytes,
    )


@dataclass(frozen=True)
class ResponseRoom:
    """The one response budget, as the live step of ``expand`` sees it.

    ``served_bytes`` measures the WHOLE response -- graph rows, the envelope
    under test, session metadata -- exactly as the SDK serves it, so the live
    step shrinks against the same bytes the wrapper enforces. There is no
    second measure to disagree with it.
    """

    budget_bytes: int
    served_bytes: Callable[[dict[str, Any]], int]

    def fits(self, envelope: dict[str, Any]) -> bool:
        return self.served_bytes(envelope) <= self.budget_bytes


class PreparedLiveStep(Protocol):
    """One ``expand(live=...)`` call, resolved and validated, adapter not run."""

    #: The generation the live relation was resolved at. The graph step is
    #: pinned to it, so the whole call reads one generation.
    generation_id: Any

    def reserve_envelope(self, graph: dict[str, Any]) -> dict[str, Any]:
        """The largest envelope the live step can return beside ``graph``
        and not shrink."""

    def refuse(self, reason: str) -> None:
        """Record a call refused before the adapter; never runs it."""

    def run(
        self, graph: dict[str, Any], room: ResponseRoom,
    ) -> dict[str, Any]:
        """Run the adapter exactly once and return an envelope that fits."""


LiveStep = Callable[[dict[str, Any]], PreparedLiveStep]

#: `expand(next=<live token>)` (#3666, Live build 3): the call's arguments,
#: the served-size measure and the call's budget -> the page payload.
LivePage = Callable[[dict[str, Any], Callable[[dict[str, Any]], int], int], dict[str, Any]]


def _is_live_next(value: Any) -> bool:
    from okg.substrate.mcp.live_result_sets import is_next_token

    return is_next_token(value)


def _is_pageable(operator_name: str, arguments: dict[str, Any]) -> bool:
    if operator_name in {"search", "filter", "map"}:
        return True
    if operator_name == "expand":
        # `next` continues a path read (PACT read-model-s6-page-next), which
        # pages by its token, never by offset.
        return (
            not arguments.get("path")
            and not arguments.get("next")
            and int(arguments.get("depth") or 1) <= 1
        )
    if operator_name == "aggregate":
        value = arguments.get("input")
        if (
            not isinstance(value, dict)
            or value.get("node_id")
            or value.get("start_node")
        ):
            return False
        if value.get("ref"):
            # A result reference's groups page like a set's (#3222).
            return True
        is_set = bool(
            value.get("subtype")
            or value.get("filter")
            or value.get("kind") in {"nodes", "edges"}
        )
        is_legacy_edge_rollup = (
            value.get("kind") == "edges"
            and arguments.get("by") == ["src_subtype", "edge_type", "dst_subtype"]
            and not value.get("filter")
            and not value.get("edge_type")
        )
        return is_set and not is_legacy_edge_rollup
    return False


#: `inspect` kinds whose `select` narrows the result. Every other kind
#: ignores `select`, so an oversize refusal must not suggest it there (PACT
#: tool-text-accuracy: C4 agents retried `session_start` with select and
#: ever larger budgets, 33 refusals).
_INSPECT_SELECT_KINDS = frozenset({"graph", "coverage", "node"})


#: Where an argument agents put at the wrong level actually goes (PACT
#: tool-text-accuracy; C4: search `subtype` 9 refusals, inspect top-level
#: `at` 2). Said in the refusal only; the argument stays refused.
_ARGUMENT_INSTEAD: dict[tuple[str, str], str] = {
    ("search", "subtype"): 'pass where={"op":"eq","field":"type","value":"<type>"}',
    ("search", "type"): 'pass where={"op":"eq","field":"type","value":"<type>"}',
    ("inspect", "at"): (
        'put it inside target: target={"kind":"text","node_id":...,"at":{...}}'
    ),
    ("inspect", "node_id"): 'pass target={"kind":"node","node_id":...}',
    ("expand", "node_id"): 'pass input={"node_id":...}',
}


#: Added to a measured page size before it is offered as a budget: a page
#: rebuilt at a larger budget states that budget (`response_byte_budget`),
#: which can be a digit longer than the one it was measured under.
_NEEDED_MARGIN_BYTES = 64


#: How many of a node's largest parts an oversize refusal names.
_LARGEST_PARTS_SHOWN = 3


def _largest_node_parts(payload: Any) -> str:
    """The largest top-level fields of an inspect(node) payload, with the
    largest `attrs` keys inside `attrs`, as "attrs 118,402 B (description
    80,113 B, comments 30,020 B), edges 2,311 B"; "" when not a node."""
    if not isinstance(payload, Mapping) or "node_id" not in payload:
        return ""

    def size(value: Any) -> int:
        return len(json.dumps(value, default=str, ensure_ascii=False).encode("utf-8"))

    fields = sorted(
        ((key, size(value)) for key, value in payload.items()
         if key not in {"node_id", "subtype"}),
        key=lambda item: -item[1],
    )[:_LARGEST_PARTS_SHOWN]
    out = []
    for key, n in fields:
        text = f"{key} {n:,} B"
        value = payload.get(key)
        if key == "attrs" and isinstance(value, Mapping) and value:
            inner = sorted(((k, size(v)) for k, v in value.items()),
                           key=lambda item: -item[1])[:_LARGEST_PARTS_SHOWN]
            text += " (" + ", ".join(f"{k} {m:,} B" for k, m in inner) + ")"
        out.append(text)
    return ", ".join(out)


def _oversize_remedy(
    operator_name: str,
    arguments: Optional[Mapping[str, Any]],
    *,
    single_row: bool,
    needed: Optional[int],
    payload: Any = None,
    default_budget: Optional[int] = None,
) -> str:
    """What to do about an oversize page, for this tool and inspect kind.

    A budget is offered only when the server accepts it: above
    `MAX_RESPONSE_BYTES` the remedy says the page cannot be served whole.
    A continuation keeps its signed budget and refuses select, so it is
    told to restart the read from its target.

    PACT read-passage-rows-3666 (R9): a larger budget is offered only up to
    `default_budget` (the deployment's default, else
    `DEFAULT_RESPONSE_BYTES`). A refusal told an agent to pass a max_bytes
    of 123,064, almost twice the default; past the default the remedy names
    a narrower read instead."""
    budget = None if needed is None else needed + _NEEDED_MARGIN_BYTES
    default = default_budget if default_budget is not None else DEFAULT_RESPONSE_BYTES
    over = budget is not None and budget > MAX_RESPONSE_BYTES
    beyond = budget is not None and budget > default
    if budget is None:
        larger = "request a larger valid max_bytes"
    elif over:
        larger = f"it needs more than the {MAX_RESPONSE_BYTES}-byte maximum"
    elif beyond:
        larger = (
            f"read less per call: it needs more than the {default}-byte "
            "default, and no larger max_bytes is offered"
        )
    else:
        larger = f"pass max_bytes of at least {budget}"
    args = arguments or {}
    target = args.get("target")
    kind = target.get("kind") if isinstance(target, Mapping) else None
    if args.get("next") is not None:
        if beyond:
            how = " with a smaller limit or a narrower select"
        elif budget is not None:
            how = f" with max_bytes of at least {budget}"
        else:
            how = ""
        return (
            "a continuation keeps its signed budget; restart the read from its "
            "target" + how
        )
    if operator_name == "inspect" and kind == "node":
        node_id = target.get("node_id") if isinstance(target, Mapping) else None
        text_call = (
            'inspect(target={"kind":"text","node_id":' + json.dumps(node_id) + "})"
            if isinstance(node_id, str) else 'inspect(target={"kind":"text","node_id":...})'
        )
        parts = _largest_node_parts(payload)
        if parts:
            # #3605: name what makes the node large and a call that reads it
            # page by page, so the next call fits instead of guessing.
            return (
                f"the largest parts are {parts}; read the text page by page "
                f"with {text_call}, or select fields without them, or " + larger
            )
        return (
            "narrow with select (e.g. node_id, subtype, attrs), read the text "
            f"with {text_call}, or " + larger
        )
    if operator_name == "inspect" and kind is not None and kind not in _INSPECT_SELECT_KINDS:
        if over:
            return (
                f"inspect kind {kind!r} ignores select and {larger}; it cannot "
                "be served whole on this server"
            )
        return f"inspect kind {kind!r} ignores select; {larger}"
    if operator_name == "query":
        # Issue #3666: say how to make the reply fit, not only that it did not.
        if isinstance(payload, Mapping) and payload.get("payload_truncated"):
            # Cut at the tool's own cap: no max_bytes serves these rows whole.
            cap = payload.get("payload_byte_cap")
            return (
                "the query tool's own "
                f"{f'{cap}-byte ' if cap is not None else ''}payload cap was "
                "reached, so a larger max_bytes will not help; add LIMIT, select "
                "fewer columns, cut long text with left(attrs->>'text', 200), "
                "or page by key (WHERE node_id > the last one seen ORDER BY node_id)"
            )
        return (
            "add LIMIT, select fewer columns, or cut long text with "
            f"left(attrs->>'text', 200); or {larger}"
        )
    if operator_name == "search":
        # D3 (PACT read-passage-rows-3666): search does not publish
        # max_bytes, so no search hint names it. A page over the budget has
        # had its passages and selected values shortened already (D4); what
        # is left is fewer rows or fewer fields.
        if args.get("select") is not None or args.get("max_chars") is not None:
            return (
                "select fewer fields, or drop select and max_chars: the "
                "default rows are short; or pass a smaller limit and page "
                "with offset"
            )
        return "pass a smaller limit and page with offset"
    if single_row:
        return f"request a narrower select/projection, or {larger}"
    return f"narrow the request (select/projection/filter), or {larger}"


def _oversize_refusal(
    operator_name: str,
    *,
    budget: int,
    single_row: bool,
    needed: Optional[int] = None,
    arguments: Optional[Mapping[str, Any]] = None,
    payload: Any = None,
    ceiling: Optional[int] = None,
    default_budget: Optional[int] = None,
) -> dict[str, Any]:
    """The refusal for a page that cannot fit `budget`. `needed` is the
    served size of the smallest page the fitter built, when it built one:
    the message says it, so the caller sets a budget that fits on the next
    call instead of guessing upward. `ceiling` is the deployment's largest
    max_bytes, when it sets one. `default_budget` is the deployment's
    default max_bytes: no budget above it is offered, in words or as a
    `query` retry (R9)."""
    initial = payload
    size = f"; this page needs about {needed} bytes" if needed is not None else ""
    if single_row:
        message = (
            "one complete result row plus the required response envelope exceeds "
            f"the {budget}-byte MCP response budget{size}"
        )
    else:
        message = (
            f"the {operator_name} result exceeds the {budget}-byte MCP response "
            f"budget and this result shape has no safe continuation cursor{size}"
        )
    remedy = _oversize_remedy(
        operator_name, arguments, single_row=single_row, needed=needed,
        payload=payload, default_budget=default_budget,
    )
    payload = _refusal(
        operator_name,
        code="response_byte_budget_exceeded",
        message=message,
        max_bytes=budget,
        remedy=remedy,
    )
    payload["truncated"] = True
    payload["truncation_reason"] = BYTE_TRUNCATION_REASON
    payload["response_byte_budget"] = budget
    if needed is not None:
        payload["error"]["needed_bytes"] = needed
    if operator_name == "query":
        retry = _query_budget_retry(
            arguments, initial, needed=needed, ceiling=ceiling,
            default_budget=default_budget,
        )
        if retry is not None:
            payload["error"]["retry_call"] = retry
        elif isinstance(initial, Mapping) and initial.get("payload_truncated"):
            # The tool cut this page at its own cap: say so, and keep the
            # wrapper's general retry from offering a budget that cannot help.
            payload["payload_truncated"] = True
            payload["payload_byte_cap"] = initial.get("payload_byte_cap")
    return payload


#: A `query` page's next page wraps the caller's statement in this form; a
#: statement already in it gets a larger OFFSET, never a second wrapper.
_QUERY_PAGE_WRAP = re.compile(
    r"\ASELECT \* FROM \(\n(?P<inner>.*)\n\) AS _okg_page OFFSET (?P<offset>[0-9]+)\Z",
    re.S,
)

#: What a `query` page's `next_query` says about the order of its pages.
QUERY_PAGE_ORDER_NOTE = (
    "the same statement from the next row, at the same pin; pages follow the "
    "statement's ORDER BY, so without one over unique columns a page can "
    "repeat or skip rows"
)


def _query_next_call(
    arguments: Optional[Mapping[str, Any]], initial: Mapping[str, Any], shown: int,
) -> dict[str, Any] | None:
    """The call that reads the rows after the first `shown` of this page:
    the statement wrapped as ``SELECT * FROM (<statement>) AS _okg_page
    OFFSET <n>``, at the page's pin, with the caller's own row_cap,
    timeout_ms and max_bytes. A statement already wrapped so gets a larger
    OFFSET. Exact only when the statement orders its rows uniquely; the
    page says so (`QUERY_PAGE_ORDER_NOTE`)."""
    args = arguments or {}
    sql = args.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return None
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, checked_run_sql

    try:
        checked = checked_run_sql(sql)
    except RunSqlInvalid:
        return None
    match = _QUERY_PAGE_WRAP.match(checked)
    if match is not None:
        inner, offset = match.group("inner"), int(match.group("offset"))
    else:
        inner, offset = checked, 0
    call_arguments: dict[str, Any] = {
        "sql": f"SELECT * FROM (\n{inner}\n) AS _okg_page OFFSET {offset + shown}",
    }
    pin = initial.get("pinned_generation_id")
    if pin is not None:
        call_arguments["generation_id"] = pin
    for name, default in (("row_cap", 1_000), ("timeout_ms", 5_000)):
        value = args.get(name)
        if value is not None and value != default:
            call_arguments[name] = value
    if args.get("max_bytes") is not None:
        call_arguments["max_bytes"] = args["max_bytes"]
    call: dict[str, Any] = {"tool": "query", "arguments": call_arguments}
    if args.get("authority"):
        call["repeat"] = ["authority"]
    return call


def _query_fitted_page(
    initial: Any,
    *,
    budget: int,
    measure: Callable[[dict[str, Any]], int],
    arguments: Optional[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """The first rows of an over-budget `query` page that fit `budget`, or
    None when not even one row fits (then the refusal says how to fit).

    The page is cut at a row: it carries `truncated`, the byte bound it
    reached, and `next_query`, the call for the rest (`_query_next_call`).
    No row is shortened here; a cell over 8,192 characters was already cut
    by the tool itself and says so.
    """
    if not isinstance(initial, Mapping):
        return None
    rows = initial.get("rows")
    if not isinstance(rows, list) or len(rows) < 2:
        return None

    def page(shown: int) -> dict[str, Any]:
        out = dict(initial)
        out.pop("payload_bytes", None)
        out["rows"] = rows[:shown]
        out["returned_count"] = shown
        out["truncated"] = True
        out["truncation_reason"] = BYTE_TRUNCATION_REASON
        out["response_byte_budget"] = budget
        nxt = _query_next_call(arguments, initial, shown)
        if nxt is not None:
            out["next_query"] = {**nxt, "note": QUERY_PAGE_ORDER_NOTE}
        return out

    low, high, best = 1, len(rows) - 1, None
    while low <= high:
        mid = (low + high) // 2
        candidate = page(mid)
        if measure(candidate) <= budget:
            best, low = candidate, mid + 1
        else:
            high = mid - 1
    return best


def _query_budget_retry(
    arguments: Optional[Mapping[str, Any]],
    initial: Any,
    *,
    needed: Optional[int],
    ceiling: Optional[int],
    default_budget: Optional[int] = None,
) -> dict[str, Any] | None:
    """The one exact retry of an oversize `query` page, or None (#3666).

    The same SQL at the same pin with `max_bytes` large enough for the page
    the server already built: it serves the whole answer, so it is offered
    only when that budget is allowed here, is not above the default budget
    (R9: a retry never asks for more), and the page is not itself cut
    (`payload_truncated`: rows dropped at the tool's own 256 KiB cap, where
    a larger budget still could not serve them all). Any other way to fit
    (LIMIT, fewer columns, `left()`) changes the question, so it is named in
    the remedy, never offered as a call. The SQL is echoed when the call
    stays small; otherwise the caller is asked to repeat it. The caller's
    `authority` is never echoed.
    """
    if needed is None or not isinstance(initial, Mapping):
        return None
    if initial.get("payload_truncated"):
        return None
    wanted = needed + _NEEDED_MARGIN_BYTES
    upper = min(
        ceiling if ceiling is not None else MAX_RESPONSE_BYTES,
        default_budget if default_budget is not None else DEFAULT_RESPONSE_BYTES,
    )
    if wanted > upper:
        return None
    args = arguments or {}
    sql = args.get("sql")
    if not isinstance(sql, str):
        return None
    call_arguments: dict[str, Any] = {"sql": sql}
    pin = initial.get("pinned_generation_id")
    if pin is not None:
        call_arguments["generation_id"] = pin
    for name, default in (("row_cap", 1_000), ("timeout_ms", 5_000)):
        value = args.get(name)
        if value is not None and value != default:
            call_arguments[name] = value
    call_arguments["max_bytes"] = wanted
    repeat = ["authority"] if args.get("authority") else []
    call: dict[str, Any] = {
        "tool": "query", "mode": "corrected", "arguments": call_arguments,
    }
    if len(json.dumps(call, default=str)) > _RETRY_CALL_BYTES:
        del call_arguments["sql"]
        repeat.insert(0, "sql")
    if repeat:
        call["repeat"] = repeat
    return call


def _execution_refusal(
    operator_name: str,
    *,
    budget: int,
    error_id: str,
) -> dict[str, Any]:
    """Return a fixed-size error without reflecting an unbounded exception.

    The exception text never reaches the caller: it can carry internal or
    untrusted data. `error.data.error_id` is the handle an operator uses to
    find the logged traceback for this refusal (issue #2793).
    """

    payload = _refusal(
        operator_name,
        code="operator_execution_failed",
        message="the operator failed before producing a bounded MCP response",
        max_bytes=budget,
        remedy="narrow or correct the request, then retry",
    )
    payload["error"]["data"] = {"error_id": error_id}
    return payload


def _expected_error_refusal(
    operator_name: str,
    error: Any,
    *,
    budget: int,
) -> dict[str, Any]:
    """Preserve a typed substrate failure without reflecting unbounded data."""

    code = str(getattr(error, "code", "substrate_error"))[:128]
    message = str(getattr(error, "message", str(error)))[:1_024]
    fix_hint = getattr(error, "fix_hint", None)
    raw_data = getattr(error, "data", {})
    if (
        code == InvalidOperatorArguments.code
        and isinstance(raw_data, dict) and "tool" not in raw_data
    ):
        raw_data = {"tool": operator_name, **raw_data}
    data = _safe_error_data(raw_data)
    payload = {
        "status": "refused",
        "operator_name": operator_name,
        "error": {
            "code": code,
            "message": message,
            **({"fix_hint": str(fix_hint)[:1_024]} if fix_hint else {}),
            "data": data,
            "max_bytes": budget,
        },
    }
    retry_call = getattr(error, "retry_call", None)
    if isinstance(retry_call, dict):
        payload["error"]["retry_call"] = retry_call
    return _fit_error_payload(payload, budget=budget)


#: Prefix kept of an over-budget error `message` / `fix_hint`, and its marker.
_CLIPPED_ERROR_CHARS = 384
_CLIPPED_ERROR_MARKER = " [clipped to fit the response byte budget]"


def _fit_error_payload(
    payload: dict[str, Any],
    *,
    budget: int,
) -> dict[str, Any]:
    """Trim an error after every public envelope field has been attached.

    Measured in the served error form, in the call's format, because that is
    how every payload fitted here is served: one text block carrying the
    whole bounded error (issue #3059). Returned refusals are fitted here too,
    by ``ByteBoundedFastMCP.call_tool``, the same way raised ones are.
    """

    def fits() -> bool:
        return public_call_tool_result_bytes(payload, is_error=True) <= budget

    if fits() or not isinstance(payload.get("error"), dict):
        return payload
    # THE ORDER OF SACRIFICE, in one place. A refusal that does not fit
    # gives up, in this order: (1) `data`; (2) the tail of `fix_hint` and of
    # `message`, cut to a marked prefix; (3) `fix_hint` whole, and `message`
    # for a fixed sentence; (4) only then `retry_call`. The corrected call
    # is the part a caller can act on without reading anything else, so it
    # goes last; with its `mode` and `repeat` it is at most
    # `_RETRY_CALL_BYTES`, which the minimum budget holds beside the code.
    payload["error"]["data"] = {
        "details_omitted": "response_byte_budget",
    }
    if fits():
        return payload
    # Two 1 KiB `message`/`fix_hint` values plus the envelope can still
    # overflow a 4 KiB budget. Keep a marked prefix of each before giving
    # them up entirely.
    for key in ("message", "fix_hint"):
        value = payload["error"].get(key)
        if isinstance(value, str) and len(value) > _CLIPPED_ERROR_CHARS:
            payload["error"][key] = (
                value[:_CLIPPED_ERROR_CHARS] + _CLIPPED_ERROR_MARKER
            )
    if fits():
        return payload
    payload["error"]["message"] = "the request was refused by a typed substrate check"
    payload["error"].pop("fix_hint", None)
    if fits():
        return payload
    if payload["error"].pop("retry_call", None) is not None and fits():
        return payload

    # Preserve the stable typed code and logical-session contract even in the
    # final fixed-size fallback. Generation/logical ids are server-controlled
    # bounded identifiers, so this envelope fits the 4 KiB minimum.
    error = payload["error"]
    return {
        "status": "refused",
        "operator_name": payload.get("operator_name"),
        "error": {
            "code": str(error.get("code") or "substrate_error")[:128],
            "message": "the request was refused by a typed substrate check",
            "data": {"details_omitted": "response_byte_budget"},
            "max_bytes": budget,
        },
        **{
            key: payload[key]
            for key in (
                "logical_session_id",
                "pin_scope",
                "session_default_generation_id",
                "warning",
                "schema_behind_by",
            )
            if key in payload
        },
    }


def _active_trace_id() -> str | None:
    """The trace a caller could use to fetch the full error, if there is one."""

    try:
        from okg.substrate.telemetry.store import current_trace_identity

        identity = current_trace_identity()
    except Exception:  # noqa: BLE001 - telemetry must never break a refusal.
        return None
    if not getattr(identity, "is_contextual", False):
        return None
    trace_id = getattr(identity, "trace_id", None)
    return str(trace_id) if trace_id else None


def bound_error_data(payload: Any) -> Any:
    """Cap one envelope's `error.data` at `ERROR_DATA_CAP_BYTES`.

    `code`, `message` and `fix_hint` are never touched: they are the part a
    caller acts on, and they are already length-bounded where they are built.
    Only `data` is replaced, and only when it does not fit, by a marker that
    says so rather than by a shorter version that would read as complete.

    Applied where the structured envelope is BUILT, not in each operator, so
    a refusal raised by one and a refusal returned by another are bounded the
    same way. Below the cap the payload is returned unchanged — identical
    object, not a rebuilt copy — so a small error is byte-for-byte what the
    operator produced.
    """

    if not isinstance(payload, dict):
        return payload
    error = payload.get("error")
    if not isinstance(error, dict):
        return payload
    data = error.get("data")
    if data is None:
        return payload
    serialized = len(pydantic_core.to_json(data, fallback=str))
    if serialized <= ERROR_DATA_CAP_BYTES:
        return payload
    marker: dict[str, Any] = {"truncated": True, "data_bytes": serialized}
    trace_id = _active_trace_id()
    if trace_id is not None:
        marker["trace_id"] = trace_id
    return {
        **payload,
        "error": {**error, "data": marker},
    }


def _safe_error_data(value: Any, *, depth: int = 0) -> Any:
    """Bound typed metadata and remove identity/credential-bearing fields."""

    if depth >= 4:
        return "<details-omitted>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:1_024]
    if isinstance(value, dict):
        return {
            str(key)[:128]: _safe_error_data(item, depth=depth + 1)
            for key, item in list(value.items())[:64]
            if not _SENSITIVE_ERROR_KEY.search(str(key))
        }
    if isinstance(value, (list, tuple)):
        return [_safe_error_data(item, depth=depth + 1) for item in value[:64]]
    return str(value)[:1_024]


def _validation_field_path(location: tuple[Any, ...]) -> str:
    """Render a bounded field path without reflecting caller values."""

    path = ""
    for item in location[:16]:
        if isinstance(item, int):
            path += f"[{max(0, item)}]"
            continue
        if not isinstance(item, str):
            item = f"<{type(item).__name__}>"
        elif _SENSITIVE_ERROR_KEY.search(item):
            item = "<sensitive-field>"
        else:
            item = item[:128]
        path += ("." if path else "") + item
    return path or "<arguments>"


def _misplaced_top_level_arguments(
    rows: list[Any], declared: set[str],
) -> list[tuple[str, str]]:
    """`(key, outer argument)` for each refused extra key nested inside an
    argument that is itself one of the tool's top-level arguments (#3066)."""

    out: list[tuple[str, str]] = []
    for row in rows[:32]:
        location = tuple(row.get("loc") or ())
        if row.get("type") != "extra_forbidden" or len(location) < 2:
            continue
        outer, key = location[0], location[-1]
        if (
            isinstance(outer, str) and isinstance(key, str)
            and key in declared and key != outer
            and (key, outer) not in out
        ):
            out.append((key, outer))
    return out


#: Longest corrected call a rewrite refusal spells on its `retry:` line, and
#: the largest it carries in `error.retry_call`; a longer one is described
#: in words instead, so the refusal still fits the smallest budget.
_RETRY_CALL_CHARS = 600
_RETRY_CALL_BYTES = 1536


#: The arguments an offered retry may carry, per tool: the ones whose every
#: check the tool makes on them can be made here, before any read. A call
#: with any other argument, and any call of a tool not listed, is described
#: in words. An `aggregate` call is offered only when every `by` key is a
#: column or an attribute: a date period or a list element is judged by the
#: read, on the values it finds.
_RETRY_ARGUMENTS: dict[str, frozenset[str]] = {
    "search": frozenset({
        "query", "where", "limit", "select", "method",
        # Step 2b: a record scope ranked by each row's own text, and the
        # windows read of grep; both are checked below from the arguments.
        "scope", "rank_by", "view", "matching", "context_chars",
        # Grep's path scope, offered for a ranked search that sent one.
        "path_prefix",
    }),
    # `select` is checked below against the fields the call's rows have
    # (issue #3666, 2c: a filter call with select got no offered call).
    "filter": frozenset({"input", "predicate", "limit", "offset", "select"}),
    "map": frozenset({"input", "limit", "offset"}),
    "expand": frozenset({
        "input", "edge_type", "direction", "filter", "limit", "offset", "select",
    }),
    "aggregate": frozenset({"input", "by", "metrics", "limit", "offset"}),
    # Only the grammar's own offer of a flat `kind`/`node_id` call moved into
    # `target` (issue #3961); `_inspect_call_servable` checks it.
    "inspect": frozenset({"target", "select", "generation_id"}),
}


def _inspect_call_servable(arguments: Mapping[str, Any]) -> bool:
    """Whether an `inspect` call a refusal offers names a subject the read takes.

    The target must be an object with a known `kind` and, for a record
    read, a text `node_id`. A `select` is offered only where it names what
    the kind selects from (a record's fields, the graph's blocks): the tool
    body checks the names, and an offered call is never refused itself.
    """
    from okg.substrate.mcp.operator_models import (
        INSPECT_KIND_ALIASES,
        INSPECT_TARGET_KINDS,
    )
    from okg.substrate.mcp.tools import _GET_NODE_FIELDS, INSPECT_GRAPH_BLOCKS

    target = arguments.get("target")
    if not isinstance(target, Mapping):
        return False
    kind = target.get("kind")
    if not isinstance(kind, str) or (
        INSPECT_KIND_ALIASES.get(kind, kind) not in INSPECT_TARGET_KINDS
    ):
        return False
    if kind in ("node", "text", "attr") and not (
        isinstance(target.get("node_id"), str) and target["node_id"]
        or isinstance(target.get("path"), str) and target["path"]
    ):
        return False
    select = arguments.get("select")
    if select is None:
        return True
    if not isinstance(select, list) or not select or not all(
        isinstance(name, str) for name in select
    ):
        return False
    if kind in ("node", "nodes"):
        return set(select) <= set(_GET_NODE_FIELDS)
    if kind == "graph":
        return set(select) <= set(INSPECT_GRAPH_BLOCKS)
    return False


def _plain_group_keys(by: Any, metrics: Any, scope: Mapping[str, Any]) -> bool:
    """Whether an aggregate's `by` and `metrics` are ones no read refuses:
    1 to 4 distinct columns of the set or `attrs.<key>`, and a count."""
    from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS

    if metrics != ["count"] or not isinstance(by, list) or not 1 <= len(by) <= 4:
        return False
    if len(set(map(str, by))) != len(by):
        return False
    if scope.get("kind") == "edges" and by == ["src_subtype", "edge_type", "dst_subtype"]:
        return False
    columns = EFFECTIVE_VIEW_FIELD_COLUMNS[scope["kind"]]
    return all(
        isinstance(key, str) and (
            key in columns
            or (key.startswith("attrs.") and len(key) > len("attrs.")
                and "(" not in key and ")" not in key)
        )
        for key in by
    )


def _condition_leaves(tree: Any):
    """Each comparison of a long-form condition."""
    if not isinstance(tree, Mapping) or not tree:
        return
    op = tree.get("op")
    if op in ("and", "or"):
        for clause in tree.get("clauses") or []:
            yield from _condition_leaves(clause)
    elif op == "not":
        yield from _condition_leaves(tree.get("clause"))
    else:
        yield tree


def _proven_servable(
    name: str, arguments: Mapping[str, Any], *, ceiling: int | None,
    text_served: bool = False,
    like_served: Callable[[], bool] | None = None,
) -> bool:
    """Whether the tool's own argument checks take this rewritten call.

    Run on a call a refusal is about to offer. It makes, with no read, the
    checks the tool body makes on its arguments: search's `where`
    normalizer, the scope check, the substring-`contains` refusal, the
    text clause's own check, the comparisons each path runs, the byte
    budget. Anything it cannot decide here counts as not proven, and the
    refusal then offers words, not a call: an offered call that is refused
    is worse than none.

    `text_served` is whether access rules are known to be off; only then is
    a `matches` clause on `text` served, so only then is one offered.

    `like_served` says whether a like or like_exact test is served here:
    access rules known off and the database's fold check passed
    (loose-match-3666). It is asked only for a call that holds one; with no
    answer such a call is not offered, since the read would refuse it.
    """
    allowed = _RETRY_ARGUMENTS.get(name)
    if allowed is None or set(arguments) - allowed - {"max_bytes"}:
        return False
    from okg.substrate.mcp import text_fold

    scope_argument = arguments.get("input")
    if text_fold.holds_like_clause(
        arguments.get("where"), arguments.get("predicate"), arguments.get("filter"),
        scope_argument.get("filter") if isinstance(scope_argument, Mapping) else None,
    ) and (like_served is None or not like_served()):
        return False
    budget = arguments.get("max_bytes")
    if budget is not None:
        upper = ceiling if ceiling is not None else MAX_RESPONSE_BYTES
        if isinstance(budget, bool) or not isinstance(budget, int):
            return False
        if not MIN_RESPONSE_BYTES <= budget <= upper:
            return False
    if arguments.get("select") is not None and name == "filter":
        # filter's `select` is its projection: a column of the set, or one
        # attribute (issue #3666, 2c); any other name is not offered.
        from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS

        select, scope = arguments.get("select"), arguments.get("input")
        kind = scope.get("kind", "nodes") if isinstance(scope, Mapping) else None
        if kind not in EFFECTIVE_VIEW_FIELD_COLUMNS or not isinstance(select, list) or not select:
            return False
        for field in select:
            if not isinstance(field, str) or not (
                field in EFFECTIVE_VIEW_FIELD_COLUMNS[kind]
                or (field.startswith("attrs.") and len(field) > len("attrs."))
            ):
                return False
    elif arguments.get("select") is not None and name != "inspect":
        # Every name must be a field this call's own rows have: the check
        # the tool body makes on `select` (step 3a).
        from okg.substrate.mcp.select_vocabulary import served_as_selected

        if not served_as_selected(name, arguments):
            return False
    if name == "search":
        from okg.substrate.mcp.search_where import normalize_where

        from okg.substrate.mcp.search_where import (
            normalize_children_scope,
            normalize_record_scope,
        )

        query = arguments.get("query")
        if not isinstance(query, str) or not query.strip():
            return False
        method = arguments.get("method")
        view, rank_by = arguments.get("view"), arguments.get("rank_by")
        windows = {
            name: arguments.get(name) for name in ("matching", "context_chars")
        }
        scope = arguments.get("scope")
        record_ids = None
        if scope is not None:
            # A list of named records only; any other scope is not proven.
            if normalize_children_scope(scope) is not None:
                return False
            record_ids = normalize_record_scope(scope)
            if record_ids is None:
                return False
        if method == "grep":
            # Grep's own argument checks: its query grammar, and no `where`
            # (grep judges a condition by rules this check does not make).
            from okg.substrate.mcp.grep_query import parse_grep_query

            if arguments.get("where") is not None or rank_by is not None:
                return False
            parse_grep_query(query)
            path_prefix = arguments.get("path_prefix")
            if path_prefix is not None and (
                view is not None or not isinstance(path_prefix, str) or not path_prefix.strip()
            ):
                # Proven on grep cards only, as one non-empty string.
                return False
            if view == "windows":
                # The windows read's own check of every argument it takes.
                from okg.substrate.mcp.record_window_search import validate_window_call

                validate_window_call(
                    view="windows", method="grep", record_ids=record_ids,
                    query=query, select=arguments.get("select"), **windows,
                )
                return True
            # Grep cards take no scope and no window argument.
            return view is None and scope is None and not any(
                value is not None for value in windows.values()
            )
        if method not in (None, "lexical") or arguments.get("path_prefix") is not None:
            return False
        if view is not None or any(value is not None for value in windows.values()):
            return False
        if method == "lexical" and scope is None:
            # A named default method is proven only for the scoped read.
            return False
        if rank_by is not None and (rank_by != "self" or scope is None):
            return False
        normalize_where(arguments.get("where"))
        # Whether a named date is declared for the type is known only by a
        # read: a call that carries a date clause is never offered.
        return not any(
            isinstance(leaf.get("field"), str) and leaf["field"].startswith("date")
            for leaf in _condition_leaves(arguments.get("where"))
        )
    if name == "inspect":
        return _inspect_call_servable(arguments)
    conditions: list[Any]
    if name == "expand":
        start = arguments.get("input")
        if isinstance(start, Mapping):
            start = start.get("node_id")
        if not isinstance(start, str) or not start:
            return False
        if arguments.get("direction") not in (None, "out", "in"):
            return False
        conditions = [arguments.get("filter")]
    else:
        from okg.substrate.mcp.set_operators import validate_set_scope

        scope = arguments.get("input")
        if (
            not isinstance(scope, Mapping) or "ref" in scope
            or scope.get("kind") not in ("nodes", "edges")
        ):
            return False
        validate_set_scope(scope["kind"], dict(scope))
        conditions = [arguments.get("predicate"), scope.get("filter")]
        if name == "aggregate" and not _plain_group_keys(
            arguments.get("by"), arguments.get("metrics"), scope,
        ):
            return False
    from okg.substrate.mcp import text_match

    held = [condition for condition in conditions if text_match.holds_text_clause(condition)]
    if held:
        # The tool body's text checks (loose-match-3666, the `matches` call
        # a like on `text` offers): records only, one text clause standing
        # alone or in one top-level `and`, its value in grep's grammar, and
        # access rules off. The rest of the condition is checked below.
        if not text_served or len(held) > 1:
            return False
        if name != "expand" and scope["kind"] != "nodes":
            return False
        rest, _condition = text_match.split_text_condition(held[0], tool=name)
        conditions = [rest if condition is held[0] else condition for condition in conditions]
    refuse_substring_contains(name, *conditions)
    from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS

    # The far end of a link is a record; a scope says its own kind.
    columns = EFFECTIVE_VIEW_FIELD_COLUMNS["nodes" if name == "expand" else scope["kind"]]
    for condition in conditions:
        for leaf in _condition_leaves(condition):
            op, field = leaf.get("op"), leaf.get("field")
            if not isinstance(field, str) or field == "text_hash":
                return False
            # A clause the grammar passed on unchecked (the long form in an
            # argument the tool already had) may still name a field the read
            # cannot lower, or carry an operand it cannot use.
            whole_map = field == "attrs"
            attribute = field.startswith("attrs.") and len(field) > len("attrs.")
            if not (whole_map or attribute or field in columns):
                return False
            if whole_map and op in ("lt", "gt", "between", "prefix", "suffix"):
                return False
            if op in ("prefix", "suffix") and not (
                isinstance(leaf.get("value"), str) and leaf["value"]
            ):
                return False
            if op == "in" and not (isinstance(leaf.get("values"), list) and leaf["values"]):
                return False
            if op == "between" and not (
                isinstance(leaf.get("values"), list) and len(leaf["values"]) == 2
            ):
                return False
    return True


def _rewrite_operator_call(
    name: str, arguments: Any,
    *, accepts: Callable[[str, dict[str, Any]], bool] | None = None,
    search_methods: frozenset[str] | None = None,
    access_rules_on: Callable[[], bool] | None = None,
    restartable: Callable[[str, dict[str, Any]], bool] | None = None,
) -> tuple[Any, "_CallRewrite | None"]:
    """The call a read operator is given, and what the rewrite did.

    `search_methods` is the search methods this server serves, when known:
    a `select` note then never names a method the caller would be refused.
    `restartable` says whether a refused call's own arguments without its
    continuation token are taken (`_continuation_intake`).

    Only the read operators are rewritten. A refusal is carried, not raised:
    it is served where argument refusals are, after authorization.
    `accepts` says whether the tool's own argument checks take a call; a
    retry a refusal would offer is dropped, for words, when they do not.
    """
    from okg.substrate.mcp.operators import OPERATOR_TOOLS

    if name not in OPERATOR_TOOLS:
        return arguments, None
    try:
        rewritten = rewrite_call(name, arguments, search_methods=search_methods)
    except GrammarRefusal as exc:
        fix_hint = exc.fix_hint
        retry_call: dict[str, Any] | None = None
        if exc.access_rules_hint is not None and access_rules_on is not None and (
            access_rules_on()
        ):
            # A call this deployment's access rules refuse is never named:
            # the words say the count is not available here (#3666).
            exc.fix_hint = fix_hint = exc.access_rules_hint
            exc.retry_arguments = None
        # A substring test in search's `where` is offered as filter's call
        # (loose-match-3666); every other retry is the same tool's.
        retry_tool = exc.retry_tool or name
        if exc.retry_arguments is not None and (
            accepts is None or accepts(retry_tool, exc.retry_arguments)
        ):
            # A call the rewrite itself takes as written: on the `retry:`
            # line when it fits one, and for a program in `error.retry_call`.
            # `corrected` when the caller's input determines it; a `guess`
            # keeps the words that say what it assumes.
            spelled = render_call(retry_tool, exc.retry_arguments)
            if exc.retry_repeat:
                # Named, never echoed: the caller passes its own again.
                spelled += ", with the same " + " and ".join(exc.retry_repeat)
            if len(spelled) <= _RETRY_CALL_CHARS:
                fix_hint = (
                    f"{exc.fix_hint}: {spelled}" if exc.retry_mode == "guess" else spelled
                )
            candidate = {
                "tool": retry_tool, "mode": exc.retry_mode, "arguments": exc.retry_arguments,
                **({"repeat": list(exc.retry_repeat)} if exc.retry_repeat else {}),
            }
            if len(json.dumps(candidate, default=str)) <= _RETRY_CALL_BYTES:
                retry_call = candidate
        if exc.code == "select_unknown_field":
            # A `select` no row of a fixed-row view can answer: the code
            # and data every other `select` refusal has.
            from okg.substrate.mcp.envelope import SelectUnknownField

            refusal: SubstrateError = SelectUnknownField(
                exc.message,
                data={"tool": name, "retry_select": None, **(exc.select_data or {})},
                fix_hint=fix_hint,
            )
        else:
            refusal = InvalidOperatorArguments(
                exc.message,
                data={
                    "tool": name,
                    "invalid_fields": [
                        _validation_field_path((field,)) for field in exc.fields[:32]
                    ],
                    "validation_kinds": ["argument_check"],
                },
                fix_hint=fix_hint,
            )
        refusal.retry_call = retry_call
        return arguments, _CallRewrite(refusal=refusal)
    select_retry: dict[str, Any] | None = None
    if rewritten.select_retry is not None and (
        accepts is None or accepts(name, rewritten.select_retry)
    ):
        candidate = {
            "tool": name, "mode": rewritten.select_retry_mode,
            "arguments": rewritten.select_retry,
            **(
                {"repeat": list(rewritten.select_retry_repeat)}
                if rewritten.select_retry_repeat else {}
            ),
        }
        if len(json.dumps(candidate, default=str)) <= _RETRY_CALL_BYTES:
            select_retry = candidate
    served, token_notes, token_refusal = _continuation_intake(
        name, arguments, rewritten.arguments,
        accepts=_restart_check(name, accepts, restartable),
    )
    if token_refusal is not None:
        return arguments, _CallRewrite(refusal=token_refusal)
    return served, _CallRewrite(
        adjusted=(*rewritten.adjusted, *token_notes), spellings=rewritten.spellings,
        row_keys=rewritten.row_keys, select_retry=select_retry,
        sent_select=rewritten.sent_select,
        arguments=(
            dict(served)
            if isinstance(served, Mapping) else None
        ),
    )


def _restart_check(
    name: str,
    accepts: Callable[[str, dict[str, Any]], bool] | None,
    restartable: Callable[[str, dict[str, Any]], bool] | None,
) -> Callable[[str, dict[str, Any]], bool] | None:
    """What a restart offer must pass before a token refusal offers it.

    The proof a retry offer needs (`accepts`, `_okg_takes_as_written`),
    which also runs the checks the tool body makes on its arguments, so an
    offered restart is never itself refused for them (lane ri, tokens: a
    restart of `expand(order_by="date.created desc", ...)` was). The proof
    covers no inspect call, and a path read's `path` and `depth`: those
    are offered when the rewrite and the argument model take them
    (`restartable`), the checks inspect and a path read make without a
    read.
    """
    if accepts is None or restartable is None:
        return accepts if accepts is not None else restartable

    def check(tool: str, candidate: dict[str, Any]) -> bool:
        if accepts(tool, candidate):
            return True
        unproven = tool == "inspect" or (
            tool == "expand" and (candidate.get("path") is not None or "depth" in candidate)
        )
        return unproven and restartable(tool, candidate)

    return check


#: Tools whose `next` argument takes a signed continuation token.
_NEXT_TOOLS = frozenset({"inspect", "expand", "filter", "map"})

#: The argument a restarted read cannot do without, per tool: a restart
#: offered without it would itself be refused.
_RESTART_NEEDS: dict[str, tuple[str, ...]] = {
    "inspect": ("target",),
    "expand": ("input",),
    "filter": ("input", "where", "predicate"),
    "map": ("input", "where"),
    "search": ("query",),
}

#: Token refusals a restart answers: the token does not open here. A token
#: for another tool or kind keeps its own retry (`call expand(next=...)`).
_RESTART_REASONS = frozenset({
    "malformed", "signature_invalid", "payload_invalid", "version_invalid",
    "signing_key_changed", "token_unknown",
})


def _continuation_intake(
    name: str, sent: Any, arguments: Any,
    *, accepts: Callable[[str, dict[str, Any]], bool] | None = None,
) -> tuple[Any, tuple[dict[str, str], ...], SubstrateError | None]:
    """Check a call's continuation token before anything else reads the call.

    Lane ri, tokens: nine lab runs passed 96 tokens back; 40 were refused,
    and 22 of those refusals named an argument beside `next` while the real
    fault was the token (made up, or miscopied), so the retry the refusal
    asked for failed again. Here, first:

    - a `next` (inspect, expand, filter, map) or `continuation_token`
      (search, expand, an inspect target) that does not open is refused by
      its own reason, with the opaque-token hint and, when the caller's own
      arguments form a read, the call that restarts it (`retry_call`, mode
      `restart`);
    - arguments beside a `next` that opens and that repeat what the token
      already reads are read as `next` alone, with an `adjusted` note. An
      argument naming a different read is left for the tool to refuse.

    Pure on the arguments (the token check is a signature check). Returns
    `(arguments, notes, refusal)`. A live answer's `next` (expand) is the
    live page's to check.
    """
    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        open_continuation,
        repeated_read_arguments,
        route_next,
    )

    if not isinstance(arguments, Mapping):
        return arguments, (), None
    token = arguments.get("next")
    if name in _NEXT_TOOLS and token is not None and not (
        name == "expand" and _is_live_next(token)
    ):
        try:
            binding = route_next(token, tool=name)
        except ContinuationTokenInvalid as exc:
            return arguments, (), _token_refusal(
                name, exc, sent=sent, served=arguments, slot="next", accepts=accepts,
            )
        names = repeated_read_arguments(name, arguments, binding)
        if not names:
            return arguments, (), None
        note = {
            "rule": "next_repeats_read",
            "sent": ", ".join(f"`{n}`" for n in names) + " beside `next`",
            "served": "`next` alone, which already reads "
            + ("it" if len(names) == 1 else "them"),
        }
        return {k: v for k, v in arguments.items() if k not in names}, (note,), None
    slot: str | None = None
    # Only grep pages issue search tokens: a token sent to search with
    # another method keeps that method's own refusal (`continuation_token_
    # not_applicable`), and one sent with no method is read as grep only
    # when it opens (`read_continuation_method`).
    takes_token = name == "expand" or (name == "search" and arguments.get("method") == "grep")
    if takes_token and arguments.get("continuation_token") is not None:
        slot, token = "continuation_token", arguments["continuation_token"]
    elif name == "inspect":
        target = arguments.get("target")
        if isinstance(target, Mapping) and target.get("continuation_token") is not None:
            slot, token = "target.continuation_token", target["continuation_token"]
    if slot is None:
        return arguments, (), None
    try:
        open_continuation(token)
    except ContinuationTokenInvalid as exc:
        return arguments, (), _token_refusal(
            name, exc, sent=sent, served=arguments, slot=slot, accepts=accepts,
        )
    return arguments, (), None


def _without_token(arguments: Mapping[str, Any], slot: str) -> dict[str, Any]:
    """`arguments` without the token in `slot` and without `authority`."""
    out = {k: v for k, v in arguments.items() if k not in ("authority", "next")}
    if slot == "continuation_token":
        out.pop("continuation_token", None)
        out.pop("offset", None)
    elif slot == "target.continuation_token" and isinstance(out.get("target"), Mapping):
        out["target"] = {
            k: v for k, v in out["target"].items() if k != "continuation_token"
        }
    return out


def _token_refusal(
    name: str, exc: SubstrateError, *, sent: Any, served: Mapping[str, Any],
    slot: str, accepts: Callable[[str, dict[str, Any]], bool] | None,
) -> SubstrateError:
    """`exc` with the call that restarts the read, when there is one.

    The restart is the caller's own call without the token: as sent, or
    else as the rewrite read it, whichever the server takes as written. A
    call that names no read (`inspect(next="x")`) gets no restart: the hint
    says to copy the token, or to start the read without one.
    """
    from okg.substrate.mcp.continuation import OPAQUE_REASONS
    from okg.substrate.mcp.read_grammar import render_call

    data = getattr(exc, "data", None)
    reason = data.get("reason") if isinstance(data, dict) else None
    if reason not in _RESTART_REASONS:
        return exc
    needs = _RESTART_NEEDS.get(name, ())
    copy_hint = (
        "pass the token exactly as the reply printed it after `next:` (a "
        "token is opaque: never build, shorten or edit one)"
    )
    for source in (sent, served):
        if not isinstance(source, Mapping):
            continue
        candidate = _without_token(source, slot)
        if not any(candidate.get(key) is not None for key in needs):
            continue
        if accepts is not None and not accepts(name, candidate):
            continue
        repeat = ["authority"] if "authority" in source else []
        call = {
            "tool": name, "mode": "restart", "arguments": candidate,
            **({"repeat": repeat} if repeat else {}),
        }
        if len(json.dumps(call, default=str)) > _RETRY_CALL_BYTES:
            continue
        spelled = render_call(name, candidate)
        if repeat:
            spelled += ", with the same authority"
        restart = (
            f"restart the read: {spelled}" if len(spelled) <= _RETRY_CALL_CHARS
            else "restart the read with the call in retry_call"
        )
        exc.fix_hint = (
            f"{copy_hint}, or {restart}" if reason in OPAQUE_REASONS else restart
        )
        exc.retry_call = call
        return exc
    return exc


_UNSET = object()


async def run_sync_in_request_context(
    fn: Callable[..., Any],
    /,
    *args: Any,
    executor: concurrent.futures.Executor | None = None,
    **kwargs: Any,
) -> Any:
    """Run sync ``fn`` in a worker thread, as if it ran in the caller's context.

    Issue #3493. FastMCP calls a sync tool on the event loop, so one blocking
    call stalled every session on the process. Here the call runs in a worker
    thread instead, and the request's per-call state still behaves exactly as
    it did on the loop:

    * The thread runs in a COPY of the calling task's context, taken now. It
      sees every context variable the request set before the call (the MCP
      request, the result format, the serve scope, the image slot, ...), and
      a mutable object one of them holds is the same object, so what the
      tool appends to it the caller sees.
    * What the tool SETS in that copy is then adopted into the caller's own
      context once the thread has finished, whether ``fn`` returned or
      raised. A variable set and reset inside the tool is unchanged and
      adopts nothing; one left set is seen by the code that runs after it.

    Why a copy and not the task's own ``Context`` object: a ``Context`` can be
    entered by one thread at a time. The task's context is entered on the
    loop until the task reaches its ``await``, and is entered again by any
    callback the loop runs for that task (a wake-up, a cancellation), so
    running the thread inside it races the loop and can raise "cannot enter
    context" there. The copy is entered only by the worker, and the adopt
    step runs back on the loop after the thread finished.

    The request task is suspended at the ``await`` for the whole thread run,
    so nothing else writes its context in between. Each request is its own
    task with its own context (the SDK starts one per message), so one call's
    adopted state never reaches another's.

    ``executor`` bounds the threads (``None`` is the loop's default executor).
    A thread's slot is the executor's worker, so it is held until the thread
    finishes. If the awaiting task is cancelled, a call still waiting for a
    worker never starts, and one already running goes on in its worker and
    keeps that slot until it returns; its state is not adopted (the call is
    gone).
    """

    import asyncio

    context = contextvars.copy_context()
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(
        executor, functools.partial(context.run, fn, *args, **kwargs),
    )
    try:
        result = await future
    except asyncio.CancelledError:
        raise
    except BaseException:
        _adopt_context(context)
        raise
    _adopt_context(context)
    return result


def _adopt_context(context: contextvars.Context) -> None:
    """Set in the running context every variable ``context`` changed.

    ``context`` started as a copy of the running one, and a copy can only add
    or replace values (a reset token from the parent is refused in a copy),
    so this replay is the whole difference.
    """

    for var, value in context.items():
        if var.get(_UNSET) is not value:
            var.set(value)


class _RawResultFastMCP(FastMCP):
    """FastMCP whose ``call_tool`` hands back the tool's own return value.

    The SDK would otherwise convert a returned dictionary itself: with an
    output schema into a JSON text copy plus ``structuredContent``, without
    one into pretty-printed JSON text. ``ByteBoundedFastMCP.call_tool`` builds
    the one served result from the raw value instead (issue #3059). Only the
    last link of the ``call_tool`` chain changes, so the session tool
    projection and the dispatch checks above it still run.

    Nothing that can block runs on the event loop (issue #3493). Every sync
    step goes through ``_okg_run_blocking``: a worker thread in the request's
    context (``run_sync_in_request_context``). That covers the tool itself,
    here, at the one place every call passes -- graph reads, write and admin
    tools, deployment tools -- with the registered function and its published
    schema unchanged; a sync resource read (``read_resource``); and, in
    subclasses and the dispatch gate, the principal gates and the result
    serving, which borrow pool connections and take a session's state lock.
    Async tools still run on the loop.

    ``_okg_tool_call_width`` is how many such steps run at once: the worker
    count of this server's executor. The OKG server sets it from its read
    pool (``build_server``); ``None`` uses the loop's default executor. A step
    over the width waits for a worker on the loop, where the client can still
    cancel it, instead of holding a thread that would wait on the pool and
    fail at its checkout timeout. Steps of one call run one after another,
    never nested, so a call needs one worker at a time and cannot deadlock
    itself even at width 1.
    """

    _okg_tool_call_width: int | None = None

    def _okg_tool_executor(self) -> concurrent.futures.ThreadPoolExecutor | None:
        """This server's worker threads, sized by ``_okg_tool_call_width``.

        Not bound to an event loop, so a server driven by several loops (tests
        call it through ``anyio.run`` per call) is bounded by one width.
        """

        width = self._okg_tool_call_width
        if width is None:
            return None
        current = self.__dict__.get("_okg_tool_executor_state")
        if current is not None and current[0] == width:
            return current[1]
        with _EXECUTOR_LOCK:
            current = self.__dict__.get("_okg_tool_executor_state")
            if current is not None and current[0] == width:
                return current[1]
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=width, thread_name_prefix="okg-mcp-call",
            )
            self.__dict__["_okg_tool_executor_state"] = (width, executor)
        if current is not None:
            current[1].shutdown(wait=False)
        return executor

    def _okg_shutdown_tool_executor(self) -> None:
        """Shut this server's request workers down, without waiting.

        Called when the server closes (its read pool's close). A step still
        queued is cancelled; one running finishes in its thread, which then
        exits. A later blocking step would start a fresh executor, but a
        closed server has no pool for it to use.
        """

        with _EXECUTOR_LOCK:
            current = self.__dict__.pop("_okg_tool_executor_state", None)
        if current is not None:
            current[1].shutdown(wait=False, cancel_futures=True)

    async def _okg_run_blocking(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any,
    ) -> Any:
        """Run one sync step of a request off the loop, in its context."""

        return await run_sync_in_request_context(
            fn, *args, executor=self._okg_tool_executor(), **kwargs,
        )

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        context = self.get_context()
        tool = self._tool_manager.get_tool(name)
        handler = getattr(self, "_okg_run_handler", None)
        if name == "run" and tool is not None and handler is not None:
            # `run` (#3666): its steps are dispatched through this server's
            # own `call_tool`, each as a call of its own.
            return await handler(self, arguments)
        resolved = None
        if (
            tool is not None and name in _live_resolver.RESOLVED_TOOLS
            and getattr(self, "_okg_live_types", None) is not None
        ):
            # A read of a live type (#3666, live-graph-rucio-sites): resolved
            # here, after the rewrite, the tool-name gate and validation, and
            # before the tool body, in run's three phases (no call worker or
            # connection held while the upstream system answers). A call that
            # reaches no live type passes through unchanged.
            outcome = await _live_resolver.resolve(self, name, arguments)
            if isinstance(outcome, dict):
                # The tool body's own argument refusal, from the shared
                # pre-check, before any upstream call.
                return outcome
            if outcome is not None:
                arguments, resolved = outcome
        if tool is not None and not tool.is_async:
            # The same `Tool.run` (argument validation, context injection, the
            # ToolError wrapping callers unwrap), on a per-call copy whose
            # function awaits the sync one in a worker thread.
            sync_fn = tool.fn

            async def offloaded(**kwargs: Any) -> Any:
                return await self._okg_run_blocking(sync_fn, **kwargs)

            tool = tool.model_copy(update={"fn": offloaded, "is_async": True})
            if resolved is not None:
                # The rows reach the tool body's own draft scope through a
                # context variable (B4'): `run_sync_in_request_context` copies
                # the context into the worker.
                with _live_resolver.handoff_scope(resolved):
                    return await tool.run(arguments, context=context, convert_result=False)
            return await tool.run(arguments, context=context, convert_result=False)
        return await self._tool_manager.call_tool(
            name, arguments, context=context, convert_result=False,
        )

    async def read_resource(self, uri: Any) -> Any:
        """Read a resource; a sync resource function runs off the loop.

        The OKG resources are sync functions that resolve the caller's
        principal (a pool connection) and may build a schema digest from the
        database. ``FunctionResource.read`` would call them on the loop; a
        per-read copy whose function awaits the sync one in a worker thread
        keeps the SDK's own conversion and error wrapping.
        """

        from mcp.server.fastmcp.resources.types import FunctionResource
        from mcp.server.lowlevel.helper_types import ReadResourceContents

        context = self.get_context()
        resource = await self._resource_manager.get_resource(uri, context=context)
        if not resource:
            from mcp.server.fastmcp.exceptions import ResourceError

            raise ResourceError(f"Unknown resource: {uri}")
        if isinstance(resource, FunctionResource) and not inspect.iscoroutinefunction(
            resource.fn
        ):
            sync_fn = resource.fn

            async def offloaded() -> Any:
                return await self._okg_run_blocking(sync_fn)

            resource = resource.model_copy(update={"fn": offloaded})
        try:
            content = await resource.read()
        except Exception as exc:
            from mcp.server.fastmcp.exceptions import ResourceError

            _log.exception("Error reading resource %s", uri)
            raise ResourceError(str(exc)) from exc
        return [
            ReadResourceContents(
                content=content, mime_type=resource.mime_type, meta=resource.meta,
            )
        ]


#: Guards the lazy creation of a server's tool executor.
_EXECUTOR_LOCK = threading.Lock()


def _requested_result_format(server: FastMCP) -> tuple[Optional[str], Any]:
    """The format the request's ``_meta`` names, and the raw value seen.

    ``(None, None)`` when the request names none, or when the call does not
    come through a request at all (an in-process ``call_tool``).
    """

    try:
        request_context = server.get_context().request_context
    except (LookupError, RuntimeError, ValueError, AttributeError):
        return None, None
    meta = getattr(request_context, "meta", None)
    extra = getattr(meta, "model_extra", None) if meta is not None else None
    if not extra or RESULT_FORMAT_META_KEY not in extra:
        return None, None
    value = extra[RESULT_FORMAT_META_KEY]
    if isinstance(value, str) and value in RESULT_FORMATS:
        return value, value
    return "", value


def _inspect_kind(name: str, arguments: Mapping[str, Any]) -> Optional[str]:
    if name != "inspect":
        return None
    target = arguments.get("target") if isinstance(arguments, Mapping) else None
    if isinstance(target, Mapping):
        kind = target.get("kind")
        return str(kind) if isinstance(kind, str) else None
    if isinstance(target, str):
        return target
    return None


#: A bare `select` name that may be read as a stored attribute key: plain,
#: with no dot (a dotted name is a field path of its own).
_BARE_ATTR_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]{0,63}\Z")


def bare_names_as_attr_keys(
    name: str, arguments: Mapping[str, Any], exc: BaseException,
    *, allow_live: bool = False,
) -> Optional[tuple[dict[str, Any], list[str]]]:
    """`(arguments, keys)` with each refused bare `select` name as
    `attrs.<key>`, or None when the refusal is not one this reads. Pure.

    Only a `select_unknown_field` refusal of search, expand or inspect(node)
    whose every refused name is a plain name with no other answer: not a
    name the refusal corrects to a field (`id`, `type`, ...), not one it
    explains (`external_live`), not `text` (the text read), and no dotted
    name. The caller keeps the read only when the rows store every key
    (:func:`rows_store_keys`).

    Never for a call whose read reaches outside the graph: an expand with
    `live=` or an inspect whose target asks `hydrate` calls an external
    system (and writes its cache), which a read that may be discarded must
    not do. `allow_live` admits the live expand for a caller that first
    reads the keys on the graph rows alone and calls out only to serve
    (`ByteBoundedFastMCP._okg_select_stored_keys`, #3666, lane RD)."""
    from okg.substrate.mcp.envelope import _CONVENTIONAL_NAMES, _REPLY_KEYS_NOT_FIELDS

    typed = exc if isinstance(exc, SubstrateError) else exc.__cause__
    if getattr(typed, "code", None) != "select_unknown_field":
        return None
    if name == "inspect":
        target = arguments.get("target")
        if not isinstance(target, Mapping) or target.get("kind") != "node":
            return None
        if target.get("hydrate") not in (None, False, [], ()):
            return None
    elif name == "expand":
        if arguments.get("live") not in (None, {}) and not allow_live:
            return None
    elif name != "search":
        return None
    select = arguments.get("select")
    data = getattr(typed, "data", None)
    unknown = data.get("unknown_fields") if isinstance(data, Mapping) else None
    if (
        not isinstance(select, list) or not isinstance(unknown, list) or not unknown
        or not all(isinstance(item, str) for item in select)
    ):
        return None
    from okg.substrate.mcp.select_vocabulary import WHERE_DATES_IN_ROWS, WHERE_NAMES_IN_ROWS

    excluded = {"text", *_CONVENTIONAL_NAMES, *_REPLY_KEYS_NOT_FIELDS}
    keys: list[str] = []
    for field_name in unknown:
        if name == "search" and (
            field_name in WHERE_NAMES_IN_ROWS or field_name in WHERE_DATES_IN_ROWS
        ):
            # Left as sent: the rewrite of the new call reads it as the row
            # field that holds it, once no other name is refused beside it.
            continue
        if (
            not isinstance(field_name, str) or field_name in excluded
            or not _BARE_ATTR_NAME.match(field_name) or field_name not in select
        ):
            return None
        if field_name not in keys:
            keys.append(field_name)
    if not keys:
        return None
    served: list[str] = []
    for item in select:
        item = f"attrs.{item}" if item in keys else item
        if item not in served:
            served.append(item)
    return {**arguments, "select": served}, keys


def rows_store_keys(name: str, payload: Mapping[str, Any], keys: Sequence[str]) -> bool:
    """Whether each of `keys` is held by at least one served row. Pure.

    A row holds a key in its `attrs`, or as `attrs.<key>` in its `selected`
    (a search row). inspect(node)'s reply is its one row. A page with no
    rows holds nothing."""
    if payload.get("status") == "refused":
        return False
    # A 1-hop expand page holds its rows in `neighbors`; search and a path
    # expand in `results`.
    rows = [payload] if name == "inspect" else payload.get(
        "neighbors" if isinstance(payload.get("neighbors"), list) else "results"
    )
    if not isinstance(rows, list):
        return False

    def holds(row: Any, key: str) -> bool:
        if not isinstance(row, Mapping):
            return False
        attrs = row.get("attrs")
        selected = row.get("selected")
        return (isinstance(attrs, Mapping) and key in attrs) or (
            isinstance(selected, Mapping) and f"attrs.{key}" in selected
        )

    return bool(keys) and all(any(holds(row, key) for row in rows) for key in keys)


def _select_names(arguments: Mapping[str, Any]) -> Optional[tuple[str, ...]]:
    """The call's ``select`` names for the text view, or None when it named none.

    PACT read-model-s2-handle: the text view prints a search row's
    select-only fields (``result_text.SELECT_ONLY_ROW_FIELDS``) only when the
    caller named them. The operator validates ``select`` itself; this only
    reads it.
    """
    select = arguments.get("select") if isinstance(arguments, Mapping) else None
    if isinstance(select, (list, tuple)):
        return tuple(str(item) for item in select)
    return None


class ByteBoundedFastMCP(SessionScopedToolsMixin, _RawResultFastMCP):
    """FastMCP server that serves every result once and bounds read errors.

    ``SessionScopedToolsMixin`` sits ahead of the SDK so a per-session tool
    projection (issue #1458) filters ``list_tools``/``call_tool`` before the
    result is built. It is inert unless a launch installs one.

    ``call_tool`` builds the served result for every tool (issue #3059): one
    text block in the call's format, ``isError`` for a refusal. Registered
    functions keep returning plain dictionaries to their direct Python
    callers; only the MCP path renders them.

    Successful read dictionaries are bounded inside each registered read
    function. Exceptions must be handled one layer later: catching them in
    the function wrapper changes the direct Python tool contract, while
    FastMCP otherwise reflects the complete exception text into an unbounded
    error result.

    ``response_default_bytes`` / ``response_ceiling_bytes`` are the
    deployment's effective ``agent_surface.response_bytes``, installed by
    ``register_core_read_tools`` so the raised-error path resolves the same
    budget as the wrapper. ``None`` means the deployment declares none.
    """

    response_default_bytes: int | None = None
    response_ceiling_bytes: int | None = None
    #: Set by the OKG server when the release verdict is `read_only` (PACT
    #: unknown-s8-mcp-readonly): the normalized report from
    #: `read_only_release_from_report`. ``None`` means the database is not
    #: behind, and every tool dispatches as before.
    _okg_read_only_release: dict[str, Any] | None = None
    #: The `inspect` kinds this launch serves, set by the OKG server from the
    #: same `inspect_kind_set` the published union and the router guard use.
    #: ``None`` (a bare ByteBoundedFastMCP that never ran `build_server`)
    #: leaves an unknown kind on the schema refusal, as before.
    _okg_inspect_served_kinds: frozenset[str] | None = None

    def _refuse_write_while_schema_behind(self, name: str) -> None:
        """Refuse every tool that is not a read operator, naming the skew.

        Checked after authorization and session visibility, before argument
        validation: a write tool refuses for the database's state, whatever
        arguments it was sent.
        """
        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        release = self._okg_read_only_release
        if release is None or name in OPERATOR_TOOLS:
            return
        raise SchemaBehindReadOnly(
            f"{name} is refused: this server is {release['verdict_message']}. "
            "Write and admin tools stay off until the database is migrated.",
            data={
                "tool": name,
                "schema_behind_by": release["schema_behind_by"],
                "behind_migrations": list(release["behind_migrations"]),
            },
            fix_hint=release["fix_hint"],
        )

    def _refuse_unknown_operator_arguments(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> None:
        """Refuse an argument the operator does not declare, naming the key.

        The SDK builds each tool's argument model from its signature and
        DROPS any keyword that model does not declare. So a caller that sent
        `search(scope=...)` — a parameter this server advertised until issue
        #2009 removed it — got a perfectly normal, completely unscoped page,
        and nothing in the response said the argument had been ignored. That
        is the silent-narrowing failure the read surface exists to avoid, and
        it applies to every misspelling, not just to `scope`.

        Checked here because this is the last place that still sees the RAW
        argument dictionary; by the time the tool function is called the
        unknown keys are already gone.
        """
        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        if name not in OPERATOR_TOOLS:
            return
        tool = self._tool_manager._tools.get(name)
        if tool is None:
            return
        # `alias or name`: FastMCP renames a parameter that would shadow a
        # BaseModel attribute (`schema`, `copy`, `json`, `dict`, `validate`)
        # to `field_<name>` and keeps the real name as the alias. Reading the
        # field names alone would make the wire name look undeclared and
        # refuse a perfectly good call.
        declared = {
            info.alias or field_name
            for field_name, info in tool.fn_metadata.arg_model.model_fields.items()
        }
        unknown = sorted(set(arguments) - declared)
        if not unknown:
            return
        instead = {
            key: _ARGUMENT_INSTEAD[(name, key)]
            for key in unknown if (name, key) in _ARGUMENT_INSTEAD
        }
        raise UnknownOperatorArgument(
            f"{name} does not accept {', '.join(repr(key) for key in unknown)}. "
            "An undeclared argument is refused rather than ignored: a call "
            "that silently dropped it would return a normal-looking answer "
            "to a question nobody asked.",
            data={
                "tool": name, "unknown_arguments": unknown,
                **({"instead": instead} if instead else {}),
            },
            fix_hint=(
                "; ".join(f"{key}: {hint}" for key, hint in instead.items())
                + ("; " if instead else "")
                + f"remove {unknown[0]!r}, or re-read {name}'s inputSchema in "
                "tools/list for the arguments it does declare"
            ),
        )

    def _validate_operator_arguments(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> None:
        """Validate the registered SDK argument model before tool execution.

        This seam distinguishes caller input failures from Pydantic errors a
        tool body or output model may raise later. Only field locations and
        validation kinds are retained; Pydantic's raw inputs and context are
        deliberately never copied into the public refusal. The one exception
        is the unknown-inspect-kind refusal (`_refuse_unknown_inspect_kind_by_name`),
        which echoes the caller's kind string, bounded by the response byte
        budget.
        """
        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        if name not in OPERATOR_TOOLS:
            return
        tool = self._tool_manager._tools.get(name)
        if tool is None:
            return
        try:
            tool.fn_metadata.arg_model.model_validate(arguments)
        except pydantic_core.ValidationError as exc:
            self._refuse_unknown_inspect_kind_by_name(name, arguments)
            if name == "inspect":
                # A batch record read of the wrong shape names its fix (#3657).
                from okg.substrate.mcp.node_batch import shape_refusal

                shape = shape_refusal(arguments.get("target"))
                if shape is not None:
                    raise InvalidOperatorArguments(
                        shape[0] + ".",
                        data={
                            "tool": name, "invalid_fields": shape[2],
                            "validation_kinds": ["argument_check"],
                        },
                        fix_hint=shape[1],
                    ) from exc
            rows = exc.errors(
                include_url=False,
                include_input=False,
                include_context=False,
            )
            invalid_fields = list(
                dict.fromkeys(
                    _validation_field_path(tuple(row.get("loc") or ()))
                    for row in rows[:32]
                )
            )
            validation_kinds = list(
                dict.fromkeys(str(row.get("type") or "invalid")[:128] for row in rows[:32])
            )
            displayed = ", ".join(repr(field) for field in invalid_fields[:8])
            misplaced = _misplaced_top_level_arguments(
                rows, set(tool.fn_metadata.arg_model.model_fields),
            )
            message = f"{name} input validation failed at {displayed}."
            fix_hint = (
                f"correct the named field(s), or re-read {name}'s "
                "inputSchema in tools/list"
            )
            if misplaced:
                # A top-level argument nested inside another one (#3066):
                # `max_bytes` inside `target` is the common case.
                moves = "; ".join(
                    f"{key} goes at the top level of the {name} call, "
                    f"beside {outer}, not inside it"
                    for key, outer in misplaced
                )
                message += f" {moves}."
                fix_hint = "move " + ", ".join(
                    f"{key} out of {outer}" for key, outer in misplaced
                ) + " to the top level of the call"
                moved = {f"{outer}." for _key, outer in misplaced}
                if len(misplaced) < len(rows[:32]) or not all(
                    field.startswith(tuple(moved)) for field in invalid_fields
                ):
                    fix_hint += ", and correct the other named field(s)"
            raise InvalidOperatorArguments(
                message,
                data={
                    "tool": name,
                    "invalid_fields": invalid_fields,
                    "validation_kinds": validation_kinds,
                },
                fix_hint=fix_hint,
            ) from exc

    def _refuse_unknown_inspect_kind_by_name(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> None:
        """Give an unknown `inspect` kind the guard's named refusal.

        The validator keeps the full kind union so a WITHHELD kind reaches the
        router guard inside `inspect` and is refused by name. A kind that is
        in no union never gets that far: pydantic refuses it here with
        `union_tag_invalid`, a schema-shaped answer that cannot tell "this
        kind does not exist" from "this launch withholds it" (Jason, 179a).
        So when validation has already failed and the kind is a string that
        names no inspect kind, raise the guard's own refusal with the served
        set the guard uses. Anything else -- a missing or non-string kind, or
        a real kind with bad other fields -- keeps the schema refusal.
        """
        served = self._okg_inspect_served_kinds
        if name != "inspect" or served is None:
            return
        target = arguments.get("target")
        if not isinstance(target, Mapping):
            return
        kind = target.get("kind")
        if not isinstance(kind, str):
            return
        from okg.substrate.mcp.effective_surface import (
            assert_inspect_kind_served,
        )
        from okg.substrate.mcp.operator_models import (
            INSPECT_KIND_ALIASES,
            INSPECT_TARGET_KINDS,
        )

        if INSPECT_KIND_ALIASES.get(kind, kind) in INSPECT_TARGET_KINDS:
            return
        assert_inspect_kind_served(kind, served_kinds=served)

    def _okg_access_rules_state(self) -> bool | None:
        """Whether this server's deployment evaluates access rules (audit or
        enforce), under which `aggregate` refuses every set count; None when
        that cannot be told."""
        deployment = getattr(self, "_okg_read_deployment_name", None)
        if not isinstance(deployment, str):
            return None
        try:
            from okg.substrate.nomos.runtime_enforcement import (
                deployment_requires_nomos_runtime,
            )

            return bool(deployment_requires_nomos_runtime(deployment))
        except Exception:  # noqa: BLE001 - unknown is said as unknown, never as on.
            return None

    def _okg_aggregate_refused(self) -> bool:
        """Whether an `aggregate` count may not be offered: access rules are
        on, or whether they are cannot be told (no offer, never a wrong one)."""
        return self._okg_access_rules_state() is not False

    def _okg_access_rules_on(self) -> bool:
        """Whether access rules are known to be on: only then does a refusal
        say counting is not available on this deployment. When that cannot
        be told, the plain refusal is served, with no call and no claim."""
        return self._okg_access_rules_state() is True

    def _okg_like_served(self) -> bool:
        """Whether a like or like_exact read is served here (loose-match-3666):
        access rules known off and the database's fold check passed. The
        read refuses it otherwise, so a refusal never offers one then."""
        if self._okg_access_rules_state() is not False:
            return False
        problem = getattr(self, "_okg_like_database_problem", None)
        return callable(problem) and problem() is None

    def _okg_takes_as_written(self, name: str, arguments: dict[str, Any]) -> bool:
        """Whether `name(**arguments)` passes every argument check as sent.

        The checks a dispatched call meets, in their order: the grammar
        rewrite, with no note; the declared-argument check; the tool's
        argument model; and the checks the tool body makes on its arguments
        (`_proven_servable`). Used before a refusal offers the call as a
        retry, so a retry is never itself refused for its arguments.
        """
        if name == "aggregate" and self._okg_aggregate_refused():
            # Under access rules every set count is refused (no rule check
            # runs inside a GROUP BY): an offered count would be too.
            return False
        try:
            rewritten = rewrite_call(name, arguments)
            if rewritten.adjusted:
                return False
            self._refuse_unknown_operator_arguments(name, rewritten.arguments)
            self._validate_operator_arguments(name, rewritten.arguments)
            return _proven_servable(
                name, rewritten.arguments, ceiling=self.response_ceiling_bytes,
                text_served=self._okg_access_rules_state() is False,
                like_served=self._okg_like_served,
            )
        except Exception:  # noqa: BLE001 - any refusal of the candidate means no offer.
            return False

    def _okg_restartable(self, name: str, arguments: dict[str, Any]) -> bool:
        """Whether a refused call's own arguments, without its token, pass
        the rewrite, the declared-argument check and the argument model.

        What a restart offer needs (lane ri, tokens): the call is the
        caller's own, refused only for its token, so it is offered when the
        checks that need no read take it; `_okg_takes_as_written` is for a
        corrected call, and proves too little for inspect and path reads.
        """
        try:
            rewritten = rewrite_call(name, arguments)
            self._refuse_unknown_operator_arguments(name, rewritten.arguments)
            self._validate_operator_arguments(name, rewritten.arguments)
        except Exception:  # noqa: BLE001 - any refusal of the restart means no offer.
            return False
        return True

    def _refuse_query_under_access_rules(self, name: str) -> None:
        """Refuse `query` plainly where access rules are on (issue #3666).

        The mode read at build (`_okg_query_off_mode`) took `query` off the
        surface. A server built with the rules off reads the mode again on
        every call, so rules switched on after start (audit included, which
        would otherwise only record the SQL and run it) refuse it too, as
        does a mode that cannot be read.
        """
        if name != "query":
            return
        mode = getattr(self, "_okg_query_off_mode", None)
        deployment = getattr(self, "_okg_query_mode_deployment", None)
        if mode is None and deployment is not None:
            from okg.substrate.mcp.effective_surface import (
                query_withheld_by_access_rules,
            )

            mode = query_withheld_by_access_rules(deployment)
        if mode is None:
            return
        raise QueryOffUnderAccessRules(
            "query is off on this deployment: its access rules are "
            f"{mode}, and SQL cannot be filtered by them, so this server "
            "does not serve query",
            data={"tool": "query", "access_rules": mode},
            fix_hint=(
                "use search, filter, map, inspect, expand or aggregate, "
                "which apply the access rules"
            ),
        )

    def _okg_before_tool_dispatch(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> None:
        self._refuse_query_under_access_rules(name)
        self._refuse_write_while_schema_behind(name)
        rewrite = _CALL_REWRITE.get()
        if (
            rewrite is not None and rewrite.refusal is not None
            and self._tool_manager.get_tool(name) is not None
        ):
            # The grammar rewrite ran before authorization (it reads only
            # the arguments); its refusal is served here, past the principal
            # gate and the session's visibility check, so a caller who may
            # not call the tool is still told that first. A tool this server
            # does not register gets no grammar refusal: it keeps the
            # unknown-tool answer it always got.
            raise rewrite.refusal
        self._refuse_unknown_operator_arguments(name, arguments)
        try:
            self._validate_operator_arguments(name, arguments)
        except InvalidOperatorArguments as exc:
            if rewrite is not None and rewrite.spellings:
                # The refusal names the argument the call was read as.
                exc.fix_hint = (exc.fix_hint or "") + "; " + "; ".join(
                    f"`{sent}` is read as `{served}`"
                    for sent, served in rewrite.spellings
                )
            raise

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> Any:
        """One call: a read call naming a set (``$r7``) or asking ``retain``
        goes through the set hook (issue #3666), which serves it through
        ``_okg_call_tool_plain``; every other call goes there directly."""
        hook = getattr(self, "_okg_sets_hook", None)
        if hook is not None and name in _SET_TOOLS:
            return await hook(self, name, arguments, self._okg_call_tool_plain)
        return await self._okg_call_tool_plain(name, arguments)

    async def _okg_call_tool_plain(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> Any:
        """Dispatch one call and serve its result once, in the call's format.

        The format comes from the request ``_meta`` key ``okg/result_format``
        (``text`` by default, ``json`` for programs). It is set into a
        ``ContextVar`` for the whole call, so every byte measurer inside the
        tool measures the form that is served. An unknown value is refused,
        never ignored.
        """

        requested, raw_value = _requested_result_format(self)
        if requested == "":
            return self._okg_invalid_result_format(name, raw_value)
        # The grammar rewrite (issue #3666), first: everything below, the
        # authorization and validation included, sees the rewritten call.
        # It runs for every read-tool name, whether or not this server
        # registers the tool or shows it to this session: what a caller the
        # gate refuses is told must not depend on either.
        capabilities = getattr(self, "_okg_capabilities", None)
        available = getattr(capabilities, "available_methods", None)
        arguments, rewrite = _rewrite_operator_call(
            name, arguments, accepts=self._okg_takes_as_written,
            search_methods=frozenset(available()) if callable(available) else None,
            access_rules_on=self._okg_access_rules_on,
            restartable=self._okg_restartable,
        )
        token = _CALL_REWRITE.set(rewrite)
        try:
            context = ResultContext(
                format=requested or default_result_format(),
                tool=name,
                kind=_inspect_kind(name, arguments),
                select=_select_names(arguments),
            )
            # Result references a read drafts are committed only at the served
            # point below (`commit_served`); the scope releases the rest.
            # A live read's resolved rows are held for the whole MCP call, so
            # a second entry (the bare-select retry) calls nothing (R4').
            with (
                result_context(context), result_references.serve_scope(),
                _live_resolver.holder_scope(),
            ):
                return await self._okg_call_and_serve(name, arguments)
        finally:
            _CALL_REWRITE.reset(token)

    def _okg_invalid_result_format(self, name: str, value: Any) -> CallToolResult:
        shown = repr(value)
        if len(shown) > 64:
            shown = shown[:64] + "..."
        payload = {
            "status": "refused",
            "operator_name": name,
            "error": {
                "code": InvalidResultFormat.code,
                "message": (
                    f"request _meta {RESULT_FORMAT_META_KEY!r} is {shown}; "
                    f"this server serves {', '.join(RESULT_FORMATS)}"
                ),
                "fix_hint": (
                    f"send _meta {RESULT_FORMAT_META_KEY!r}='json' for "
                    "compact JSON, or omit the key for text"
                ),
            },
        }
        return served_call_tool_result(
            payload, is_error=True, tool=name, result_format="text",
        )

    async def _okg_call_and_serve(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> Any:
        # A fresh image slot per call, reset however the call ends, so an
        # image queued by a call that then failed can never reach another.
        slot = _PENDING_IMAGES.set([] if name in IMAGE_CONTENT_TOOLS else None)
        try:
            try:
                result = await super().call_tool(name, arguments)
            except Exception as exc:  # noqa: BLE001 - served (or re-raised) by `_okg_serve_outcome`.
                outcome: tuple[bool, Any] = (False, exc)
                read = await self._okg_select_stored_keys(name, arguments, exc)
                if read is not None:
                    # The bare names are stored attribute keys of the rows:
                    # served as `attrs.<key>`, with a note, and the text view
                    # reads the select the rows were built with.
                    arguments, result, fresh = read
                    # Served under the new call's own rewrite state.
                    rewrite_token = _CALL_REWRITE.set(fresh)
                    try:
                        with result_context(dataclasses.replace(
                            current_result_context(), select=_select_names(arguments),
                        )):
                            return await self._okg_run_blocking(
                                self._okg_serve_outcome, name, arguments, (True, result),
                            )
                    finally:
                        _CALL_REWRITE.reset(rewrite_token)
            else:
                outcome = (True, result)
            # Serving runs off the loop too (issue #3493): it stamps the
            # logical session and reads its stale-notice state under that
            # session's lock, which another call of the same session may hold
            # across database work in its own thread.
            return await self._okg_run_blocking(
                self._okg_serve_outcome, name, arguments, outcome,
            )
        finally:
            _PENDING_IMAGES.reset(slot)

    async def _okg_select_stored_keys(
        self, name: str, arguments: dict[str, Any], exc: BaseException,
    ) -> Optional[tuple[dict[str, Any], Any, "_CallRewrite"]]:
        """The call read again with its bare `select` names as `attrs.<key>`,
        when every one is a stored attribute key of the rows it returns.

        Issue #3666 (lane RD): agents select a key they saw on an earlier
        reply's attributes (`created_at`, `path`, `label`) as if it were a
        field. Only after the tool refused the names as no field of it, on
        search, expand and inspect(node), and only for plain names with no
        other answer in the refusal. The read is kept only when each name is
        held by at least one served row: a name no row stores keeps the
        tool's own refusal, never an empty page.

        A live expand (#3666, lane RD: 5 of 269 real expand(live=...) calls
        named bare stored keys) is read in two steps, so no
        external call is ever made for a read that is then discarded: the
        new call WITHOUT `live` first, which reads only the graph rows; only
        when they store every key, the new call with `live`, once, which is
        the reply. The first call was refused in its graph step, before its
        adapter ran, so the live system is asked once in all."""
        retry = bare_names_as_attr_keys(name, arguments, exc, allow_live=True)
        if retry is None:
            return None
        retry_arguments, keys = retry
        live_call = name == "expand" and retry_arguments.get("live") not in (None, {})
        first = _CALL_REWRITE.get()
        # The new call is read by the grammar as any call is, into a state
        # of its own (review P2-1): a name the first refusal kept beside the
        # bare ones (a search `title`) is now read as its row field, and the
        # new call's `select`, arguments and spellings are what the tool body
        # reads (a path page's `next` token signs the new call's select,
        # never the first call's bare names). The first call's state is not
        # touched, so a read that is not served leaves its refusal as it was.
        capabilities = getattr(self, "_okg_capabilities", None)
        available = getattr(capabilities, "available_methods", None)
        try:
            retry_arguments, fresh = _rewrite_operator_call(
                name, retry_arguments, accepts=self._okg_takes_as_written,
                search_methods=frozenset(available()) if callable(available) else None,
                access_rules_on=self._okg_access_rules_on,
            )
        except Exception:  # noqa: BLE001 - not served this way: the first refusal stands.
            return None
        if (
            fresh is None or fresh.refusal is not None or fresh.row_keys is not None
            or not isinstance(retry_arguments, dict)
        ):
            return None
        # The reply carries what the first call was read as, the new call's
        # own records, and the bare names read as keys, in the order the
        # reply always listed them. Noted before the read, so the reply the
        # read tool stamps (and the byte budget it reserves for its notes)
        # carries them.
        first_body = [dict(r) for r in first.body_notes] if first is not None else []
        first_adjusted = tuple(first.adjusted) if first is not None else ()
        noted = [*first_body, *(dict(r) for r in first_adjusted)]
        fresh = dataclasses.replace(
            fresh,
            adjusted=first_adjusted,
            body_notes=[
                *first_body,
                *(dict(r) for r in fresh.adjusted if dict(r) not in noted),
                *(
                    {
                        "rule": "select_attr_key",
                        "sent": f"select `{key}`",
                        "served": f"`attrs.{key}`, an attribute the rows store",
                    }
                    for key in keys
                ),
            ],
        )
        if live_call:
            # The graph rows alone, under a copy of the new call's state, so
            # nothing the probe's body notes reaches the served reply.
            probe_arguments = {k: v for k, v in retry_arguments.items() if k != "live"}
            probe_state = dataclasses.replace(
                fresh, body_notes=[dict(r) for r in fresh.body_notes],
            )
            token = _CALL_REWRITE.set(probe_state)
            try:
                probe = await super().call_tool(name, probe_arguments)
            except Exception:  # noqa: BLE001 - not served this way: the first refusal stands.
                return None
            finally:
                _CALL_REWRITE.reset(token)
            if not isinstance(probe, dict) or not rows_store_keys(name, probe, keys):
                return None
        token = _CALL_REWRITE.set(fresh)
        try:
            result = await super().call_tool(name, retry_arguments)
        except Exception:  # noqa: BLE001 - not served this way: the first refusal stands.
            return None
        finally:
            _CALL_REWRITE.reset(token)
        if not isinstance(result, dict):
            return None
        if not live_call and not rows_store_keys(name, result, keys):
            return None
        # A live read is served once made: its keys were read on the graph
        # rows above, and discarding it would discard an external call.
        return retry_arguments, result, fresh

    def _okg_serve_outcome(
        self,
        name: str,
        arguments: dict[str, Any],
        outcome: tuple[bool, Any],
    ) -> Any:
        """Serve a finished call: its result, or its raised error, bounded."""

        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        succeeded, value = outcome
        try:
            if not succeeded:
                raise value
            result = value
            served = self._okg_serve_result(name, arguments, result)
            # Inside the try: a capacity refusal at commit is served as a
            # typed refusal like any other raised error.
            committed = result_references.commit_served(served)
            # A grown live set replaces the one it grew from only now that
            # it is stored (#3666, N6).
            _live_resolver.after_commit(committed)
            pending = _PENDING_IMAGES.get()
            # A returned refusal is served with isError set
            # (`_okg_serve_result`), so this is exactly a successful call.
            # Images go on after the commit, which reads only text blocks.
            if pending and isinstance(served, CallToolResult) and not served.isError:
                served = _with_images(served, pending)
            return served
        except Exception as exc:  # noqa: BLE001 - public errors must obey the byte boundary.
            if isinstance(exc, SchemaBehindReadOnly):
                # A rendered refusal, not the SDK's bare error text, so the
                # caller reads the code, the skew and `schema_behind_by` the
                # same way it reads them on every read response.
                payload = _expected_error_refusal(
                    name, exc, budget=MIN_RESPONSE_BYTES,
                )
                payload["schema_behind_by"] = exc.data["schema_behind_by"]
                payload = _fit_error_payload(payload, budget=MIN_RESPONSE_BYTES)
                return self._okg_serve_payload(
                    name, payload, is_error=True, raised=True,
                )
            if name not in OPERATOR_TOOLS:
                # A write or admin tool's raised exception stays the SDK's
                # own single error text block (`str(exc)`), as before.
                raise
            budget = self._okg_call_budget(name, arguments)
            # FastMCP's Tool.run wraps every tool exception in ToolError while
            # preserving the original as ``__cause__``.  Unwrap exactly that
            # causal slot; never dispatch on reflected ToolError text.
            typed_error = exc if isinstance(exc, SubstrateError) else exc.__cause__
            rewrite = _CALL_REWRITE.get()
            if (
                isinstance(typed_error, SubstrateError)
                and rewrite is not None and rewrite.select_retry is not None
                and getattr(typed_error, "code", None) == "select_unknown_field"
                and getattr(typed_error, "retry_call", None) is None
            ):
                # The tool's own refusal, and beside it the call with the
                # `select` the caller's input determines (step 3a). The
                # refusal's own `retry_select` and hint are made to say the
                # same thing: one answer, not two.
                from okg.substrate.mcp.envelope import align_select_retry

                typed_error.retry_call = rewrite.select_retry
                align_select_retry(typed_error, rewrite.select_retry)
            if isinstance(typed_error, SubstrateError):
                payload = _expected_error_refusal(
                    name,
                    typed_error,
                    budget=budget,
                )
            else:
                # An untyped exception is a server fault the caller cannot
                # act on. Log it with its traceback under an id the refusal
                # also carries, so the two can be matched (issue #2793). The
                # handler FastMCP installs writes to stderr, never to stdout,
                # which is the stdio MCP stream.
                error_id = uuid.uuid4().hex[:12]
                _log.error(
                    "operator %s raised an untyped exception, refused as "
                    "operator_execution_failed error_id=%s",
                    name,
                    error_id,
                    exc_info=exc,
                    extra={"okg_error_id": error_id, "okg_operator": name},
                )
                payload = _execution_refusal(
                    name, budget=budget, error_id=error_id
                )

            # The OKG server installs this closure so raised public errors carry
            # the same logical-session contract as returned refusals. Generic
            # ByteBoundedFastMCP users remain unchanged.
            stamper = getattr(self, "_okg_logical_session_stamper", None)
            if callable(stamper):
                try:
                    context = self.get_context()
                except (LookupError, RuntimeError, ValueError):
                    context = None
                payload = stamper(
                    payload,
                    context=context,
                    requested_generation=arguments.get("generation_id"),
                )
            adjusted = call_adjusted()
            if adjusted:
                # A refusal of a rewritten call says what it was read as.
                payload["adjusted"] = adjusted
            payload = _fit_error_payload(payload, budget=budget)
            return self._okg_serve_payload(
                name, payload, is_error=True, raised=True,
            )

    def _okg_call_budget(self, name: str, arguments: Mapping[str, Any]) -> int:
        """The budget the byte-bound wrapper resolves for this call.

        The deployment's default and ceiling apply, so a caller that omitted
        `max_bytes` on a 16 KiB deployment gets a 16 KiB-bounded error; an
        invalid `max_bytes` gets the 4 KiB minimum.
        """

        budget, _invalid = resolve_budget(
            name,
            arguments,
            deployment_default_bytes=self.response_default_bytes,
            deployment_ceiling_bytes=self.response_ceiling_bytes,
        )
        return MIN_RESPONSE_BYTES if budget is None else budget

    def _okg_serve_result(
        self,
        name: str,
        arguments: Mapping[str, Any],
        result: Any,
    ) -> Any:
        """Build the served result from a tool's raw return value."""

        from okg.substrate.mcp.operators import OPERATOR_TOOLS

        if isinstance(result, pydantic.BaseModel) and not isinstance(result, CallToolResult):
            # A tool that returns its result model rather than a dictionary:
            # served as the model's own JSON dump, as the SDK would have.
            result = result.model_dump(mode="json", by_alias=True)
        if isinstance(result, CallToolResult):
            if name in OPERATOR_TOOLS and name != "run":
                # `run` (#3666) serves its steps' own served results, joined.
                raise ToolResultShapeChanged(
                    f"{name} returned a prebuilt CallToolResult where the raw "
                    "operator dictionary was expected",
                    data={"tool": name, "result_type": "CallToolResult"},
                    fix_hint=(
                        "check response_bounds._RawResultFastMCP against the "
                        "pinned SDK's ToolManager.call_tool"
                    ),
                )
            return result
        if not isinstance(result, dict):
            if name in OPERATOR_TOOLS:
                # Every read operator returns a dictionary. Anything else
                # means the SDK's dispatch changed under us; passing it on
                # would serve a form nothing here measured.
                raise ToolResultShapeChanged(
                    f"{name} returned {type(result).__name__} where the raw "
                    "operator dictionary was expected",
                    data={"tool": name, "result_type": type(result).__name__},
                    fix_hint=(
                        "check response_bounds._RawResultFastMCP against the "
                        "pinned SDK's ToolManager.call_tool"
                    ),
                )
            # A non-dictionary result (text, an image, a content list) keeps
            # the SDK's own conversion.
            tool = self._tool_manager.get_tool(name)
            if tool is None:
                return result
            return tool.fn_metadata.convert_result(result)
        payload = result
        # A step of `run` (#3666): its raw reply, for the steps after it.
        _note_run_payload(payload)
        tool = self._tool_manager.get_tool(name)
        metadata = getattr(tool, "fn_metadata", None)
        output_model = getattr(metadata, "output_model", None)
        if output_model is not None:
            # A tool registered with its own output model (okg's own
            # registrations declare none) keeps the SDK's output check: an
            # invalid result is an error, never served.
            output_model.model_validate(
                {"result": payload} if metadata.wrap_output else payload
            )
        if name in OPERATOR_TOOLS:
            from okg.substrate.mcp.operator_models import OperatorResult

            # A server-side check of the envelope. It validates but serves
            # the operator's own dictionary, so no coercion rewrites it.
            OperatorResult.model_validate(payload)
            if payload.get("status") == "refused":
                # A returned refusal is fitted in its served error form, the
                # same way a raised one is.
                payload = _fit_error_payload(
                    payload, budget=self._okg_call_budget(name, arguments),
                )
        return self._okg_serve_payload(
            name, payload, is_error=payload.get("status") == "refused",
        )

    def _okg_serve_payload(
        self,
        name: str,
        payload: dict[str, Any],
        *,
        is_error: bool,
        raised: bool = False,
    ) -> CallToolResult:
        """Serve one payload, with the session's stale-notice state if any.

        A call that names its own ``generation_id`` (``pin_scope``
        ``explicit_call``) still belongs to its MCP session, so it shares the
        once-per-session notice state too (PACT read-model-s2-handle; a
        benchmark that pins every call saw the stale banner on every call).
        A call with no stable session identity gets no state from
        ``_okg_notice_session`` and prints the notice every time.
        """

        if is_error and getattr(self, "_okg_lean_hidden_lines", None) is not None:
            _publish_retry_arguments(name, payload)
        notice_session = getattr(self, "_okg_notice_session", None)
        if (
            callable(notice_session)
            and isinstance(payload, dict)
            and payload.get("pin_scope") in ("transport_session", "explicit_call")
            and current_result_context().format == "text"
        ):
            try:
                context = self.get_context()
            except (LookupError, RuntimeError, ValueError):
                context = None
            with notice_session(context) as state:
                return served_call_tool_result(
                    payload,
                    is_error=is_error,
                    tool=name,
                    raised=raised,
                    session_state=state,
                )
        return served_call_tool_result(
            payload, is_error=is_error, tool=name, raised=raised,
        )


#: Hidden arguments a search retry call drops (PACT read-passage-rows-3666,
#: gate 1). Only a size control, whose meaning a retry does not lose: the
#: reply is fitted to the default budget instead, keeping every row (D4).
#: An argument that carries the call's scope or conditions is never dropped.
_SEARCH_RETRY_DROPPED = ("max_bytes",)


def _publish_retry_arguments(name: str, payload: Any) -> None:
    """Take `max_bytes` out of a search refusal's `retry_call`: search does
    not publish it, and a hint names published arguments only. The caller's
    own value was echoed back: a search with `max_bytes=12000` whose
    `select` was refused got `retry: search(..., max_bytes=12000)`. It
    stays accepted. Other tools, and a continuation retry (mode
    `continue`, its token as sent), are left alone."""
    if name != "search":
        return
    error = payload.get("error") if isinstance(payload, dict) else None
    call = error.get("retry_call") if isinstance(error, dict) else None
    if not isinstance(call, dict) or call.get("mode") == "continue":
        return
    arguments = call.get("arguments")
    if not isinstance(arguments, dict) or not set(arguments) & set(_SEARCH_RETRY_DROPPED):
        return
    error["retry_call"] = {
        **call,
        "arguments": {
            k: v for k, v in arguments.items() if k not in _SEARCH_RETRY_DROPPED
        },
    }


def bound_read_tool(
    operator_name: str,
    fn: Callable[..., dict[str, Any]],
    *,
    measure_response: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    | None = None,
    deployment_default_bytes: int | None = None,
    deployment_ceiling_bytes: int | None = None,
    live_step: LiveStep | None = None,
    live_page: LivePage | None = None,
) -> Callable[..., dict[str, Any]]:
    """Wrap one synchronous read tool with its public response byte contract.

    ``live_page`` (``expand`` only, #3666 Live build 3) serves
    ``expand(next=<token>)`` when the token is a cut live answer's: the page
    is built from the kept rows and fitted to the budget here, never by
    rerunning the operator, and no adapter is called. ``max_bytes`` beside
    such a token sizes the page.

    Pageable responses are re-executed with smaller native limits at the first
    call's generation pin; the first page only predicts the limit to rerun at
    (see ``_fit_to_budget``).  The wrapper never serves a slice of a completed
    page because operators such as Nomos-filtered ``expand`` advance by raw
    positions rather than by the number of visible rows.

    ``deployment_default_bytes`` and ``deployment_ceiling_bytes`` come from
    ``agent_surface.response_bytes`` (PACT ``effective-surface-declaration``).

    The budget is resolved once per call by ``resolve_budget``: the caller's
    ``max_bytes`` when it is a non-null value, else the deployment default.
    The signature declares ``max_bytes: Optional[int] = None`` and publishes
    no default, so the published schema -- and every startup byte baseline --
    is the same whichever deployment is loaded, and an omitted argument is
    distinguishable from an explicit one even though the MCP SDK passes
    every argument, defaults included (issue #2156). The resolved budget is
    written back into the operator's ``max_bytes`` so nothing downstream
    recomputes it.

    ``live_step`` is given for ``expand`` only. When the call carries
    ``live``, the wrapper runs two ordered steps (issue #2141): the graph
    step, with ``live`` removed, paged to fit the budget minus the reserve
    the live step declares before its adapter runs; then the live step,
    exactly once, into the room the graph left. Nothing after the live step
    re-runs anything.
    """

    signature = inspect.signature(fn)
    # Each run of the operator is one attempt of the call's result-reference
    # draft scope (issue #3222): see `wrapped` below.
    attempted = result_references.attempt(fn)

    def bounded(*args: Any, **kwargs: Any) -> dict[str, Any]:
        bound = signature.bind(*args, **kwargs)
        bound.apply_defaults()
        # Two views of the same call, on purpose.
        #
        # `call_arguments` is what the operator gets: the values the SDK
        # bound, untouched except for `max_bytes` (below). `arguments` is a
        # normalized copy for INSPECTION only, because on a real MCP call the
        # SDK hands the tool VALIDATED MODELS rather than dicts — `input`
        # arrives as an `AggregateScope` or a `NodeAnchor` — and everything
        # below reads these with `.get(...)` and `isinstance(..., dict)`.
        # Without the copy, `_is_pageable` silently answered False for every
        # typed aggregate, and a set aggregate over budget came back as a
        # flat refusal claiming "no safe continuation cursor" for a shape
        # that had one.
        #
        # `ctx` is excluded from the copy AND the retry is built from the
        # untouched values, because `as_mapping` would flatten a live
        # FastMCP `Context` to `{}`. No operator reads `ctx` today (they go
        # through `_CURRENT_MCP_CONTEXT`), so this is inert — but the first
        # one that did would fail only on the over-budget retry path, which
        # is the hardest possible place to notice.
        call_arguments = dict(bound.arguments)
        arguments = {
            name: value if name == "ctx" else as_mapping(value)
            for name, value in call_arguments.items()
        }
        # The public search default is None so explicit10 remains distinguishable
        # from a list's omitted100. Resolve once before measuring or rerunning;
        # explicit invalid values are still validated by the producer.
        if operator_name == "search" and arguments.get("limit") is None:
            from okg.substrate.mcp.search_arguments import continuation_kind
            from okg.substrate.mcp.search_views import (
                default_search_limit,
                implied_search_view,
            )

            # PACT read-names-list-3666: the view the tool body will serve
            # (a grep call naming no view is the names list), on the method
            # it will run (a grep page's token sent without one is grep; an
            # explicit other method with such a token is refused by the body,
            # so the limit resolved here is never served).
            search_method = arguments.get("method") or "lexical"
            if (
                search_method == "lexical"
                and arguments.get("continuation_token") is not None
                and continuation_kind(arguments["continuation_token"]) == "grep_page"
            ):
                search_method = "grep"
            search_view = implied_search_view(
                arguments.get("view"), method=search_method,
                within=arguments.get("within"), arguments=arguments,
            )
            limit = default_search_limit(search_view, search_method)
            arguments["limit"] = limit
            call_arguments["limit"] = limit
            bound.arguments["limit"] = limit
        # Recover a signed subtype page budget BEFORE the byte wrapper sizes
        # the response. Defaults injected below must not masquerade as a
        # caller's budget; explicit numeric overrides change the signed read.
        # `restored_from`: the token whose signed budget this call resumes
        # under, when the caller passed none. A restored budget the current
        # ceiling refuses gets a retry the caller can actually make.
        restored_from: Any = None
        live_next = (
            live_page is not None and operator_name == "expand"
            and _is_live_next(arguments.get("next"))
        )
        if (
            operator_name in {"inspect", "expand"} and arguments.get("next") is not None
            and not live_next
        ):
            given = arguments.get("max_bytes")
            if given is not None:
                # `nextbytes A` (#3605): the token's own page size changes
                # nothing. `nextresize A` (#3634): another valid size goes on
                # from the same place at that size, the token re-signed with
                # it. Both are then read as `next` alone. expand, a token
                # that signs no size, and an invalid size are refused.
                signed = (
                    _signed_next_budget(operator_name, arguments["next"])
                    if operator_name == "inspect" else None
                )
                if signed is None:
                    # The token first (lane ri, tokens): a token that does
                    # not open is refused by its own reason, never as an
                    # argument beside `next`, so the retry asked for is not
                    # refused again. The server already checked it
                    # (`_continuation_intake`); a direct call is checked here.
                    from okg.substrate.mcp.continuation import route_next

                    route_next(arguments["next"], tool=operator_name)
                if signed is None or not _valid_page_size(given, deployment_ceiling_bytes):
                    return _next_budget_refusal(
                        operator_name, arguments["next"], given=given,
                        ceiling=deployment_ceiling_bytes,
                    )
                if given != signed:
                    resized = _resized_next(arguments["next"], given)
                    arguments["next"] = resized
                    call_arguments["next"] = resized
                    bound.arguments["next"] = resized
                arguments["max_bytes"] = None
                call_arguments["max_bytes"] = None
                bound.arguments["max_bytes"] = None
            if operator_name == "inspect" and arguments.get("target") is None and arguments.get("select") is None:
                from okg.substrate.mcp.continuation import route_next

                binding = route_next(arguments["next"], tool=operator_name)
                projection = binding.get("projection")
                if binding.get("kind") in _SIGNED_BUDGET_KINDS and isinstance(projection, dict):
                    arguments["max_bytes"] = projection.get("max_bytes")
                    restored_from = arguments["next"]
            if operator_name == "expand":
                # A path page signs the budget it was served under; `next`
                # resumes under it (a token from before that has none, and
                # its rebuild is refused by the operator).
                arguments["max_bytes"] = _signed_path_budget(arguments["next"])
                restored_from = arguments["next"]
        if (
            operator_name == "expand"
            and arguments.get("next") is None
            and arguments.get("max_bytes") is None
            and arguments.get("continuation_token")
        ):
            # The legacy spelling resumes under the signed budget too when
            # max_bytes is omitted. A max_bytes it passes overrides the
            # signed one: the operator compares the token's own budget and
            # signs the new one into the next token.
            arguments["max_bytes"] = _signed_path_budget(arguments["continuation_token"])
            restored_from = arguments["continuation_token"]
        if operator_name == "inspect" and arguments.get("next") is None and arguments.get("max_bytes") is None:
            target = arguments.get("target")
            if isinstance(target, dict) and target.get("kind") in {"text", "subtype"} and target.get("continuation_token"):
                from okg.substrate.mcp.continuation import route_next

                binding = route_next(target["continuation_token"], tool="inspect")
                projection = binding.get("projection")
                expected_kind = f"inspect_{target['kind']}"
                if binding.get("kind") == expected_kind and isinstance(projection, dict):
                    arguments["max_bytes"] = projection.get("max_bytes")
                    restored_from = target["continuation_token"]
        raised = _raise_small_budget(arguments, restored_from=restored_from)
        _MAX_BYTES_RAISED.set(raised)
        budget, invalid = resolve_budget(
            operator_name,
            arguments,
            deployment_default_bytes=deployment_default_bytes,
            deployment_ceiling_bytes=deployment_ceiling_bytes,
        )
        if invalid is not None:
            if restored_from is not None and arguments.get("max_bytes") is not None:
                return _restored_budget_refusal(
                    invalid,
                    restored_from,
                    restored=arguments["max_bytes"],
                    ceiling=deployment_ceiling_bytes,
                )
            return invalid
        assert budget is not None
        # The operator receives the resolved budget, never the raw argument,
        # so anything inside it that sizes work against `max_bytes` (inspect
        # subtype paging, the live envelope) uses this same number.
        if "max_bytes" in signature.parameters:
            bound.arguments["max_bytes"] = budget
            call_arguments["max_bytes"] = budget
        # The budget a call gets without max_bytes. A size refusal never
        # offers more than this (R9, PACT read-passage-rows-3666).
        default_budget = min(
            deployment_default_bytes
            if deployment_default_bytes is not None else DEFAULT_RESPONSE_BYTES,
            deployment_ceiling_bytes
            if deployment_ceiling_bytes is not None else MAX_RESPONSE_BYTES,
        )

        # Fitted with the note the served page will carry; a read tool
        # called from inside another stamps none.
        noted = _READ_TOOL_DEPTH.get() <= 1

        def response_bytes(payload: dict[str, Any]) -> int:
            if raised is not None:
                # Fitted with the disclosure the served page will carry.
                payload = {**payload, "max_bytes_raised": dict(raised)}
            # Read when measured: the tool body may add a record on its
            # first run (step 2b), and that record is counted too.
            adjusted = call_adjusted() if noted else []
            if adjusted:
                payload = {**payload, "adjusted": adjusted}
            state = _CALL_REWRITE.get() if noted else None
            if (
                state is not None and state.matched_loosely
                and payload.get("status") != "refused"
            ):
                # loose-match-3666: fitted with the field the reply carries.
                payload = {**payload, "matched_loosely": list(state.matched_loosely)}
            measured = (
                measure_response(dict(payload), arguments)
                if measure_response is not None
                else payload
            )
            return public_call_tool_result_bytes(measured, tool=operator_name)

        if live_next:
            # The next page of a cut live answer: built from its kept rows
            # and fitted here, against the bytes this call serves.
            assert live_page is not None
            return live_page(call_arguments, response_bytes, budget)

        # `expand(live=...)`: the live call is taken OUT of the operator
        # call before anything runs (issue #2141). The paging search below
        # re-runs the operator, and with `live` still set every re-run was
        # another outside call and another sample. The graph step runs with
        # `live=None`, fitted to the budget minus the reserve; the live step
        # runs once, afterwards, into whatever room the graph left.
        prepared: PreparedLiveStep | None = None
        if live_step is not None and call_arguments.get("live") is not None:
            prepared = live_step(call_arguments)
            from okg.substrate.mcp.run_steps import keep_live_prepared

            if keep_live_prepared(prepared):
                # A `run` live step (#3666, lane ri): this call checked the
                # caller, the surface and the arguments and resolved the
                # relation; the run makes the upstream calls itself, off
                # the call workers. No graph step, no adapter here.
                return {
                    "status": "ok",
                    "operator_name": operator_name,
                    "run_live_prepared": True,
                    "pinned_generation_id": prepared.generation_id,
                }
            bound.arguments["live"] = None
            call_arguments["live"] = None
            if (
                prepared.generation_id is not None
                and "generation_id" in signature.parameters
            ):
                bound.arguments["generation_id"] = prepared.generation_id
                call_arguments["generation_id"] = prepared.generation_id

        def graph_bytes(payload: dict[str, Any]) -> int:
            if prepared is None:
                return response_bytes(payload)
            return response_bytes({
                **payload,
                "external_live": prepared.reserve_envelope(payload),
            })

        # The reruns below change only `limit`, so a read whose statement
        # does not depend on it runs once per call (PACT
        # mcp-filtered-passage-cost; `call_read_cache`).
        with call_read_cache(), date_call():
            graph = _fit_to_budget(
                operator_name,
                attempted,
                bound=bound,
                signature=signature,
                call_arguments=call_arguments,
                arguments=arguments,
                budget=budget,
                measure=graph_bytes,
                ceiling=deployment_ceiling_bytes,
                default_budget=default_budget,
            )
        adjusted = call_adjusted() if noted else []
        if (
            adjusted
            and isinstance(graph, dict)
            and graph.get("status") == "refused"
            and isinstance(graph.get("error"), dict)
            and graph["error"].get("code") == "response_byte_budget_exceeded"
        ):
            # The note is part of what did not fit. Say so, with its size:
            # the same call in its corrected spelling carries no note and
            # needs that much less.
            note_bytes = adjusted_reserve_bytes()
            graph["error"]["adjusted_note_bytes"] = note_bytes
            graph["error"]["message"] = (
                f"{graph['error'].get('message') or ''}; up to {note_bytes} of "
                "those bytes are the `adjusted` note, which the same call "
                "with its arguments where the note says they belong does not carry"
            )
        resumed = arguments.get("next") or arguments.get("continuation_token")
        if (
            operator_name == "expand"
            and resumed
            and isinstance(graph, dict)
            and graph.get("status") == "refused"
            and isinstance(graph.get("error"), dict)
            and graph["error"].get("code") == "response_byte_budget_exceeded"
        ):
            # A path continuation that does not fit: narrowing `select`
            # would change the read the token is bound to, and `next`
            # refuses `max_bytes`. Name the retries that are accepted.
            _point_to_budget_retry(
                graph["error"],
                resumed,
                budget=budget,
                ceiling=deployment_ceiling_bytes,
                default_budget=default_budget,
            )
        if (
            prepared is None
            and isinstance(graph, dict)
            and graph.get("status") == "refused"
            and isinstance(graph.get("error"), dict)
            and graph["error"].get("code") == "response_byte_budget_exceeded"
            and graph["error"].get("retry_call") is None
            and not graph.get("payload_truncated")
        ):
            # #3666 step 3b: a size refusal that stands carries the same
            # call at a budget it fits, when that budget is within the
            # default (R9); past it, a narrower call the server serves.
            # Not a `query` page cut at the tool's own cap: no budget serves
            # it whole, so `_query_budget_retry` offered none on purpose.
            retry = _oversize_retry_call(
                operator_name, arguments, signature,
                needed=graph["error"].get("needed_bytes"),
                ceiling=deployment_ceiling_bytes,
                default_budget=default_budget,
            )
            if retry is not None:
                graph["error"]["retry_call"] = retry
                if response_bytes(graph) > budget:
                    # Never at the cost of the budget: the words stay.
                    del graph["error"]["retry_call"]
        if prepared is None:
            return graph
        if graph.get("status") == "refused":
            # Not even one graph row plus the reserve fits, or the graph
            # step refused for its own reason. Refused BEFORE the adapter:
            # the call is recorded, the outside system is never contacted.
            error = graph.get("error") if isinstance(graph.get("error"), dict) else {}
            code = str(error.get("code") or "graph_step_refused")
            prepared.refuse(code)
            if code == "response_byte_budget_exceeded":
                # Say what did not fit. The generic text blames "one result
                # row plus the envelope"; here the live relation's reserve is
                # part of what was charged, and the caller can act on that.
                reserve_bytes = len(pydantic_core.to_json(
                    prepared.reserve_envelope(graph), fallback=str,
                ))
                if _is_pageable(operator_name, arguments):
                    error["message"] = (
                        f"one graph row plus the {reserve_bytes}-byte envelope "
                        "reserved for the live relation exceeds the "
                        f"{budget}-byte MCP response budget; the live call "
                        "was not made"
                    )
                    error["remedy"] = (
                        "request a larger valid max_bytes, a narrower "
                        "select/projection, or call without `live` if the "
                        "graph rows alone are what you need"
                    )
                else:
                    # `path` or `depth>1`: the graph result is not paged, so
                    # "one graph row" would be false. The whole result plus
                    # the reserve did not fit.
                    error["message"] = (
                        f"the {operator_name} result plus the "
                        f"{reserve_bytes}-byte envelope reserved for the live "
                        f"relation exceeds the {budget}-byte MCP response "
                        "budget and this result shape has no safe "
                        "continuation cursor; the live call was not made"
                    )
                    error["remedy"] = (
                        "narrow the request (select/projection/filter), "
                        "request a larger valid max_bytes, or call without "
                        "`live` if the graph rows alone are what you need"
                    )
                error["reserve_bytes"] = reserve_bytes
            return graph

        room = ResponseRoom(
            budget_bytes=budget,
            served_bytes=lambda envelope: response_bytes(
                {**graph, "external_live": envelope},
            ),
        )
        graph["external_live"] = prepared.run(graph, room)
        if response_bytes(graph) > budget:
            # Unreachable by construction: the dispatcher's final check
            # measured this same envelope with `room.served_bytes`, which is
            # this same `response_bytes` over this same graph payload, and
            # fell back to its bounded error form if it did not fit. Only a
            # measure that answers differently twice for one input reaches
            # here. If it does, the answer still stays inside the budget --
            # a fixed-size refusal, never the over-budget envelope -- and
            # the sample, already written, recorded the envelope the
            # dispatcher checked, so the discrepancy is logged loudly.
            _log.error(
                "expand(live=...): served result over the %s-byte budget "
                "after the live step; the sample records the envelope the "
                "dispatcher checked, the caller receives "
                "response_bound_failed",
                budget,
            )
            return _refusal(
                operator_name,
                code="response_bound_failed",
                message=(
                    f"the live envelope did not fit the {budget}-byte "
                    "response budget after the live call ran"
                ),
                max_bytes=budget,
                remedy="report this; retry with a larger valid max_bytes",
            )
        return graph

    @functools.wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> dict[str, Any]:
        # A result reference a producer drafts goes on only when the payload
        # this call finally returns carries its id and is not a refusal:
        # inside an MCP call it is handed up and committed at the served
        # point (`ByteBoundedFastMCP._okg_call_and_serve`); for a direct
        # Python caller it is committed here. Every byte-fitting rerun's
        # draft, a refusal and a raised error release their drafts, so only
        # the served reference counts against the store's capacity. Calls
        # that never draft are unchanged. See `result_references.DraftScope`.
        token = _MAX_BYTES_RAISED.set(None)
        outermost = _READ_TOOL_DEPTH.get() == 0
        depth = _READ_TOOL_DEPTH.set(_READ_TOOL_DEPTH.get() + 1)
        continued = None
        direct = None
        if outermost and _CALL_REWRITE.get() is None:
            # A direct Python call: no rewrite ran, but the tool body may
            # still note what it read the call as (step 2b).
            direct = _CALL_REWRITE.set(_CallRewrite())
        #: The call's own records, without a continued page's select note.
        own_adjusted: tuple[dict[str, str], ...] = ()
        if operator_name == "expand" and outermost:
            # A path page continued by `next` carries the `select` note its
            # first page carried, counted in the budget like any other.
            resumed = signature.bind_partial(*args, **kwargs).arguments.get("next")
            notes = _next_select_notes(resumed) if resumed is not None else ()
            if notes:
                state = _CALL_REWRITE.get() or _CallRewrite()
                own_adjusted = (*state.body_notes, *state.adjusted)
                continued = _CALL_REWRITE.set(dataclasses.replace(
                    state, adjusted=(*state.adjusted, *notes),
                ))
        loosely = None
        if outermost:
            # loose-match-3666: what each like test compared, for the reply
            # and for the bytes a read that cuts its own page reserves.
            loose = _matched_loosely(operator_name, signature, args, kwargs)
            if loose:
                state = _CALL_REWRITE.get() or _CallRewrite()
                loosely = _CALL_REWRITE.set(dataclasses.replace(
                    state, matched_loosely=tuple(loose),
                ))
        try:
            with result_references.draft_scope() as drafts:
                # A live read resolved at the asynchronous layer (#3666,
                # live-graph-rucio-sites, B4'): its rows seed this scope's
                # memo, which the kept-set reader checks first, so the body
                # and every byte-fitting rerun read them with no upstream
                # call. The keep rule (D8) mints the kept set here, in this
                # scope (`live_resolver.seed_call_memo`).
                # Asked first, so a stored read binds no arguments for it.
                if outermost and _live_resolver.has_handoff(operator_name):
                    _live_resolver.seed_call_memo(
                        operator_name,
                        signature.bind_partial(*args, **kwargs).arguments,
                    )
                result = bounded(*args, **kwargs)
                if outermost:
                    # A reply the byte budget cut is kept now (R5) or
                    # refused at capacity (N7); the reply starts with the
                    # live line (spec 5).
                    result = _live_resolver.finish_reply(operator_name, result)
                raised = _MAX_BYTES_RAISED.get()
                if raised is not None and isinstance(result, dict):
                    # On a refusal too, so a budget the caller did not ask
                    # for (4096 for their 1000) is explained.
                    result["max_bytes_raised"] = dict(raised)
                rewrite = _CALL_REWRITE.get() if outermost else None
                if rewrite is not None and rewrite.row_keys is not None:
                    # A view with fixed rows, served with a `select` (step
                    # 3a): the page was built and fitted exactly as without
                    # it; each row is cut to the selected keys only now.
                    from okg.substrate.mcp.select_vocabulary import project_rows

                    result = project_rows(result, rewrite.row_keys)
                adjusted = call_adjusted() if outermost else []
                if (
                    continued is not None and isinstance(result, dict)
                    and result.get("status") == "refused"
                    and "adjusted_note_bytes" not in (result.get("error") or {})
                ):
                    # A refused `next` call was refused for its own reason,
                    # not for the first page's select: it carries the note
                    # only when the note's bytes are what did not fit.
                    adjusted = [dict(record) for record in own_adjusted]
                if adjusted and isinstance(result, dict):
                    # Issue #3666: the call was served through a rewrite
                    # that is not a plain accepted spelling.
                    result["adjusted"] = adjusted
                state = _CALL_REWRITE.get() if outermost else None
                if (
                    state is not None and state.matched_loosely
                    and isinstance(result, dict) and result.get("status") != "refused"
                ):
                    # loose-match-3666: a substring test says what it compared.
                    result["matched_loosely"] = [dict(r) for r in state.matched_loosely]
                drafts.finish(result)
                return result
        finally:
            if loosely is not None:
                _CALL_REWRITE.reset(loosely)
            if continued is not None:
                _CALL_REWRITE.reset(continued)
            if direct is not None:
                _CALL_REWRITE.reset(direct)
            _READ_TOOL_DEPTH.reset(depth)
            _MAX_BYTES_RAISED.reset(token)

    return wrapped


#: The row list each pageable operator returns. Read ONLY to predict the first
#: rerun's limit; no served page is ever cut from these lists.
#: `search(method="identifier_mentions")` returns one list under both
#: `results` and `mentions`, so both are trimmed for the estimate.
_PAGE_ROW_KEYS: dict[str, tuple[str, ...]] = {
    "search": ("results", "mentions"),
    "filter": ("items",),
    "map": ("items",),
    "expand": ("neighbors",),
    "aggregate": ("groups",),
}


def _stamp_byte_truncation(
    page: dict[str, Any],
    *,
    budget: int,
    requested_limit: int,
    effective_limit: int,
) -> dict[str, Any]:
    page["truncated"] = True
    page["truncation_reason"] = BYTE_TRUNCATION_REASON
    page["response_byte_budget"] = budget
    page["requested_limit"] = requested_limit
    page["effective_limit"] = effective_limit
    return page


def _truncation_remedy(operator_name: str, arguments: Mapping[str, Any]) -> Optional[str]:
    """Why a byte-cut search page holds so few rows, when the cause is the
    caller's own `select` (issue #3467). An explicit `select` naming `attrs`
    serves every attribute string whole (previews apply only to the default
    projection, by contract), so a few long ticket texts fill the budget.
    Said on the cut page; the rows themselves are unchanged."""
    select = arguments.get("select")
    if operator_name != "search" or not isinstance(select, (list, tuple)):
        return None
    if "attrs" not in select:
        return None
    return (
        "select names attrs, so every attribute string is served whole; "
        "drop attrs from select for previewed cards and more rows, or page "
        "with offset"
    )


def _visible_rows(operator_name: str, page: dict[str, Any]) -> int | None:
    """How many rows `page` shows, read from the operator's row list."""
    lists = [
        page[key] for key in _PAGE_ROW_KEYS.get(operator_name, ())
        if isinstance(page.get(key), list)
    ]
    return max(len(rows) for rows in lists) if lists else None


@dataclass(frozen=True)
class _Prediction:
    #: Visible rows of the first page whose prefix fits the budget.
    rows: int
    #: The native limit to rerun at: `rows` scaled from visible rows to the
    #: raw positions the first page consumed to show them.
    limit: int


def _predict_fitting_limit(
    operator_name: str,
    initial: dict[str, Any],
    *,
    offset: int,
    ceiling: int,
    budget: int,
    requested_limit: int,
    measure: Callable[[dict[str, Any]], int],
) -> _Prediction | None:
    """The largest prefix of the first page's rows that fits `budget`.

    A prediction, never a page: each prefix is measured and discarded, and the
    caller reruns the operator at the predicted limit. A prefix keeps every
    page-level field of the first page at full size (summary blocks, maps
    keyed by row), so it usually weighs more than a rerun at that limit, and
    the prediction errs short rather than long.

    Two corrections make the prefix look like the rerun it predicts:

    * Raw positions. Nomos-filtered `expand`, `filter` and `map` advance
      their cursor by the RAW positions a page consumed, hidden rows
      included, so a limit counts raw positions, not visible rows. The
      visible-row count is scaled by the first page's consumption
      (`next_offset - offset` over its visible rows). That ratio is an
      average; where hidden rows cluster, the caller's upward search
      corrects it.
    * The cursor. A rerun below the requested limit always has a next page,
      so it carries `next_offset` and, from the operators that stamp one, the
      pagination block. The first page may be the LAST page and carry
      neither; every prefix is given a cursor, and the pagination block is
      rendered when the first page had one or had no cursor at all.

    Returns `None` when the first page carries no row list this module
    knows. `rows == 0` when not even one prefix row fits.
    """
    keys = [
        key for key in _PAGE_ROW_KEYS.get(operator_name, ())
        if isinstance(initial.get(key), list)
    ]
    if not keys:
        return None
    row_count = max(len(initial[key]) for key in keys)
    next_offset = initial.get("next_offset")
    consumed = (
        next_offset - offset
        if isinstance(next_offset, int)
        and not isinstance(next_offset, bool)
        and next_offset > offset
        else None
    )
    render_pagination = "pagination" in initial or consumed is None

    def raw_limit(rows: int) -> int:
        if consumed is None or row_count == 0 or consumed <= row_count:
            return min(max(rows, 1), ceiling)
        return min(max(rows * consumed // row_count, rows, 1), ceiling)

    low, high, best = 1, min(row_count, ceiling), 0
    while low <= high:
        rows = (low + high) // 2
        limit = raw_limit(rows)
        prefix = dict(initial)
        for key in keys:
            prefix[key] = list(initial[key][:rows])
        prefix["limit"] = limit
        prefix["next_offset"] = offset + limit
        _stamp_byte_truncation(
            prefix,
            budget=budget,
            requested_limit=requested_limit,
            effective_limit=limit,
        )
        if render_pagination:
            # Imported here: `server` imports this module at load time.
            from okg.substrate.mcp.server import add_pagination_hint

            add_pagination_hint(prefix, operator_name)
        if measure(prefix) <= budget:
            best = rows
            low = rows + 1
        else:
            high = rows - 1
    return _Prediction(rows=best, limit=raw_limit(best))


def _without_exact_matches(payload: Any) -> Any:
    """`payload` with its `exact_matches` block cut to one status per span,
    or None when it has no block (PACT lexical-exact-identifier-hits).

    The block rides beside a lexical page and does not shrink with the
    page's limit, so on a tight budget it can be what stops even one row
    from fitting. Before such a call is refused, the fitter serves the page
    with the block reduced to its spans, each marked as dropped for the
    budget, so the page fits as it would without the pass and the caller
    still learns the pass ran."""
    if not isinstance(payload, dict) or not isinstance(payload.get("exact_matches"), dict):
        return None
    block = payload["exact_matches"]
    reduced = {
        "method": block.get("method"),
        "spans": [
            {"span": entry.get("span"), "status": "omitted:response_byte_budget"}
            for entry in block.get("spans") or [] if isinstance(entry, dict)
        ],
    }
    if block.get("dropped_spans"):
        reduced["dropped_spans"] = block["dropped_spans"]
    return {**payload, "exact_matches": reduced}


def _refuse_or_drop_exact(
    operator_name: str,
    initial: Any,
    *,
    budget: int,
    measure: Callable[[dict[str, Any]], int],
    single_row: bool,
    smallest: Any = None,
    has_cursor: Optional[Callable[[Any], bool]] = None,
    needed: Optional[int] = None,
    arguments: Optional[Mapping[str, Any]] = None,
    ceiling: Optional[int] = None,
    cut_page: Any = None,
    default_budget: Optional[int] = None,
) -> dict[str, Any]:
    """Before refusing: the first page with its exact-match block cut, when
    that fits (the page the call returns without the pass); else the
    smallest rerun page with its block cut, when that fits and keeps a
    cursor to the rows it left out, as any byte-fitted page must; else a
    node, the ontology or `cut_page`'s one row served with its largest
    values cut and marked (#3666 step 3b: size alone never refuses); else
    the oversize refusal the fitter would have returned."""
    reduced = _without_exact_matches(initial)
    if reduced is not None and measure(reduced) <= budget:
        return reduced
    reduced = _without_exact_matches(smallest)
    if (
        reduced is not None and measure(reduced) <= budget
        and (has_cursor is None or has_cursor(reduced))
    ):
        return reduced
    target = (arguments or {}).get("target")
    if operator_name == "inspect" and isinstance(target, Mapping) and target.get("kind") == "node":
        partial = _partial_node(initial, budget=budget, measure=measure)
        if partial is not None:
            return partial
    if operator_name == "inspect" and isinstance(target, Mapping) and target.get("kind") == "ontology":
        partial = _partial_ontology(initial, budget=budget, measure=measure)
        if partial is not None:
            return partial
    if cut_page is not None:
        partial = _cut_one_row(operator_name, cut_page, budget=budget, measure=measure)
        if partial is not None:
            return partial
    return _oversize_refusal(
        operator_name, budget=budget, single_row=single_row, needed=needed,
        arguments=arguments, payload=initial, ceiling=ceiling,
        default_budget=default_budget,
    )


_FENCED = re.compile(r"\A(⟦UNTRUSTED_DATA[^⟧]*⟧\n?)(.*?)(\n?⟦/UNTRUSTED_DATA⟧\s*)\Z", re.S)
#: Characters a cut value keeps at least, below which it is emptied.
_MIN_KEPT_CHARS = 64


def _cut_value(value: str, keep: int) -> str:
    """`value` cut to `keep` characters of its text, an untrusted fence kept
    whole around what is left (the fence is never split)."""
    fenced = _FENCED.match(value)
    if fenced:
        head, body, tail = fenced.groups()
        return head + body[:keep] + tail
    return value[:keep]


def _text_length(value: str) -> int:
    fenced = _FENCED.match(value)
    return len(fenced.group(2)) if fenced else len(value)


#: The page key naming the parts of a cut inspect(node) outside `attrs`.
NODE_PARTS_CUT_KEY = "parts_cut"


def record_comment_chars(comments: Any) -> int:
    """Characters of the comment texts in `comments` (a thread's
    `comments` list), an untrusted fence not counted."""
    total = 0
    for comment in comments or []:
        if isinstance(comment, Mapping) and isinstance(comment.get("text"), str):
            total += _text_length(comment["text"])
    return total


def _record_text_chars(block: Mapping[str, Any]) -> int:
    """Characters of a ticket thread's description and comment texts, the
    parts a cut thread shortens (its fields stay whole)."""
    description = block.get("description")
    return (
        (_text_length(description) if isinstance(description, str) else 0)
        + record_comment_chars(block.get("comments"))
    )


def _whole_thread_chars(block: Mapping[str, Any]) -> Optional[int]:
    """Characters of the WHOLE thread `block` was fitted from: its own text
    plus the comments the node read already left out
    (`comments_left_chars`). None when comments were left out and their
    size was not recorded."""
    left = block.get("comments_left")
    left_chars = block.get("comments_left_chars")
    if isinstance(left, int) and left > 0 and not isinstance(left_chars, int):
        return None
    return _record_text_chars(block) + (left_chars if isinstance(left_chars, int) else 0)


def _record_text_cut_shape(
    given: Mapping[str, Any], served: Mapping[str, Any],
) -> dict[str, Any]:
    """Which parts of the whole thread a cut `record_text` serves, for its
    `parts_cut` entry: the description's characters served of how many,
    and the comments shown by position in the whole thread (1-based), with
    how many are left out before and after them. `given` is the block the
    node read built (it may already hold only the newest comments; the
    ones it left out come first); `served` is its cut."""
    shape: dict[str, Any] = {}
    description = given.get("description")
    if isinstance(description, str):
        kept = served.get("description")
        shape["description"] = {
            "chars": _text_length(description),
            "served_chars": _text_length(kept) if isinstance(kept, str) else 0,
        }
    given_comments = [c for c in given.get("comments") or [] if isinstance(c, Mapping)]
    kept_comments = [c for c in served.get("comments") or [] if isinstance(c, Mapping)]
    earlier = given.get("comments_left")
    earlier = earlier if isinstance(earlier, int) and earlier > 0 else 0
    total = given.get("comments_total")
    total = total if isinstance(total, int) else earlier + len(given_comments)
    shown = len(kept_comments)
    last_cut = bool(shown) and (
        record_comment_chars(kept_comments[-1:])
        < record_comment_chars(given_comments[shown - 1:shown])
    )
    shape["comments"] = {
        "total": total,
        "shown": shown,
        **({"first": earlier + 1} if shown else {}),
        "earlier_left": earlier,
        "later_left": total - earlier - shown,
        "last_cut": last_cut,
    }
    return shape


def _cut_record_text(block: Mapping[str, Any], keep: int) -> dict[str, Any]:
    """A ticket thread cut to the first `keep` characters of its text in
    reading order: the description, then the comments the block holds,
    oldest first. The comment that crosses the cut keeps its start; later
    ones are left out and counted in `comments_left` with those the block
    had already left out. Marked not whole. `_record_text_cut_shape` names
    which comments of the whole thread remain."""
    out = dict(block)
    left = keep
    description = block.get("description")
    if isinstance(description, str):
        length = _text_length(description)
        if length > left:
            out["description"] = _cut_value(description, left) if left > 0 else None
        left = max(left - length, 0)
    comments = [c for c in block.get("comments") or [] if isinstance(c, Mapping)]
    kept: list[dict[str, Any]] = []
    for comment in comments:
        if left <= 0:
            break
        text = comment.get("text")
        length = _text_length(text) if isinstance(text, str) else 0
        if length <= left:
            kept.append(dict(comment))
            left -= length
            continue
        kept.append({**comment, "text": _cut_value(text, left)})
        left = 0
    previously_left = block.get("comments_left")
    previously_left = previously_left if isinstance(previously_left, int) else 0
    out["comments"] = kept
    out["comments_shown"] = len(kept)
    out["comments_left"] = previously_left + len(comments) - len(kept)
    # The node read's count of left-out characters no longer covers what
    # is left out; the `parts_cut` entry says what the cut serves.
    out.pop("comments_left_chars", None)
    out["whole"] = False
    return out


def attr_read_call(node_id: Any, needed: int) -> Optional[dict[str, Any]]:
    """The narrowest call that returns a left-out attribute whole: the node
    read with select=["attrs"] at a max_bytes its whole attributes fit
    (`needed`, rounded up to a KiB, at least the 4 KiB floor). None for a
    node with no id, or when that page is over the largest any read serves:
    no single call returns the value then, and none is offered."""
    if not isinstance(node_id, str):
        return None
    max_bytes = max(-(-needed // 1024) * 1024, 4096)
    if max_bytes > MAX_RESPONSE_BYTES:
        return None
    return {
        "tool": "inspect", "target": {"kind": "node", "node_id": node_id},
        "select": ["attrs"], "max_bytes": max_bytes,
    }


#: Node-read parts a cut page lists by name (then by count) instead of
#: serving: the subtype's live relation listing, the documents derived from
#: the node, and the live overlay of its external-backed fields.
_NODE_LISTED_PARTS = ("live_relations", "documents", "external_live")


def _partial_node(
    payload: Any, *, budget: int, measure: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """An inspect(node) over its budget, served within it with its largest
    parts cut (#3647, Jason `nodepartial A`; #3666 lane RD). Lane X agents
    asked 4-12 KB for nodes that needed 12 KB at the median and 84 KB at the
    75th percentile, and every one was refused; later runs were refused when
    the large part was a ticket's thread or the subtype's live relation
    listing, not an attribute.

    What is cut:

    - the live relation listing (`live_relations`, every relation's argument
      schema) goes first, to its relation names, and to their count only if
      still over; it is a copy of what inspect(kind=subtype) returns;
    - attribute values: text keeps its start, a list or object is left out
      whole; each is listed in `attrs_cut` with its full size, a left-out
      one with the select=["attrs"] read that returns it (`read`);
    - the record text (`record_text`, a ticket's thread) keeps its start in
      reading order, the description then the oldest of the comments the
      node read holds (on a long thread that read already kept only the
      newest), and is left out when under a few dozen characters would
      remain; its `parts_cut` entry counts the whole thread's characters
      and names the comments shown by position, with how many are left out
      before and after them. In the text form the attributes are not
      printed while the thread is, so they are not cut then;
    - the derived documents (`documents`) and the live overlay
      (`external_live`) are listed by name (document ids, field names), and
      by count only if still over, each with the call that reads it.

    Parts are cut largest first. Each part cut outside
    `attrs` is named in `parts_cut` with its full size and the one call that
    reads it whole; the page is marked truncated, so it never reads as
    complete. None when the node's identity and envelope alone do not fit
    (the refusal stands)."""
    if not isinstance(payload, Mapping):
        return None
    attrs_value = payload.get("attrs")
    if attrs_value is not None and not isinstance(attrs_value, Mapping):
        return None
    if not any(
        isinstance(payload.get(key), (Mapping, list))
        for key in ("attrs", "record_text", *_NODE_LISTED_PARTS)
    ):
        return None
    node_id = payload.get("node_id")
    subtype = payload.get("subtype")
    page = copy.deepcopy(dict(payload))
    attrs = page["attrs"] if isinstance(page.get("attrs"), Mapping) else {}
    cut: dict[str, dict[str, Any]] = {}
    parts: dict[str, dict[str, Any]] = {}
    page["attrs_cut"] = cut
    page[NODE_PARTS_CUT_KEY] = parts
    page["truncated"] = True
    page["truncation_reason"] = BYTE_TRUNCATION_REASON
    page["response_byte_budget"] = budget
    text_call = (
        {"tool": "inspect", "target": {"kind": "text", "node_id": node_id}}
        if isinstance(node_id, str) else None
    )
    if text_call is not None:
        page["read_whole"] = text_call

    def size(value: Any) -> int:
        return len(json.dumps(value, default=str, ensure_ascii=False).encode("utf-8"))

    def finished() -> dict[str, Any]:
        if not cut:
            # Each part cut outside `attrs` names its own whole read.
            page.pop("attrs_cut")
            page.pop("read_whole", None)
        if not parts:
            page.pop(NODE_PARTS_CUT_KEY)
        return page

    # Attributes the served form leaves out are not cut or listed: cutting
    # them frees nothing, and the reader never sees them. The searchable-text
    # copy is left out here as every response leaves it out; an ok lineage
    # envelope stays for the response shaping to drop.
    from okg.substrate.mcp.response_shape import (
        LINEAGE_ATTR, PRODUCER_TEXT_ATTR, _lineage_is_ok,
    )

    attrs.pop(PRODUCER_TEXT_ATTR, None)
    unserved = {LINEAGE_ATTR} if _lineage_is_ok(attrs.get(LINEAGE_ATTR)) else set()
    # What a select=["attrs"] read of this node serves: its identity and
    # its whole attributes. A left-out list or object is offered that read.
    attrs_needed = measure({
        **{key: page[key] for key in (
            "node_id", "subtype", "provenance", "generation_id", "pinned_generation_id",
        ) if key in page},
        "attrs": copy.deepcopy(attrs),
    }) + 1024 if attrs else 0

    if measure(page) - budget > 0 and isinstance(page.get("live_relations"), Mapping):
        listing = page.pop("live_relations")
        entry: dict[str, Any] = {
            "bytes": size(listing),
            "relations": len(listing),
            "names": [str(name) for name in listing],
        }
        if isinstance(subtype, str) and subtype:
            entry["read_whole"] = {
                "tool": "inspect", "target": {"kind": "subtype", "subtype": subtype},
            }
        parts["live_relations"] = entry

    thread = page.get("record_text")
    thread_whole = copy.deepcopy(thread) if isinstance(thread, Mapping) else None
    # The characters of the block the node read built (it may hold only the
    # newest comments), which the cut shortens.
    thread_chars = _record_text_chars(thread_whole) if thread_whole is not None else 0
    # The text form of a node with a record text prints the thread, not the
    # attributes (result_text `_record_text_lines`): while the thread is on
    # the page, cutting an attribute frees nothing there and would add a
    # notice about values the reader is never shown.
    text_form = current_result_context().format == "text"

    for _ in range(256):
        over = measure(page) - budget
        if over <= 0:
            return finished()
        # What can still shrink: text with characters left, a list or object
        # not yet left out, the record text not yet left out, and the live
        # relation names not yet left out.
        attrs_served = not (text_form and isinstance(page.get("record_text"), Mapping))
        candidates: list[tuple[int, str, str]] = [
            (size(value), "attr", key) for key, value in attrs.items()
            if attrs_served and key not in unserved and (
                (isinstance(value, str) and _text_length(value) > 0)
                or (value is not None and not isinstance(value, (str, int, float, bool)))
            )
        ]
        if thread_whole is not None and "record_text" in page:
            candidates.append((size(page["record_text"]), "record_text", "record_text"))
        for part in ("documents", "external_live"):
            if isinstance(page.get(part), (Mapping, list)):
                candidates.append((size(page[part]), "listed_part", part))
        for part in _NODE_LISTED_PARTS:
            names = parts.get(part, {}).get("names")
            if names:
                candidates.append((size(names), "names", part))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        largest, kind, key = candidates[0]
        second = candidates[1][0] if len(candidates) > 1 else 0
        if kind == "names":
            del parts[key]["names"]
            continue
        if kind == "listed_part":
            parts[key] = _listed_part(key, page.pop(key), node_id=node_id, size=size)
            continue
        if kind == "record_text":
            assert thread_whole is not None
            entry = parts.setdefault("record_text", {
                "bytes": size(thread_whole),
                # The whole thread's characters, the comments the node read
                # already left out counted too (None: not recorded).
                "chars": _whole_thread_chars(thread_whole),
                **({"read_whole": text_call} if text_call is not None else {}),
            })
            served = entry.get("served_chars", thread_chars)
            per_char = max(largest / max(served, 1), 1.0)
            keep = served - max(int((over + 32) / per_char) + 1, 256)
            if keep < _MIN_KEPT_CHARS:
                page.pop("record_text")
                entry["served_chars"] = 0
                entry.pop("description", None)
                entry.pop("comments", None)
                given = thread_whole.get("comments_total")
                entry["comments_total"] = (
                    given if isinstance(given, int)
                    else len(thread_whole.get("comments") or [])
                    + int(thread_whole.get("comments_left") or 0)
                )
            else:
                cut_thread = _cut_record_text(thread_whole, keep)
                page["record_text"] = cut_thread
                entry["served_chars"] = _record_text_chars(cut_thread)
                entry.update(_record_text_cut_shape(thread_whole, cut_thread))
            continue
        value = attrs[key]
        entry = cut.setdefault(key, {"bytes": size(value)})
        if isinstance(value, str):
            entry.setdefault("chars", _text_length(value))
            # Shrink the largest value toward the next largest (never past
            # the overflow), so long fields share the room and each keeps
            # its start; under a few dozen characters it is emptied. Values
            # of near-equal size share what is still over between them, so
            # a large node fits in a few passes and none is emptied for the
            # others. Sizes are bytes and the cut is characters: the value's
            # own bytes per character converts one to the other.
            near = sum(1 for other, _, _ in candidates if other >= largest - 256)
            want = (
                min(over + 32, largest - second) if near == 1
                else (over + 32) // near
            )
            per_char = max(largest / max(_text_length(value), 1), 1.0)
            step = max(int(want / per_char) + 1, 256)
            keep = _text_length(value) - step
            if keep < _MIN_KEPT_CHARS:
                keep = 0
            attrs[key] = _cut_value(value, keep)
            entry["served_chars"] = keep
        else:
            attrs[key] = None
            entry["served_chars"] = 0
            read = attr_read_call(node_id, attrs_needed)
            if read is not None:
                entry["read"] = read
    return None


def _listed_part(
    key: str, value: Any, *, node_id: Any, size: Callable[[Any], int],
) -> dict[str, Any]:
    """The `parts_cut` entry of a node-read part listed by name instead of
    served: its full size, its count, its names and the call that reads it.
    `documents` names the derived documents' ids, read with the incoming
    `derived_from` expand; `external_live` names the overlay's fields, read
    by hydrating them again with no other field selected."""
    if key == "documents":
        rows = value if isinstance(value, list) else []
        names = [
            str(row.get("node_id")) for row in rows
            if isinstance(row, Mapping) and row.get("node_id") is not None
        ]
        count = len(rows)
        read = (
            {"tool": "expand", "input": node_id, "edge_type": "derived_from",
             "direction": "in"}
            if isinstance(node_id, str) else None
        )
    else:
        fields = value.get("fields") if isinstance(value, Mapping) else None
        names = [str(name) for name in fields] if isinstance(fields, Mapping) else []
        count = len(names)
        read = (
            {"tool": "inspect",
             "target": {"kind": "node", "node_id": node_id, "hydrate": names or True},
             "select": ["node_id"]}
            if isinstance(node_id, str) else None
        )
    entry: dict[str, Any] = {"bytes": size(value), "count": count, "names": names}
    if read is not None:
        entry["read_whole"] = read
    return entry


#: Row fields a cut row keeps whole: they say which record or group the row
#: is, and are small.
_ROW_IDENTITY_KEYS = frozenset({
    "node_id", "edge_id", "target_id", "subtype", "edge_type", "src", "dst",
    "id", "key", "type", "rank", "score", "count", "status",
})

#: The row key a cut row names its cut values under. A row that already has
#: a field of this name is not cut (its own field is never overwritten).
ROW_CUT_KEY = "values_cut"


def _cut_one_row(
    operator_name: str, page: Any, *, budget: int,
    measure: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """A one-row page over its budget, served with that row's largest values
    cut (#3666 step 3b: a page is shortened, or one record is cut and marked
    `cut`; size alone never refuses). The page keeps its cursor, so the rows
    after it are one `next` or `offset` away. Text keeps its start; a list
    or object is left out whole. Each cut value is named in the row's `cut`
    with its full size; the page is marked truncated, so it never reads as
    whole. None when the page has not exactly one row, or when the row's
    identity and the page's own fields alone do not fit (the refusal
    stands)."""
    if not isinstance(page, Mapping):
        return None
    keys = [
        key for key in _PAGE_ROW_KEYS.get(operator_name, ())
        if isinstance(page.get(key), list)
    ]
    if not keys or len(page[keys[0]]) != 1 or not isinstance(page[keys[0]][0], Mapping):
        return None
    if ROW_CUT_KEY in page[keys[0]][0]:
        return None
    out = copy.deepcopy(dict(page))
    row = out[keys[0]][0]
    for other in keys[1:]:
        # `search(method="identifier_mentions")` serves one list under two keys.
        if len(out[other]) == 1:
            out[other] = [row]
    cut: dict[str, dict[str, Any]] = {}
    row[ROW_CUT_KEY] = cut
    out["truncated"] = True
    out["truncation_reason"] = BYTE_TRUNCATION_REASON
    out["response_byte_budget"] = budget
    from okg.substrate.mcp.response_shape import PRODUCER_TEXT_ATTR

    def size(value: Any) -> int:
        return len(json.dumps(value, default=str, ensure_ascii=False).encode("utf-8"))

    def slots() -> list[tuple[int, str, dict[str, Any], str]]:
        found = []
        for key, value in row.items():
            if key in _ROW_IDENTITY_KEYS or key == ROW_CUT_KEY:
                continue
            if key == "attrs" and isinstance(value, dict):
                for inner, item in value.items():
                    if inner != PRODUCER_TEXT_ATTR and _shrinkable(item):
                        found.append((size(item), f"attrs.{inner}", value, inner))
            elif (
                key == "link" and isinstance(value, dict)
                and isinstance(value.get("attrs"), dict)
            ):
                # A one-hop expand record row (#3666) carries the link's own
                # attributes inside `link`: cut them there, and keep the
                # link's identity (edge_id, edge_type, direction) whole.
                for inner, item in value["attrs"].items():
                    if inner != PRODUCER_TEXT_ATTR and _shrinkable(item):
                        found.append((size(item), f"link.attrs.{inner}", value["attrs"], inner))
            elif _shrinkable(value):
                found.append((size(value), key, row, key))
        return found

    for _ in range(256):
        over = measure(out) - budget
        if over <= 0:
            if not cut:
                return None
            return out
        candidates = slots()
        if not candidates:
            return None
        candidates.sort(key=lambda item: (-item[0], item[1]))
        largest, name, holder, key = candidates[0]
        second = candidates[1][0] if len(candidates) > 1 else 0
        value = holder[key]
        entry = cut.setdefault(name, {"bytes": size(value)})
        if isinstance(value, str):
            entry.setdefault("chars", _text_length(value))
            want = min(over + 32, max(largest - second, 1))
            per_char = max(largest / max(_text_length(value), 1), 1.0)
            step = max(int(want / per_char) + 1, 256)
            keep = _text_length(value) - step
            if keep < _MIN_KEPT_CHARS:
                keep = 0
            holder[key] = _cut_value(value, keep)
            entry["served_chars"] = keep
        else:
            holder[key] = None
            entry["served_chars"] = 0
    return None


#: The page key that says a search page's text values were shortened to fit
#: the reply budget: the longest value kept, in characters; 0 means head
#: lines only (PACT read-passage-rows-3666, D4).
PASSAGES_SHORTENED_KEY = "passages_shortened_to"
#: The page key counting the whole matched comments and descriptions a
#: shortened search page kept past `passages_shortened_to` (D4).
PASSAGES_KEPT_WHOLE_KEY = "passages_kept_whole"


def _middle(text: str, keep: int) -> str:
    """`keep` characters from the middle of `text`, an untrusted fence kept
    whole. A passage is centred on its match, so its middle keeps it."""
    fenced = _FENCED.match(text)
    head, body, tail = fenced.groups() if fenced else ("", text, "")
    if len(body) <= keep:
        return text
    start = (len(body) - keep) // 2
    return head + body[start:start + keep] + tail


def _search_texts(row: Mapping[str, Any], attrs_selected: bool) -> list[int]:
    """The lengths of a search row's shortenable text values (D4): its
    passage, excerpt, grep match lines, selected values, and its attribute
    values when `attrs` was selected whole."""
    lengths: list[int] = []
    passage = row.get("passage")
    if isinstance(passage, Mapping) and isinstance(passage.get("text"), str):
        lengths.append(_text_length(passage["text"]))
    if isinstance(row.get("excerpt"), str):
        lengths.append(_text_length(row["excerpt"]))
    for line in row.get("match_lines") or ():
        if isinstance(line, Mapping) and isinstance(line.get("text"), str):
            lengths.append(_text_length(line["text"]))
    holders = [row.get("selected")] + ([row.get("attrs")] if attrs_selected else [])
    for holder in holders:
        if isinstance(holder, Mapping):
            lengths.extend(_text_length(v) for v in holder.values() if isinstance(v, str))
    return lengths


def _has_row_text(row: Any, attrs_selected: bool) -> bool:
    """Whether a search row is in the passage-row shape D4 shortens: a
    head line, a matched passage, grep match lines, selected values, or
    whole `attrs` the call selected. An older row with only an `excerpt`
    is not: its page is still cut by rows, as before."""
    if not isinstance(row, Mapping):
        return False
    passage = row.get("passage")
    return (
        isinstance(row.get("head"), Mapping)
        or (isinstance(passage, Mapping) and isinstance(passage.get("text"), str))
        or isinstance(row.get("match_lines"), list)
        or isinstance(row.get("selected"), Mapping)
        or (attrs_selected and isinstance(row.get("attrs"), Mapping))
    )


#: The page key that says a search page's head lines were shortened to fit
#: the reply budget, after its passages were dropped (D4): `source_links`
#: (each row's `source: <url>` line left out), `source_names` (each row's
#: source name left out; the page lists the names once, in `row_sources`),
#: `title_chars` (titles cut to that many characters).
HEADS_SHORTENED_KEY = "head_lines_shortened"
#: The page key listing the distinct source names of a page whose rows had
#: their source names left out to fit (D4).
ROW_SOURCES_KEY = "row_sources"
#: Characters a head line's title keeps at least when titles are cut.
_MIN_TITLE_CHARS = 24


def _cut_title(title: str, keep: int) -> str:
    """`title` cut to `keep` characters, the last one an ellipsis, an
    untrusted fence kept whole around it."""
    fenced = _FENCED.match(title)
    head, body, tail = fenced.groups() if fenced else ("", title, "")
    if len(body) <= keep:
        return title
    return head + body[:max(keep - 1, 0)].rstrip() + "…" + tail


def _title_is_a_field(head: Mapping[str, Any]) -> bool:
    """A code row's title repeats one of its head fields and does not print
    on its own (result_text `_passage_head`): it is never cut."""
    fields = head.get("fields")
    return isinstance(fields, list) and any(
        isinstance(item, (list, tuple)) and len(item) == 2 and item[1] == head.get("title")
        for item in fields
    )


def _row_source(row: Mapping[str, Any]) -> Any:
    provenance = row.get("provenance", row.get("read_provenance"))
    return provenance.get("source") if isinstance(provenance, Mapping) else None


#: What a search row keeps when its other fields are left out to fit (D4):
#: the identity floor no `select` removes (`_LEXICAL_ALWAYS`,
#: `_SEMANTIC_ALWAYS`), the head line, the trust a renderer fences by, and
#: what the caller asked for (selected values, whole `attrs`, grep match
#: lines and list fields). The rest repeat the head line or describe the
#: ranking: a JSON page of 20 head-only rows measured about 15,000 bytes
#: with them, so a 12,000-byte page dropped rows. Each row's `read_more`
#: (about 95 bytes) becomes one page `read_more` naming the ids of each
#: target kind, as the text prints one read pointer per reply (D5).
_ROW_FLOOR = frozenset({
    "node_id", "subtype", "score", "distance", "evidence_kind",
    "evidence_kind_provenance", "embedding_provenance", "phrase_match",
    "head", "passage", "provenance", "read_provenance",
    "selected", "attrs", "match_lines", "matches", "first_match",
})
#: Page fields that explain the read rather than answer it, left out with
#: the row fields above (D4). A JSON page of 15 head-only long records
#: measured 11,160 bytes, about 4,000 of them page fields (the recency
#: block's absent reason alone about 400). Counts, paging, the pin, trust,
#: freshness and coverage stay.
_PAGE_EXPLANATIONS = (
    "recency", "parent_relation", "concept_expansion", "pagination_hint",
    "truncation_remedy", "excerpt_window_basis", "source_family_applicable",
    "path_exclusion_applicable", "diversity_applicable", "source_families_declared",
)


def _search_row_head_at(
    row: Any, *, links: bool, names: bool, title_chars: Optional[int],
    fields: bool = False,
) -> Any:
    """`row` with its head line shortened (D4): the citation URL left out
    (`links`), the source name left out (`names`), the title cut to
    `title_chars`, and with `fields` every row field outside `_ROW_FLOOR`
    left out. Rows are copied, never changed in place."""
    if not isinstance(row, Mapping):
        return row
    out = {k: v for k, v in row.items() if k in _ROW_FLOOR} if fields else dict(row)
    if fields:
        # Whole-attrs values already cut to nothing are left out by name
        # too (four empty strings were 60 bytes a row); the page's
        # `passages_shortened_to: 0` says they were cut. `selected` keeps
        # its keys: a missing one reads as a key the record lacks.
        holder = out.get("attrs")
        if isinstance(holder, Mapping):
            out["attrs"] = {name: value for name, value in holder.items() if value != ""}
    head = row.get("head")
    if isinstance(head, Mapping):
        new = dict(head)
        if links and new.get("citation_url") is not None:
            new["citation_url"] = None
        title = new.get("title")
        if (
            title_chars is not None and isinstance(title, str)
            and _text_length(title) > title_chars and not _title_is_a_field(head)
        ):
            new["title"] = _cut_title(title, title_chars)
        out["head"] = new
    if names:
        for key in ("provenance", "read_provenance"):
            provenance = row.get(key)
            if isinstance(provenance, Mapping) and provenance.get("source") is not None:
                out[key] = {**provenance, "source": None}
    return out


def _search_heads_at(
    page: Mapping[str, Any], *, links: bool, names: bool, title_chars: Optional[int],
    fields: bool = False,
) -> dict[str, Any]:
    """`page` (its passages already dropped) with every head line shortened
    the same way, and the page saying what was left out."""
    out = dict(page)
    rows = [row for row in page["results"] if isinstance(row, Mapping)]
    heads = [row["head"] for row in rows if isinstance(row.get("head"), Mapping)]
    out["results"] = [
        _search_row_head_at(
            row, links=links, names=names, title_chars=title_chars, fields=fields,
        )
        for row in page["results"]
    ]
    # Only what was actually left out is said.
    said: dict[str, Any] = {}
    if fields:
        dropped = sorted({key for row in rows for key in row if key not in _ROW_FLOOR})
        if dropped:
            said["row_fields"] = dropped
        left = [key for key in _PAGE_EXPLANATIONS if key in out]
        for key in left:
            del out[key]
        if left:
            said["page_fields"] = left
        kinds: dict[str, list[Any]] = {}
        for row in rows:
            call = row.get("read_more")
            arguments = call.get("arguments") if isinstance(call, Mapping) else None
            target = arguments.get("target") if isinstance(arguments, Mapping) else None
            if (
                isinstance(call, Mapping) and call.get("tool") == "inspect"
                and isinstance(target, Mapping) and isinstance(target.get("kind"), str)
            ):
                kinds.setdefault(target["kind"], []).append(target.get("node_id"))
        if kinds:
            out["read_more"] = {"tool": "inspect", "kinds": kinds}
    if links and any(head.get("citation_url") is not None for head in heads):
        said["source_links"] = True
    sources: list[Any] = []
    for row in rows:
        source = _row_source(row)
        if source is not None and source not in sources:
            sources.append(source)
    if names and sources:
        said["source_names"] = True
        out[ROW_SOURCES_KEY] = sources
    if title_chars is not None and any(
        isinstance(head.get("title"), str) and _text_length(head["title"]) > title_chars
        and not _title_is_a_field(head)
        for head in heads
    ):
        said["title_chars"] = title_chars
    if said:
        out[HEADS_SHORTENED_KEY] = said
    return out


def _longest_title(page: Mapping[str, Any]) -> int:
    longest = 0
    for row in page["results"]:
        head = row.get("head") if isinstance(row, Mapping) else None
        if isinstance(head, Mapping) and isinstance(head.get("title"), str):
            if not _title_is_a_field(head):
                longest = max(longest, _text_length(head["title"]))
    return longest


def _shortest_heads(page: Mapping[str, Any]) -> dict[str, Any]:
    """The smallest form D4 serves a row in: no passage, no source link, no
    source name, only the floor fields, the title at its floor."""
    return _search_heads_at(
        page, links=True, names=True, title_chars=_MIN_TITLE_CHARS, fields=True,
    )


def _shortened_heads(
    page: dict[str, Any], *, budget: int, measure: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """`page` (passages dropped) with its head lines shortened in order
    until it fits (D4): source links left out, then source names, then the
    row fields outside `_ROW_FLOOR` (JSON carries them; the text prints
    none of them on a search row), then titles cut evenly to the longest
    length that fits. None when even the shortest head lines do not fit."""
    if measure(page) <= budget:
        return page
    for links, names, fields in ((True, False, False), (True, True, False),
                                 (True, True, True)):
        candidate = _search_heads_at(
            page, links=links, names=names, title_chars=None, fields=fields,
        )
        if measure(candidate) <= budget:
            return candidate
    longest = _longest_title(page)
    if longest <= _MIN_TITLE_CHARS:
        return None

    def at(title_chars: int) -> dict[str, Any]:
        return _search_heads_at(
            page, links=True, names=True, title_chars=title_chars, fields=True,
        )

    low, high = _MIN_TITLE_CHARS, longest - 1
    if measure(at(low)) > budget:
        return None
    while low < high:
        middle = (low + high + 1) // 2
        if measure(at(middle)) <= budget:
            low = middle
        else:
            high = middle - 1
    return at(low)


def _whole_unit(row: Any) -> bool:
    """Whether a search row's passage is a whole matched comment or
    description block short enough to be shown whole (`UNIT_WHOLE_CHARS`):
    the text D4 cuts last."""
    passage = row.get("passage") if isinstance(row, Mapping) else None
    return (
        isinstance(passage, Mapping) and passage.get("whole") is True
        and passage.get("where") in ("comment", "description")
        and isinstance(passage.get("text"), str)
        and _text_length(passage["text"]) <= UNIT_WHOLE_CHARS
    )


def _match_line_at(line: Any, keep: int) -> Any:
    """A grep match line cut to `keep` characters of its text. A comment
    or prose line (it carries `chars_left`) adds the characters cut here to
    it, so the line still says how much of the comment it does not show."""
    if not (
        isinstance(line, Mapping) and isinstance(line.get("text"), str)
        and _text_length(line["text"]) > keep
    ):
        return line
    out = {**line, "text": _cut_value(line["text"], keep)}
    left = line.get("chars_left")
    if isinstance(left, int) and not isinstance(left, bool):
        fenced = _FENCED.match(line["text"])
        body = fenced.group(2) if fenced else line["text"]
        # A cut line's closing `...` is a marker, not the source's text.
        marker = 3 if left > 0 and body.endswith("...") else 0
        out["chars_left"] = left + max(0, len(body) - marker - keep)
    return out


def _search_row_at(
    row: Any, keep: int, attrs_selected: bool, keep_units: bool = False,
) -> Any:
    """`row` with every shortenable text value cut to `keep` characters: the
    passage from its middle (0 drops it: the head line only), the other
    values from their start. With `keep_units`, a whole matched comment or
    description (`_whole_unit`) is not cut. Rows are copied, never changed
    in place."""
    if not isinstance(row, Mapping):
        return row
    out = dict(row)
    passage = row.get("passage")
    if keep_units and _whole_unit(row):
        pass
    elif isinstance(passage, Mapping) and isinstance(passage.get("text"), str):
        if keep == 0:
            out["passage"] = None
        elif _text_length(passage["text"]) > keep:
            out["passage"] = {**passage, "text": _middle(passage["text"], keep), "whole": False}
    if isinstance(row.get("excerpt"), str) and _text_length(row["excerpt"]) > keep:
        out["excerpt"] = _cut_value(row["excerpt"], keep)
    lines = row.get("match_lines")
    if isinstance(lines, list):
        out["match_lines"] = [_match_line_at(line, keep) for line in lines]
    for key in ("selected", "attrs") if attrs_selected else ("selected",):
        holder = row.get(key)
        if isinstance(holder, Mapping):
            out[key] = {
                name: _cut_value(value, keep)
                if isinstance(value, str) and _text_length(value) > keep else value
                for name, value in holder.items()
            }
    return out


def _attrs_selected(arguments: Optional[Mapping[str, Any]]) -> bool:
    select = (arguments or {}).get("select")
    return isinstance(select, (list, tuple)) and "attrs" in select


def _search_page_at(
    page: Mapping[str, Any], keep: int, attrs_selected: bool, rows: Optional[int] = None,
    keep_units: bool = False,
) -> dict[str, Any]:
    out = dict(page)
    shown = page["results"] if rows is None else page["results"][:rows]
    out["results"] = [
        _search_row_at(row, keep, attrs_selected, keep_units) for row in shown
    ]
    if any(
        n > 0 for row in shown if isinstance(row, Mapping)
        for n in _search_texts(row, attrs_selected)
    ):
        # Only a page that had text to shorten says it was shortened: a
        # list-view page of head lines alone never had passages.
        out[PASSAGES_SHORTENED_KEY] = keep
    if keep_units:
        # The cap does not hold for these rows: the page says how many
        # passages it kept whole past it (measured with the page).
        kept = sum(
            1 for row in shown
            if _whole_unit(row) and _text_length(row["passage"]["text"]) > keep
        )
        if kept:
            out[PASSAGES_KEPT_WHOLE_KEY] = kept
    return out


def _shortened_search_page(
    page: Any,
    arguments: Optional[Mapping[str, Any]],
    *,
    budget: int,
    measure: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """A search page over its budget, served with EVERY requested row and
    its text values shortened to fit (PACT read-passage-rows-3666, D4):
    passages, excerpts, grep match lines, selected values, and attribute
    values when `attrs` was selected. One cap for every value, the largest
    that fits, so long values share the room evenly; the page says it in
    `passages_shortened_to` (0: head lines only). A whole matched comment
    or description (`_whole_unit`) is outside the cap while the others,
    cut to nothing, leave it room; its passage still says `whole`. When head lines alone do
    not fit, they are shortened too (`_shortened_heads`): source links, then
    source names, then titles. None when the rows are not in the
    passage-row shape, or when even the shortest head lines do not fit."""
    if not isinstance(page, Mapping) or not isinstance(page.get("results"), list):
        return None
    attrs_selected = _attrs_selected(arguments)
    if not any(_has_row_text(row, attrs_selected) for row in page["results"]):
        return None
    lengths = [
        n for row in page["results"] if isinstance(row, Mapping)
        for n in _search_texts(row, attrs_selected)
    ]
    heads = _search_page_at(page, 0, attrs_selected)
    if not lengths or max(lengths) == 0 or measure(heads) > budget:
        return _shortened_heads(heads, budget=budget, measure=measure)
    # A whole matched comment or description (`_whole_unit`) is cut last:
    # every other value shrinks first, down to nothing, before it is cut.
    # A 29-name list in one comment, cut evenly with 49 other rows, kept 17
    # names. Only when the units alone do not fit is every value cut evenly.
    keep_units = any(_whole_unit(row) for row in page["results"]) and measure(
        _search_page_at(page, 0, attrs_selected, keep_units=True),
    ) <= budget
    low, high = 0, max(lengths) - 1
    while low < high:
        middle = (low + high + 1) // 2
        if measure(_search_page_at(
            page, middle, attrs_selected, keep_units=keep_units,
        )) <= budget:
            low = middle
        else:
            high = middle - 1
    return _search_page_at(page, low, attrs_selected, keep_units=keep_units)


def _search_rows_refusal(
    page: Mapping[str, Any],
    arguments: Mapping[str, Any],
    signature: inspect.Signature,
    *,
    budget: int,
    default_budget: Optional[int],
    measure: Callable[[dict[str, Any]], int],
) -> dict[str, Any]:
    """The refusal of a search page whose head lines alone do not fit (D4:
    search never drops rows to fit). Its retry is the same search without
    `select` and `max_chars` when either was given, else with the largest
    `limit` whose head lines fit the default budget. It never names
    `max_bytes` (D3)."""
    attrs_selected = _attrs_selected(arguments)
    needed = measure(_shortest_heads(_search_page_at(page, 0, attrs_selected)))
    refusal = _oversize_refusal(
        "search", budget=budget, single_row=False, needed=needed,
        arguments=arguments, payload=page, default_budget=default_budget,
    )
    retry = _narrower_retry_call("search", arguments, signature)
    if retry is None:
        room = min(budget, default_budget if default_budget is not None else DEFAULT_RESPONSE_BYTES)
        low, high = 0, len(page["results"]) - 1
        while low < high:
            middle = (low + high + 1) // 2
            if measure(_shortest_heads(
                _search_page_at(page, 0, attrs_selected, rows=middle),
            )) <= room:
                low = middle
            else:
                high = middle - 1
        if low >= 1:
            retry = _retry_without("search", arguments, signature, set(), max_bytes=None)
            if retry is not None:
                retry["arguments"]["limit"] = low
    if retry is not None:
        refusal["error"]["retry_call"] = retry
        if measure(refusal) > budget:
            del refusal["error"]["retry_call"]
    return refusal


def _shrinkable(value: Any) -> bool:
    """Text with characters left, or a list or object not yet left out."""
    if isinstance(value, str):
        return _text_length(value) > 0
    return value is not None and not isinstance(value, (int, float, bool))


def _partial_ontology(
    payload: Any, *, budget: int, measure: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """`inspect(ontology)` over its budget, served with the metadata of its
    largest subtypes left out (#3666 step 3b; measured: the comp-ops
    ontology needed 68,706 B against the 65,536 B default and was refused
    on every call). Every subtype name and every narrowing is still served;
    a subtype whose metadata was left out says `cut`, and
    `inspect(target={"kind":"subtype","subtype":<name>})` reads it. Fewest
    subtypes cut, largest first. None when even no metadata at all does
    not fit (the refusal stands)."""
    if not isinstance(payload, Mapping) or not isinstance(payload.get("subtypes"), list):
        return None

    def size(value: Any) -> int:
        return len(json.dumps(value, default=str, ensure_ascii=False).encode("utf-8"))

    order = sorted(
        (
            index for index, entry in enumerate(payload["subtypes"])
            if isinstance(entry, Mapping) and entry.get("metadata") is not None
        ),
        key=lambda index: (-size(payload["subtypes"][index]["metadata"]), index),
    )

    def with_cut(count: int) -> dict[str, Any]:
        page = dict(payload)
        chosen = set(order[:count])
        page["subtypes"] = [
            {**entry, "metadata": None, "cut": "metadata"} if index in chosen else entry
            for index, entry in enumerate(payload["subtypes"])
        ]
        page["truncated"] = True
        page["truncation_reason"] = BYTE_TRUNCATION_REASON
        page["response_byte_budget"] = budget
        page["metadata_cut"] = {
            "subtypes": count,
            "read_one": {"tool": "inspect", "target": {"kind": "subtype", "subtype": "<name>"}},
        }
        return page

    if not order or measure(with_cut(len(order))) > budget:
        return None
    low, high = 1, len(order)
    while low < high:
        middle = (low + high) // 2
        if measure(with_cut(middle)) <= budget:
            high = middle
        else:
            low = middle + 1
    return with_cut(low)


def _oversize_retry_call(
    operator_name: str,
    arguments: Mapping[str, Any],
    signature: inspect.Signature,
    *,
    needed: Any,
    ceiling: int | None,
    default_budget: int | None = None,
) -> dict[str, Any] | None:
    """The same call at a budget it fits, for a size refusal that stands
    (#3666 step 3b: where a reply cannot be cut exactly, the refusal carries
    a call that is served). None when the call continues a token (its budget
    is signed), or the call is too long to carry.

    R9 (PACT read-passage-rows-3666): the budget offered is never above
    `default_budget` (the deployment default, else `DEFAULT_RESPONSE_BYTES`;
    the ceiling when lower). A refusal asked an agent for 123,064 bytes.
    Past the default the retry is a narrower call the server serves within
    it (`_narrower_retry_call`), or none."""
    if isinstance(needed, bool) or not isinstance(needed, int):
        return None
    if arguments.get("next") is not None or arguments.get("continuation_token"):
        return None
    target = arguments.get("target")
    if isinstance(target, Mapping) and target.get("continuation_token"):
        return None
    budget = needed + _NEEDED_MARGIN_BYTES
    upper = min(
        ceiling if ceiling is not None else MAX_RESPONSE_BYTES,
        default_budget if default_budget is not None else DEFAULT_RESPONSE_BYTES,
    )
    if budget > upper or operator_name == "search":
        # D3: search does not publish max_bytes; its retry is never a
        # larger budget.
        return _narrower_retry_call(operator_name, arguments, signature)
    return _retry_without(operator_name, arguments, signature, set(), max_bytes=budget)


def _narrower_retry_call(
    operator_name: str,
    arguments: Mapping[str, Any],
    signature: inspect.Signature,
) -> dict[str, Any] | None:
    """A narrower call for a size refusal past the default budget (R9), or
    None. Only calls the server serves within the default by construction:

    * `inspect` of one node: the record's text, `inspect(target={"kind":
      "text", "node_id": <id>})`, which pages by bytes;
    * `search` with `select` or `max_chars`: the same search without them,
      at the default budget. Its rows are the short default rows, and a page
      over the budget is cut to the rows that fit, with `offset` for the rest.

    Anything else gets words only: a call that is refused again is worse
    than none."""
    target = arguments.get("target")
    if operator_name == "inspect":
        if not (isinstance(target, Mapping) and target.get("kind") == "node"):
            return None
        node_id = target.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            return None
        text: dict[str, Any] = {"target": {"kind": "text", "node_id": node_id}}
        if arguments.get("generation_id") is not None:
            text["generation_id"] = arguments["generation_id"]
        return {"tool": "inspect", "mode": "corrected", "arguments": text}
    if operator_name == "search" and (
        arguments.get("select") is not None or arguments.get("max_chars") is not None
    ):
        return _retry_without(
            operator_name, arguments, signature, {"select", "max_chars"}, max_bytes=None,
        )
    return None


def _retry_without(
    operator_name: str,
    arguments: Mapping[str, Any],
    signature: inspect.Signature,
    dropped: set[str],
    *,
    max_bytes: int | None,
) -> dict[str, Any] | None:
    """The caller's call as a `corrected` retry: arguments at their default
    and `dropped` left out, and `max_bytes` set when given (omitted, the
    call gets the default budget). The caller's `authority` is never echoed.
    None when the call is too long to carry."""
    call: dict[str, Any] = {}
    for name, value in arguments.items():
        if name in {"ctx", "authority", "max_bytes"} or name in dropped or value is None:
            continue
        parameter = signature.parameters.get(name)
        if (
            parameter is not None
            and parameter.default is not inspect.Parameter.empty
            and type(value) is type(parameter.default)
            and value == parameter.default
        ):
            continue
        call[name] = value
    if max_bytes is not None:
        call["max_bytes"] = max_bytes
    retry = {"tool": operator_name, "mode": "corrected", "arguments": call}
    try:
        if len(json.dumps(retry)) > _RETRY_CALL_BYTES:
            return None
    except (TypeError, ValueError):
        return None
    return retry


#: The widest total a count field can render (12 digits), for the size test
#: only; no served value is ever this number.
_WIDEST_TOTAL = 999_999_999_999

#: The `notes` entry a search lookup adds only when its count is exact
#: (`completeness="exact"`, server.py `_search_attrs_lookup`). Shared so the
#: size test builds the same bytes the producer does.
COUNT_EXACT_NOTE: dict[str, str] = {
    "count": "exact",
    "how": (
        "every condition of this call is decided in SQL, so "
        "one COUNT over the whole set gave the total"
    ),
}

#: Set by `note_count_may_vary` while a read is being fitted: the call ran a
#: bounded COUNT (row cap and time budget), so its total can come out exact,
#: a lower bound or unknown. Pages whose count is fixed by the read itself
#: (aggregate, result-reference reads, semantic and lexical search) never set
#: it and are sized as they are. So is grep: its own timed count
#: (grep_search.py `count_grep_matches`) has other count fields
#: (`count_basis_reasons`, `scan_cap`, basis `timed_out`, a proof-dependent
#: retrieval basis) that the forms here do not model.
_COUNT_MAY_VARY: ContextVar[Optional[list[bool]]] = ContextVar(
    "okg_count_may_vary", default=None,
)


def note_count_may_vary() -> None:
    """Called by the bounded COUNT: this call's total has several outcomes."""
    box = _COUNT_MAY_VARY.get()
    if box is not None:
        box[0] = True


def _page_has_more(page: dict[str, Any]) -> bool:
    """Whether the page points at rows past it (the only pages whose count
    is a COUNT query that can time out). A page that holds the whole rest of
    the set gets its exact total without counting, so it has one form."""
    next_offset = page.get("next_offset")
    return (
        page.get("truncated") is True
        or (isinstance(next_offset, int) and not isinstance(next_offset, bool))
        or bool(page.get("continuation_token"))
    )


def _count_variants_apply(operator_name: str, page: Any) -> bool:
    """Whether `_count_variants` has forms for this page (see there)."""
    return (
        operator_name in _PAGE_ROW_KEYS
        and isinstance(page, dict)
        and page.get("error") is None
        and page.get("status") not in {"refused", "error"}
        and "total_count" in page
        and any(isinstance(page.get(key), list) for key in _PAGE_ROW_KEYS[operator_name])
        and _page_has_more(page)
    )


def _count_variants(
    operator_name: str, page: Any, *, exact_note: bool = False,
) -> list[dict[str, Any]]:
    """The forms a page's total count can take, as copies for sizing only.

    The COUNT runs under a time budget and a row cap, so one request can be
    served with any of these, and each renders different bytes:

    * exact: `total_count`, basis `exact`, `retrieval_exhaustive` True
      (coverage.retrieval `exhaustive`), no `coverage.selection.size`, and
      the search lookup's exact-only `notes` entry (`COUNT_EXACT_NOTE`) when
      the page has it or the call asked for `completeness="exact"`;
    * lower bound: basis `lower_bound` (`N+`, retrieval `bounded`),
      `selection.size` null;
    * unknown: no total, basis `unknown`, `selection.size` null, retrieval
      `unknown`;
    * unknown and timed out: the same plus `total_count_reason:
      count_not_computed_in_time` (its notice and the "retrieval exact for
      these rows, more not counted" fact).

    Each copy gets its coverage block rebuilt by `with_read_coverage`, from
    the page's own selection unit and basis. Empty for a page the rule does
    not apply to: not a count-bearing operator, an error or refused page, no
    row list, no `total_count`, or a page with no rows past it (one form).
    """
    from okg.substrate.mcp.envelope import with_read_coverage

    if not _count_variants_apply(operator_name, page):
        return []
    own_notes = page.get("notes")
    own_notes = own_notes if isinstance(own_notes, list) else []
    has_note = any(
        isinstance(note, dict) and note.get("count") == "exact" for note in own_notes
    )
    other_notes = [
        note for note in own_notes
        if not (isinstance(note, dict) and note.get("count") == "exact")
    ]

    def form(basis: str, total: int | None, *, exhaustive: bool = False,
             reason: bool = False) -> dict[str, Any]:
        out = dict(page)
        out["total_count"] = total
        out["total_count_basis"] = basis
        out.pop("total_count_reason", None)
        out.pop("retrieval_exhaustive", None)
        if exhaustive:
            out["retrieval_exhaustive"] = True
        if reason:
            out["total_count_reason"] = "count_not_computed_in_time"
        notes = list(other_notes)
        # The server adds the note only for an exact, SQL-decided count with
        # a cursor (server.py `_search_attrs_lookup`); a page that stopped at
        # the paging cap never has one. `sql_exact` is not on the page, so a
        # `completeness="exact"` call is sized with it otherwise (a few
        # bytes over where the lookup was not SQL-decided).
        if exhaustive and (has_note or (exact_note and not page.get("pagination_capped"))):
            notes.append(dict(COUNT_EXACT_NOTE))
        if notes:
            out["notes"] = notes
        else:
            out.pop("notes", None)
        coverage = out.get("coverage")
        if isinstance(coverage, dict):
            coverage = dict(coverage)
            coverage.pop("retrieval", None)
            selection = coverage.get("selection")
            if isinstance(selection, dict):
                selection = dict(selection)
                selection.pop("size", None)
                if basis != "exact":
                    # An exact size is `total_count` itself, so it is
                    # omitted; any other count says `size: null`.
                    selection["size"] = None
                coverage["selection"] = selection
            out["coverage"] = coverage
        if out.get("operator_name") is None:
            out["operator_name"] = operator_name
            return {k: v for k, v in with_read_coverage(out).items() if k != "operator_name"}
        return with_read_coverage(out)

    return [
        form("exact", _WIDEST_TOTAL, exhaustive=True),
        form("lower_bound", _WIDEST_TOTAL),
        form("unknown", None),
        form("unknown", None, reason=True),
    ]


def _placeholder(value: Any) -> Any:
    """`value` with the same keys and nesting and empty leaves."""
    if isinstance(value, dict):
        return {key: _placeholder(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_placeholder(item) for item in value]
    if isinstance(value, str):
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return 0
    # Booleans stay: a row flag (hidden, redacted, dropped) can decide how a
    # row is shown, and a bool costs nothing to keep.
    return value


def _count_stable(
    operator_name: str,
    measure: Callable[[dict[str, Any]], int],
    *,
    may_vary: Callable[[], bool] = lambda: True,
    exact_note: bool = False,
) -> Callable[[dict[str, Any]], int]:
    """`measure`, as the largest size any count outcome gives the same rows.

    The size test must not depend on whether the count finished, or the same
    request returns a row more or fewer (and a different `next_offset`) from
    run to run. The size of a page is the largest of its own and of every
    count form built on its rows (`_count_variants`), so every outcome's page
    sizes to the same number. Only pages whose count can vary get this
    (`may_vary`: the call ran a bounded COUNT, see `note_count_may_vary`).

    Rendering five full pages per size test is five times the cost, so the
    extra is measured on a stand-in instead: the same page with each row
    replaced by an empty row of the same shape. The count fact, the coverage
    block and the notices depend on the NUMBER of rows (a lower bound equal to
    the rows shown reads "more follow"; the span reads "1-N"), not on what is
    in them, so `extra(N)` is the same for the stand-in and the real page, and
    `size = measure(page) + extra`. The extra is kept per call, keyed on the
    page with its rows removed (every other field, so the key covers whatever
    the extra can depend on) plus each list's row count and first row's keys.
    `test_the_fast_stable_size_is_the_full_five_render_max` holds this to the
    five-render maximum. Only the size test sees this; the page served keeps
    its own count.
    """
    extras: dict[Any, int] = {}
    keys = _PAGE_ROW_KEYS.get(operator_name, ())

    def shape_of(rows: Any) -> Any:
        if not isinstance(rows, list):
            return None
        first = rows[0] if rows else None
        return len(rows), tuple(sorted(first)) if isinstance(first, dict) else None

    def key_of(page: dict[str, Any]) -> Any:
        rest = {k: v for k, v in page.items() if k not in keys}
        try:
            return (
                tuple(shape_of(page.get(k)) for k in keys),
                json.dumps(rest, sort_keys=True, default=repr),
            )
        except (TypeError, ValueError):
            # Keys of mixed types cannot be sorted, and `default=` does not
            # apply to keys. A call never fails because of the memo: no key,
            # no memo.
            return None

    def compute(page: dict[str, Any]) -> int:
        stand = dict(page)
        for name in keys:
            if isinstance(page.get(name), list):
                stand[name] = [_placeholder(row) for row in page[name]]
        own = measure(stand)
        return max([0, *(
            measure(v) - own
            for v in _count_variants(operator_name, stand, exact_note=exact_note)
        )])

    def extra_for(page: dict[str, Any]) -> int:
        key = key_of(page)
        if key is None:
            return compute(page)
        if key not in extras:
            extras[key] = compute(page)
        return extras[key]

    def stable(page: dict[str, Any]) -> int:
        size = measure(page)
        if not may_vary() or not _count_variants_apply(operator_name, page):
            return size
        return size + extra_for(page)

    return stable


def _fit_to_budget(
    operator_name: str, fn: Callable[..., dict[str, Any]], **kw: Any,
) -> dict[str, Any]:
    """`_fit_to_budget_run`, with the count-may-vary flag for this call."""
    token = _COUNT_MAY_VARY.set([False])
    try:
        return _fit_to_budget_run(operator_name, fn, **kw)
    finally:
        _COUNT_MAY_VARY.reset(token)


def _fit_to_budget_run(
    operator_name: str,
    fn: Callable[..., dict[str, Any]],
    *,
    bound: inspect.BoundArguments,
    signature: inspect.Signature,
    call_arguments: dict[str, Any],
    arguments: dict[str, Any],
    budget: int,
    measure: Callable[[dict[str, Any]], int],
    ceiling: int | None = None,
    default_budget: int | None = None,
) -> dict[str, Any]:
    """Run the operator, then page it down until `measure` fits `budget`.

    Every served page is a real rerun of the operator at a smaller native
    limit, pinned to the first call's generation. A page is never cut from a
    completed one: several operators build page-level fields from the rows on
    the page, and Nomos-filtered `filter`, `map` and `expand` advance their
    cursor by raw positions, not visible rows.

    The first rerun's limit is PREDICTED from the first page: the largest
    prefix of its rows that fits, scaled to raw positions (see
    `_predict_fitting_limit`). A hit costs two executions in all, where a
    blind binary search over `[1, limit-1]` reran the operator four or five
    times on a 20-row page, each rerun repeating the ranking and the Nomos
    read filter. Then:

    * the predicted page does not fit, or has no cursor: search BELOW the
      prediction only;
    * it fits but shows fewer rows than the prefix predicted (hidden raw
      positions inside the window): search ABOVE it, keeping it as the
      floor, so an empty or short page is never served while a larger limit
      could still fit;
    * otherwise serve it.

    Either search is a binary search over the rest of `[1, limit-1]`, so a
    call runs the operator at most `2 + ceil(log2(limit - 1))` times -- one
    more than the blind search's worst case, and 7 on a 20-row page.

    The prediction errs short, never long: the prefix carries the first
    page's page-level fields at full size, and a rerun at the predicted
    limit is served without probing above it when it shows every predicted
    row. A page can therefore hold fewer rows than the largest that fits --
    by as many rows as the first page's page-level fields outweigh the
    rerun's, typically a row or two. Its cursor still reaches every row. The
    searches assume the served size grows with the limit, as the blind
    search did; page-level fields make that not strictly true.

    `measure` is the served size, plus the live reserve when a live call
    will follow.
    """
    # The count's outcome must not move the cut (PACT mcp-byte-fit-count-stable).
    may_vary_box = _COUNT_MAY_VARY.get() or [False]
    measure = _count_stable(
        operator_name, measure,
        may_vary=lambda: may_vary_box[0],
        exact_note=arguments.get("completeness") == "exact",
    )
    initial = fn(*bound.args, **bound.kwargs)
    initial_bytes = measure(initial)
    if initial_bytes <= budget:
        return initial
    #: The smallest served size seen, for the refusal's `needed_bytes`.
    smallest_bytes = [initial_bytes]

    #: D4 (PACT read-passage-rows-3666): a search page in the passage-row
    #: shape keeps every requested row, its text values shortened to fit.
    #: Only when even its head lines do not fit is it read again at the
    #: largest limit whose head lines fit (and shortened), with `offset` for
    #: the rest; failing that, refused with a call that fits. Rows with no
    #: text to shorten page by limit as before.
    passage_rows = False
    if operator_name == "search" and isinstance(initial, dict):
        shortened = _shortened_search_page(
            initial, arguments, budget=budget, measure=measure,
        )
        if shortened is not None:
            return shortened
        rows = initial.get("results")
        attrs_selected = _attrs_selected(arguments)
        passage_rows = isinstance(rows, list) and any(
            _has_row_text(row, attrs_selected) for row in rows
        )

    if not _is_pageable(operator_name, arguments):
        if operator_name == "query":
            # Issue #3666: a query reply over budget is served as the rows
            # that fit, with the call that reads the next page.
            page = _query_fitted_page(
                initial, budget=budget, measure=measure, arguments=arguments,
            )
            if page is not None:
                return page
        return _refuse_or_drop_exact(
            operator_name, initial, budget=budget, measure=measure, single_row=False,
            needed=initial_bytes, arguments=arguments, ceiling=ceiling,
            default_budget=default_budget,
        )

    requested_limit = int(arguments.get("limit") or 0)
    initial_limit = initial.get("limit") if isinstance(initial, dict) else None
    if isinstance(initial_limit, int) and initial_limit > 0:
        requested_limit = min(requested_limit, initial_limit)
    if requested_limit <= 1 and passage_rows:
        return _search_rows_refusal(
            initial, arguments, signature, budget=budget,
            default_budget=default_budget, measure=measure,
        )
    if requested_limit <= 1:
        return _refuse_or_drop_exact(
            operator_name, initial, budget=budget, measure=measure, single_row=True,
            needed=initial_bytes, arguments=arguments, cut_page=initial,
            default_budget=default_budget,
        )

    pin = initial.get("pinned_generation_id") or initial.get("generation_id")
    #: The smallest-limit rerun page so far: the exact-match drop is tried
    #: on it, after the first page, before a refusal.
    smallest: dict[str, Any] = {"limit": None, "page": None}
    current_offset = int(arguments.get("offset") or 0)

    def cursor(candidate: dict[str, Any]) -> bool:
        """Whether a smaller page points at the rows it left out."""
        next_offset = candidate.get("next_offset")
        return (
            isinstance(next_offset, int)
            and not isinstance(next_offset, bool)
            and next_offset > current_offset
        ) or (
            # A retained search pages its reference, not search (#3222, H1):
            # the rows this page could not hold are read with
            # `map(input={"ref": id}, offset=<rows on this page>)`.
            candidate.get("next_offset_reason") == "page_reference"
            and isinstance(candidate.get("result_reference"), dict)
        ) or (
            # A read of a retained live answer whose next page lies past the
            # published offset cap ends its cursor and says so in
            # `paging_limit` (narrow with a predicate); that page is servable.
            isinstance(candidate.get("paging_limit"), dict)
        ) or (
            # `filter` and `map` past the offset cap page by a keyset token
            # (#3666 step 3b), signed after this page's last row.
            operator_name in {"filter", "map"}
            and isinstance(candidate.get("continuation_token"), str)
        )

    def rerun(effective_limit: int) -> tuple[dict[str, Any], bool]:
        """One real execution at `effective_limit`; True when it is servable."""
        retry_arguments = dict(call_arguments)
        retry_arguments["limit"] = effective_limit
        if pin is not None and "generation_id" in signature.parameters:
            retry_arguments["generation_id"] = pin
        candidate = _stamp_byte_truncation(
            fn(**retry_arguments),
            budget=budget,
            requested_limit=requested_limit,
            effective_limit=effective_limit,
        )
        remedy = _truncation_remedy(operator_name, arguments)
        if remedy is not None:
            candidate["truncation_remedy"] = remedy
        if smallest["limit"] is None or effective_limit < smallest["limit"]:
            smallest["limit"], smallest["page"] = effective_limit, candidate
        size = measure(candidate)
        if size > budget:
            # Only a page that did not fit says what a fitting budget is; a
            # page that fit but had no cursor is refused for that, not size.
            smallest_bytes.append(size)
        return candidate, size <= budget and cursor(candidate)

    if passage_rows:
        # D4: head lines alone do not fit. Read again at the largest limit
        # whose head lines fit, shorten that page's text, and serve it with
        # its cursor; never a slice of the first page.
        attrs_selected = _attrs_selected(arguments)
        low, high = 0, min(len(initial["results"]), requested_limit) - 1
        while low < high:
            middle = (low + high + 1) // 2
            page_heads = _shortest_heads(
                _search_page_at(initial, 0, attrs_selected, rows=middle),
            )
            if measure(page_heads) <= budget:
                low = middle
            else:
                high = middle - 1
        # The prefix carries no paging stamps, so it errs long by a few
        # bytes: up to three smaller reruns, a tenth fewer rows each.
        for _ in range(4):
            if low < 1:
                break
            candidate, servable = rerun(low)
            fitted = candidate if servable else _shortened_search_page(
                candidate, arguments, budget=budget, measure=measure,
            )
            if fitted is not None and cursor(fitted):
                return fitted
            low = min(low - 1, low * 9 // 10)
        return _search_rows_refusal(
            initial, arguments, signature, budget=budget,
            default_budget=default_budget, measure=measure,
        )

    low, high = 1, requested_limit - 1
    best: dict[str, Any] | None = None
    predicted = _predict_fitting_limit(
        operator_name,
        initial,
        offset=current_offset,
        ceiling=high,
        budget=budget,
        requested_limit=requested_limit,
        measure=measure,
    )
    if predicted is None:
        # The search below is the documented path for a page shape the
        # prediction cannot read, and the log names the operator, so this is
        # never a quiet change of strategy.
        _log.info(
            "%s: byte-budget paging found no row list on the first page; "
            "searching limits 1..%s by rerun",
            operator_name,
            high,
        )
    else:
        # `rows == 0` means not even one prefix row fit. A rerun at limit 1
        # can still fit (a Nomos-filtered raw window may hold fewer visible
        # rows), so it is tried once before the refusal, never skipped.
        guess = predicted.limit
        candidate, servable = rerun(guess)
        if servable:
            shown = _visible_rows(operator_name, candidate)
            if shown is None or shown >= predicted.rows or guess >= high:
                return candidate
            # Servable but short of the rows the prefix said would fit:
            # hidden raw positions sit inside the predicted window (the
            # consumption ratio is an average). Serving it would hand the
            # caller a short or empty page while fitting rows remain, so
            # search ABOVE the prediction, keeping this page as the floor.
            best = candidate
            low = guess + 1
        else:
            # A miss: search BELOW the prediction only.
            high = guess - 1

    while low <= high:
        effective_limit = (low + high) // 2
        candidate, servable = rerun(effective_limit)
        if servable:
            best = candidate
            low = effective_limit + 1
        else:
            high = effective_limit - 1

    if best is None:
        # The one row to serve cut: the limit-1 rerun when it points past
        # itself, else the first page when it holds the whole set and that
        # set is one row.
        cut_page = None
        if smallest["limit"] == 1 and cursor(smallest["page"]):
            cut_page = smallest["page"]
        elif (
            _visible_rows(operator_name, initial) == 1
            and not initial.get("truncated")
            and initial.get("next_offset") is None
            and not initial.get("continuation_token")
        ):
            cut_page = initial
        return _refuse_or_drop_exact(
            operator_name, initial, budget=budget, measure=measure, single_row=True,
            smallest=smallest["page"], has_cursor=cursor,
            needed=min(smallest_bytes), arguments=arguments, cut_page=cut_page,
            default_budget=default_budget,
        )
    return best
