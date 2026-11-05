"""Substrate MCP tool-call tracing.

Every MCP tool invocation writes one row to the
``okg.mcp_trace_events`` TimescaleDB hypertable. Substrate-native
replacement for the legacy ``okg.observability`` JSONL trace
file.

Design point — tracing is a side effect that MUST NOT break the
tool call. If the trace insert fails (DB down, table missing, JSON
serialization error), the wrapper logs and swallows; the tool
call's result or exception propagates unchanged.

Environment controls:

- ``OKG_MCP_TRACE_ENABLED=0`` — disable tracing entirely. The
  ``make_tracer`` factory returns an identity decorator so there
  is no per-call overhead.
- ``OKG_MCP_SESSION_ID`` — explicitly fix the session id for this server.
  Otherwise the tracer adopts the calling agent's stable session/thread id
  when its runtime exports one, and only falls back to a process-local uuid.
- ``OKG_MCP_SESSION_QUESTION`` — free-text label stamped on every
  row for this process. Used by the navigability/scent-loss
  research to group calls by the question being investigated.

Internal callers can set a per-trace label that overrides the environment
default. The former public ``start_trace`` / ``end_trace`` tools were removed.

A client (a benchmark harness) names the question and try each call serves
on the request itself (issue #3773): the HTTP headers ``X-OKG-Question`` and
``X-OKG-Try``, or the request ``_meta`` keys ``okg/question`` and ``okg/try``
(any transport, stdio included). They are read from the SDK's per-request
context, so concurrent calls never share a label. Precedence: the request's
value, then the in-process label, then ``OKG_MCP_SESSION_QUESTION``. An
invalid value refuses the call (``mcp_question_label_invalid``) and the
refusal is still logged, so a malformed id never becomes an unlabeled or
mislabeled row. See ``call_question`` for the accepted shapes.
"""

from __future__ import annotations

import functools
import inspect
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from psycopg_pool import ConnectionPool

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.operator_models import as_mapping
from okg.substrate.mcp.retry_edits import arguments_only
from okg.substrate.telemetry import (
    TelemetrySpan,
    child_span_identity,
    record_span,
    trace_identity_from_generation,
)

logger = logging.getLogger(__name__)

_COST_MODEL_VERSION = "mcp_tool_cost_v1"
_PLAN_COST_RE = re.compile(
    r"cost=(?P<startup>[0-9]+(?:\.[0-9]+)?)"
    r"\.\.(?P<total>[0-9]+(?:\.[0-9]+)?)"
    r"\s+rows=(?P<rows>[0-9]+)"
)


# Module-level "current trace" state. Set by start_trace, cleared
# by end_trace. The lock is fine-grained (only held while reading
# or mutating the label, never across DB IO) so it can't deadlock
# with the connection pool.
_LABEL_LOCK = threading.Lock()
_CURRENT_LABEL: Optional[str] = None


class TraceAlreadyActiveError(RuntimeError):
    """Raised when start_trace is called while another trace is
    still open. The substrate keeps a single-active-trace; the
    caller must end_trace before starting another. Carries a
    structured `code` so MCP error envelopes can surface it.
    """

    code = "trace_already_active"

    def __init__(self, current_label: str) -> None:
        super().__init__(
            f"trace_already_active: '{current_label}' is in progress; "
            "call end_trace before starting another"
        )
        self.current_label = current_label


def get_current_label() -> Optional[str]:
    """Return the active in-process trace label, or None.

    Read-side accessor. Used by the tracer wrapper to stamp every
    tool call with the current label for as long as a trace is
    open.
    """
    with _LABEL_LOCK:
        return _CURRENT_LABEL


def set_current_label(label: str) -> None:
    """Open a trace bracket. Raises TraceAlreadyActiveError if
    one is already open — the substrate refuses to multiplex.

    Empty/whitespace-only labels are rejected so the trace
    grouping is meaningful.
    """
    global _CURRENT_LABEL
    if not isinstance(label, str) or not label.strip():
        raise ValueError("trace label must be a non-empty string")
    with _LABEL_LOCK:
        if _CURRENT_LABEL is not None:
            raise TraceAlreadyActiveError(_CURRENT_LABEL)
        _CURRENT_LABEL = label


def clear_current_label() -> Optional[str]:
    """Close the trace bracket, returning the label that was
    cleared (or None if no trace was open). Idempotent — calling
    end_trace with no active trace is a no-op rather than an
    error, so a defensive end_trace at the end of an agent's run
    can't fail.
    """
    global _CURRENT_LABEL
    with _LABEL_LOCK:
        prior = _CURRENT_LABEL
        _CURRENT_LABEL = None
        return prior


def _tracing_enabled() -> bool:
    return os.environ.get("OKG_MCP_TRACE_ENABLED", "1") != "0"


def _process_session_id() -> str:
    """Best available stable identity for the agent session.

    An explicit OKG override wins. Agent-runtime identities are next so MCP
    reconnects/server restarts do not split one Codex or Claude session into
    unrelated random rows. A process uuid is the last-resort legacy fallback.
    """
    for name in (
        "OKG_MCP_SESSION_ID",
        "OKG_AGENT_SESSION_ID",
        "CODEX_THREAD_ID",
        "CODEX_SESSION_ID",
        "CLAUDE_SESSION_ID",
        "AGENT_SESSION_ID",
    ):
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    if not hasattr(_process_session_id, "_id"):
        _process_session_id._id = str(uuid.uuid4())  # type: ignore[attr-defined]
    return _process_session_id._id  # type: ignore[attr-defined]


def _process_question_label() -> Optional[str]:
    return os.environ.get("OKG_MCP_SESSION_QUESTION") or None


#: Per-request question context (issue #3773). Headers serve
#: streamable-HTTP clients that can set per-request headers; the ``_meta``
#: keys serve any transport, stdio included.
QUESTION_HEADER = "X-OKG-Question"
TRY_HEADER = "X-OKG-Try"
QUESTION_META_KEY = "okg/question"
TRY_META_KEY = "okg/try"
QUESTION_ID_MAX_CHARS = 200
TRY_NUMBER_MAX = 999_999
_TRY_RE = re.compile(r"[1-9][0-9]{0,5}\Z")


class QuestionLabelInvalid(SubstrateError):
    """A request named its question or try in a shape the log cannot hold.

    The call is refused rather than run unlabeled: a harness that sends a
    label expects every one of its calls to carry it, and a silently dropped
    label is indistinguishable from a call that sent none.
    """

    code = "mcp_question_label_invalid"


@dataclass(frozen=True)
class CallQuestion:
    """The question (and try) one tool call served.

    ``source`` is ``request`` (header or ``_meta``), ``label`` (the
    in-process label) or ``env`` (``OKG_MCP_SESSION_QUESTION``). Only a
    request names a try.
    """

    question_id: str
    try_number: Optional[int]
    source: str

    @property
    def label(self) -> str:
        """The ``mcp_trace_events.question_label`` text: ``<id>#try<n>``."""
        if self.try_number is None:
            return self.question_id
        return f"{self.question_id}#try{self.try_number}"


def _question_refusal(detail: str, *, field: str, channel: str) -> QuestionLabelInvalid:
    # The value is NOT echoed: it is caller-controlled request data and the
    # message reaches logs and evidence artifacts.
    return QuestionLabelInvalid(
        f"call refused: the request's {field} {detail}",
        data={"field": field, "channel": channel},
        fix_hint=(
            f"send {QUESTION_HEADER} (or _meta {QUESTION_META_KEY!r}) as one "
            f"printable-ASCII token of at most {QUESTION_ID_MAX_CHARS} "
            "characters with no '#' or ',', and "
            f"{TRY_HEADER} (or _meta {TRY_META_KEY!r}) as a whole number from "
            f"1 to {TRY_NUMBER_MAX}, or send neither"
        ),
    )


def _checked_question_id(value: Any, *, channel: str) -> str:
    if not isinstance(value, str):
        raise _question_refusal("is not a string", field="question", channel=channel)
    text = value.strip()
    if not text:
        raise _question_refusal("is empty", field="question", channel=channel)
    if len(text) > QUESTION_ID_MAX_CHARS:
        raise _question_refusal(
            f"is longer than {QUESTION_ID_MAX_CHARS} characters",
            field="question", channel=channel,
        )
    if any(not ("!" <= ch <= "~") or ch in "#," for ch in text):
        raise _question_refusal(
            "is not one printable-ASCII token without '#' or ','",
            field="question", channel=channel,
        )
    return text


def _checked_try_number(value: Any, *, channel: str) -> int:
    if isinstance(value, bool):
        raise _question_refusal("is not a whole number", field="try", channel=channel)
    if isinstance(value, int):
        number = value
        if not 1 <= number <= TRY_NUMBER_MAX:
            raise _question_refusal(
                f"is outside 1..{TRY_NUMBER_MAX}", field="try", channel=channel,
            )
        return number
    if isinstance(value, str) and _TRY_RE.match(value.strip()):
        return int(value.strip())
    raise _question_refusal(
        f"is not a whole number from 1 to {TRY_NUMBER_MAX}",
        field="try", channel=channel,
    )


def _single_header(headers: Any, name: str) -> Optional[str]:
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        values = list(getlist(name))
        if len(values) > 1:
            raise _question_refusal(
                f"header {name!r} was sent {len(values)} times",
                field="try" if name == TRY_HEADER else "question",
                channel="header",
            )
    raw = headers.get(name)
    return None if raw is None else str(raw)


def _request_parts() -> tuple[Any, Any]:
    """``(headers, meta_extra)`` of the in-flight MCP request, or Nones.

    Read through the SDK's per-request ``request_ctx`` (the same channel
    ``principal_mapping.current_request_headers`` uses), never a global, so
    two concurrent requests each see their own values.
    """
    try:
        from mcp.server.lowlevel.server import request_ctx
    except Exception:  # pragma: no cover - SDK import shape
        return None, None
    try:
        context = request_ctx.get()
    except LookupError:
        return None, None
    headers = getattr(getattr(context, "request", None), "headers", None)
    meta = getattr(context, "meta", None)
    extra = getattr(meta, "model_extra", None) if meta is not None else None
    return headers, extra


def request_call_question() -> Optional[CallQuestion]:
    """The question and try the in-flight request names, or None.

    Raises ``QuestionLabelInvalid`` for a malformed or ambiguous value: a
    repeated header, a header and ``_meta`` that disagree, a try without a
    question, or a value outside the accepted shape.
    """
    headers, extra = _request_parts()
    found: dict[str, dict[str, Any]] = {"question": {}, "try": {}}
    if headers is not None:
        for field, name in (("question", QUESTION_HEADER), ("try", TRY_HEADER)):
            raw = _single_header(headers, name)
            if raw is not None:
                found[field]["header"] = raw
    if extra:
        for field, key in (("question", QUESTION_META_KEY), ("try", TRY_META_KEY)):
            if key in extra and extra[key] is not None:
                found[field]["meta"] = extra[key]
    question: Optional[str] = None
    try_number: Optional[int] = None
    for channel, raw in found["question"].items():
        checked = _checked_question_id(raw, channel=channel)
        if question is not None and checked != question:
            raise _question_refusal(
                "differs between the header and _meta",
                field="question", channel="header+meta",
            )
        question = checked
    for channel, raw in found["try"].items():
        checked_try = _checked_try_number(raw, channel=channel)
        if try_number is not None and checked_try != try_number:
            raise _question_refusal(
                "differs between the header and _meta",
                field="try", channel="header+meta",
            )
        try_number = checked_try
    if question is None:
        if try_number is not None:
            raise _question_refusal(
                "was sent without a question", field="try",
                channel="+".join(sorted(found["try"])),
            )
        return None
    return CallQuestion(question, try_number, "request")


def call_question() -> Optional[CallQuestion]:
    """The question this call served: request > in-process label > env.

    Raises ``QuestionLabelInvalid`` when the request names one badly; the
    lower-precedence sources are never used to cover a refused request value.
    """
    requested = request_call_question()
    if requested is not None:
        return requested
    label = get_current_label()
    if label:
        return CallQuestion(label, None, "label")
    env_label = _process_question_label()
    if env_label:
        return CallQuestion(env_label, None, "env")
    return None


def _to_jsonb_str(obj: Any) -> Optional[str]:
    """Serialize to JSON, swallowing non-serializable bits.

    Tool args / results are usually dicts of scalars + lists, but
    occasionally carry psycopg rows or custom dataclasses. Best
    effort: default=str lets us stamp a trace even if a value
    isn't JSON-ready.
    """
    if obj is None:
        return None
    try:
        return json.dumps(obj, default=str)
    except Exception:  # noqa: BLE001 — trace payload best-effort; replace with repr stub so tracing never crashes the tool path.
        return json.dumps({"__trace_repr_error__": repr(obj)[:500]})


#: A retained live answer's id (``live_result_sets``). It names a set that
#: holds another system's rows for one reader, so it is never stored in a
#: trace, call-log or telemetry row: each id is replaced by a stable stand-in
#: (a short sha256 of the id), so rows of one session still correlate while
#: the id itself cannot be read back. Graph result-reference ids (``r1.``)
#: are left as they are.
_LIVE_ID = re.compile(
    r"l1\.[A-Za-z0-9_-]{8}\.[0-9]{1,12}\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{22}"
)
LIVE_ID_STAND_IN_PREFIX = "l1.withheld."


def _live_id_stand_in(match: re.Match[str]) -> str:
    digest = hashlib.sha256(match.group(0).encode("ascii")).hexdigest()[:12]
    return LIVE_ID_STAND_IN_PREFIX + digest


def _redact_live_ids(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return _LIVE_ID.sub(_live_id_stand_in, text)


def _stored_json(obj: Any) -> Optional[str]:
    """`_to_jsonb_str` for a stored column: live ids redacted."""
    return _redact_live_ids(_to_jsonb_str(obj))


def _extract_result_count(response: Any) -> Optional[int]:
    """Best-effort extraction of a result count for the
    `okg.mcp_call_log.result_count` column.

    Per `improve-mcp-tool-surface`, every list-returning tool's
    response carries `total_count`; that's the canonical source.
    For tools that don't return a list (e.g. `get_node` returning
    one node), we report None — the column nullably encodes
    "this isn't a counted result."

    `returned_count` wins where an operator reports both. `expand` now
    distinguishes the two — `total_count` is how many edges matched, which on a
    truncated page is far more than it returned — and this column has always
    meant rows returned. Preferring `returned_count` keeps a single meaning in
    the column across the change (PACT fix-expand-pagination-contract).
    """
    if not isinstance(response, dict):
        return None
    rc = response.get("returned_count")
    if isinstance(rc, int):
        return rc
    # Prefer the canonical envelope field.
    tc = response.get("total_count")
    if isinstance(tc, int):
        return tc
    # Fall back to common list-shaped result keys.
    for key in (
        "items",
        "results",
        "neighbors",
        "counts",
        "mentions",
        "suggestions",
        "active",
        "generations",
        "rows",
        "subtypes",
    ):
        v = response.get(key)
        if isinstance(v, list):
            return len(v)
    return None


def _extract_error_code(error: Optional[BaseException]) -> Optional[str]:
    """Lift the structured `code` off a SubstrateError, else None.

    `okg.mcp_call_log.error_code` exists so observation queries
    can group errors without parsing free-text. Untyped exceptions
    get the class name as a poor-man's code; None on success.
    """
    if error is None:
        return None
    code = getattr(error, "code", None)
    if isinstance(code, str) and code:
        return code
    return type(error).__name__


def _json_size_bytes(obj: Any) -> Optional[int]:
    if obj is None:
        return 0
    encoded = _to_jsonb_str(obj)
    if encoded is None:
        return None
    return len(encoded.encode("utf-8"))


def _token_estimate(byte_count: Optional[int]) -> Optional[int]:
    if byte_count is None:
        return None
    # Deliberately rough: this is an MCP response-size proxy for
    # agent-context cost, not a model-specific tokenizer.
    return max(1, (byte_count + 3) // 4) if byte_count else 0


def _extract_plan_cost(summary: Any) -> dict[str, Any]:
    if not isinstance(summary, str):
        return {}
    match = _PLAN_COST_RE.search(summary)
    if match is None:
        return {"query_plan_summary": summary}
    return {
        "query_plan_summary": summary,
        "query_plan_startup_cost": float(match.group("startup")),
        "query_plan_total_cost": float(match.group("total")),
        "query_plan_rows": int(match.group("rows")),
    }


def _extract_call_cost(
    *,
    args: Any,
    response: Any,
    elapsed_ms: Optional[int],
    result_count: Optional[int],
    error_code: Optional[str],
    tool_name: Optional[str] = None,
) -> dict[str, Any]:
    """Return the explicit per-tool-call cost vector.

    The MCP surface has no direct USD charge. The useful optimization
    model is operational and context cost: latency, produced rows,
    truncation, argument/result byte size, rough token estimates, and
    SQL planner estimates when the response came from ``query`` or a
    compiled set operator.
    """
    args_bytes = _json_size_bytes(args)
    response_bytes = _json_size_bytes(response)
    if isinstance(response, dict) and tool_name is not None:
        from okg.substrate.mcp.operators import OPERATOR_TOOLS
        from okg.substrate.mcp.response_bounds import (
            public_call_tool_result_bytes,
        )

        if tool_name in OPERATOR_TOOLS:
            # The served form, in the call's format (issue #3059).
            response_bytes = public_call_tool_result_bytes(
                response, tool=tool_name,
            )
    cost: dict[str, Any] = {
        "model": _COST_MODEL_VERSION,
        "elapsed_ms": elapsed_ms,
        "result_count": result_count,
        "error_code": error_code,
        "args_bytes": args_bytes,
        "args_token_estimate": _token_estimate(args_bytes),
        "response_bytes": response_bytes,
        "response_token_estimate": _token_estimate(response_bytes),
    }
    if isinstance(response, dict):
        truncated = response.get("truncated")
        if isinstance(truncated, bool):
            cost["truncated"] = truncated
        rows = response.get("rows")
        if isinstance(rows, list):
            cost["row_count"] = len(rows)
        cost.update(_extract_plan_cost(response.get("query_plan_summary")))
    return {k: v for k, v in cost.items() if v is not None}


def log_call_to_log(
    pool: ConnectionPool,
    *,
    deployment_name: str,
    tool_name: str,
    args: Any,
    pin: Optional[int | str],
    elapsed_ms: Optional[int],
    result_count: Optional[int],
    error_code: Optional[str],
    response: Any = None,
    session_id: Optional[str] = None,
    cost: Mapping[str, Any] | None = None,
    question: Optional[CallQuestion] = None,
) -> None:
    """Write one row to ``okg.mcp_call_log`` — the leaner cousin
    of ``okg.mcp_trace_events``.

    ``question`` fills ``question_id`` / ``try_number`` (issue #3773). The
    caller resolves it (``call_question``); None stores NULLs.

    Per `improve-mcp-tool-surface` (2026-04-27, task 3.4): one
    row per MCP tool invocation, fire-and-forget. Failure logs
    a warning and swallows; the tool call's result/exception
    propagates unchanged.

    The args column carries the agent's call kwargs only (not
    the full request envelope, not the response). This is the
    minimum needed to recognize a recurring primitive
    composition for the future macro-materialization rule:
    "this filter shape was observed N times across K sessions."
    """
    if not _tracing_enabled():
        return
    operator_name: Optional[str] = None
    replacement_operator: Optional[str] = None
    try:
        from okg.substrate.mcp.operators import classify_tool

        operator_name, replacement_operator = classify_tool(tool_name)
    except Exception:  # noqa: BLE001 — classify_tool is best-effort; missing operator-name leaves the trace unclassified, not unwritten.
        pass
    cost_payload = (
        dict(cost)
        if cost is not None
        else _extract_call_cost(
            args=args,
            response=response,
            elapsed_ms=elapsed_ms,
            result_count=result_count,
            error_code=error_code,
            tool_name=tool_name,
        )
    )
    try:
        # Use a short, separate connection so the log write
        # doesn't share a transaction with the tool call.
        # `app_rw` (the substrate-internal role) writes via the
        # pool's privileged connection.
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.mcp_call_log (
                      session_id, deployment_name, tool_name, args, pin,
                      elapsed_ms, result_count, error_code,
                      operator_name, replacement_operator, cost,
                      question_id, try_number
                    ) VALUES (
                      %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s,
                      %s::jsonb, %s, %s
                    )
                    """,
                    (
                        session_id or _process_session_id(),
                        deployment_name,
                        tool_name,
                        _stored_json(args) or "{}",
                        None if pin is None else str(pin),
                        elapsed_ms,
                        result_count,
                        error_code,
                        operator_name,
                        replacement_operator,
                        _to_jsonb_str(cost_payload) or "{}",
                        None if question is None else question.question_id,
                        None if question is None else question.try_number,
                    ),
                )
    except Exception as log_err:
        logger.warning(
            "mcp_call_log write failed for %s: %s",
            tool_name,
            log_err,
        )


#: ``trace_tool_call``'s default: read ``OKG_MCP_SESSION_QUESTION``.
_ENV_QUESTION_LABEL: Any = object()


def trace_tool_call(
    pool: ConnectionPool,
    *,
    deployment_name: str,
    tool_name: str,
    request: Any,
    response: Any,
    error: Optional[BaseException] = None,
    duration_ms: Optional[int] = None,
    session_id: Optional[str] = None,
    generation_id: Optional[int | str] = None,
    question_label: Any = _ENV_QUESTION_LABEL,
    branch_name: Optional[str] = None,
    route_id: Optional[str] = None,
    route_manifest_checksum: Optional[str] = None,
) -> None:
    """Write one row to ``okg.mcp_trace_events``.

    Swallows its own exceptions — tracing failure never breaks
    the tool call.

    ``question_label`` defaults to ``OKG_MCP_SESSION_QUESTION``; an explicit
    value, None included, is written as given.

    ``route_id`` / ``route_manifest_checksum`` (AKMON Phase 6.2)
    attribute this tool call to the route recommendation that led
    to it. Callers obtain the values from a prior ``agent_context``
    response's ``recommended_routes[i].route_id`` and
    ``route_manifest_checksum``. Untagged calls (legacy / ad-hoc)
    pass None.
    """
    if not _tracing_enabled():
        return
    effective_session_id = session_id or _process_session_id()
    # Left unset, the label falls back to OKG_MCP_SESSION_QUESTION; the
    # tracer wrapper passes the label it resolved (``call_question``), where
    # None means the call served no named question.
    effective_question_label = (
        _process_question_label()
        if question_label is _ENV_QUESTION_LABEL
        else question_label
    )
    effective_branch_name = branch_name or "default"
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.mcp_trace_events (
                      session_id, deployment_name, generation_id,
                      tool_name, question_label,
                      request_json, response_json, error_text,
                      duration_ms, branch_name,
                      route_id, route_manifest_checksum
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s)
                    """,
                    (
                        effective_session_id,
                        deployment_name,
                        None if generation_id is None else str(generation_id),
                        tool_name,
                        effective_question_label,
                        _stored_json(request),
                        _stored_json(response),
                        _redact_live_ids(
                            f"{type(error).__name__}: {error}" if error else None
                        ),
                        duration_ms,
                        effective_branch_name,
                        route_id,
                        route_manifest_checksum,
                    ),
                )
    except Exception as trace_err:
        logger.warning(
            "MCP trace write failed for %s: %s",
            tool_name,
            trace_err,
        )
    try:
        with pool.connection() as conn:
            _write_unified_mcp_span(
                conn,
                deployment_name=deployment_name,
                tool_name=tool_name,
                request=request,
                response=response,
                error=error,
                duration_ms=duration_ms,
                session_id=effective_session_id,
                generation_id=generation_id,
                question_label=effective_question_label,
                branch_name=effective_branch_name,
                route_id=route_id,
            )
    except Exception as trace_err:
        logger.warning(
            "Unified MCP telemetry write failed for %s: %s",
            tool_name,
            trace_err,
        )


def _write_unified_mcp_span(
    conn: Any,
    *,
    deployment_name: str,
    tool_name: str,
    request: Any,
    response: Any,
    error: Optional[BaseException],
    duration_ms: Optional[int],
    session_id: str,
    generation_id: Optional[int | str],
    question_label: Optional[str],
    branch_name: str,
    route_id: Optional[str],
) -> None:
    """Mirror one MCP call into unified telemetry and configured samples."""
    identity = child_span_identity()
    if not identity.is_contextual:
        identity = (
            trace_identity_from_generation(
                conn,
                deployment_name=deployment_name,
                generation_id=generation_id,
            )
            or identity
        )
    ended_at = datetime.now(timezone.utc)
    started_at = (
        ended_at - timedelta(milliseconds=duration_ms)
        if duration_ms is not None
        else ended_at
    )
    result_count = _extract_result_count(response)
    error_code = _extract_error_code(error)
    cost = _extract_call_cost(
        args=request,
        response=response,
        elapsed_ms=duration_ms,
        result_count=result_count,
        error_code=error_code,
        tool_name=tool_name,
    )
    record_span(
        conn,
        TelemetrySpan(
            trace_id=identity.trace_id,
            span_id=identity.span_id,
            parent_span_id=identity.parent_span_id,
            span_name=f"mcp.{tool_name}",
            span_kind="server",
            status_code="error" if error is not None else "ok",
            status_message=_redact_live_ids(
                f"{type(error).__name__}: {error}" if error else None
            ),
            deployment_name=deployment_name,
            branch_name=branch_name,
            generation_id=None if generation_id is None else str(generation_id),
            tool_name=tool_name,
            route_id=route_id,
            session_id=session_id,
            started_at=started_at,
            ended_at=ended_at,
            attributes={
                "component": "mcp",
                "question_label": question_label,
                "result_count": result_count,
                "error_code": error_code,
            },
            payload_summary=cost,
            root_name=f"mcp.{tool_name}",
            root_kind="mcp_call",
        ),
    )
    record_call_sample_for_tool_call(
        conn,
        deployment_name=deployment_name,
        tool_name=tool_name,
        request=request,
        response=response,
        error=error,
        trace_id=identity.trace_id,
        session_id=session_id,
    )


def record_call_sample_for_tool_call(
    conn: Any,
    *,
    deployment_name: str,
    tool_name: str,
    request: Any,
    response: Any,
    error: Optional[BaseException],
    trace_id: str,
    session_id: str,
    registry: Any = None,
) -> tuple[Any, ...]:
    """Leave call samples for a node-targeted call, per its relation policy.

    Issue #2011 (`call-sample-policy`). Returns the samples written, which is
    an empty tuple whenever nothing was to be written.

    Three gates, each with a visible outcome:

    1. The call must name at least one node. `search` and the other
       whole-graph operators name none and are silently skipped -- correctly,
       there is no node the sample would belong to. A call that IS node-
       targeted but whose anchor cannot be read is a gap, and is logged with
       the tool name and the argument SHAPE (never the values).
    2. The relation's policy must be a GRAPH-READ policy. This wrapper has no
       authorization decision of its own; it can only supply the graph-read
       default (the runtime principal, `allow`). Stamping that on a live
       relation would record a Nomos deny as an allow, so a live relation
       reaching here is skipped with a warning naming it. Its sample is
       written by the dispatcher, which holds the real decision, through
       `record_call_sample`.
    3. The policy must actually store something (`none` stores nothing, and is
       the default for every undeclared relation).

    A multi-node anchor writes ONE SAMPLE PER NODE, so an answer stays
    attributable to the node it came from.

    A failed write is logged at error level and swallowed, because tracing
    must never break a tool call. It is never turned into a written sample
    with invented values.
    """
    from okg.substrate.external_backed import call_sample as call_sample_module

    kwargs = request.get("kwargs") if isinstance(request, Mapping) else None
    args = request.get("args") if isinstance(request, Mapping) else None
    args = tuple(args) if isinstance(args, (list, tuple)) else ()

    # Everything before the write loop is fenced separately. It runs inside
    # the unified-telemetry writer, whose own handler would otherwise report a
    # CallSampleError from here as "Unified MCP telemetry write failed" -- a
    # message naming the wrong subsystem.
    try:
        target = call_sample_module.resolve_call_target(
            tool_name, args=args, kwargs=kwargs
        )
        if not target.node_ids:
            if target.node_targeted:
                logger.warning(
                    "call sample skipped: %s is node-targeted but no node id "
                    "could be read from its arguments (%s)",
                    tool_name,
                    target.arg_shape,
                )
            return ()

        relation = call_sample_module.relation_for_call(tool_name, kwargs=kwargs)
        policy = (registry or call_sample_module.default_registry()).policy_for(
            relation
        )
        if not policy.graph_read:
            logger.warning(
                "call sample skipped: relation %s is a live relation, and the "
                "tracing wrapper has no authorization decision to record; its "
                "dispatcher writes the sample",
                relation,
            )
            return ()
        if not policy.writes:
            return ()

        identity = call_sample_module.graph_read_identity(
            trace_id=trace_id, session_id=session_id
        )
        # The stored re-fetch pointer carries the argument SHAPE, plus only the
        # argument values the relation named in refetch_query_keys. A caller's
        # kwargs are not copied wholesale into a row that lives for up to 730
        # days.
        refetch = call_sample_module.refetch_pointer_for_call(
            policy,
            kwargs=kwargs,
            arg_shape=target.arg_shape,
            source=f"mcp:{tool_name}",
        )
    except Exception as setup_err:
        logger.error(
            "call sample skipped for %s: %s", tool_name, setup_err
        )
        return ()

    status = "ok" if error is None else "error"
    written: list[Any] = []
    for node_id in target.node_ids:
        try:
            sample = call_sample_module.build_sample(
                deployment_name=deployment_name,
                node_id=node_id,
                relation=relation,
                policy=policy,
                identity=identity,
                status=status,
                payload=response,
                refetch=refetch,
                tool_name=tool_name,
            )
            if sample is None:
                continue
            written.append(call_sample_module.record_call_sample(conn, sample))
        except Exception as sample_err:
            logger.error(
                "call sample write failed for relation %s on %s: %s",
                relation,
                node_id,
                sample_err,
            )
    return tuple(written)


def make_tracer(
    pool: ConnectionPool,
    *,
    deployment_name: str,
    branch_name: str = "default",
) -> Callable[[str], Callable]:
    """Build a decorator factory bound to this server's pool +
    deployment.

    Usage inside ``build_server``::

        traced = make_tracer(pool, deployment_name="default")

        @mcp.tool()
        @traced("graph_stats")
        def graph_stats(...): ...

    Returns an identity decorator when tracing is disabled, so
    the overhead is a single env-var check per server boot.
    """
    if not _tracing_enabled():

        def identity(tool_name: str):
            def decorator(fn):
                return fn

            return decorator

        return identity

    def factory(tool_name: str):
        def decorator(fn: Callable) -> Callable:
            parameters = tuple(inspect.signature(fn).parameters)
            context_position = parameters.index("ctx") if "ctx" in parameters else None

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                # FastMCP injects its Context object into core read wrappers.
                # It is transport state, not a caller argument, and may retain
                # request/session internals; never serialize it into traces.
                # The SDK also hands validated operator models to this wrapper
                # before the tool body converts them back to caller-shaped
                # mappings. Normalize only the trace copies: the callable and
                # byte-budget retry still receive the original bound values.
                trace_kwargs = {
                    key: as_mapping(value)
                    for key, value in kwargs.items()
                    if key != "ctx"
                }
                trace_args = tuple(
                    as_mapping(value)
                    for index, value in enumerate(args)
                    if index != context_position
                )
                if arguments_only() or _skip_trace(
                    tool_name, args=trace_args, kwargs=trace_kwargs,
                ):
                    # A body run only to check an offered call's arguments
                    # is no tool call (PACT read-refusal-retry-calls): it
                    # writes no trace or call-log row.
                    return fn(*args, **kwargs)
                start = time.monotonic()
                generation_id = kwargs.get("generation_id")
                # Resolve the question at call entry (issue #3773): the
                # request's header or _meta, else the in-process label, else
                # OKG_MCP_SESSION_QUESTION. The request value comes from the
                # SDK's per-request context, so a concurrent call never sees
                # another call's question.
                try:
                    question = call_question()
                except QuestionLabelInvalid as refusal:
                    # Refused before the tool runs, and still logged with
                    # its reason, so a bad label is visible in both tables
                    # and is never written as an unlabeled or env-labeled
                    # row.
                    trace_tool_call(
                        pool,
                        deployment_name=deployment_name,
                        tool_name=tool_name,
                        request={"args": trace_args, "kwargs": trace_kwargs},
                        response=None,
                        error=refusal,
                        duration_ms=0,
                        generation_id=generation_id,
                        question_label=None,
                        branch_name=branch_name,
                    )
                    log_call_to_log(
                        pool,
                        deployment_name=deployment_name,
                        tool_name=tool_name,
                        args=trace_kwargs,
                        response=None,
                        pin=generation_id,
                        elapsed_ms=0,
                        result_count=None,
                        error_code=refusal.code,
                        question=None,
                    )
                    raise
                captured_label = None if question is None else question.label
                try:
                    result = fn(*args, **kwargs)
                except BaseException as e:
                    duration_ms = int((time.monotonic() - start) * 1000)
                    trace_tool_call(
                        pool,
                        deployment_name=deployment_name,
                        tool_name=tool_name,
                        request={"args": trace_args, "kwargs": trace_kwargs},
                        response=None,
                        error=e,
                        duration_ms=duration_ms,
                        generation_id=generation_id,
                        question_label=captured_label,
                        branch_name=branch_name,
                    )
                    # Per `improve-mcp-tool-surface` task 3.4 —
                    # also write the leaner observation row.
                    log_call_to_log(
                        pool,
                        deployment_name=deployment_name,
                        tool_name=tool_name,
                        args=trace_kwargs,
                        response=None,
                        pin=generation_id,
                        elapsed_ms=duration_ms,
                        result_count=None,
                        error_code=_extract_error_code(e),
                        question=question,
                    )
                    raise
                duration_ms = int((time.monotonic() - start) * 1000)
                # If the response carries a generation_id (it usually does),
                # prefer the effective read pin resolved by the session layer.
                effective_gen = generation_id
                if isinstance(result, dict):
                    effective_gen = (
                        result.get("pinned_generation_id")
                        or result.get("generation_id")
                        or generation_id
                    )
                trace_tool_call(
                    pool,
                    deployment_name=deployment_name,
                    tool_name=tool_name,
                    request={"args": trace_args, "kwargs": trace_kwargs},
                    response=result,
                    duration_ms=duration_ms,
                    generation_id=effective_gen,
                    question_label=captured_label,
                    branch_name=branch_name,
                )
                # Per `improve-mcp-tool-surface` task 3.4 — also
                # write the leaner observation row. `args` is the
                # agent's kwargs only (no positional sentinels),
                # `pin` comes from the effective response envelope when
                # available; `result_count` is derived from that envelope.
                log_call_to_log(
                    pool,
                    deployment_name=deployment_name,
                    tool_name=tool_name,
                    args=trace_kwargs,
                    response=result,
                    pin=effective_gen,
                    elapsed_ms=duration_ms,
                    result_count=_extract_result_count(result),
                    error_code=None,
                    question=question,
                )
                return result

            return wrapper

        return decorator

    return factory


def _skip_trace(
    tool_name: str,
    *,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> bool:
    """Keep process-local introspection independent of deployment DB health."""
    if tool_name != "inspect":
        return False
    target = kwargs.get("target")
    if target is None and args and isinstance(args[0], dict):
        target = args[0]
    return isinstance(target, dict) and target.get("kind") == "surface"
