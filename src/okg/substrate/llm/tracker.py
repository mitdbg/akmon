"""Per-call LLM token + cost tracking.

One ``okg.llm_calls`` row per outgoing LLM call. Best-effort: a DB
failure logs ``WARNING`` and is dropped — the user-visible LLM call
must succeed even if telemetry cannot be persisted (per
``add-substrate-llm-call-tracking`` design.md).

Cost extraction targets the OpenRouter-via-litellm path: OpenRouter
returns ``usage.cost`` (USD) in its response body when ``usage:
{include: true}`` is set on the request, and litellm's OpenRouter
handler stashes the value at
``model_response._hidden_params['additional_headers']
['llm_provider-x-litellm-response-cost']``. Token counts come from
the standard ``model_response.usage`` object.

Other providers may leave any subset of these fields empty; the
extractor returns ``None`` per missing field and the inserted row
records ``NULL`` columns. The ``okg.llm_call_stats`` view's
``SUM(...)`` aggregates ignore NULLs by SQL semantics.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import logging
import threading
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from okg.substrate.telemetry import (
    TelemetrySpan,
    child_span_identity,
    record_span,
    trace_identity_from_generation,
)
from okg.substrate.db.connection import connect as db_connect

logger = logging.getLogger(__name__)


class _Counter:
    """Process-local counter for telemetry rows dropped to budget exhaustion."""

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> None:
        with self._lock:
            self._value += 1

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0


_BUDGET_DROPS = _Counter()
_WRITE_FAILURES = _Counter()


class TelemetryBudgetExhausted(RuntimeError):
    """This process ran out of its OWN declared telemetry budget.

    Deliberately distinct from ``PoolTimeout``. psycopg_pool raises
    ``PoolTimeout`` both when the pool is genuinely contended AND when the
    database is unreachable (failed background connection attempts surface to
    waiting clients that way). Reusing it for slot exhaustion meant a crashed
    Postgres logged "this is not a database fault" on every telemetry write —
    an explicit misdiagnosis pointing the operator at the wrong repair.
    """


#: The slot semaphore paired with the width it was built for. One global, not
#: two: the width is only ever meaningful as a property of the semaphore beside
#: it, and splitting them left the width assigned in a branch that never read it
#: back within the same call — indistinguishable, to a reader or to a static
#: analyzer, from a dead store.
_TELEMETRY_SLOTS: tuple[int, threading.BoundedSemaphore] | None = None
_SLOTS_LOCK = threading.Lock()
#: How long a telemetry write waits for a slot before giving up. Short by
#: design: telemetry must never become backpressure on the model-call lane.
_SLOT_WAIT_SECONDS = 2.0
#: How long it then waits for a pooled connection, for the same reason.
_POOL_WAIT_SECONDS = 5.0


def _telemetry_slots() -> threading.BoundedSemaphore:
    """The slot semaphore, rebuilt when the declared width changes.

    Caching the semaphore at first use froze whatever width was current then —
    so a manifest override applied later was silently ignored, and the cached
    object also leaked across tests in one process. That is the same
    silently-diverging-from-the-declaration defect this program exists to
    remove, so the width is re-read and the semaphore rebuilt on change.
    """
    global _TELEMETRY_SLOTS
    from okg.substrate.db.concurrency_budget import width

    current = width("llm.telemetry")
    with _SLOTS_LOCK:
        if _TELEMETRY_SLOTS is None or _TELEMETRY_SLOTS[0] != current:
            _TELEMETRY_SLOTS = (current, threading.BoundedSemaphore(current))
        return _TELEMETRY_SLOTS[1]


@contextmanager
def _telemetry_connection(dsn: str):
    """Yield a pooled connection for the telemetry write.

    Two bounds, for two different reasons. The `llm.telemetry` semaphore caps
    how much of the shared pool telemetry may hold at once, so a wide
    model-call fan-out cannot let its telemetry starve the publish cycle it
    runs inside — the pool is shared with the gate and the publisher, and a
    PoolTimeout there is far more expensive than a dropped metrics row. The
    pool itself then bounds total connections.

    Falls back to a direct connect ONLY when the pool cannot be built at all
    (no pool machinery in this process, e.g. a narrow unit-test context). That
    fallback is deliberately NOT extended to exhaustion: quietly opening a
    direct connection when the budget is spent would defeat the accounting
    this lane was moved onto, turning a bounded lane unbounded at exactly the
    moment it is under pressure.
    """
    try:
        from okg.substrate.db.pool import get_pool
    except ImportError:
        # No pool machinery in this process at all. This is the ONLY condition
        # that may fall back to a direct connect.
        with db_connect(
            dsn, autocommit=True, application_name="okg-llm-telemetry-direct",
        ) as conn:
            yield conn
        return

    # Deliberately NOT wrapped in `except Exception`. An earlier version was,
    # and it swallowed BudgetError — so a misdeclared width, which the registry
    # raises loudly precisely so it cannot pass unnoticed, would have been
    # converted into silent per-call direct connections: the exact unbounded,
    # unaccounted behavior this lane was moved onto the pool to end, triggered
    # at the moment the configuration is broken.
    pool = get_pool(dsn, application_name="okg-llm-telemetry")

    slots = _telemetry_slots()
    if not slots.acquire(timeout=_SLOT_WAIT_SECONDS):
        raise TelemetryBudgetExhausted(
            f"telemetry could not acquire an `llm.telemetry` slot within "
            f"{_SLOT_WAIT_SECONDS}s"
        )
    try:
        # An explicit short timeout. The slot bounds only the FIRST wait;
        # psycopg_pool's default (~30s) would let a contended pool turn each
        # telemetry write into a ~30s stall inside the synchronous model-call
        # path — telemetry becoming backpressure on the lane it observes,
        # which is precisely what this module must never be.
        with pool.connection(timeout=_POOL_WAIT_SECONDS) as conn:
            yield conn
    finally:
        slots.release()


def _attr_or_key(obj: Any, name: str) -> Any:
    """Read ``name`` from either an attribute or a dict key."""
    v = getattr(obj, name, None)
    if v is None and isinstance(obj, dict):
        v = obj.get(name)
    return v


def _maybe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def extract_usage(response: Any) -> dict:
    """Pull every useful field from a litellm ``ModelResponse``.

    Returns a dict with first-class fields used by the
    ``okg.llm_calls`` columns plus an ``extras`` sub-dict suitable
    for the JSONB column. All fields are best-effort: providers may
    omit any of them. Never raises — extraction failure is logged.

    First-class fields:
      ``prompt_tokens``, ``completion_tokens``, ``total_tokens``,
      ``cost_usd``.

    Cost extraction order:
      1. OpenRouter-via-litellm: ``_hidden_params['additional_headers']
         ['llm_provider-x-litellm-response-cost']``.
      2. litellm's own cost calculator output, surfaced as
         ``_hidden_params['response_cost']`` (set by litellm
         middleware for providers it knows how to price —
         Anthropic, OpenAI, Bedrock, Gemini, Vertex, …).
      3. Fall back to calling ``litellm.completion_cost(response)``
         which uses litellm's embedded rate cards for the same
         providers without depending on middleware setup.
      4. ``None`` if all of the above fail.

    ``extras`` collects the non-first-class fields worth keeping:
      ``reasoning_tokens``, ``cached_tokens`` (prompt-cache reads),
      ``cache_creation_tokens`` / ``cache_read_input_tokens``
      (Anthropic prompt-cache writes / reads),
      ``audio_tokens``, ``text_tokens``, ``image_tokens``,
      ``custom_llm_provider``, ``response_id``,
      ``system_fingerprint``, ``response_model`` (model returned by
      provider, may differ from request model after litellm
      routing), ``cache_hit`` (litellm's response-cache hit flag).
    """
    out: dict = {
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
        "cost_usd": None,
        "extras": {},
    }
    extras = out["extras"]

    try:
        usage = getattr(response, "usage", None)
        if usage is not None:
            out["prompt_tokens"] = _maybe_int(_attr_or_key(usage, "prompt_tokens"))
            out["completion_tokens"] = _maybe_int(_attr_or_key(usage, "completion_tokens"))
            out["total_tokens"] = _maybe_int(_attr_or_key(usage, "total_tokens"))

            # Anthropic-style prompt caching (top-level on Usage).
            for k in ("cache_creation_tokens", "cache_read_input_tokens",
                      "cache_creation_input_tokens"):
                v = _maybe_int(_attr_or_key(usage, k))
                if v is not None:
                    extras[k] = v

            # prompt_tokens_details — cached_tokens, audio_tokens.
            ptd = _attr_or_key(usage, "prompt_tokens_details")
            if ptd is not None:
                for k in ("cached_tokens", "audio_tokens"):
                    v = _maybe_int(_attr_or_key(ptd, k))
                    if v is not None:
                        extras[f"prompt_{k}"] = v

            # completion_tokens_details — reasoning_tokens (huge on
            # thinking models), text_tokens, audio_tokens,
            # image_tokens.
            ctd = _attr_or_key(usage, "completion_tokens_details")
            if ctd is not None:
                for k in ("reasoning_tokens", "text_tokens",
                          "audio_tokens", "image_tokens"):
                    v = _maybe_int(_attr_or_key(ctd, k))
                    if v is not None:
                        extras[f"completion_{k}"] = v

        # Hidden params: cost (multiple sources) + provenance.
        hidden = getattr(response, "_hidden_params", None) or {}
        if not isinstance(hidden, dict):
            hidden = {}

        # 1. OpenRouter-specific header (most accurate when present).
        headers = hidden.get("additional_headers") or {}
        if isinstance(headers, dict):
            v = headers.get("llm_provider-x-litellm-response-cost")
            if v is not None:
                try:
                    out["cost_usd"] = float(v)
                except (TypeError, ValueError):
                    logger.debug("could not parse LLM response cost", exc_info=True)

        # 2. litellm's own cost calculator output (set by
        #    middleware for known providers).
        if out["cost_usd"] is None:
            v = hidden.get("response_cost")
            if v is not None:
                try:
                    out["cost_usd"] = float(v)
                except (TypeError, ValueError):
                    # Cost metadata is advisory; leave it unknown if parsing fails.
                    pass

        # 3. Last-resort: ask litellm to compute it now from the
        #    response. Works for any provider with an embedded rate
        #    card. Wrapped in try/except because some response
        #    shapes (e.g. test mocks) don't satisfy the helper's
        #    expectations.
        if out["cost_usd"] is None:
            try:
                import litellm  # local import; tracker stays import-light

                v = litellm.completion_cost(completion_response=response)
                if v is not None:
                    out["cost_usd"] = float(v)
            except Exception as exc:  # noqa: BLE001
                # `fix-substrate-silent-failures` Phase 1.5: per-call cost
                # extraction failure is observability degradation, not a
                # correctness issue. We must NOT raise — the LLM call's
                # user-visible result needs to flow back to the caller.
                # But we surface the failure at ERROR so it shows up in
                # operator logs without a stdout scrape.
                logger.error(
                    "completion_cost extraction failed: %s (model=%r)",
                    exc, getattr(response, "model", None),
                )

        # Provenance + debug fields → extras.
        for k in ("custom_llm_provider", "cache_hit", "api_base",
                  "model_id"):
            v = hidden.get(k)
            if v is not None:
                extras[k] = v

        # Top-level response fields worth keeping.
        for src_attr, dst_key in (
            ("id", "response_id"),
            ("system_fingerprint", "system_fingerprint"),
            ("model", "response_model"),
        ):
            v = getattr(response, src_attr, None)
            if v is not None:
                extras[dst_key] = v

    except Exception as exc:  # never let extraction break the LLM call
        # `fix-substrate-silent-failures` Phase 1.5: bumped to ERROR.
        # Silent telemetry loss is what makes operator-visible cost
        # spikes invisible until the bill arrives.
        logger.error("extract_usage failed: %s", exc)

    return out


def record_llm_call(
    dsn: str,
    *,
    caller: str,
    model: str,
    fragment_id: str | None,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    total_tokens: int | None,
    cost_usd: float | None,
    latency_ms: int | None,
    success: bool,
    error_kind: str | None = None,
    deployment: str | None = None,
    generation_id: int | str | None = None,
    extras: dict | None = None,
    outcome_key_digest: str | None = None,
) -> None:
    """Insert one row into ``okg.llm_calls``.

    Best-effort. On any DB failure, logs and returns; never raises. The
    contract is that the LLM call's user-visible result (the message
    content) must be returned to the caller regardless of telemetry
    success.

    Borrows from the process-shared pool rather than opening its own
    connection. It previously called ``psycopg.connect`` per model call —
    fine while every model-call site was serial, but under a fan-out of
    width N that is N concurrent connections outside every pool and
    outside every budget. The `llm.telemetry` term accounts for it.

    One failure mode is deliberately NOT folded into the generic
    best-effort path: a drop caused by pool exhaustion is logged as its
    own condition and counted, because it is silent in a specific and
    damaging way — the model result returns normally while the row that
    carries ``outcome_key_digest`` never lands, breaking the join between
    a call and its recorded outcome. An operator reading "telemetry is
    behind" needs to know whether the database rejected the write or
    whether the process ran out of its own connection budget; those have
    different fixes.
    """
    # D2: okg.llm_calls.generation_id is the TEXT checkpoint ref, the same
    # value telemetry records. The former bigint coercion silently stored
    # NULL for every Chronos checkpoint ref (they are not numeric), which is
    # what broke the documented work_ledger <-> llm_calls cost join.
    telemetry_generation_id = (
        None if generation_id is None else str(generation_id)
    )
    try:
        with _telemetry_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.llm_calls
                        (deployment_name, caller, model, fragment_id,
                         prompt_tokens, completion_tokens, total_tokens,
                         cost_usd, latency_ms, success, error_kind,
                         generation_id, extras, outcome_key_digest)
                    VALUES
                        (%s, %s, %s, %s,
                         %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s)
                    """,
                    (
                        deployment, caller, model, fragment_id,
                        prompt_tokens, completion_tokens, total_tokens,
                        cost_usd, latency_ms, success, error_kind,
                        telemetry_generation_id,
                        Jsonb(extras) if extras else None,
                        outcome_key_digest,
                    ),
                )
            try:
                identity = child_span_identity()
                if (
                    not identity.is_contextual
                    and deployment is not None
                    and telemetry_generation_id is not None
                ):
                    identity = (
                        trace_identity_from_generation(
                            conn,
                            deployment_name=deployment,
                            generation_id=telemetry_generation_id,
                        )
                        or identity
                    )
                ended_at = datetime.now(timezone.utc)
                started_at = (
                    ended_at - timedelta(milliseconds=latency_ms)
                    if latency_ms is not None else ended_at
                )
                deployment_name = deployment or "unknown"
                record_span(
                    conn,
                    TelemetrySpan(
                        trace_id=identity.trace_id,
                        span_id=identity.span_id,
                        parent_span_id=identity.parent_span_id,
                        span_name=f"llm.{caller}",
                        span_kind="client",
                        status_code="ok" if success else "error",
                        status_message=error_kind,
                        deployment_name=deployment_name,
                        generation_id=telemetry_generation_id,
                        started_at=started_at,
                        ended_at=ended_at,
                        attributes={
                            "component": "llm",
                            "caller": caller,
                            "model": model,
                            "fragment_id": fragment_id,
                            "deployment_unavailable_reason": (
                                "record_llm_call.deployment_not_supplied"
                                if deployment is None else None
                            ),
                        },
                        payload_summary={
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": total_tokens,
                            "cost_usd": cost_usd,
                            "latency_ms": latency_ms,
                            "success": success,
                            "error_kind": error_kind,
                            "extras": extras or {},
                        },
                        unavailable_reason=(
                            "llm_call.partial_context"
                            if deployment is None else None
                        ),
                        root_name=f"llm.{caller}",
                        root_kind="llm_call",
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - keep LLM ledger best-effort.
                _WRITE_FAILURES.increment()
                logger.warning(
                    "unified telemetry span write failed "
                    "(event=llm_tracking_write_failed caller=%r model=%r): %s",
                    caller, model, exc,
                )
    except TelemetryBudgetExhausted as exc:
        # This branch is ONLY reached when the slot semaphore timed out, which
        # is unambiguously this process overspending its own declared width.
        _BUDGET_DROPS.increment()
        _WRITE_FAILURES.increment()
        # The digest VALUE is interpolated, not merely named. An earlier
        # version put the words "outcome_key_digest" in the format string and
        # nothing else — which read as though the digest were recorded, and a
        # test asserting that substring passed against the constant text
        # forever. An operator repairing the call-to-outcome join needs the
        # value; the word is worthless.
        logger.error(
            "llm_calls insert DROPPED — telemetry could not acquire an "
            "`llm.telemetry` slot within %.1fs (caller=%r model=%r "
            "fragment=%r outcome_key_digest=%s, drops this process=%d): the "
            "model result was returned to the caller but its okg.llm_calls row "
            "is LOST, so this call cannot be joined to its recorded outcome. "
            "This is connection-budget exhaustion, not a database fault — "
            "widen `db.shared_pool`/`llm.telemetry` or narrow the fanning-out "
            "lane. Underlying: %s",
            _SLOT_WAIT_SECONDS, caller, model, fragment_id,
            outcome_key_digest or "<none>", _BUDGET_DROPS.value, exc,
        )
    except PoolTimeout as exc:
        # AMBIGUOUS on purpose. psycopg_pool raises this both for genuine pool
        # contention and for an unreachable database, and from here the two are
        # indistinguishable. Say so rather than asserting a diagnosis: claiming
        # "not a database fault" here would send an operator to widen a pool
        # while Postgres is down.
        _WRITE_FAILURES.increment()
        logger.error(
            "llm_calls insert failed (event=llm_tracking_write_failed) — "
            "no pooled connection within %.1fs "
            "(caller=%r model=%r outcome_key_digest=%s). This is EITHER pool "
            "contention from a wide lane OR an unreachable database; check "
            "database reachability before re-sizing anything. Underlying: %s",
            _POOL_WAIT_SECONDS, caller, model,
            outcome_key_digest or "<none>", exc,
        )
    except Exception as exc:
        # `fix-substrate-silent-failures` Phase 1.5: bumped to ERROR.
        # llm_calls is the source of truth for `okg metrics --llm`;
        # silent insert failure means spend/latency/error-rate
        # dashboards undercount until somebody scrapes worker stdout.
        _WRITE_FAILURES.increment()
        logger.error(
            "llm_calls insert failed (event=llm_tracking_write_failed "
            "caller=%r model=%r): %s",
            caller, model, exc,
        )


def telemetry_budget_drops() -> int:
    """How many telemetry rows this process dropped to budget exhaustion.

    Exposed so a test — and an operator diagnostic — can assert the drop was
    counted rather than trusting that a log line was emitted.
    """
    return _BUDGET_DROPS.value


def telemetry_write_failures() -> int:
    """Number of best-effort tracking writes that failed in this process."""
    return _WRITE_FAILURES.value


__all__ = [
    "extract_usage",
    "record_llm_call",
    "telemetry_budget_drops",
    "telemetry_write_failures",
]
