"""LLM dispatch layer — Protocol + two implementations.

The umbrella ``views-contract`` capability declares
``no-silent-fallback`` for LLM dispatch: callers MUST pass an
``LLMClient`` explicitly when generated fragments are involved.
This module supplies the Protocol and two implementations:

* :class:`LiteLLMClient` — production. Wraps ``litellm.completion``
  with ``tenacity`` retry on transient errors and one
  OpenTelemetry span per call. ~60 lines including imports.

* :class:`DeterministicStubLLM` — testing convenience. ``complete``
  is a pure function of its inputs (sha256 over a canonical
  ``system + sep + user`` rendering). Useful for byte-stable tests
  of the framework's plumbing; not a substantive claim about
  fresh-regen byte-identity in production.

Notes:

* We do not measure LLM byte-identity at temperature 0. LLM
  non-determinism is accepted as a fact (per the proposal's
  redesign on 2026-04-27); Property A protects production refresh
  from this without needing a measurement.

* ``LLMClient`` is a Protocol (PEP 544). Both implementations
  satisfy it via structural subtyping; neither inherits from a
  base class. Tests can pass any callable-shaped object.

* The ``fragment_id`` keyword on ``complete`` is informational —
  it goes into the OpenTelemetry span and the failure-mode
  exception message. Pass it through wherever an LLMClient is
  invoked so dashboards and stack traces tie back to the offending
  fragment.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Protocol, runtime_checkable

# `litellm` is intentionally NOT imported at module level. The
# import is ~1.9 s of cold-start work (pulls in fastapi, aiohttp,
# pydantic, tokenizers, …). Importing it eagerly meant that every
# downstream that touches `okg.views.execution.llm` — including the
# publish-cycle Publisher, which imports `okg.views` for view
# materialization scaffolding even when no view actually calls an
# LLM — paid the full 1.9 s at worker boot.
#
# Instead, `_litellm()` lazily imports + caches the module on first
# real use inside `LiteLLMClient._call_with_retry`. Test paths and
# deployments that don't construct a `LiteLLMClient` (e.g. the
# palimpzest commit-replay bench) never pay the cost.
import tenacity
from opentelemetry import trace

from okg.substrate.llm.tracker import extract_usage, record_llm_call
from okg.substrate.llm.tracking import resolve_tracking_dsn
from okg.views.execution.errors import LLMCallFailed, LLMResponseTruncated

_LITELLM = None

# Fields the client owns on every ``litellm.completion`` call. A
# caller-supplied ``completion_kwargs`` may not carry any of them:
# silently overriding the model or the messages would make the
# recorded ``model`` column a lie, and silently dropping the caller's
# key would be the default-value-as-fallback the project treats as a
# P0. Collisions raise at construction instead.
#
# The last three are not fields this client sets, but ones whose
# semantics it depends on:
#
# * ``stream`` — a streaming call returns a ``CustomStreamWrapper``,
#   which has no ``.choices``, so the response read below dies with an
#   AttributeError that points at the parser instead of at the config.
# * ``n`` — the client reads ``choices[0]`` and drops the rest. The
#   caller would be billed for N completions and receive one, with no
#   signal that the others existed.
# * ``num_retries`` — litellm's own retry loop, which would nest
#   inside the tenacity loop and silently multiply ``max_retries``.
_CLIENT_OWNED_COMPLETION_KEYS = frozenset(
    {
        "model",
        "messages",
        "temperature",
        "timeout",
        "max_tokens",
        "stream",
        "n",
        "num_retries",
    }
)

# Finish reasons that mean "the output budget ran out". litellm
# normalizes most providers to OpenAI's ``length``, but a passthrough
# or a provider it doesn't map can surface the native spelling —
# Gemini's ``MAX_TOKENS``, for one. Matching only ``length`` would
# send those straight back to the content-filter misdiagnosis this
# module exists to avoid, so match the known spellings case-folded.
_TRUNCATION_FINISH_REASONS = frozenset({"length", "max_tokens"})


def _finish_reason(resp) -> str | None:
    """Best-effort read of the first choice's ``finish_reason``.

    Providers are inconsistent: some omit the field, some return an
    object without ``choices`` at all on a filtered response. Any
    failure to read it yields ``None``, which callers must treat as
    "unknown" rather than "not truncated" — an unknown reason keeps
    the pre-existing diagnosis rather than inventing a new one.
    """
    try:
        reason = resp.choices[0].finish_reason
    except Exception:  # pragma: no cover - defensive, provider-shaped
        return None
    return reason if isinstance(reason, str) else None


def _is_truncation(finish_reason: str | None) -> bool:
    """True when the provider stopped because the budget ran out."""
    if not finish_reason:
        return False
    return finish_reason.strip().lower() in _TRUNCATION_FINISH_REASONS


def _litellm():
    """Import and cache litellm on first call.

    The module-level `import litellm` would otherwise be paid by
    every importer of `okg.views.execution.llm` (transitively
    pulled in by `okg.views.__init__`, which the Publisher chain
    imports for view-materialization scaffolding).
    """
    global _LITELLM
    if _LITELLM is None:
        import litellm as _ll

        _LITELLM = _ll
    return _LITELLM


tracer = trace.get_tracer(__name__)

# ---------------------------------------------------------------------------
# Protocol — single method, deliberately minimal
# ---------------------------------------------------------------------------


@runtime_checkable
class LLMClient(Protocol):
    """Single-method LLM dispatch abstraction.

    Implementations carry their own model identifier and apply
    whatever retry / observability discipline they choose. The
    framework treats ``complete`` as a black box: input is two
    strings (system + user), output is one string.

    The ``caller`` keyword is required when the implementation is
    wired with token / cost tracking (per
    ``add-substrate-llm-call-tracking``). It identifies the call
    site in roll-ups (e.g. ``views:af_pubs.publication_overview``).
    """

    model: str

    def complete(
        self,
        system: str,
        user: str,
        *,
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:  # pragma: no cover - Protocol stub
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Production — LiteLLMClient (litellm + tenacity + OTel)
# ---------------------------------------------------------------------------


def _retrying_with_rate_limit_count(
    litellm,
    *,
    max_retries: int,
    retry_stats: dict | None,
):
    """The shared tenacity policy, counting retried rate-limit errors.

    A 429 that is retried and then succeeds writes no failure row, so
    three 429s followed by success used to produce one clean
    ``success=true`` row — rate saturation was invisible in
    ``okg.llm_calls`` and diagnosable only by correlating timings. The
    count accumulates into ``retry_stats["rate_limit_hits"]`` and the
    caller stamps it onto the terminal row's ``extras``. Only RETRIED
    hits are counted here (``before_sleep`` fires when another attempt
    follows); a terminal rate-limit failure is already distinguishable
    by its ``error_kind='RateLimitError'`` row.
    """

    def _count(retry_state) -> None:
        if retry_stats is None or retry_state.outcome is None:
            return
        exc = retry_state.outcome.exception()
        if isinstance(exc, litellm.RateLimitError):
            retry_stats["rate_limit_hits"] = retry_stats.get("rate_limit_hits", 0) + 1

    return tenacity.Retrying(
        retry=tenacity.retry_if_exception_type(
            (
                litellm.APIError,
                litellm.RateLimitError,
                litellm.Timeout,
            )
        ),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        stop=tenacity.stop_after_attempt(max_retries),
        reraise=True,
        before_sleep=_count,
    )


def _provider_failure_extras(
    exc: BaseException,
    retry_stats: dict | None,
) -> dict:
    """Extras for a terminal provider-failure row.

    ``provider_status_code`` carries the HTTP status when the SDK
    exception exposes one (litellm exceptions do), so a 429 is
    queryable as a number, not only as an exception-class name.
    """
    extras: dict = {}
    hits = (retry_stats or {}).get("rate_limit_hits", 0)
    if hits:
        extras["rate_limit_hits"] = hits
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and not isinstance(status, bool):
        extras["provider_status_code"] = status
    return extras


class LiteLLMClient:
    """Production :class:`LLMClient` using ``litellm.completion``.

    Configuration:

    * ``model`` — any ``litellm`` model string (e.g.
      ``"claude-sonnet-4-5-20250929"``,
      ``"gemini/gemini-2.5-pro"``,
      ``"openrouter/qwen/qwen3.6-plus"``).
    * ``temperature`` — defaults to ``0.0``. Note: temperature 0
      is NOT a determinism guarantee at the live API; it is a
      sampling-policy hint.
    * ``max_retries`` — tenacity stop-after-attempt count.
    * ``timeout_s`` — per-provider-call timeout passed through to
      ``litellm.completion``. This keeps publish-cycle view
      materialization from waiting indefinitely on a live provider.
    * ``tracking_dsn`` — optional Postgres DSN for the substrate
      ``okg.llm_calls`` table. When set, every ``complete()`` call
      records one row with token counts (from
      ``model_response.usage``) and OpenRouter cost (from
      ``model_response._hidden_params['additional_headers']
      ['llm_provider-x-litellm-response-cost']``). When ``None``,
      tracking is disabled (legacy back-compat). When set,
      ``complete(..., caller="...")`` is required.
    * ``tracking_mode`` — ``disabled`` (default when no explicit DSN),
      ``optional``, or ``required``. An enabled mode with no explicit
      ``tracking_dsn`` uses ``runtime_dsn`` supplied by the worker.
    * ``runtime_dsn`` — the already-resolved worker database authority;
      it never enables tracking by itself.
    * ``deployment`` — optional label persisted alongside each row
      for cross-deployment roll-ups.
    * ``max_output_tokens`` — optional cap sent as ``max_tokens``.
      Omitted from the call entirely when unset, so the provider
      default is unchanged for construction sites that don't set it.
      Mirrors the argument of the same name on :class:`VLMClient`.
    * ``completion_kwargs`` — optional dict forwarded verbatim into
      ``litellm.completion``. This is the escape hatch for provider
      controls that don't deserve a constructor argument each, e.g.
      ``{"reasoning": {"enabled": False}}`` to turn a reasoning
      model's trace off, or ``{"extra_body": {...}}``. Keys the
      client owns (``model``, ``messages``, ``temperature``,
      ``timeout``, ``max_tokens``) are rejected at construction.

    Behavior:

    * Retries on ``litellm.APIError`` /
      ``litellm.RateLimitError`` / ``litellm.Timeout`` with
      exponential backoff (1s..30s, 3 attempts by default).
    * Non-retryable exceptions propagate without wrapping (so a
      caller catching ``ValueError`` from an
      ``InvalidRequestError`` keeps seeing it).
    * On final retry-budget exhaustion, raises
      :exc:`LLMCallFailed` with the fragment id in the message
      and the underlying exception as ``__cause__``.
    * Emits one ``"view.llm_call"`` OpenTelemetry span per
      invocation with attributes ``model``, ``fragment_id``,
      ``caller``, ``input_chars``, ``output_chars``,
      ``prompt_tokens``, ``completion_tokens``, ``cost_usd``.
    """

    def __init__(
        self,
        model: str,
        *,
        max_retries: int = 3,
        temperature: float = 0.0,
        timeout_s: float | None = 60.0,
        max_output_tokens: int | None = None,
        completion_kwargs: dict | None = None,
        tracking_dsn: str | None = None,
        tracking_mode: str | None = None,
        runtime_dsn: str | None = None,
        deployment: str | None = None,
    ) -> None:
        if not model:
            raise ValueError("LiteLLMClient requires a non-empty model string")
        if timeout_s is not None and timeout_s <= 0:
            raise ValueError("LiteLLMClient timeout_s must be positive")
        if max_output_tokens is not None and max_output_tokens <= 0:
            raise ValueError("LiteLLMClient max_output_tokens must be positive")
        if completion_kwargs is not None:
            if not isinstance(completion_kwargs, dict):
                raise ValueError(
                    "LiteLLMClient completion_kwargs must be a dict of "
                    f"litellm.completion keyword arguments, got "
                    f"{type(completion_kwargs).__name__}"
                )
            collisions = sorted(set(completion_kwargs) & _CLIENT_OWNED_COMPLETION_KEYS)
            if collisions:
                raise ValueError(
                    "LiteLLMClient completion_kwargs may not override "
                    f"client-owned call fields: {', '.join(collisions)}. "
                    "Set max_tokens via max_output_tokens=, temperature "
                    "via temperature=, and timeout via timeout_s=."
                )
        self.model = model
        self._max_retries = max_retries
        self._temperature = temperature
        self._timeout_s = timeout_s
        self._max_output_tokens = max_output_tokens
        self._completion_kwargs = dict(completion_kwargs or {})
        self._tracking_dsn = resolve_tracking_dsn(
            tracking_mode=tracking_mode,
            explicit_tracking_dsn=tracking_dsn,
            runtime_dsn=runtime_dsn,
            component="LiteLLMClient",
        )
        self._deployment = deployment
        # Last successful call's usage / cost — populated by
        # ``complete()`` for callers who need real token counts
        # without a DB round-trip back to ``okg.llm_calls`` (e.g.
        # the af_pubs semantic_verifier records ``cost_tokens`` on
        # its verdict node-fact). ``None`` if no call has succeeded
        # yet on this thread, or if the provider returned no usage
        # info. Thread-LOCAL: one client instance is shared across
        # the fan-out's worker threads, and a per-instance slot
        # would let thread A read thread B's cost — spend settlement
        # would then charge the wrong item's actual. Same-thread
        # read-after-complete() semantics are unchanged.
        self._call_stats_local = threading.local()

    @property
    def last_call_stats(self) -> dict | None:
        return getattr(self._call_stats_local, "stats", None)

    @last_call_stats.setter
    def last_call_stats(self, value: dict | None) -> None:
        self._call_stats_local.stats = value

    def complete(
        self,
        system: str,
        user: str,
        *,
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:
        # Loud failure: when tracking is wired, the caller string is
        # required (per add-substrate-llm-call-tracking spec). Legacy
        # construction sites that don't pass tracking_dsn keep the
        # back-compat empty-caller path.
        if self._tracking_dsn is not None and not caller:
            raise ValueError(
                "LiteLLMClient: caller= is required when tracking is "
                "enabled (tracking_dsn was set at construction). Pass "
                "a stable caller string like "
                "'views:<view_def_id>' or 'invariants:<name>'."
            )

        with tracer.start_as_current_span("view.llm_call") as span:
            span.set_attribute("model", self.model)
            span.set_attribute("fragment_id", fragment_id)
            span.set_attribute("caller", caller)
            span.set_attribute("input_chars", len(system) + len(user))
            if generation_id is not None:
                span.set_attribute("generation_id", generation_id)
            if outcome_key_digest is not None:
                span.set_attribute("outcome_key_digest", outcome_key_digest)

            t0 = time.monotonic()
            resp = None
            error_kind: str | None = None
            retry_stats: dict = {"rate_limit_hits": 0}
            try:
                content, resp = self._call_with_retry(
                    system,
                    user,
                    retry_stats,
                )
            except Exception as exc:
                span.record_exception(exc)
                error_kind = type(exc).__name__
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=None,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=_provider_failure_extras(exc, retry_stats),
                )
                # Wrap so callers can `except LLMCallFailed` without
                # importing litellm. Preserve __cause__ chain.
                raise LLMCallFailed(
                    f"LLM call failed for fragment "
                    f"{fragment_id!r} (model={self.model!r}): {exc}"
                ) from exc

            latency_ms = int((time.monotonic() - t0) * 1000)

            # Read why the provider stopped before judging the body.
            # A reasoning model can burn its entire output budget on
            # the reasoning trace and return a zero-length content
            # body with finish_reason="length" — a truncation, not a
            # content filter. Diagnosing it as the latter sent an
            # operator looking in the wrong place for an hour (#907).
            finish_reason = _finish_reason(resp)
            truncated = _is_truncation(finish_reason)
            if finish_reason is not None:
                span.set_attribute("finish_reason", finish_reason)
            span.set_attribute("truncated", truncated)

            if not content and truncated:
                error_kind = "TruncatedProviderResponse"
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=extract_usage(resp),
                    latency_ms=latency_ms,
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=(
                        {"rate_limit_hits": retry_stats["rate_limit_hits"]}
                        if retry_stats["rate_limit_hits"]
                        else None
                    ),
                )
                cap = (
                    f"max_output_tokens={self._max_output_tokens}"
                    if self._max_output_tokens is not None
                    else "no max_output_tokens was set, so the provider default applied"
                )
                raise LLMResponseTruncated(
                    f"LLM response was truncated before any content was "
                    f"emitted for fragment {fragment_id!r} "
                    f"(model={self.model!r}, "
                    f"finish_reason={finish_reason!r}, "
                    f"{cap}). The whole output budget went to the "
                    f"response prefix — on a reasoning model that is "
                    f"the reasoning trace. Raise max_output_tokens, or "
                    f"pass completion_kwargs={{'reasoning': "
                    f"{{'enabled': False}}}} to turn the trace off."
                )

            # Some providers (Gemini in particular) occasionally
            # return a "successful" response with an empty/null
            # body — typically a content-filter trip that doesn't
            # raise. Reject any falsy completion (``None`` OR the
            # empty string ``""``): both are content-free. Guarding
            # only on ``content is None`` let an empty string through
            # as a billed ``success=True`` result that callers then
            # cached as a stable fragment (#693). Treat it as a failed
            # call so callers can decide whether to retry, skip, or
            # surface — and never record it as success or return it.
            # fail-closed: an empty completion is not a usable answer.
            if not content:
                error_kind = "EmptyProviderResponse"
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=extract_usage(resp),
                    latency_ms=latency_ms,
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=(
                        {"rate_limit_hits": retry_stats["rate_limit_hits"]}
                        if retry_stats["rate_limit_hits"]
                        else None
                    ),
                )
                raise LLMCallFailed(
                    f"LLM call returned empty content for "
                    f"fragment {fragment_id!r} "
                    f"(model={self.model!r}); likely a "
                    f"content-filter response with no text body"
                )
            span.set_attribute("output_chars", len(content))

            # Best-effort tracking. Extract usage even if tracking is
            # disabled, so the span still carries token / cost attrs
            # for OTel consumers and ``last_call_stats`` is populated
            # for callers who need real token counts.
            usage = extract_usage(resp)
            if retry_stats["rate_limit_hits"]:
                # Survived rate limiting: without this stamp, a 429
                # storm that eventually succeeds is indistinguishable
                # from a clean run in okg.llm_calls.
                usage["extras"]["rate_limit_hits"] = retry_stats["rate_limit_hits"]
            if truncated:
                # A truncated non-empty body is billed as a success, so
                # without this the tracked row cannot be told apart from
                # a clean stop, and the downstream symptom — a parse
                # failure on a body cut mid-JSON — has nothing persisted
                # to explain it. Set only when true, so untruncated rows
                # keep the extras shape they already had.
                usage["extras"]["truncated"] = True
            if usage["prompt_tokens"] is not None:
                span.set_attribute("prompt_tokens", usage["prompt_tokens"])
            if usage["completion_tokens"] is not None:
                span.set_attribute("completion_tokens", usage["completion_tokens"])
            if usage["cost_usd"] is not None:
                span.set_attribute("cost_usd", usage["cost_usd"])
            # Surface a few high-signal extras on the span so OTel
            # consumers see thinking-model reasoning tokens and
            # Anthropic cache activity without parsing the row.
            for k in (
                "completion_reasoning_tokens",
                "cache_read_input_tokens",
                "prompt_cached_tokens",
            ):
                v = usage["extras"].get(k)
                if v is not None:
                    span.set_attribute(k, v)

            self.last_call_stats = {
                "prompt_tokens": usage["prompt_tokens"],
                "completion_tokens": usage["completion_tokens"],
                "total_tokens": usage["total_tokens"],
                "cost_usd": usage["cost_usd"],
                "extras": usage["extras"],
                "latency_ms": latency_ms,
                # A non-empty body that stopped on "length" is still
                # partial output. Whether that is usable is the
                # caller's to judge — a summary survives being cut
                # short, a JSON array does not — but it must not be
                # invisible, which it was before #907.
                "truncated": truncated,
            }

            self._record(
                caller=caller,
                fragment_id=fragment_id,
                usage=usage,
                latency_ms=latency_ms,
                success=True,
                error_kind=None,
                generation_id=generation_id,
                outcome_key_digest=outcome_key_digest,
            )
            return content

    def _record(
        self,
        *,
        caller: str,
        fragment_id: str,
        usage: dict | None,
        latency_ms: int,
        success: bool,
        error_kind: str | None,
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
        merge_extras: dict | None = None,
    ) -> None:
        if self._tracking_dsn is None:
            return
        u = usage or {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
            "cost_usd": None,
            "extras": {},
        }
        extras = dict(u["extras"] or {})
        if merge_extras:
            extras.update(merge_extras)
        record_llm_call(
            self._tracking_dsn,
            caller=caller,
            model=self.model,
            fragment_id=fragment_id or None,
            prompt_tokens=u["prompt_tokens"],
            completion_tokens=u["completion_tokens"],
            total_tokens=u["total_tokens"],
            cost_usd=u["cost_usd"],
            latency_ms=latency_ms,
            success=success,
            error_kind=error_kind,
            deployment=self._deployment,
            generation_id=generation_id,
            extras=extras or None,
            outcome_key_digest=outcome_key_digest,
        )

    def _call_with_retry(
        self,
        system: str,
        user: str,
        retry_stats: dict | None = None,
    ):
        # Build the retry decorator dynamically so ``max_retries``
        # is honored on a per-instance basis (a class-level decorator
        # would bake a single attempt count into the method).
        litellm = _litellm()
        retrying = _retrying_with_rate_limit_count(
            litellm,
            max_retries=self._max_retries,
            retry_stats=retry_stats,
        )
        for attempt in retrying:
            with attempt:
                call_kwargs = {
                    "model": self.model,
                    "temperature": self._temperature,
                    "timeout": self._timeout_s,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                }
                # Absent, not None: a provider that reads
                # ``max_tokens=None`` as "cap at zero" would turn an
                # unset knob into a truncation.
                if self._max_output_tokens is not None:
                    call_kwargs["max_tokens"] = self._max_output_tokens
                call_kwargs.update(self._completion_kwargs)
                resp = litellm.completion(**call_kwargs)
                return resp.choices[0].message.content, resp
        # Unreachable; tenacity raises when the budget is exhausted
        # and reraise=True propagates the underlying exception.
        raise RuntimeError(  # pragma: no cover
            "tenacity.Retrying exited without yielding"
        )


# ---------------------------------------------------------------------------
# VLMClient — multimodal LiteLLM calls (image + text)
# ---------------------------------------------------------------------------


class VLMClient:
    """Production vision-language client using ``litellm.completion``.

    Wraps a multimodal message (base64-encoded ``image_url`` content
    block + text prompt) with the same tenacity retry policy, OTel
    span, and optional ``okg.llm_calls`` tracking as
    :class:`LiteLLMClient`.

    The ``complete_image`` method accepts raw image bytes and a text
    prompt and returns the model's string reply.  Image bytes are
    base64-encoded and sent as an ``image_url`` content block with a
    data URI (``data:<mime>;base64,<b64>``).

    Configuration mirrors :class:`LiteLLMClient` (``model``,
    ``max_retries``, ``temperature``, ``timeout_s``, ``max_output_tokens``,
    ``tracking_dsn``, ``tracking_mode``, ``runtime_dsn``, ``deployment``).
    """

    def __init__(
        self,
        model: str,
        *,
        max_retries: int = 3,
        temperature: float = 0.0,
        timeout_s: float | None = 60.0,
        max_output_tokens: int | None = None,
        tracking_dsn: str | None = None,
        tracking_mode: str | None = None,
        runtime_dsn: str | None = None,
        deployment: str | None = None,
    ) -> None:
        if not model:
            raise ValueError("VLMClient requires a non-empty model string")
        if timeout_s is not None and timeout_s <= 0:
            raise ValueError("VLMClient timeout_s must be positive")
        if max_output_tokens is not None and max_output_tokens <= 0:
            raise ValueError("VLMClient max_output_tokens must be positive")
        self.model = model
        self._max_retries = max_retries
        self._temperature = temperature
        self._timeout_s = timeout_s
        self._max_output_tokens = max_output_tokens
        self._tracking_dsn = resolve_tracking_dsn(
            tracking_mode=tracking_mode,
            explicit_tracking_dsn=tracking_dsn,
            runtime_dsn=runtime_dsn,
            component="VLMClient",
        )
        self._deployment = deployment
        # Thread-LOCAL, same rationale as LiteLLMClient: one client is
        # shared across the VLM table fan-out's worker threads, and a
        # per-instance slot would hand one thread another thread's numbers.
        self._call_stats_local = threading.local()

    @property
    def last_call_stats(self) -> dict | None:
        return getattr(self._call_stats_local, "stats", None)

    @last_call_stats.setter
    def last_call_stats(self, value: dict | None) -> None:
        self._call_stats_local.stats = value

    def complete_image(
        self,
        image_bytes: bytes,
        prompt: str,
        *,
        mime_type: str = "image/png",
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:
        """Send image + text prompt; return the model's reply string."""
        import base64

        if self._tracking_dsn is not None and not caller:
            raise ValueError("VLMClient: caller= is required when tracking is enabled.")

        b64 = base64.b64encode(image_bytes).decode("ascii")
        data_uri = f"data:{mime_type};base64,{b64}"

        with tracer.start_as_current_span("view.vlm_call") as span:
            span.set_attribute("model", self.model)
            span.set_attribute("fragment_id", fragment_id)
            span.set_attribute("caller", caller)
            span.set_attribute("image_bytes", len(image_bytes))
            span.set_attribute("prompt_chars", len(prompt))
            if generation_id is not None:
                span.set_attribute("generation_id", generation_id)

            t0 = time.monotonic()
            resp = None
            error_kind: str | None = None
            retry_stats: dict = {"rate_limit_hits": 0}
            try:
                content, resp = self._call_with_retry(
                    data_uri,
                    prompt,
                    retry_stats,
                )
            except Exception as exc:
                span.record_exception(exc)
                error_kind = type(exc).__name__
                latency_ms = int((time.monotonic() - t0) * 1000)
                failure_extras = _provider_failure_extras(exc, retry_stats)
                # Keep the thread-local seam authoritative even when the
                # provider exhausts retries.  CaptionService reads this after
                # the exception so terminal 429s retain their retried-hit
                # count without persisting the provider message.
                self.last_call_stats = {
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "cost_usd": None,
                    "extras": failure_extras,
                    "latency_ms": latency_ms,
                    "truncated": False,
                    "success": False,
                    "error_kind": error_kind,
                }
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=None,
                    latency_ms=latency_ms,
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=failure_extras,
                )
                raise LLMCallFailed(
                    f"VLM call failed for fragment "
                    f"{fragment_id!r} (model={self.model!r}): {exc}"
                ) from exc

            latency_ms = int((time.monotonic() - t0) * 1000)
            finish_reason = _finish_reason(resp)
            truncated = _is_truncation(finish_reason)
            if finish_reason is not None:
                span.set_attribute("finish_reason", finish_reason)
            span.set_attribute("truncated", truncated)
            if not content and truncated:
                error_kind = "TruncatedProviderResponse"
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=extract_usage(resp),
                    latency_ms=latency_ms,
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=(
                        {"rate_limit_hits": retry_stats["rate_limit_hits"]}
                        if retry_stats["rate_limit_hits"]
                        else None
                    ),
                )
                raise LLMResponseTruncated(
                    f"VLM response was truncated before any content was "
                    f"emitted for fragment {fragment_id!r} "
                    f"(model={self.model!r}, finish_reason={finish_reason!r})"
                )
            # Treat an empty string like a missing response: some providers
            # return content="" (rather than raising) on a content-filter
            # trip. Accepting it would record a successful VLM call that
            # produced no caption and never gets cached, so it re-burns on
            # every subsequent run.
            if not content:
                error_kind = "EmptyProviderResponse"
                self._record(
                    caller=caller,
                    fragment_id=fragment_id,
                    usage=extract_usage(resp),
                    latency_ms=latency_ms,
                    success=False,
                    error_kind=error_kind,
                    generation_id=generation_id,
                    outcome_key_digest=outcome_key_digest,
                    merge_extras=(
                        {"rate_limit_hits": retry_stats["rate_limit_hits"]}
                        if retry_stats["rate_limit_hits"]
                        else None
                    ),
                )
                raise LLMCallFailed(
                    f"VLM call returned empty content for fragment "
                    f"{fragment_id!r} (model={self.model!r})"
                )

            span.set_attribute("output_chars", len(content))
            usage = extract_usage(resp)
            if retry_stats["rate_limit_hits"]:
                usage["extras"]["rate_limit_hits"] = retry_stats["rate_limit_hits"]
            if truncated:
                usage["extras"]["truncated"] = True
            if usage["prompt_tokens"] is not None:
                span.set_attribute("prompt_tokens", usage["prompt_tokens"])
            if usage["completion_tokens"] is not None:
                span.set_attribute("completion_tokens", usage["completion_tokens"])
            if usage["cost_usd"] is not None:
                span.set_attribute("cost_usd", usage["cost_usd"])

            self.last_call_stats = {
                "prompt_tokens": usage["prompt_tokens"],
                "completion_tokens": usage["completion_tokens"],
                "total_tokens": usage["total_tokens"],
                "cost_usd": usage["cost_usd"],
                "extras": usage["extras"],
                "latency_ms": latency_ms,
                "truncated": truncated,
            }

            self._record(
                caller=caller,
                fragment_id=fragment_id,
                usage=usage,
                latency_ms=latency_ms,
                success=True,
                error_kind=None,
                generation_id=generation_id,
                outcome_key_digest=outcome_key_digest,
            )
            return content

    def _record(
        self,
        *,
        caller: str,
        fragment_id: str,
        usage: dict | None,
        latency_ms: int,
        success: bool,
        error_kind: str | None,
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
        merge_extras: dict | None = None,
    ) -> None:
        if self._tracking_dsn is None:
            return
        u = usage or {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
            "cost_usd": None,
            "extras": {},
        }
        extras = dict(u["extras"] or {})
        if merge_extras:
            extras.update(merge_extras)
        record_llm_call(
            self._tracking_dsn,
            caller=caller,
            model=self.model,
            fragment_id=fragment_id or None,
            prompt_tokens=u["prompt_tokens"],
            completion_tokens=u["completion_tokens"],
            total_tokens=u["total_tokens"],
            cost_usd=u["cost_usd"],
            latency_ms=latency_ms,
            success=success,
            error_kind=error_kind,
            deployment=self._deployment,
            generation_id=generation_id,
            extras=extras or None,
            outcome_key_digest=outcome_key_digest,
        )

    def _call_with_retry(
        self,
        data_uri: str,
        prompt: str,
        retry_stats: dict | None = None,
    ):
        litellm = _litellm()
        retrying = _retrying_with_rate_limit_count(
            litellm,
            max_retries=self._max_retries,
            retry_stats=retry_stats,
        )
        for attempt in retrying:
            with attempt:
                call_kwargs = {
                    "model": self.model,
                    "temperature": self._temperature,
                    "timeout": self._timeout_s,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image_url",
                                    "image_url": {"url": data_uri},
                                },
                                {
                                    "type": "text",
                                    "text": prompt,
                                },
                            ],
                        }
                    ],
                }
                if self._max_output_tokens is not None:
                    call_kwargs["max_tokens"] = self._max_output_tokens
                resp = litellm.completion(
                    **call_kwargs,
                )
                return resp.choices[0].message.content, resp
        raise RuntimeError(  # pragma: no cover
            "tenacity.Retrying exited without yielding"
        )


class StubVLMClient:
    """Test double for :class:`VLMClient`.

    Returns a deterministic string (sha256 of image bytes + prompt)
    without calling any LLM.  Tracks calls in ``calls`` for assertion.
    """

    model = "stub-vlm"

    def __init__(self, response: str | None = None) -> None:
        self.response = response
        self.calls: list[dict] = []

    def complete_image(
        self,
        image_bytes: bytes,
        prompt: str,
        *,
        mime_type: str = "image/png",
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:
        self.calls.append({"image_len": len(image_bytes), "prompt": prompt})
        if self.response is not None:
            return self.response
        h = hashlib.sha256(image_bytes + b"\n" + prompt.encode()).hexdigest()[:16]
        return f"[STUB_VLM {h}] image={len(image_bytes)}b prompt={len(prompt)}c"


# ---------------------------------------------------------------------------
# Test double — DeterministicStubLLM
# ---------------------------------------------------------------------------


class DeterministicStubLLM:
    """Test :class:`LLMClient` whose output is a pure function of
    inputs.

    Useful for tests of framework plumbing where calling a real LLM
    would be slow or non-deterministic. NOT a substantive claim
    about LLM byte-identity in production: production code uses
    :class:`LiteLLMClient` and accepts that fresh regen may vary.
    """

    model = "stub"

    def complete(
        self,
        system: str,
        user: str,
        *,
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:
        # ``caller``, ``generation_id`` and ``outcome_key_digest`` are
        # accepted for Protocol parity with `LiteLLMClient` but ignored —
        # the stub never records to ``okg.llm_calls``.
        h = hashlib.sha256((system + "\n---\n" + user).encode("utf-8")).hexdigest()[:16]
        return f"[STUB {h}] system={len(system)}c user={len(user)}c"


__all__ = [
    "LLMClient",
    "LiteLLMClient",
    "DeterministicStubLLM",
    "VLMClient",
    "StubVLMClient",
    "LLMCallFailed",
    "LLMResponseTruncated",
]
