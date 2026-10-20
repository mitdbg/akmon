"""External integration helpers (Phase 2 §5).

Surfaces deployment workflows use to interact with non-DB resources
while keeping durability + idempotency invariants intact:

  * :func:`llm_call` — wraps ``litellm.completion`` with a typed
    response and an optional :class:`cost_budget.consume` integration.
  * :func:`wait_for_signal` — wraps ``DBOS.recv`` with deployment-
    namespaced topic semantics.
  * :func:`correlate_workflow` / :func:`find_workflow_by_correlation`
    — UPSERT against ``okg.deployment_correlations`` so external
    handlers (FastAPI routes, webhook receivers) can find a workflow
    waiting on a callback.
  * :func:`request_republish` — enqueues the deployment's publisher
    workflow on the partitioned queue. Closes the boundary leak
    Phase 0 surfaced in scenario 1.

Stream wrappers (``write_derived_edges_stream``,
``consume_derived_edges_stream``) live in :mod:`._streams`; the
:class:`DerivedEdgeSpec` dataclass is shared from :mod:`._stubs` for
forward compatibility.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from numbers import Number
from typing import Any, Literal

from .context import WorkflowContext, current_context


__all__ = [
    "LLMPricingError",
    "LLMResponse",
    "correlate_workflow",
    "find_workflow_by_correlation",
    "llm_call",
    "request_republish",
    "wait_for_signal",
]


log = logging.getLogger(__name__)


# Topic prefix for wait_for_signal; the symmetric send side is the
# substrate's response surfaces or external integrations using
# DBOSClient.send.
_TOPIC_NAMESPACE_PREFIX = "deployment"


# ---------------------------------------------------------------------------
# llm_call
# ---------------------------------------------------------------------------


class LLMPricingError(RuntimeError):
    """Raised when a completed LLM call cannot be assigned a valid price."""

    pricing_status: Literal["unsettled"] = "unsettled"

    def __init__(
        self,
        *,
        requested_model: str,
        resolved_model: str,
        input_tokens: int | None,
        output_tokens: int | None,
        reason: str,
    ) -> None:
        self.requested_model = requested_model
        self.resolved_model = resolved_model
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.reason = reason
        self.guidance = (
            "Use a model supported by LiteLLM pricing or configure a declared "
            "pricing policy before retrying."
        )
        usage = (
            f"input_tokens={input_tokens!r}, output_tokens={output_tokens!r}"
        )
        super().__init__(
            "LLM pricing is unsettled after the provider response: "
            f"requested_model={requested_model!r}, "
            f"resolved_model={resolved_model!r}, {usage}; {reason}. "
            f"{self.guidance}"
        )


@dataclass(frozen=True)
class LLMResponse:
    """The typed result of :func:`llm_call`.

    Attributes
    ----------
    text : str
        Raw model output.
    parsed : Any
        Result of running ``response_schema.validate(text)`` (if
        provided). None when no schema was passed.
    cost_usd : float
        litellm's cost-tracking estimate. Wired through
        :meth:`cost_budget.consume` when the caller passes
        ``budget=...``.
    input_tokens : int | None
        litellm's prompt-token count, or None when usage is unavailable.
    output_tokens : int | None
        litellm's completion-token count, or None when usage is unavailable.
    model : str
        Model id the call resolved to (litellm may rewrite for
        provider-specific routing).
    pricing_status : Literal["settled"]
        Confirms LiteLLM supplied a finite, nonnegative numeric price.
    """

    text: str
    parsed: Any
    cost_usd: float
    input_tokens: int | None
    output_tokens: int | None
    model: str
    pricing_status: Literal["settled"] = "settled"


def llm_call(
    prompt: str,
    *,
    model: str,
    seed: int | None = None,
    budget: Any | None = None,
    response_schema: Any | None = None,
    max_tokens: int | None = None,
) -> LLMResponse:
    """Synchronous LLM completion via litellm.

    Documented as NOT cached on its own per Phase 0 C-correction; the
    canonical pattern wraps the call inside ``@deployment_step`` so
    DBOS's step cache by input hash provides idempotency under retry.

    Parameters
    ----------
    prompt : str
        User-role prompt content. Passed as a single user message;
        callers needing system+user roles can pass a multi-line string
        or build their own ``litellm.completion`` call.
    model : str
        litellm model identifier (e.g. ``"openai/gpt-4o-mini"``).
    seed : int | None
        Forwarded as ``seed`` to litellm when supported.
    budget : cost_budget | None
        If provided, the response's ``cost_usd`` is recorded via
        ``budget.consume(cost_usd)`` after the call returns. A
        ``BudgetExhaustedError`` raised by ``consume`` propagates
        — the LLM call itself is NOT rolled back (the model
        already charged us).
    response_schema : Any | None
        Optional schema with a ``.validate(text)`` method. The
        returned LLMResponse.parsed carries the validation result.
    max_tokens : int | None
        Forwarded as ``max_tokens`` to litellm.
    """
    try:
        import litellm
    except ImportError as e:  # pragma: no cover — declared dep
        raise RuntimeError(
            "litellm is required for okg.deployment.llm_call; "
            "ensure your environment has it installed"
        ) from e

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    }
    if seed is not None:
        kwargs["seed"] = seed
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens

    log.debug("llm_call: model=%s seed=%s prompt_len=%d",
              model, seed, len(prompt))

    response = litellm.completion(**kwargs)

    # litellm responses are OpenAI-shaped. Pull the first choice.
    text = response.choices[0].message.content or ""
    usage = getattr(response, "usage", None)
    known_input_tokens = (
        getattr(usage, "prompt_tokens", None) if usage is not None else None
    )
    known_output_tokens = (
        getattr(usage, "completion_tokens", None) if usage is not None else None
    )
    input_tokens = known_input_tokens
    output_tokens = known_output_tokens
    resolved_model = getattr(response, "model", model)

    try:
        raw_cost = litellm.completion_cost(completion_response=response)
        if isinstance(raw_cost, bool) or not isinstance(raw_cost, Number):
            raise TypeError(
                "completion_cost must return a numeric value, "
                f"got {type(raw_cost).__name__}"
            )
        cost_usd = float(raw_cost)
        if not math.isfinite(cost_usd) or cost_usd < 0:
            raise ValueError(
                "completion_cost must return a finite, nonnegative value, "
                f"got {raw_cost!r}"
            )
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        raise LLMPricingError(
            requested_model=model,
            resolved_model=resolved_model,
            input_tokens=known_input_tokens,
            output_tokens=known_output_tokens,
            reason=reason,
        ) from exc

    parsed = None
    if response_schema is not None:
        try:
            parsed = response_schema.validate(text)
        except AttributeError:
            # Plain pydantic / dataclass: try parse_raw or similar.
            for method in ("model_validate_json", "parse_raw"):
                if hasattr(response_schema, method):
                    parsed = getattr(response_schema, method)(text)
                    break

    out = LLMResponse(
        text=text,
        parsed=parsed,
        cost_usd=cost_usd,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model=resolved_model,
    )

    if budget is not None:
        # Consume against the budget. Propagates BudgetExhaustedError.
        budget.consume(cost_usd)

    return out


# ---------------------------------------------------------------------------
# wait_for_signal
# ---------------------------------------------------------------------------


def wait_for_signal(
    topic: str,
    *,
    timeout_seconds: float,
    payload_schema: Any | None = None,
) -> Any:
    """Durable wait for an external callback.

    Topic is auto-prefixed with the deployment name
    (``deployment:<name>:<topic>``) so two deployments can't
    accidentally cross-receive each other's signals.

    Returns the received payload (or the result of
    ``payload_schema.validate(payload)`` when a schema is provided).
    Returns None on timeout.
    """
    try:
        from dbos import DBOS
    except ImportError as e:
        raise RuntimeError(
            "wait_for_signal requires the dbos package"
        ) from e

    ctx = current_context()
    full_topic = (
        f"{_TOPIC_NAMESPACE_PREFIX}:{ctx.deployment}:{topic}"
    )
    payload = DBOS.recv(topic=full_topic, timeout_seconds=timeout_seconds)
    if payload is None or payload_schema is None:
        return payload
    try:
        return payload_schema.validate(payload)
    except AttributeError:
        for method in ("model_validate", "parse_obj"):
            if hasattr(payload_schema, method):
                return getattr(payload_schema, method)(payload)
    return payload


# ---------------------------------------------------------------------------
# correlate_workflow / find_workflow_by_correlation
# ---------------------------------------------------------------------------


def correlate_workflow(
    *,
    topic: str,
    correlation_key: str,
) -> None:
    """Register the current workflow under ``(deployment, topic,
    correlation_key)`` so an external handler can wake it later.

    Idempotent UPSERT into ``okg.deployment_correlations``: a retry of
    the same workflow step replaces the workflow_id (which is the
    same value), keeping exactly one row per key.
    """
    ctx = current_context()
    if not ctx.workflow_id:
        raise RuntimeError(
            "correlate_workflow requires an active workflow context "
            "(call inside a @deployment_workflow)"
        )

    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.deployment_correlations
              (deployment, topic, correlation_key, workflow_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (deployment, topic, correlation_key)
              DO UPDATE SET workflow_id = EXCLUDED.workflow_id,
                            registered_at = now()
            """,
            (ctx.deployment, topic, correlation_key, ctx.workflow_id),
        )
    log.info(
        "correlate_workflow: deployment=%s topic=%s key=%s -> wf=%s",
        ctx.deployment, topic, correlation_key, ctx.workflow_id,
    )


def find_workflow_by_correlation(
    *,
    deployment: str,
    topic: str,
    correlation_key: str,
) -> str | None:
    """Reverse lookup: return the workflow_id registered under
    ``(deployment, topic, correlation_key)``, or None if no row exists.

    External handlers (FastAPI routes, webhook receivers) use this to
    find the workflow waiting on their callback, then call
    ``DBOSClient.send(workflow_id, payload, topic=...)`` to wake it.
    """
    ctx = current_context()
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_id FROM okg.deployment_correlations
             WHERE deployment = %s AND topic = %s
               AND correlation_key = %s
            """,
            (deployment, topic, correlation_key),
        )
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# request_republish
# ---------------------------------------------------------------------------


def request_republish(
    *,
    reason: str,
    branch_name: str = "default",
    force_enrichers: bool = False,
) -> str:
    """Trigger a publish-cycle workflow for the current deployment.

    Enqueues the substrate's registered publisher workflow
    (``okg_publisher_cycle``, see
    :mod:`okg.substrate.runtime.publisher_workflow`) on the
    deployment's partitioned queue and returns its workflow id.
    Closes the boundary leak Phase 0 surfaced in scenario 1:
    deployment authors can ask for a republish without importing
    ``okg.substrate.*``.

    ``branch_name`` and ``force_enrichers`` are forwarded positionally
    and are exactly the registered workflow's own arguments, so the
    enqueued row always binds to a real worker entry point.

    ``reason`` is a required, non-blank operator annotation. Its
    defined behavior is auditing only: it is validated and written to
    this module's INFO log next to the returned workflow id. It is not
    forwarded to the publisher workflow, because the registered
    workflow has no parameter for it.

    Refuses, before returning any id, when the deployment's database
    has never had a DBOS application version registered — i.e. no
    worker has ever booted against it, so the row could not be
    dispatched or later restamped. This is a has-ever-run check, not a
    liveness check: an enqueue made while a registered worker happens
    to be stopped is retained and restamped onto the live application
    version at the next worker boot
    (``services.stamp_replaceable_deployment_enqueued``).

    Reaching a terminal SUCCESS means the cycle ran, not that it
    published: ``Publisher.cycle`` returns a skip summary when the
    staging replay guard halts or an active source sync owns
    publishing. Read the workflow's output summary, not just its
    status.

    Raises:
        ValueError: ``reason`` is empty or blank.
        RuntimeError: the ``dbos`` package is missing, or no DBOS
            application version has ever been registered for this
            deployment's database.
        Exception: connection failures reading the registered version
            propagate unchanged, so an unreachable database is never
            reported as a missing worker.
    """
    try:
        import dbos  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "request_republish requires the dbos package"
        ) from e

    if not reason or not reason.strip():
        raise ValueError(
            "request_republish requires a non-blank reason; it is the "
            "audit annotation for the enqueued publish cycle"
        )

    from okg.substrate.runtime.dbos_enqueue import (
        enqueue_deployment_workflow,
    )
    from okg.substrate.runtime.publisher_schedule import (
        # The operator path deliberately shares the scheduled cycle's
        # budget; split the constant if the two ever need to diverge.
        PUBLISHER_WORKFLOW_TIMEOUT_SECONDS,
    )
    from okg.substrate.runtime.publisher_workflow import (
        PUBLISHER_WORKFLOW_NAME,
    )

    ctx = current_context()
    dsn = _context_dsn(ctx)

    if _registered_app_version(dsn) is None:
        raise RuntimeError(
            "request_republish: deployment "
            f"{ctx.deployment!r} has no registered DBOS application "
            f"version, so no worker has ever booted against its "
            f"database; refusing to enqueue {PUBLISHER_WORKFLOW_NAME} "
            "because the row could not be dispatched or restamped. "
            "Start the deployment's worker and retry."
        )

    result = enqueue_deployment_workflow(
        deployment=ctx.deployment,
        dsn=dsn,
        workflow_name=PUBLISHER_WORKFLOW_NAME,
        args=(branch_name, force_enrichers),
        workflow_timeout=PUBLISHER_WORKFLOW_TIMEOUT_SECONDS,
    )
    log.info(
        "request_republish: deployment=%s reason=%r branch=%s "
        "force_enrichers=%s workflow_name=%s workflow_id=%s",
        ctx.deployment, reason, branch_name, force_enrichers,
        PUBLISHER_WORKFLOW_NAME, result.workflow_id,
    )
    return result.workflow_id


def _registered_app_version(dsn: str) -> str | None:
    """Return the newest registered DBOS application version, strictly.

    ``dbos_enqueue.latest_registered_app_version`` swallows every
    exception because it only decorates an enqueue with a hint. This
    reads the same row as a REFUSAL predicate, so connection and query
    failures must propagate: "the database is unreachable" and "no
    worker has ever booted here" are different answers and must not
    collapse into the same refusal message. The strict reader lives
    beside the enqueue helper so the two never drift apart.
    """
    from okg.substrate.runtime.dbos_enqueue import registered_app_version

    return registered_app_version(
        dsn, application_name="okg-request-republish", connect_timeout=5,
    )


def _context_dsn(ctx: WorkflowContext) -> str:
    """Reconstruct a DSN from the bound SDK connection.

    Same pattern as ``escalation_response._send_wake``: the SDK holds a
    live connection, while DBOS enqueue helpers take a URL.
    """
    import urllib.parse

    info = ctx.conn.info
    password = info.password
    auth = (
        f"{info.user}:{urllib.parse.quote(password, safe='')}@"
        if password else f"{info.user}@"
    )
    return f"postgresql://{auth}{info.host}:{info.port}/{info.dbname}"
