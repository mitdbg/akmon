"""One bounded, admitted fan-out for every per-item model call.

Seven per-item model-call sites in the substrate once ran serially. Their work
is almost entirely network wait, and it happens inside the publish cycle
holding it open. The shared fan-out makes those provider calls parallel; live
issue #913 measurements show a 3.14x median speedup at width 8. What makes it
non-trivial is that the call is parallelisable and its BOOKKEEPING is not:

  * ``llm.complete`` touches no publish-cycle connection.
  * The outcome recorder touches ``conn`` on both sides — ``store.get`` in
    replay, ``store.record`` in record mode — and a psycopg connection is not
    thread-safe.

So this helper is two-phase: provider round-trips run in a bounded pool that
touches no connection, then outcome recording, spend settlement and
input-failure accounting run on the calling thread, in input order.

Doing it once matters because two of the three ways to get it wrong are
SILENT. Deriving the OutcomeKey differently on the parallel path makes every
later replay miss — surfacing as unexplained ``ReplayExclusion``s, not an
error. Dropping ``outcome_key_digest`` breaks the join between a call and its
recorded outcome and shows up as nothing at all. Both are structural here: the
key is derived by the same helper the serial path uses, and the digest rides
the same recorder call.

**Admission is a SEAM here, not yet an implementation.** Width bounds occupancy
and nothing else; it does not bound cost. Fanning out to 8 does not change what
a corpus costs, but it makes a runaway burn the budget 8x faster. This helper
therefore takes `admit`, `rate` and `settle` hooks and calls them for every
job. It does NOT yet wire them to `cost_budget` / `okg.deployment_cost_ledger`
— that is `model-call-admission`, whose tasks are open. Until it lands, callers
that pass no `admit` get an admission-FREE path, which is the current
production default. Do not read this module as evidence that substrate model
calls are economically bounded; they are not.

**Replay is serial.** Replay reads the database and has no network to overlap;
fanning out would put concurrent work on the publish-cycle connection for no
gain.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, TypeVar

__all__ = [
    "AdmissionDecision",
    "ModelCallJob",
    "ModelCallOutcome",
    "RateLimiter",
    "BoundedCallResult",
    "bounded_map",
    "complete_many",
]

log = logging.getLogger(__name__)

_T = TypeVar("_T")
_R = TypeVar("_R")


@dataclass(frozen=True)
class BoundedCallResult:
    """One result from :func:`bounded_map`, aligned to its input position.

    Provider exceptions stay in memory as typed failures. Callers decide what
    safe, durable category to record; this helper never serializes exception
    text or writes it to a provider-facing or graph-facing record.
    """

    value: Any = None
    error: Optional[BaseException] = None


def bounded_map(
    items: Sequence[_T],
    call: Callable[[_T], _R],
    *,
    width: Optional[int] = None,
    budget_term: str = "enrichment.model_calls",
    serial: bool = False,
) -> list[BoundedCallResult]:
    """Run independent provider calls with one bounded, ordered fan-out.

    ``width`` defaults to the deployment's shared concurrency budget. Results
    are always returned in input order, even though worker completion order is
    arbitrary. Exceptions are captured per item so a caller can hold its
    watermark while still emitting successful siblings; raw exception text is
    deliberately not persisted by this helper.
    """
    if width is None:
        from okg.substrate.db.concurrency_budget import width as configured_width

        width = configured_width(budget_term)
    if not isinstance(width, int) or width < 1:
        raise ValueError(f"{budget_term} width must be a positive int, got {width!r}")
    if not items:
        return []

    def _run(item: _T) -> BoundedCallResult:
        try:
            return BoundedCallResult(value=call(item))
        except Exception as exc:  # noqa: BLE001 - isolate one provider call
            return BoundedCallResult(error=exc)

    if serial or width <= 1 or len(items) <= 1:
        return [_run(item) for item in items]
    with ThreadPoolExecutor(
        max_workers=min(width, len(items)),
        thread_name_prefix="okg-model-call",
    ) as pool:
        return list(pool.map(_run, items))


@dataclass(frozen=True)
class ModelCallJob:
    """One provider call: what to send, and what it belongs to."""

    system: str
    user: str
    typed_input: Any
    item_ref: str
    fragment_id: str = ""
    #: Estimated USD, used to RESERVE before the call so overshoot is bounded
    #: by the reservation rather than by whatever happened to be in flight.
    estimated_usd: float = 0.0


@dataclass
class ModelCallOutcome:
    """Per-job result, positionally aligned with the input list.

    The three outcomes are distinguishable on purpose. A caller that cannot
    tell "the provider failed" from "replay had no record" from "the budget
    said no" cannot do the right thing with any of them: the first two hold
    the watermark, the third holds it too but means something different to an
    operator, and none of them may be silently treated as "no result".
    """

    content: Optional[str] = None
    provider_error: Optional[BaseException] = None
    replay_excluded: bool = False
    budget_refused: bool = False
    # Durable terminal quarantine is a successful scheduling decision, not a
    # model response and not a held failure.  Keeping it explicit prevents a
    # caller from confusing "filtered before dispatch" with authoritative []
    # while still allowing the publisher to advance past the named item.
    quarantined: bool = False

    @property
    def ok(self) -> bool:
        return self.content is not None and not self.quarantined

    @property
    def holds_watermark(self) -> bool:
        """True when this item must be retried rather than skipped."""
        return self.provider_error is not None or self.budget_refused


@dataclass
class AdmissionDecision:
    """Whether a job may be issued, and what it reserved."""

    admitted: bool
    reason: str = ""
    reserved_usd: float = 0.0


class RateLimiter:
    """Token-bucket pacing, separate from concurrency.

    Provider limits are not a function of width: 8 concurrent calls against a
    tokens-per-minute endpoint produce 429s, which route into input-failure
    accounting, hold the watermark and retry — burning MORE budget than the
    serial path they replaced. This paces up to ``max_wait_s`` and then
    refuses, rather than blocking unboundedly: an unbounded wait inside a
    publish cycle holds the cycle open against the gate's own budget.
    """

    def __init__(self, per_second: float, *, max_wait_s: float = 5.0) -> None:
        self._interval = 1.0 / per_second if per_second > 0 else 0.0
        self._max_wait = max_wait_s
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> bool:
        """Wait for this call's turn. False if the wait would exceed the bound."""
        if self._interval <= 0:
            return True
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_at)
            wait = start - now
            if wait > self._max_wait:
                return False
            self._next_at = start + self._interval
        if wait > 0:
            time.sleep(wait)
        return True


@dataclass
class _Phase1Result:
    index: int
    content: Optional[str] = None
    error: Optional[BaseException] = None
    refused: str = ""
    reserved_usd: float = 0.0
    key: Any = None


def complete_many(
    jobs: list[ModelCallJob],
    *,
    call: Callable[[ModelCallJob], str],
    derive_key: Callable[[ModelCallJob], Any],
    record: Callable[[int, Any, str], Optional[str]],
    admit: Optional[Callable[[ModelCallJob], AdmissionDecision]] = None,
    settle: Optional[
        Callable[[ModelCallJob, AdmissionDecision, "ModelCallOutcome"], None]
    ] = None,
    on_replay_miss: Optional[Callable[[int], None]] = None,
    width: int = 1,
    rate: Optional[RateLimiter] = None,
    serial: bool = False,
) -> list[ModelCallOutcome]:
    """Run ``jobs`` two-phase and return outcomes aligned with the input.

    ``call`` issues the provider request and MUST NOT touch the publish-cycle
    connection. ``record`` runs on the calling thread and may. ``derive_key``
    is the single source of the OutcomeKey for both paths — it is passed in
    rather than recomputed here so the serial and parallel paths cannot drift.

    ``serial=True`` (replay) executes phase 1 inline, spawning no threads.

    Partial failure is explicit: k of N failing never discards the N-k
    successes, and the returned list is always the same length and order as
    ``jobs`` so a caller cannot silently misalign results with inputs.
    """
    n = len(jobs)
    if n == 0:
        return []

    keys = [derive_key(job) for job in jobs]

    def _phase1(index: int) -> _Phase1Result:
        job = jobs[index]
        decision = admit(job) if admit is not None else AdmissionDecision(True)
        if not decision.admitted:
            return _Phase1Result(
                index=index,
                refused=decision.reason or "admission refused",
                key=keys[index],
            )
        if rate is not None and not rate.acquire():
            return _Phase1Result(
                index=index,
                refused="request-rate budget exhausted; paced past the wait bound",
                key=keys[index],
            )
        try:
            return _Phase1Result(
                index=index,
                content=call(job),
                reserved_usd=decision.reserved_usd,
                key=keys[index],
            )
        except Exception as exc:  # noqa: BLE001 — recorded per item in phase 2
            return _Phase1Result(
                index=index,
                error=exc,
                reserved_usd=decision.reserved_usd,
                key=keys[index],
            )

    if serial or width <= 1 or n == 1:
        phase1 = [_phase1(i) for i in range(n)]
    else:
        with ThreadPoolExecutor(
            max_workers=min(width, n),
            thread_name_prefix="okg-model-call",
        ) as pool:
            phase1 = list(pool.map(_phase1, range(n)))

    # Phase 2: single-threaded, in input order, on the caller's connection.
    outcomes: list[ModelCallOutcome] = []
    for result in sorted(phase1, key=lambda r: r.index):
        job = jobs[result.index]
        decision = AdmissionDecision(True, reserved_usd=result.reserved_usd)

        if result.refused:
            # The settle hook fires FIRST with the refusal outcome: a
            # refusal can follow a successful `admit` that already reserved
            # spend (the rate limiter refuses after admission), and a hook
            # that cannot see the refusal would either leak the reservation
            # or — worse — settle a ledger row for a call that was never
            # issued. The outcome is passed so the hook can RELEASE instead.
            outcome = ModelCallOutcome(budget_refused=True)
            if settle is not None:
                settle(job, decision, outcome)
            outcomes.append(outcome)
            log.warning(
                "model call refused for %s: %s — the CALLER must record this "
                "as an unprocessed input so the watermark holds and the item "
                "retries; this helper does not do that accounting",
                job.item_ref,
                result.refused,
            )
            continue
        if result.error is not None:
            outcome = ModelCallOutcome(provider_error=result.error)
            if settle is not None:
                settle(job, decision, outcome)
            outcomes.append(outcome)
            continue

        recorded = record(result.index, result.key, result.content or "")
        if settle is not None:
            settle(job, decision, ModelCallOutcome(content=recorded))
        if recorded is None:
            # `None` means "no content survived recording". What that MEANS
            # depends on the mode, and conflating the two is a data-loss bug:
            # in replay it is a ledgered exclusion (not retryable, replay
            # never samples fresh); in record mode the provider DID answer and
            # the bookkeeping failed, which must hold the watermark so the
            # item is retried rather than dropped.
            if on_replay_miss is not None:
                on_replay_miss(result.index)
            if serial:
                outcomes.append(ModelCallOutcome(replay_excluded=True))
            else:
                outcomes.append(
                    ModelCallOutcome(
                        provider_error=RuntimeError(
                            "model call succeeded but its outcome could not be "
                            "recorded; holding the watermark so the item retries"
                        )
                    )
                )
        else:
            outcomes.append(ModelCallOutcome(content=recorded))

    return outcomes
