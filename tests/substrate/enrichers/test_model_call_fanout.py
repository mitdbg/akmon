"""The two-phase fan-out: what runs where, and what happens when k of N fail.

`shared-model-call-fanout` and `model-call-admission` land together because
both rewrite the same chokepoint. The properties asserted here are the ones
whose violations are SILENT in production: connection affinity (phase 1 must
touch no connection), key parity between the serial and parallel paths,
positional alignment of results with inputs, and the watermark-holding
classification of failures.
"""
from __future__ import annotations

import threading

import pytest

from okg.substrate.llm.fanout import (
    AdmissionDecision,
    ModelCallJob,
    RateLimiter,
    complete_many,
)


def _jobs(n: int) -> list[ModelCallJob]:
    return [
        ModelCallJob(
            system="sys", user=f"user-{i}", typed_input={"i": i},
            item_ref=f"item-{i}", estimated_usd=0.01,
        )
        for i in range(n)
    ]


def _run(jobs, **kw):
    defaults = dict(
        call=lambda job: f"content:{job.item_ref}",
        derive_key=lambda job: ("key", job.typed_input["i"]),
        record=lambda idx, key, content: content,
        width=4,
    )
    defaults.update(kw)
    return complete_many(jobs, **defaults)


def test_results_are_positionally_aligned_with_inputs() -> None:
    """Order is a correctness property: a shortened or reordered list silently
    attaches one item's result to another item's row."""
    out = _run(_jobs(5))
    assert [o.content for o in out] == [f"content:item-{i}" for i in range(5)]


def test_phase_one_runs_concurrently_and_phase_two_does_not() -> None:
    """Provider calls overlap; bookkeeping is single-threaded. Phase 2 touches
    the publish-cycle connection, which is not thread-safe."""
    in_flight, peak = 0, 0
    lock = threading.Lock()
    record_threads: set[int] = set()

    def _call(job):
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        try:
            threading.Event().wait(0.02)
            return "ok"
        finally:
            with lock:
                in_flight -= 1

    def _record(idx, key, content):
        record_threads.add(threading.get_ident())
        return content

    _run(_jobs(8), call=_call, record=_record, width=4)

    assert peak > 1, "phase 1 did not overlap"
    assert len(record_threads) == 1, "phase 2 ran on more than one thread"


def test_replay_mode_spawns_no_threads() -> None:
    """Replay reads the database and has no network to overlap."""
    caller = threading.get_ident()
    seen: set[int] = set()

    def _call(job):
        seen.add(threading.get_ident())
        return "ok"

    _run(_jobs(6), call=_call, width=8, serial=True)
    assert seen == {caller}


def test_key_derivation_is_shared_and_reaches_the_recorder() -> None:
    """The recorder must receive the key the caller derived — not one the
    fan-out recomputed. Drift here surfaces as unexplained replay exclusions,
    never as an error."""
    derived, received = [], []

    def _derive(job):
        key = ("k", job.item_ref)
        derived.append(key)
        return key

    def _record(idx, key, content):
        received.append(key)
        return content

    _run(_jobs(4), derive_key=_derive, record=_record)
    assert received == derived


def test_one_provider_failure_does_not_discard_the_others() -> None:
    """k of N failing must keep the N-k successes AND classify the failure as
    watermark-holding, so the item retries rather than being skipped."""
    def _call(job):
        if job.item_ref == "item-2":
            raise RuntimeError("provider 503")
        return "ok"

    out = _run(_jobs(5), call=_call)

    assert [o.ok for o in out] == [True, True, False, True, True]
    assert isinstance(out[2].provider_error, RuntimeError)
    assert out[2].holds_watermark
    assert sum(1 for o in out if o.ok) == 4


def test_a_replay_miss_is_distinct_from_a_provider_failure() -> None:
    """Both yield no content, but only one holds the watermark. Conflating
    them either loses data or wedges a replay run forever.

    Note this is the REPLAY case (`serial=True`). The same `record` returning
    None in record mode means the provider answered and the bookkeeping
    failed, which does hold the watermark — see
    `test_a_recording_failure_in_record_mode_holds_the_watermark`.
    """
    out = _run(_jobs(2), record=lambda idx, key, content: None, serial=True)

    assert all(not o.ok for o in out)
    assert all(o.replay_excluded for o in out)
    assert not any(o.holds_watermark for o in out)


def test_refused_admission_holds_the_watermark_and_skips_the_provider() -> None:
    """Budget refusal must stop the call being issued at all, and must be
    retryable — not a silent skip, which is the P0 data-loss mode."""
    issued: list[str] = []

    def _call(job):
        issued.append(job.item_ref)
        return "ok"

    def _admit(job):
        if job.item_ref == "item-1":
            return AdmissionDecision(False, reason="spend cap exhausted")
        return AdmissionDecision(True, reserved_usd=job.estimated_usd)

    out = _run(_jobs(3), call=_call, admit=_admit)

    assert "item-1" not in issued
    assert out[1].budget_refused and out[1].holds_watermark
    assert out[0].ok and out[2].ok


def test_settlement_sees_the_reservation_for_admitted_calls() -> None:
    settled: list[float] = []
    _run(
        _jobs(3),
        admit=lambda job: AdmissionDecision(True, reserved_usd=job.estimated_usd),
        settle=lambda job, decision, content: settled.append(decision.reserved_usd),
    )
    assert settled == [0.01, 0.01, 0.01]


def test_rate_limiter_refuses_rather_than_blocking_unboundedly() -> None:
    """An unbounded wait inside a publish cycle holds the cycle open against
    the gate's own budget, so pacing has to give up and become a retryable
    refusal."""
    limiter = RateLimiter(per_second=1000.0, max_wait_s=0.0)
    limiter._next_at = limiter._interval * 10_000 + 1e9  # force a long wait

    out = _run(_jobs(2), rate=limiter)

    assert all(o.budget_refused for o in out)
    assert all(o.holds_watermark for o in out)


def test_rate_limiter_actually_paces() -> None:
    """Assert elapsed time, not just success. A limiter that never paces
    passes an `all(o.ok)` check, which is what the previous version of this
    test measured — i.e. nothing."""
    import time

    limiter = RateLimiter(per_second=50.0, max_wait_s=5.0)  # 20ms apart
    started = time.monotonic()
    out = _run(_jobs(5), rate=limiter, width=5)
    elapsed = time.monotonic() - started

    assert all(o.ok for o in out)
    # 5 calls at 20ms spacing cannot finish faster than ~4 intervals.
    assert elapsed >= 0.06, f"no pacing observed (elapsed {elapsed:.3f}s)"


def test_a_recording_failure_in_record_mode_holds_the_watermark() -> None:
    """`record` returning None means different things by mode. In record mode
    the provider ANSWERED and the bookkeeping failed, so the item must be
    retried; classifying it as a replay exclusion would drop paid-for work."""
    out = _run(_jobs(2), record=lambda idx, key, content: None, serial=False)

    assert all(not o.ok for o in out)
    assert all(o.holds_watermark for o in out)
    assert not any(o.replay_excluded for o in out)


def test_settlement_runs_for_failed_and_refused_jobs_too() -> None:
    """A reservation taken before the call must be settled on every path, or
    a retry loop leaks budget exactly when it is burning it."""
    settled: list[str] = []

    def _call(job):
        if job.item_ref == "item-1":
            raise RuntimeError("provider 503")
        return "ok"

    def _admit(job):
        if job.item_ref == "item-2":
            return AdmissionDecision(False, reason="cap exhausted")
        return AdmissionDecision(True, reserved_usd=job.estimated_usd)

    _run(
        _jobs(3), call=_call, admit=_admit,
        settle=lambda job, decision, content: settled.append(job.item_ref),
    )

    assert sorted(settled) == ["item-0", "item-1", "item-2"]


def test_empty_input_is_not_an_error() -> None:
    assert complete_many(
        [], call=lambda j: "", derive_key=lambda j: None,
        record=lambda i, k, c: c,
    ) == []


@pytest.mark.parametrize("width", [1, 2, 8])
def test_output_is_width_invariant(width: int) -> None:
    """Changing the width must change timing only. If results differ by width
    the fan-out is not a pure optimisation."""
    out = _run(_jobs(6), width=width)
    assert [o.content for o in out] == [f"content:item-{i}" for i in range(6)]
