"""The fan-out actually overlaps: wall-clock scales with width, not with N.

`req.shared-model-call-fanout` acceptance asks for a materially reduced
wall-clock at width 8 versus serial, with identical yield, referenced against
issue #913's measurement (60.8s serial vs 10.0s at width 8, 6.1x).

WHAT THIS PROVES AND WHAT IT DOES NOT. The provider here is a double that
sleeps, so this measures the MECHANISM: that N items at width W take
~ceil(N/W) x latency rather than N x latency, and that widening changes only
timing and never the results. It does NOT prove a number against a real
provider — real latency varies, and a live run is owed separately. Stating that
distinction matters because "3x faster" measured against a stub is not the same
claim as "3x faster in production", and conflating them is how a performance
requirement gets marked satisfied without a measurement anyone can act on.

Timing assertions use a generous margin: the point is the SHAPE (sub-linear in
N), not a tight constant, because a tight constant on a shared CI machine is a
flaky test rather than a stronger one.
"""
from __future__ import annotations

import time

import pytest

from okg.substrate.llm.fanout import ModelCallJob, complete_many

_LATENCY = 0.05


def _jobs(n: int) -> list[ModelCallJob]:
    return [
        ModelCallJob(
            system="s", user=f"u{i}", typed_input={"i": i}, item_ref=f"r{i}",
        )
        for i in range(n)
    ]


def _sleeping_call(job: ModelCallJob) -> str:
    time.sleep(_LATENCY)
    return f"content:{job.item_ref}"


def _run(n: int, width: int):
    jobs = _jobs(n)
    started = time.monotonic()
    out = complete_many(
        jobs,
        call=_sleeping_call,
        derive_key=lambda job: job.item_ref,
        record=lambda index, key, content: content,
        width=width,
    )
    return time.monotonic() - started, out


def test_width_eight_is_materially_faster_than_serial() -> None:
    """The acceptance property, on the mechanism: 8 items at width 8 must beat
    8 items serially by a wide margin."""
    serial_elapsed, serial_out = _run(8, width=1)
    wide_elapsed, wide_out = _run(8, width=8)

    assert [o.content for o in serial_out] == [o.content for o in wide_out], (
        "yield differed between widths; the fan-out is not a pure optimisation"
    )
    assert serial_elapsed > 8 * _LATENCY * 0.8
    assert wide_elapsed < serial_elapsed / 3, (
        f"expected >3x reduction at width 8; serial={serial_elapsed:.3f}s "
        f"wide={wide_elapsed:.3f}s"
    )


def test_wall_clock_tracks_ceil_n_over_width_not_n() -> None:
    """Sub-linear in N is the mechanism claim. 12 items at width 4 should take
    about three latencies, not twelve."""
    elapsed, out = _run(12, width=4)

    assert all(o.ok for o in out)
    assert elapsed < 12 * _LATENCY * 0.6, (
        f"12 items at width 4 took {elapsed:.3f}s, which is not sub-linear in N"
    )
    assert elapsed >= 3 * _LATENCY * 0.8, (
        "faster than three sequential batches — the double is not sleeping, so "
        "this test would pass even without any overlap"
    )


@pytest.mark.parametrize("width", [1, 2, 4, 8])
def test_results_are_identical_at_every_width(width: int) -> None:
    _, out = _run(6, width=width)
    assert [o.content for o in out] == [f"content:r{i}" for i in range(6)]


def test_replay_does_not_overlap() -> None:
    """Replay is serial by contract, so its wall-clock must stay linear even
    when a width is passed — otherwise the serial guarantee is nominal."""
    jobs = _jobs(4)
    started = time.monotonic()
    complete_many(
        jobs,
        call=_sleeping_call,
        derive_key=lambda job: job.item_ref,
        record=lambda index, key, content: content,
        width=8,
        serial=True,
    )
    elapsed = time.monotonic() - started

    assert elapsed > 4 * _LATENCY * 0.8, (
        f"replay finished in {elapsed:.3f}s — it overlapped despite serial=True"
    )
