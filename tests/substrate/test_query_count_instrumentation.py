"""Unit tests for count-based-perf-gate / req.counts.emit.

The profiling substrate must surface a deterministic per-run DB round-trip
count (machine-load immune), independent of whether the heavyweight detailed
`queries` list was collected. These tests pin:

  * `ProfileContext.query_count` increments for every recorded query, even on
    the lightweight path (collect_detail=False).
  * `summarize()` exposes that count on `ProfileSummary.query_count`, preferring
    the always-on counter and falling back to len(queries).
"""
from __future__ import annotations

import pytest

from okg.substrate.profiling.context import ProfileContext, summarize
from okg.substrate.profiling.model import ProfileRun, QueryTiming
from okg.substrate.profiling.policy import resolve_policy


def _ctx(*, collect_detail: bool) -> ProfileContext:
    return ProfileContext(
        deployment="test-dep",
        target_kind="ingest",
        command="okg_ingest",
        policy=resolve_policy(None),
        collect_detail=collect_detail,
    )


def test_record_query_counts_without_collect_detail() -> None:
    """The always-on counter must increment even when detailed query capture
    is off — that is the deterministic count the bench gate reads."""
    ctx = _ctx(collect_detail=False)
    for _ in range(451):
        ctx.record_query("SELECT 1", duration_ns=1000, rowcount=1)
    assert ctx.query_count == 451
    # Lightweight path does NOT populate the heavyweight list...
    assert ctx.queries == []


def test_record_query_counts_with_collect_detail() -> None:
    ctx = _ctx(collect_detail=True)
    for _ in range(7):
        ctx.record_query("INSERT INTO t VALUES (1)", duration_ns=2000, rowcount=1)
    assert ctx.query_count == 7
    assert len(ctx.queries) == 7


def test_summarize_prefers_run_query_count() -> None:
    run = ProfileRun(
        profile_run_id="r",
        deployment="d",
        target_kind="ingest",
        command="c",
        started_at="now",
        duration_ms=10.0,
        query_count=451,
    )
    summary = summarize(run)
    assert summary.query_count == 451


def test_summarize_falls_back_to_len_queries() -> None:
    run = ProfileRun(
        profile_run_id="r",
        deployment="d",
        target_kind="ingest",
        command="c",
        started_at="now",
        duration_ms=10.0,
        queries=[
            QueryTiming(fingerprint="f", stage=None, duration_ns=1),
            QueryTiming(fingerprint="g", stage=None, duration_ns=1),
        ],
    )
    summary = summarize(run)
    assert summary.query_count == 2


@pytest.mark.postgres
def test_executemany_and_copy_count_as_round_trips(clean_runtime: str) -> None:
    """Regression (PR #448 deep review): query_count is meant to be a faithful
    DB round-trip count, but the psycopg patch only wrapped Cursor.execute --
    executemany and COPY bypass it, so work shifted onto them was invisible to
    the count gate. Both must now increment the counter."""
    import psycopg

    from okg.substrate.profiling.context import profiling_context

    with profiling_context(
        deployment="t",
        target_kind="ingest",
        command="c",
        policy=resolve_policy(None),
    ) as ctx:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
             conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _qc (n int)")
            before_em = ctx.query_count
            cur.executemany("INSERT INTO _qc VALUES (%s)", [(1,), (2,), (3,)])
            after_em = ctx.query_count
            with cur.copy("COPY _qc (n) FROM STDIN") as cp:
                cp.write_row((4,))
            after_copy = ctx.query_count

    # Each is counted exactly once (one batched/streamed round-trip), not zero
    # (the pre-fix undercount) and not duplicated via Cursor.execute.
    assert after_em == before_em + 1
    assert after_copy == after_em + 1
