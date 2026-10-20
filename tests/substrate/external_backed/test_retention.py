"""req.external-backed-nodes-rollups.retention.

A per-(node_id, metric) watermark distinguishes `aged_out` (existed, evicted)
from `no_data` (never existed, or a gap within the retained window). Eviction is
forced in-test via drop_chunks; the watermark survives it. empty-equals-aged_out
is never coerced.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.external_backed.series import read_series_window, record_sample

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"
_METRIC = "gpu_util"


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def _read(conn, start, end):
    return read_series_window(
        conn, deployment_name=_DEPLOY, node_id=_NODE, metric=_METRIC,
        start=start, end=end,
    )


def test_aged_out_distinguished_from_no_data(clean_runtime: str) -> None:
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=40)          # will be evicted
    recent_a = now - timedelta(hours=2)
    recent_b = now - timedelta(hours=1)

    with _conn(clean_runtime) as conn:
        record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE, metric=_METRIC,
                      value=50.0, observed_at=old)
        record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE, metric=_METRIC,
                      value=60.0, observed_at=recent_a)
        record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE, metric=_METRIC,
                      value=64.0, observed_at=recent_b)

        # Force eviction of the old chunk (real retention mechanism). The
        # watermark table is untouched.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT drop_chunks('okg.node_series', %s::timestamptz)",
                (now - timedelta(days=30),),
            )

        # watermark survived
        with conn.cursor() as cur:
            cur.execute(
                "SELECT first_observed_at FROM okg.node_series_watermark "
                "WHERE node_id=%s AND metric=%s", (_NODE, _METRIC),
            )
            assert cur.fetchone()[0] == old

        # 1. the evicted old range -> AGED_OUT (existed, gone)
        aged = _read(conn, old - timedelta(hours=1), old + timedelta(hours=1))
        assert aged["state"] == "aged_out"
        assert aged["rows"] == []

        # 2. a gap WITHIN the retained window -> NO_DATA (not aged_out)
        gap = _read(conn, now - timedelta(minutes=90), now - timedelta(minutes=80))
        assert gap["state"] == "no_data"

        # 3. before the metric ever existed -> NO_DATA
        before = _read(conn, now - timedelta(days=60), now - timedelta(days=59))
        assert before["state"] == "no_data"
        assert before["reason"] == "before_first_sample"

        # 4. a range with surviving rows -> DATA
        data = _read(conn, recent_a - timedelta(minutes=1), now)
        assert data["state"] == "data"
        assert len(data["rows"]) == 2

        # the two states are genuinely distinct (no empty=aged_out coercion)
        assert aged["state"] != gap["state"]


def test_metric_never_reported_is_no_data(clean_runtime: str) -> None:
    now = datetime.now(timezone.utc)
    with _conn(clean_runtime) as conn:
        out = _read(conn, now - timedelta(hours=1), now)
    assert out["state"] == "no_data"
    assert out["reason"] == "metric_never_reported"


def test_all_evicted_reads_aged_out(clean_runtime: str) -> None:
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=40)
    with _conn(clean_runtime) as conn:
        record_sample(conn, deployment_name=_DEPLOY, node_id=_NODE, metric=_METRIC,
                      value=1.0, observed_at=old)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT drop_chunks('okg.node_series', %s::timestamptz)",
                (now - timedelta(days=30),),
            )
        # all samples gone, but the watermark remembers they existed
        out = _read(conn, old - timedelta(hours=1), old + timedelta(hours=1))
    assert out["state"] == "aged_out"


# ---------------------------------------------------------------------------
# task.csp.classes / req.csp.retention (issue #2011).
#
# Call samples are kept in three NAMED retention classes -- hot, warm, cold --
# each its own hypertable with its own retention and compression policy. The
# windowed reader returns data / aged_out / no_data per class, and never
# coerces an empty window to no_data when the watermark shows samples existed.
# ---------------------------------------------------------------------------

from okg.substrate.external_backed.call_sample import (  # noqa: E402
    RETENTION_CLASSES,
    RelationPolicyRegistry,
    RelationSamplePolicy,
    live_identity,
    read_sample_window,
    record_tool_call_sample,
    table_for_class,
)

_SAMPLE_RELATION = "live.cluster_jobs"

#: The intervals schema.sql declares. Distinct by design: a class exists to
#: keep data for a different length of time.
_EXPECTED_RETENTION = {"hot": "7 days", "warm": "90 days", "cold": "730 days"}
_EXPECTED_COMPRESSION = {"hot": "1 day", "warm": "7 days", "cold": "30 days"}


@pytest.fixture
def _empty_call_samples(clean_runtime: str):
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE okg.call_sample_hot, okg.call_sample_warm, "
            "okg.call_sample_cold, okg.call_sample_watermark"
        )
    yield clean_runtime


def _sample_identity():
    return live_identity(
        principal="user:operator",
        decision="allow",
        trace_id="trace-retention",
        session_id="session-retention",
    )


def _record(conn, retention_class: str, observed_at):
    record_tool_call_sample(
        conn,
        deployment_name=_DEPLOY,
        tool_name="expand",
        relation=_SAMPLE_RELATION,
        node_id=_NODE,
        identity=_sample_identity(),
        status="ok",
        payload={"rows": [1]},
        observed_at=observed_at,
        registry=RelationPolicyRegistry(
            [
                RelationSamplePolicy(
                    relation=_SAMPLE_RELATION,
                    policy="inline",
                    retention_class=retention_class,
                )
            ]
        ),
    )


def _read_class(conn, retention_class: str, start, end):
    return read_sample_window(
        conn,
        deployment_name=_DEPLOY,
        node_id=_NODE,
        relation=_SAMPLE_RELATION,
        retention_class=retention_class,
        start=start,
        end=end,
    )


def test_three_sample_hypertables_with_distinct_retention(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        for retention_class in RETENTION_CLASSES:
            table = table_for_class(retention_class).split(".", 1)[1]
            cur.execute(
                "SELECT count(*) FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema='okg' AND hypertable_name=%s",
                (table,),
            )
            assert cur.fetchone()[0] == 1, f"{table} is not a hypertable"

        cur.execute(
            "SELECT hypertable_name, proc_name, config->>'drop_after', "
            "config->>'compress_after' "
            "FROM timescaledb_information.jobs "
            "WHERE hypertable_schema='okg' "
            "AND hypertable_name IN ('call_sample_hot','call_sample_warm',"
            "'call_sample_cold')"
        )
        jobs = cur.fetchall()

    retention = {
        name: drop_after
        for name, proc, drop_after, _ in jobs
        if proc == "policy_retention"
    }
    compression = {
        name: compress_after
        for name, proc, _, compress_after in jobs
        if proc == "policy_compression"
    }
    assert retention == {
        f"call_sample_{k}": v for k, v in _EXPECTED_RETENTION.items()
    }
    assert compression == {
        f"call_sample_{k}": v for k, v in _EXPECTED_COMPRESSION.items()
    }
    # The three intervals are genuinely different -- a class that kept data for
    # the same time as another would not be a class.
    assert len(set(retention.values())) == 3


@pytest.mark.parametrize("retention_class", RETENTION_CLASSES)
def test_reader_states_per_retention_class(
    _empty_call_samples: str, retention_class: str
) -> None:
    dsn = _empty_call_samples
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=800)
    recent = now - timedelta(minutes=5)
    table = table_for_class(retention_class)

    with _conn(dsn) as conn:
        _record(conn, retention_class, old)
        _record(conn, retention_class, recent)

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT drop_chunks('{table}', %s::timestamptz)",
                (now - timedelta(days=400),),
            )
            cur.execute(
                "SELECT first_observed_at FROM okg.call_sample_watermark "
                "WHERE node_id=%s AND relation=%s AND retention_class=%s",
                (_NODE, _SAMPLE_RELATION, retention_class),
            )
            assert cur.fetchone()[0] == old, "the watermark must survive eviction"

        # 1. the evicted range existed -> aged_out, never no_data.
        aged = _read_class(
            conn, retention_class, old - timedelta(hours=1), old + timedelta(hours=1)
        )
        # 2. a range that never existed -> no_data.
        before = _read_class(
            conn,
            retention_class,
            old - timedelta(days=20),
            old - timedelta(days=19),
        )
        # 3. a surviving range -> data.
        data = _read_class(conn, retention_class, recent - timedelta(minutes=1), now)

    assert aged["state"] == "aged_out"
    assert aged["retention_class"] == retention_class
    assert aged["rows"] == []
    assert before["state"] == "no_data"
    assert before["reason"] == "before_first_sample"
    assert data["state"] == "data"
    assert len(data["rows"]) == 1
    # The three states are genuinely distinct for this class.
    assert {aged["state"], before["state"], data["state"]} == {
        "aged_out",
        "no_data",
        "data",
    }


def test_gap_inside_the_retained_window_is_no_data(_empty_call_samples: str) -> None:
    now = datetime.now(timezone.utc)
    with _conn(_empty_call_samples) as conn:
        _record(conn, "warm", now - timedelta(hours=3))
        _record(conn, "warm", now - timedelta(minutes=1))
        gap = _read_class(
            conn, "warm", now - timedelta(hours=2), now - timedelta(hours=1)
        )
    assert gap["state"] == "no_data"
    assert gap["reason"] == "gap_within_retained_window"


def test_a_class_never_answers_for_another_class(_empty_call_samples: str) -> None:
    # Samples written to hot are invisible to the warm and cold readers, and
    # their absence reads as no_data rather than as an aged-out hot sample.
    now = datetime.now(timezone.utc)
    with _conn(_empty_call_samples) as conn:
        _record(conn, "hot", now - timedelta(minutes=1))
        hot = _read_class(conn, "hot", now - timedelta(hours=1), now)
        warm = _read_class(conn, "warm", now - timedelta(hours=1), now)
        cold = _read_class(conn, "cold", now - timedelta(hours=1), now)
    assert hot["state"] == "data"
    assert warm["state"] == "no_data"
    assert warm["reason"] == "relation_never_sampled"
    assert cold["state"] == "no_data"


def test_a_partially_evicted_window_says_so(_empty_call_samples: str) -> None:
    # A window whose early samples retention removed still returns rows. Rows
    # plus silence would read as a complete answer, so the result carries
    # truncated_by_retention and names what survived.
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=800)
    recent = now - timedelta(minutes=5)
    with _conn(_empty_call_samples) as conn:
        _record(conn, "warm", old)
        _record(conn, "warm", recent)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT drop_chunks('okg.call_sample_warm', %s::timestamptz)",
                (now - timedelta(days=400),),
            )
        # The window spans both the evicted stretch and the surviving one.
        partial = _read_class(conn, "warm", old - timedelta(days=1), now)
        # A window that begins at the earliest surviving sample lost nothing,
        # so it is NOT marked.
        whole = _read_class(conn, "warm", recent, now)

    assert partial["state"] == "data"
    assert len(partial["rows"]) == 1
    assert partial["truncated_by_retention"] is True
    assert partial["first_observed_at"] == old.isoformat()
    assert "earliest_retained_at" in partial

    assert whole["state"] == "data"
    assert "truncated_by_retention" not in whole


def test_an_intact_window_is_not_marked_truncated(_empty_call_samples: str) -> None:
    now = datetime.now(timezone.utc)
    with _conn(_empty_call_samples) as conn:
        _record(conn, "warm", now - timedelta(hours=2))
        _record(conn, "warm", now - timedelta(minutes=1))
        out = _read_class(conn, "warm", now - timedelta(days=1), now)
    assert out["state"] == "data"
    assert len(out["rows"]) == 2
    assert "truncated_by_retention" not in out
