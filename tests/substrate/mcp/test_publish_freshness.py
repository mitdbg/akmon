"""harden-okg-ci-runtime / req.self-staleness-signal.

The publish-staleness signal must flag a wedged publisher that source_freshness
(poll-watermark only) reads as green, and must NOT false-positive on a
quiet-but-healthy deployment. These exercise the pure summarizer directly with
fixed timestamps, including the exact 2026-06-16 okg-ci shape.
"""
from __future__ import annotations

import datetime

from okg.substrate.mcp.freshness import (
    parse_generation_timestamp,
    summarize_publish_lag,
)

UTC = datetime.timezone.utc


def _dt(h: int, m: int, s: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 6, 16, h, m, s, tzinfo=UTC)


def test_parse_generation_timestamp_roundtrip() -> None:
    gen = "okg:okg-ci:branch:default:gen:20260616T065102706765Z:9880e8714653"
    assert parse_generation_timestamp(gen) == datetime.datetime(
        2026, 6, 16, 6, 51, 2, 706765, tzinfo=UTC,
    )


def test_parse_generation_timestamp_bad_inputs() -> None:
    assert parse_generation_timestamp(None) is None
    assert parse_generation_timestamp("not-a-gen-id") is None
    assert parse_generation_timestamp("okg:d:branch:default:gen:nope:h") is None


def test_wedged_publisher_is_flagged() -> None:
    # The 2026-06-16 okg-ci shape: polls kept landing (~05:30) while the last
    # published generation was stuck at 00:40 -> a ~5h lag must read STALE.
    out = summarize_publish_lag(
        latest_publish=_dt(0, 40),
        newest_poll=_dt(5, 30),
        now=_dt(5, 38),
        threshold_s=1800.0,
    )
    assert out is not None
    assert out["stale"] is True
    assert out["publish_lag_seconds"] > 1800
    assert "stalled" in out["warning"]


def test_healthy_publish_not_flagged() -> None:
    out = summarize_publish_lag(
        latest_publish=_dt(6, 50),
        newest_poll=_dt(6, 51),
        now=_dt(6, 51, 30),
        threshold_s=1800.0,
    )
    assert out is not None
    assert out["stale"] is False
    assert "warning" not in out


def test_quiet_deployment_not_false_flagged() -> None:
    # Nothing new to publish: the newest poll is no fresher than the last
    # publish, so the lag is ~0 even though both are old. Must NOT flag.
    out = summarize_publish_lag(
        latest_publish=_dt(1, 0),
        newest_poll=_dt(1, 0),
        now=_dt(6, 0),  # 5h since publish, but no newer poll
        threshold_s=1800.0,
    )
    assert out is not None
    assert out["stale"] is False
    assert out["publish_lag_seconds"] == 0


def test_no_poll_data_is_not_stale() -> None:
    out = summarize_publish_lag(
        latest_publish=_dt(1, 0), newest_poll=None, now=_dt(6, 0),
        threshold_s=1800.0,
    )
    assert out is not None
    assert out["stale"] is False
    assert out["publish_lag_seconds"] is None


def test_none_publish_returns_none() -> None:
    assert summarize_publish_lag(
        latest_publish=None, newest_poll=_dt(5, 0), now=_dt(5, 1),
        threshold_s=1800.0,
    ) is None
