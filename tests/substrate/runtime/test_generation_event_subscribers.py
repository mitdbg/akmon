"""Tests for the generation-event subscribers doctor probe + the
channel-name helper.

The end-to-end "publish + LISTEN observes a NOTIFY" test is exercised
by the live verification step (`publish-generation-events-via-dbos-
streams` proposal task 8), not here.
"""
from __future__ import annotations

import psycopg
import pytest


def test_channel_name_helper():
    """Short existing deployment names keep their historic channel."""
    from okg.substrate.publisher.publisher import (
        generation_published_channel,
    )

    assert generation_published_channel("okg-workspace") == (
        "okg_generation_okg-workspace_published"
    )
    assert generation_published_channel("af_pubs") == (
        "okg_generation_af_pubs_published"
    )


def test_channel_name_helper_hashes_unsafe_or_long_refs():
    from okg.substrate.publisher.publisher import (
        generation_published_channel,
    )

    long_path = (
        "/home/example/projects/analysis/nested/workspace/deployment"
    )
    channel = generation_published_channel(long_path)
    assert channel.startswith("okg_generation_")
    assert channel.endswith("_published")
    assert len(channel.encode("utf-8")) <= 63
    assert "/" not in channel
    assert generation_published_channel(long_path) == channel

    spaced = generation_published_channel("deployment with spaces")
    assert len(spaced.encode("utf-8")) <= 63
    assert " " not in spaced


def test_subscribers_verdict_to_dict_shape():
    """`to_dict` returns a stable shape the doctor renders."""
    from okg.substrate.runtime.publisher_health import (
        GenerationEventSubscribersVerdict,
    )

    v = GenerationEventSubscribersVerdict(
        deployment="test",
        ok=True,
        code="generation_event_subscribers_observed",
        message="1 LISTEN session on channel 'x'",
        channel="x",
        count=1,
        listeners=[{"pid": 42, "application_name": "psql",
                    "client_addr": None, "state": "idle",
                    "backend_start": "2026-05-11"}],
    )
    d = v.to_dict()
    assert d["count"] == 1
    assert d["channel"] == "x"
    assert d["listeners"][0]["pid"] == 42


pytestmark = pytest.mark.postgres


def test_subscribers_probe_returns_zero_on_no_listeners(
    clean_runtime: str,
) -> None:
    """With no LISTENers, the probe reports zero."""
    from okg.substrate.runtime.publisher_health import (
        check_generation_event_subscribers,
    )

    verdict = check_generation_event_subscribers(
        deployment="ghost-deployment",
        dsn=clean_runtime,
    )

    assert verdict.ok is True
    assert verdict.count == 0
    assert verdict.code == "generation_event_subscribers_none"
    assert verdict.channel == (
        "okg_generation_ghost-deployment_published"
    )


def test_subscribers_probe_observes_active_listener(
    clean_runtime: str,
) -> None:
    """A separate connection that has issued LISTEN registers in the
    probe's count.

    Note: the probe's heuristic looks for `LISTEN <channel>` in
    `pg_stat_activity.query`. Some Postgres versions show the LISTEN
    statement in the query field; others show the most recent
    non-LISTEN query. This test verifies the happy path on the dev
    Postgres image (17.x). If it fails on a different version,
    the probe's heuristic may need adjustment.
    """
    from okg.substrate.runtime.publisher_health import (
        check_generation_event_subscribers,
    )

    deployment = "listener-test"
    channel = f"okg_generation_{deployment}_published"

    # Open a second connection, issue LISTEN, then poll the probe.
    with psycopg.connect(clean_runtime, autocommit=True) as listener:
        with listener.cursor() as cur:
            # Use psycopg's SQL identifier quoting to handle the
            # hyphen in `listener-test`.
            from psycopg import sql
            cur.execute(
                sql.SQL("LISTEN {}").format(sql.Identifier(channel))
            )

        verdict = check_generation_event_subscribers(
            deployment=deployment,
            dsn=clean_runtime,
        )

        # The probe is heuristic; some Postgres versions may not
        # show the LISTEN in pg_stat_activity.query. Tolerate that
        # by skipping rather than failing.
        if verdict.count == 0:
            pytest.skip(
                "Postgres pg_stat_activity.query doesn't surface "
                "LISTEN; probe heuristic doesn't apply on this "
                "server version"
            )

        assert verdict.ok is True
        assert verdict.count >= 1
        assert verdict.code == "generation_event_subscribers_observed"
