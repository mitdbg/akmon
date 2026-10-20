"""End-to-end: a sealed-snapshot source whose input moves mid-run (#2365).

PACT runner-ack-sealed-snapshot-token. A source that reads from a snapshot it
took before reading (copied transcript bytes, pinned git ref tips) returns an
applied token that is true for the facts it emitted even when the live input
moves while the run is in flight. The runner acknowledges that token, records
``input_advanced_during_run``, and the next poll sees the newer input as a
change. A source that did not declare the sealed binding is still refused.

Uses a migrated disposable test DB (``okg_migrated``) because the proof is the
durable cursor and replay latch in ``okg.source_watermarks``.
"""
from __future__ import annotations

import hashlib

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.change_identity import identity_fingerprint
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.base import RecordSet, SourceRun
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
    tokens_equal,
)


pytestmark = pytest.mark.postgres


def _token(seed: str) -> dict:
    return compose_token(
        content={
            "kind": "content_hash",
            "value": hashlib.sha256(seed.encode()).hexdigest(),
        },
        config=hashlib.sha256(b"cfg").hexdigest(),
        emit_version=hashlib.sha256(b"emit").hexdigest(),
    )


class _LiveProbe:
    """Reports the live input's token, compared with the durable cursor."""

    def __init__(self, token: dict) -> None:
        self.live = token

    def probe(self, *, cursor, sync_scope):
        del sync_scope
        prior = cursor.get(CURSOR_TOKEN_KEY) if cursor else None
        return ProbeVerdict(
            changed=prior is None or not tokens_equal(prior, self.live),
            token=self.live,
        )


class _SnapshotAdapter:
    """Snapshots the live input, then the input advances while it reads."""

    name = "sealed-snapshot-spy"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self, probe: _LiveProbe, *, binding: str) -> None:
        self.change_probe = probe
        self.binding = binding
        self.advance_to: dict | None = None
        self.run_calls = 0
        self.last_run_stats: dict = {}

    def run(self, run_id, *, mode="cursor", cursor=None, sync_scope=None):
        del run_id, cursor, sync_scope
        self.run_calls += 1
        snapshot_token = self.change_probe.live
        if self.advance_to is not None:
            # Another writer changes the live input after the snapshot.
            self.change_probe.live = self.advance_to
            self.advance_to = None
        self.last_run_stats = {"snapshot_calls": self.run_calls}
        # The runner forces the first read into a full replay, which must
        # prove a completed record set.
        completed_scope = mode in {"reconcile", "scope_complete"}
        return SourceRun(
            facts=iter(()),
            completed_scope=completed_scope,
            next_cursor={"spy_run": self.run_calls},
            applied_raw_token=snapshot_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            applied_token_binding=self.binding,
            run_mode=mode,
            record_set=RecordSet() if completed_scope else None,
        )


@pytest.fixture
def runner(okg_migrated):
    return SourceRunner(
        okg_migrated,
        source_name="sealed-snapshot-spy",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )


def _watermark(runner: SourceRunner) -> tuple[dict, dict]:
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        cursor, replay_required = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()
    return cursor, replay_required


def test_sealed_snapshot_moved_mid_run_is_acknowledged_then_repolled(runner):
    token_a, token_b, token_c = _token("a"), _token("b"), _token("c")
    probe = _LiveProbe(token_a)
    adapter = _SnapshotAdapter(probe, binding="sealed_snapshot")
    runner.run(adapter, mode="cursor")
    assert runner.evaluate_poll(adapter).enqueue is False

    # The input moves to B; the adapter snapshots B, and while it reads, the
    # live input moves on to C. The post-run probe therefore sees C.
    probe.live = token_b
    adapter.advance_to = token_c
    result = runner.run(adapter, mode="cursor")

    assert result.input_advanced_during_run is True
    assert adapter.last_run_stats["input_advanced_during_run"] is True
    cursor, replay_required = _watermark(runner)
    assert tokens_equal(cursor[CURSOR_TOKEN_KEY]["raw_token"], token_b)
    assert replay_required == {}

    # The next poll sees the newer input as a change and reads it.
    poll = runner.evaluate_poll(adapter)
    assert poll.enqueue is True
    assert poll.changed is True
    caught_up = runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 3
    assert caught_up.input_advanced_during_run is False
    assert "input_advanced_during_run" not in adapter.last_run_stats
    cursor, _ = _watermark(runner)
    assert tokens_equal(cursor[CURSOR_TOKEN_KEY]["raw_token"], token_c)
    assert runner.evaluate_poll(adapter).enqueue is False


def test_unsealed_source_moved_mid_run_is_still_refused(runner):
    token_a, token_b, token_c = _token("a"), _token("b"), _token("c")
    probe = _LiveProbe(token_a)
    adapter = _SnapshotAdapter(probe, binding="unverified")
    runner.run(adapter, mode="cursor")

    probe.live = token_b
    adapter.advance_to = token_c
    with pytest.raises(
        SubstrateError,
        match="cannot acknowledge change identity",
    ) as refused:
        runner.run(adapter, mode="cursor")

    message = str(refused.value)
    assert "exact_applied_snapshot=False" in message
    assert "pipeline_snapshot_stable=True" in message
    assert "authoritative_replay_proof=True" in message
    assert "applied_token_binding=unverified" in message
    assert f"applied_token={identity_fingerprint(token_b)}" in message
    assert f"post_run_token={identity_fingerprint(token_c)}" in message
    assert refused.value.data["reason"] == (
        "post_probe_differs_without_sealed_snapshot"
    )
    cursor, replay_required = _watermark(runner)
    assert tokens_equal(cursor[CURSOR_TOKEN_KEY]["raw_token"], token_a)
    assert replay_required["obligation"] == "in_flight_delta"
