"""Offline executable-scope preservation; no real queue or database proof."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import socket

import psycopg
import pytest

from okg.substrate import source_sync_enqueue as enqueue
from okg.substrate.source_sync import (
    SourceSyncCompaction, SourceSyncEvent, SourceSyncJob, source_sync_scope_payload,
)


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    attempts = []

    def deny(*args, **kwargs):
        attempts.append("external I/O")
        raise AssertionError("compaction test attempted external I/O")

    monkeypatch.setattr(psycopg, "connect", deny)
    monkeypatch.setattr(psycopg.Connection, "connect", deny)
    for name in ("connect", "connect_ex", "bind", "listen"):
        monkeypatch.setattr(socket.socket, name, deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)
    yield
    assert attempts == []


def job(*keys, source="files"):
    return SourceSyncJob(
        deployment="offline", sources=(source,), run_mode="cursor",
        include_predecessors=False, include_dependents=False, publish=False,
        events=tuple(SourceSyncEvent(
            deployment="offline", source_name=source,
            trigger_kind="fs_event", scope_key=key,
        ) for key in keys),
    )


def serialized(item):
    from dbos._serialization import (
        DBOSDefaultSerializer, WorkflowSerializationFormat, serialize_args,
    )
    return serialize_args(
        enqueue._source_sync_workflow_args(item), {},
        WorkflowSerializationFormat.NATIVE, DBOSDefaultSerializer,
    )


def row(name, item):
    inputs, fmt = serialized(item)
    return (name, inputs, fmt, 10)


def decoded(record):
    return enqueue.source_sync_payload_from_dbos_inputs(
        deployment="offline", inputs=record[1], serialization=record[2],
    )


class Cursor:
    def __init__(self):
        self.writes = []

    def execute(self, query, params):
        self.writes.append((query, params))


@pytest.mark.parametrize("existing,incoming", [
    (job("a"), job("b")),
    (job("a", "b"), job("a")),
    (job("a"), job("a", "b")),
])
def test_distinct_overflow_refuses_without_changing_inputs(existing, incoming):
    left, right = existing.to_dict(), incoming.to_dict()
    before = deepcopy((left, right))
    with pytest.raises(enqueue._SourceSyncCompactionRefused):
        enqueue._merge_source_sync_payloads(
            left, right, compaction=SourceSyncCompaction(max_events=1),
        )
    assert (left, right) == before


def test_exact_cap_duplicates_preserve_scope_and_authority():
    left, right = job("a", "a").to_dict(), job("a").to_dict()
    before = deepcopy((left, right))
    merged = enqueue._merge_source_sync_payloads(
        left, right, compaction=SourceSyncCompaction(max_events=1),
    )
    result = SourceSyncJob.from_dict(merged)
    assert enqueue._source_sync_workflow_args(result) == enqueue._source_sync_workflow_args(job("a"))
    assert source_sync_scope_payload("files", result.events)["scope_keys"] == ["a"]
    assert merged["source_sync_compaction"]["dropped_event_count"] == 0
    assert (left, right) == before


@pytest.mark.parametrize("operand", ["existing", "incoming"])
def test_known_historical_loss_is_not_relabelled_complete(operand):
    payloads = {"existing": job("a").to_dict(), "incoming": job("a").to_dict()}
    payloads[operand]["source_sync_compaction"] = {"dropped_event_count": 1}
    before = deepcopy(payloads)
    with pytest.raises(enqueue._SourceSyncCompactionRefused, match="previously dropped"):
        enqueue._merge_source_sync_payloads(
            **payloads, compaction=SourceSyncCompaction(max_events=3),
        )
    assert payloads == before


def test_scope_sample_limit_does_not_limit_executable_events():
    merged = enqueue._merge_source_sync_payloads(
        job("a").to_dict(), job("b").to_dict(),
        compaction=SourceSyncCompaction(max_events=2, max_scope_keys=1),
    )
    assert merged["source_sync_compaction"]["scope_keys_sample"] == ["a"]
    assert source_sync_scope_payload("files", SourceSyncJob.from_dict(merged).events)["scope_keys"] == ["a", "b"]


@pytest.mark.parametrize("limit", [2, 3])
def test_late_candidate_fold_overflow_performs_no_writes(limit):
    # limit2 overflows at candidate3; limit3 only overflows at incoming4.
    rows = [row(str(i), job(key)) for i, key in enumerate(("a", "b", "c"))]
    incoming = job("d").to_dict()
    before = deepcopy((rows, incoming))
    cursor = Cursor()
    assert enqueue._compact_dbos_queued_source_sync_candidates(
        cur=cursor, deployment="offline", rows=rows, incoming=incoming,
        compaction=SourceSyncCompaction(max_events=limit),
    ) is None
    assert cursor.writes == []
    assert (rows, incoming) == before


def test_duplicate_candidate_still_compacts_and_serializes_one_scope():
    cursor = Cursor()
    assert enqueue._compact_dbos_queued_source_sync_candidates(
        cur=cursor, deployment="offline", rows=[row("a", job("a")), row("b", job("a"))],
        incoming=job("a").to_dict(), compaction=SourceSyncCompaction(max_events=1),
    ) == "a"
    assert len(cursor.writes) == 2
    params = cursor.writes[0][1]
    result = decoded(("a", params[0], params[1], 10))
    assert SourceSyncJob.from_dict(result) == job("a")
    assert cursor.writes[1][1][1] == "b"


@pytest.mark.parametrize("other_source", ["files", "other"])
def test_overflow_survives_primary_and_successor_collisions(monkeypatch, other_source):
    retained = [row("primary", job("a")), row("stable", job("b", source=other_source))]
    original_rows = deepcopy(retained)
    incoming = job("c")
    original_incoming = deepcopy(incoming.to_dict())
    cursor = Cursor()
    permits = []

    @contextmanager
    def permit(**kwargs):
        assert kwargs == {"deployment": "offline", "dsn": "unused", "promotion_id": "promotion"}
        permits.append(kwargs)
        yield

    def compact(**kwargs):
        assert kwargs["deployment"] == "offline"
        return enqueue._compact_dbos_queued_source_sync_candidates(
            cur=cursor, deployment="offline", rows=retained,
            incoming=kwargs["incoming"], compaction=kwargs["compaction"],
        )

    submitted = []

    def submit(**kwargs):
        assert kwargs["job"] is incoming
        assert kwargs["promotion_id"] == "promotion"
        submitted.append(kwargs["deduplication_id"])
        if len(submitted) < 3:
            return enqueue.DBOSWorkflowEnqueueResult("occupied", deduplicated=True)
        retained.append(row("unique", kwargs["job"]))
        return enqueue.DBOSWorkflowEnqueueResult("unique")

    monkeypatch.setattr(enqueue, "promotion_enqueue_permit", permit)
    for name in ("_compact_dbos_queued_source_sync", "_compact_dbos_queued_source_sync_by_prefix", "_compact_dbos_queued_source_sync_by_compatibility"):
        monkeypatch.setattr(enqueue, name, compact)
    monkeypatch.setattr(enqueue, "_enqueue_source_sync_via_dbos_result", submit)
    assert enqueue.enqueue_compacted_source_sync_via_dbos(
        deployment="offline", dsn="unused", job=incoming, deduplication_id="files:123",
        promotion_id="promotion", compaction=SourceSyncCompaction(window_seconds=60, max_events=1),
    ) == "unique"
    assert submitted[:2] == ["files:123", "files:successor"]
    assert submitted[2].startswith("files:successor:")
    assert permits and cursor.writes == []
    assert retained[:2] == original_rows
    assert incoming.to_dict() == original_incoming
    scopes = {(e["source_name"], e["scope_key"]) for r in retained for e in decoded(r)["events"]}
    assert scopes == {("files", "a"), (other_source, "b"), ("files", "c")}
    assert SourceSyncJob.from_dict(decoded(retained[-1])) == incoming
