from __future__ import annotations

from okg.substrate.source_sync import (
    SourceSyncCompaction,
    SourceSyncEvent,
    SourceSyncJob,
)
from okg.substrate.source_sync_enqueue import (
    _merge_source_sync_payloads,
    _source_sync_payloads_compatible,
)


def test_source_sync_lane_is_serialized_and_separate_from_control_lane(
    monkeypatch,
) -> None:
    from okg.substrate.runtime import partition_queue

    captured: list[dict[str, object]] = []

    class FakeQueue:
        def __init__(self, name, **kwargs):
            self.name = name
            self.kwargs = kwargs
            captured.append({"name": name, **kwargs})

    monkeypatch.setattr("dbos.Queue", FakeQueue)
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})

    control = partition_queue.register_deployment_partition_queue(
        deployment="okg-workspace",
    )
    source_sync = partition_queue.register_source_sync_partition_queue(
        deployment="okg-workspace",
    )

    assert control.name == "okg-okg-workspace"
    assert source_sync.name == "okg-okg-workspace-source-sync"
    assert captured == [
        {
            "name": "okg-okg-workspace",
            "concurrency": 1,
            "priority_enabled": False,
            "polling_interval_sec": 0.05,
        },
        {
            "name": "okg-okg-workspace-source-sync",
            "concurrency": 1,
            "priority_enabled": True,
            "polling_interval_sec": 0.05,
        },
    ]


def test_lane_payload_merge_preserves_each_source_scope_event() -> None:
    docs_event = SourceSyncEvent(
        deployment="test",
        source_name="docs",
        trigger_kind="fs_event",
        scope_key="docs/a.md",
    )
    pact_event = SourceSyncEvent(
        deployment="test",
        source_name="pact",
        trigger_kind="fs_event",
        scope_key="pact/pact.yaml",
    )
    existing = SourceSyncJob(
        deployment="test",
        sources=("docs",),
        run_mode="scope_incomplete",
        events=(docs_event,),
        dependent_fanout="direct",
    ).to_dict()
    incoming = SourceSyncJob(
        deployment="test",
        sources=("pact",),
        run_mode="scope_incomplete",
        events=(pact_event,),
        dependent_fanout="direct",
    ).to_dict()

    assert _source_sync_payloads_compatible(existing, incoming) is True
    merged = _merge_source_sync_payloads(
        existing,
        incoming,
        compaction=SourceSyncCompaction(
            window_seconds=60,
            max_events=10,
            max_scope_keys=10,
        ),
    )

    assert merged["sources"] == ["docs", "pact"]
    assert [
        (event["source_name"], event["scope_key"])
        for event in merged["events"]
    ] == [
        ("docs", "docs/a.md"),
        ("pact", "pact/pact.yaml"),
    ]
    assert merged["source_sync_compaction"] == {
        "event_count": 2,
        "stored_event_count": 2,
        "dropped_event_count": 0,
        "scope_keys_sample": ["docs/a.md", "pact/pact.yaml"],
    }


def test_superseded_source_does_not_cancel_independent_selected_lane(
    monkeypatch,
) -> None:
    from okg.substrate.runtime import source_sync_workflow as workflow

    def fake_active(**_kwargs):
        return {
            "docs": {
                "workflow_id": "wf-newer",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
            "pact": {
                "workflow_id": "wf-current",
                "candidate_workflow_ids": ["wf-current", "wf-newer"],
                "selection_reason": "preferred_fresh_completed_scope",
            },
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "active_dbos_source_sync_workflows_by_source",
        fake_active,
    )

    out = workflow._source_sync_start_usefulness(
        dsn="postgres://unused",
        deployment="test",
        workflow_id="wf-current",
        sources=["docs", "pact"],
    )

    assert out["useful"] is True
    assert out["reason"] == "at_least_one_source_still_selects_workflow"
    decisions = {
        decision["source_name"]: decision
        for decision in out["decisions"]
    }
    assert decisions["docs"]["reason"] == "superseded_by_selected_frontier"
    assert decisions["docs"]["useful"] is False
    assert decisions["pact"]["reason"] == "selected_frontier"
    assert decisions["pact"]["useful"] is True


def test_typed_mode_work_is_never_compacted() -> None:
    """A typed `okg ingest --mode` keeps its own workflow (#2486): compaction
    rebuilds merged inputs without the typed-mode flag."""
    plain = SourceSyncJob(
        deployment="test", sources=("docs",), run_mode="reconcile",
    ).to_dict()
    typed = {**plain, "sources": ["pact"], "run_mode_explicit": True}

    assert _source_sync_payloads_compatible(plain, dict(plain)) is True
    assert _source_sync_payloads_compatible(typed, plain) is False
    assert _source_sync_payloads_compatible(plain, typed) is False
    assert _source_sync_payloads_compatible(typed, dict(typed)) is False


def test_the_enqueued_input_parser_reads_the_typed_mode_flag() -> None:
    from dbos._serialization import (
        DBOSDefaultSerializer,
        WorkflowSerializationFormat,
        serialize_args,
    )

    from okg.substrate.source_sync_enqueue import (
        source_sync_payload_from_dbos_inputs,
    )

    args = (["pact"], "reconcile", True, None, None, True, True, False, [], None, False)
    for kwargs, expected in (({}, None), ({"run_mode_explicit": True}, True)):
        inputs, serialization = serialize_args(
            args, kwargs, WorkflowSerializationFormat.NATIVE, DBOSDefaultSerializer,
        )
        payload = source_sync_payload_from_dbos_inputs(
            deployment="test", inputs=inputs, serialization=serialization,
        )
        assert payload is not None
        assert payload.get("run_mode_explicit") is expected
