"""Unit tests for source sync payload contracts."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources._path_scope import path_scope_requested
from okg.substrate.library.sources.base import RecordSet

from okg.substrate.source_sync import (
    SOURCE_SYNC_TRIGGER_KINDS,
    SourceSyncCapabilities,
    SourceSyncCompaction,
    SourceSyncDependents,
    SourceSyncEvent,
    SourceSyncJob,
    SourceSyncPoll,
    SourceSyncScope,
    SourceSyncWatch,
    SourceSyncWebhook,
    source_sync_scope_narrows,
    source_sync_scope_payload,
)
from okg.substrate.source_sync_enqueue import (
    _serialize_source_sync_dbos_args,
    _source_sync_workflow_args,
    source_sync_payload_from_dbos_inputs,
)


def _event() -> SourceSyncEvent:
    return SourceSyncEvent(
        deployment="okg-workspace",
        source_name="git-files",
        trigger_kind="fs_event",
        scope_key="src/okg/substrate/source_sync.py",
        revision_hint="mtime_ns:123",
        event_id="evt-1",
        observed_at="2026-05-05T12:00:00Z",
        reason="watch debounce closed",
    )


def test_source_sync_event_round_trips_json() -> None:
    event = _event()

    encoded = event.to_json()

    assert SourceSyncEvent.from_json(encoded) == event
    assert json.loads(encoded)["trigger_kind"] == "fs_event"


def test_source_sync_job_round_trips_json_payload() -> None:
    event = _event()
    job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("git-files", "openspec"),
        run_mode="scope_complete",
        events=(event,),
        include_dependents=False,
        dependent_fanout="none",
        publish=True,
    )

    decoded = SourceSyncJob.from_json(job.to_json())

    assert decoded == job
    assert decoded.to_dict()["sources"] == ["git-files", "openspec"]
    assert decoded.to_dict()["events"] == [event.to_dict()]
    assert decoded.to_dict()["dependent_fanout"] == "none"


@pytest.mark.parametrize("legacy_value", ["", "   ", False])
def test_source_sync_job_decodes_legacy_empty_supersession_as_absent(
    legacy_value: object,
) -> None:
    decoded = SourceSyncJob.from_dict({
        "deployment": "okg-workspace",
        "sources": ["git-files"],
        "run_mode": "scope_complete",
        "supersedes_workflow_id": legacy_value,
    })

    assert decoded.supersedes_workflow_id is None
    assert decoded.to_dict()["supersedes_workflow_id"] is None


@pytest.mark.parametrize("invalid_value", [42, True])
def test_source_sync_job_still_rejects_non_string_supersession(
    invalid_value: object,
) -> None:
    with pytest.raises(ValueError, match="supersedes_workflow_id"):
        SourceSyncJob.from_dict({
            "deployment": "okg-workspace",
            "sources": ["git-files"],
            "run_mode": "scope_complete",
            "supersedes_workflow_id": invalid_value,
        })


@pytest.mark.parametrize("legacy_value", ["", "   ", False])
def test_dbos_payload_decoder_normalizes_legacy_empty_supersession(
    legacy_value: object,
    caplog: pytest.LogCaptureFixture,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.source-sync test.runtime-authority.source-sync",
    )
    inputs, serialization = _serialize_source_sync_dbos_args(
        args=(
            ["git-files"],
            "scope_complete",
            True,
            "closure",
            [],
            True,
            True,
            False,
            [],
            legacy_value,
        ),
        serialization=None,
    )

    payload = source_sync_payload_from_dbos_inputs(
        deployment="okg-workspace",
        inputs=inputs,
        serialization=serialization,
    )

    assert payload is not None
    assert payload["supersedes_workflow_id"] is None
    assert "invalid workflow inputs" not in caplog.text


def test_dbos_payload_round_trips_source_sync_trace_handoff() -> None:
    telemetry = {
        "schema": "okg.source_sync.trace.v1",
        "trace_id": "1" * 32,
        "root_span_id": "2" * 16,
        "wait_span_id": "3" * 16,
        "started_at": "2026-08-11T12:00:00+00:00",
        "ended_at": "2026-08-11T12:00:01+00:00",
    }
    job = SourceSyncJob(
        deployment="okg-workspace",
        sources=("git-files",),
        run_mode="scope_complete",
    )
    args = _source_sync_workflow_args(
        job,
        workflow_telemetry=telemetry,
    )
    inputs, serialization = _serialize_source_sync_dbos_args(
        args=args,
        serialization=None,
    )

    payload = source_sync_payload_from_dbos_inputs(
        deployment="okg-workspace",
        inputs=inputs,
        serialization=serialization,
    )

    assert len(args) == 12
    assert args[10] is False
    assert payload is not None
    assert payload["force_enrichers"] is False
    assert payload["workflow_telemetry"] == telemetry


def test_source_sync_capabilities_parse_refined_policy() -> None:
    caps = SourceSyncCapabilities.from_dict({
        "watch": {
            "roots": ["/repo"],
            "include_globs": ["**/*.md"],
        },
        "triggers": ["fs_event", "webhook", "poll"],
        "default_event_mode": "scope_incomplete",
        "reconcile_mode": "scope_complete",
        "deletes_require_reconcile": True,
        "scope": {
            "accepts": ["path"],
            "unsupported_fallback": "scope_complete",
        },
        "dependents": {
            "on_event": "direct",
            "on_reconcile": "closure",
        },
        "compaction": {
            "window_seconds": 30,
            "max_events": 100,
            "max_scope_keys": 20,
        },
        "webhook": {
            "path": "/hooks/sync/git-files",
            "secret_ref": "GIT_FILES_WEBHOOK_SECRET",
        },
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 60,
        },
    })

    assert caps is not None
    assert isinstance(caps.watch, SourceSyncWatch)
    assert caps.scope == SourceSyncScope(
        accepts=("path",),
        unsupported_fallback="scope_complete",
    )
    assert caps.dependents == SourceSyncDependents(
        on_event="direct",
        on_reconcile="closure",
    )
    assert caps.compaction == SourceSyncCompaction(
        window_seconds=30,
        max_events=100,
        max_scope_keys=20,
    )
    assert caps.webhook == SourceSyncWebhook(
        path="/hooks/sync/git-files",
        secret_ref="GIT_FILES_WEBHOOK_SECRET",
    )
    assert caps.poll == SourceSyncPoll(
        probe="change_probe",
        interval_seconds=60,
    )


def test_source_sync_capabilities_defaults_preserve_existing_behavior() -> None:
    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["fs_event"],
    })

    assert caps is not None
    assert caps.default_event_mode == "cursor"
    assert caps.dependents == SourceSyncDependents(
        on_event="closure",
        on_reconcile="closure",
    )
    assert caps.scope is None
    assert caps.compaction is None
    assert caps.webhook is None
    assert caps.poll is None


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"scope": {"accepts": ["glob"]}}, "scope.accepts"),
        ({"dependents": {"on_event": "recursive"}}, "on_event"),
        ({"compaction": {"window_seconds": -1}}, "window_seconds"),
        ({"webhook": {"signature_header": ""}}, "signature_header"),
        ({"poll": {"interval_seconds": 0}}, "interval_seconds"),
    ],
)
def test_source_sync_capabilities_reject_invalid_refined_policy(
    payload: dict,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        SourceSyncCapabilities.from_dict(payload)


@pytest.mark.parametrize(
    "probe",
    ["stat_tree", "content_hash", "ttl", "remote_etag", "unknown"],
)
def test_source_sync_poll_rejects_producer_owned_probe(probe: str) -> None:
    with pytest.raises(ValueError, match="sync.poll.probe"):
        SourceSyncPoll(probe=probe)


@pytest.mark.parametrize("probe", ["change_probe", "always_changed"])
def test_source_sync_poll_accepts_closed_probe_contract(probe: str) -> None:
    assert SourceSyncPoll(probe=probe).probe == probe


@pytest.mark.parametrize(
    "trigger_kind",
    sorted(SOURCE_SYNC_TRIGGER_KINDS),
)
def test_source_sync_event_accepts_supported_trigger_kinds(
    trigger_kind: str,
) -> None:
    event = SourceSyncEvent(
        deployment="okg-workspace",
        source_name="git-files",
        trigger_kind=trigger_kind,
    )

    assert event.trigger_kind == trigger_kind


def test_source_sync_event_rejects_unknown_trigger_kind() -> None:
    with pytest.raises(ValueError, match="trigger_kind"):
        SourceSyncEvent.from_dict(
            {
                "deployment": "okg-workspace",
                "source_name": "git-files",
                "trigger_kind": "publish",
            }
        )


def test_source_sync_event_rejects_missing_source_name() -> None:
    with pytest.raises(ValueError, match="source_name"):
        SourceSyncEvent.from_dict(
            {
                "deployment": "okg-workspace",
                "trigger_kind": "fs_event",
            }
        )


def test_source_sync_job_rejects_empty_sources() -> None:
    with pytest.raises(ValueError, match="at least one source"):
        SourceSyncJob.from_dict(
            {
                "deployment": "okg-workspace",
                "sources": [],
                "run_mode": "cursor",
            }
        )


def test_source_sync_job_rejects_unknown_run_mode() -> None:
    with pytest.raises(ValueError, match="run_mode"):
        SourceSyncJob.from_dict(
            {
                "deployment": "okg-workspace",
                "sources": ["git-files"],
                "run_mode": "full_refresh",
            }
        )


def test_source_sync_job_coerces_single_source_and_event_mapping() -> None:
    payload = {
        "deployment": "okg-workspace",
        "sources": "git-files",
        "run_mode": "cursor",
        "events": _event().to_dict(),
        "include_dependents": True,
        "publish": False,
    }

    job = SourceSyncJob.from_dict(payload)

    assert job.sources == ("git-files",)
    assert job.events == (_event(),)
    assert job.publish is False


def test_source_sync_scope_payload_does_not_narrow_reconcile_events() -> None:
    event = SourceSyncEvent(
        deployment="okg-workspace",
        source_name="doc-corpus",
        trigger_kind="reconcile",
        scope_key="docs/deleted.md",
        revision_hint="old-sha",
    )

    payload = source_sync_scope_payload("doc-corpus", (event,))

    assert payload is not None
    assert payload["event_count"] == 1
    assert payload["trigger_kinds"] == ["reconcile"]
    assert payload["scope_keys"] == []
    assert payload["revision_hints"] == []


# Issue #3314. The runtime worker always hands the runner a scope payload,
# because every worker run carries a trigger event. A reconcile or poll
# wakeup names no scope key, so it narrows nothing: the adapter reads its
# whole configured scope. The runner must then grant the same complete-scope
# deletion authority an event-less `okg ingest --inline` run gets. Poll
# events always carry a revision hint (the probe fingerprint), and a webhook
# carries the pushed revision; neither names which records to read.
def _worker_scope(
    trigger_kind: str,
    *,
    scope_key: str | None = None,
    revision_hint: str | None = None,
) -> dict:
    payload = source_sync_scope_payload(
        "slurm-snapshot",
        (
            SourceSyncEvent(
                deployment="cluster",
                source_name="slurm-snapshot",
                trigger_kind=trigger_kind,
                scope_key=scope_key,
                revision_hint=revision_hint,
            ),
        ),
    )
    assert payload is not None
    return payload


def _completed_source_run(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "run_mode": "scope_complete",
        "completed_scope": True,
        "health": None,
        "record_set": RecordSet(),
        "preserves_prior_record_set": False,
        "record_set_replacement_keys": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _bare_runner() -> SourceRunner:
    # Both authority checks read only their arguments; no database needed.
    return object.__new__(SourceRunner)


def _whole_scope_deletion_authorized(sync_scope: dict | None) -> bool:
    return _bare_runner()._completed_scope_deletion_authorized(
        mode="scope_complete",
        source_run=_completed_source_run(),
        sync_scope=sync_scope,
        acquire_health=None,
    )


def _scoped_authority(sync_scope: dict | None) -> str | None:
    return _bare_runner()._scoped_record_set_deletion_authority(
        mode="scope_complete",
        source_run=_completed_source_run(),
        sync_scope=sync_scope,
        acquire_health=None,
    )


@pytest.mark.parametrize(
    ("trigger_kind", "revision_hint"),
    [
        ("reconcile", None),
        ("poll", None),
        ("poll", "always_changed:3f1c"),
        ("webhook", "after-sha"),
        ("manual", None),
    ],
)
def test_non_narrowing_worker_scope_authorizes_whole_scope_deletion(
    trigger_kind: str,
    revision_hint: str | None,
) -> None:
    payload = _worker_scope(trigger_kind, revision_hint=revision_hint)
    assert payload["scope_keys"] == []

    assert _whole_scope_deletion_authorized(payload) is True
    # The same run must not ALSO be treated as a narrowed slice.
    assert _scoped_authority(payload) is None


def test_event_less_run_authorizes_whole_scope_deletion() -> None:
    assert _whole_scope_deletion_authorized(None) is True
    assert _whole_scope_deletion_authorized({}) is True


@pytest.mark.parametrize(
    ("trigger_kind", "revision_hint"),
    [("fs_event", None), ("webhook", "after-sha"), ("poll", "mtime:1")],
)
def test_narrowing_worker_scope_never_authorizes_whole_scope_deletion(
    trigger_kind: str,
    revision_hint: str | None,
) -> None:
    payload = _worker_scope(
        trigger_kind,
        scope_key="partitions/gpu.json",
        revision_hint=revision_hint,
    )
    assert payload["scope_keys"] == ["partitions/gpu.json"]

    assert _whole_scope_deletion_authorized(payload) is False
    # A narrowed run keeps only its bounded, verified slice authority.
    assert _scoped_authority(payload) == "merged_prior"


@pytest.mark.parametrize(
    "sync_scope",
    [
        None,
        {},
        {"scope_keys": []},
        {"scope_keys": [None, ""]},
        {"scope_keys": [], "revision_hints": ["always_changed:3f1c"]},
        {"scope_keys": [], "trigger_kinds": ["reconcile"]},
        {"scope_keys": ["notes/a.md"]},
        {"scope_keys": [None, "notes\\b.md"]},
    ],
)
def test_source_sync_scope_narrows_matches_adapter_path_scope(
    sync_scope: dict | None,
) -> None:
    # The runner's authority checks and the adapters' read narrowing must
    # agree, or a run reads its whole scope while the runner treats it as a
    # slice (issue #3314).
    assert source_sync_scope_narrows(sync_scope) is path_scope_requested(
        sync_scope,
    )


def test_whole_scope_deletion_still_needs_a_completed_healthy_run() -> None:
    payload = _worker_scope("reconcile")
    runner = _bare_runner()

    assert runner._completed_scope_deletion_authorized(
        mode="scope_complete",
        source_run=_completed_source_run(completed_scope=False),
        sync_scope=payload,
        acquire_health=None,
    ) is False
    assert runner._completed_scope_deletion_authorized(
        mode="cursor",
        source_run=_completed_source_run(run_mode="cursor"),
        sync_scope=payload,
        acquire_health=None,
    ) is False


def test_source_sync_job_derives_dependents_from_legacy_flag() -> None:
    assert SourceSyncJob.from_dict({
        "deployment": "okg-workspace",
        "sources": ["git-files"],
        "run_mode": "cursor",
        "include_dependents": True,
    }).dependent_fanout == "closure"
    assert SourceSyncJob.from_dict({
        "deployment": "okg-workspace",
        "sources": ["git-files"],
        "run_mode": "cursor",
        "include_dependents": False,
    }).dependent_fanout == "none"
