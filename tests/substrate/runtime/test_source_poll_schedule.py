"""Tests for per-source DBOS-scheduled poll workflows.

Covers pure-function helpers (id construction, cron derivation) without
spinning a DBOS singleton. The live registration + firing is exercised
by the cutover-time verification step on okg-workspace (proposal task
17), not here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.runtime.source_poll_schedule import (
    SOURCE_POLL_SCHEDULE_PREFIX,
    _disabled_source_poll_schedule_map,
    _source_poll_groups,
    cron_from_interval_seconds,
    source_poll_deduplication_id,
    source_poll_group_key,
    source_poll_schedule_id,
    source_poll_workflow_name,
)


def test_prefix_constant_is_stable():
    """Operator tooling filters on this prefix; pin it."""
    assert SOURCE_POLL_SCHEDULE_PREFIX == "okg_source_poll"


def test_id_construction():
    """Schedule + workflow names share the same prefix-based pattern
    so operators can correlate `dbos.workflow_schedules` rows with
    `dbos.workflow_status` rows by source."""
    assert source_poll_schedule_id("git-files") == (
        "okg_source_poll_git-files"
    )
    assert source_poll_workflow_name("git-files") == (
        "okg_source_poll_git-files"
    )
    assert source_poll_deduplication_id(
        deployment="okg-workspace",
        source_name="git-files",
    ) == "source-poll:okg-workspace:git-files"
    assert source_poll_deduplication_id(
        deployment="okg-workspace",
        source_name="git-files",
        scheduled_time=datetime(2026, 5, 27, 16, 10, tzinfo=timezone.utc),
        bucket_seconds=300,
    ) == "source-poll:okg-workspace:git-files:5932994"


def test_source_poll_group_key_is_readable_without_override():
    assert source_poll_group_key(interval_seconds=300) == "interval_300s"
    assert (
        source_poll_schedule_id(source_poll_group_key(interval_seconds=300))
        == "okg_source_poll_interval_300s"
    )


def test_source_poll_group_key_is_isolated_for_cron_override():
    default = source_poll_group_key(interval_seconds=300)
    overridden = source_poll_group_key(
        interval_seconds=300,
        cron="*/7 * * * *",
    )

    assert overridden != default
    assert overridden.startswith("interval_300s_")


@pytest.mark.parametrize(
    "interval_seconds, expected_cron",
    [
        (1, "*/1 * * * * *"),     # 1s — 6-field
        (30, "*/30 * * * * *"),   # 30s — 6-field
        (59, "*/59 * * * * *"),   # last sub-minute — 6-field
        (60, "*/1 * * * *"),      # 1 min — 5-field
        (120, "*/2 * * * *"),     # 2 min
        (300, "*/5 * * * *"),     # 5 min — common registry value
        (3540, "*/59 * * * *"),   # just under hour
        (3600, "*/1 * * * *"),    # hour+ falls back to 1-min cron
        (7200, "*/1 * * * *"),    # 2h falls back to 1-min cron
    ],
)
def test_cron_from_interval_seconds(interval_seconds, expected_cron):
    assert cron_from_interval_seconds(interval_seconds) == expected_cron


def test_cron_rejects_zero_and_negative():
    """A registry entry with `interval_seconds: 0` is operator error,
    not a "every second" request — surface as ValueError."""
    with pytest.raises(ValueError):
        cron_from_interval_seconds(0)
    with pytest.raises(ValueError):
        cron_from_interval_seconds(-1)


def test_enqueue_via_dbos_helper_exists():
    """The shared helper that watcher / poll / webhook all delegate
    to must remain exported."""
    from okg.substrate.source_sync_enqueue import (
        enqueue_source_sync_via_dbos,
    )

    assert callable(enqueue_source_sync_via_dbos)


def test_disabled_source_poll_schedule_map_targets_stale_rows():
    disabled = _disabled_source_poll_schedule_map(
        existing_schedule_ids={
            "okg_source_poll_doc-corpus",
            "okg_source_poll_git-files",
            "okg_embed_batch",
        },
        registered_schedule_ids={"okg_source_poll_git-files"},
    )

    assert disabled == {
        "okg_source_poll_doc-corpus": [
            "okg_source_poll_doc-corpus",
            "okg_source_poll_doc-corpus_enqueue",
        ],
    }


def test_poll_groups_exclude_explicitly_dormant_profile_candidates():
    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {
        "dormant": SimpleNamespace(
            raw={"enabled_by_default": False},
            sync=caps,
        ),
        "enabled": SimpleNamespace(
            raw={"enabled_by_default": True},
            sync=caps,
        ),
        # Omitted preserves the historical runnable-by-default behavior.
        "implicit": SimpleNamespace(raw={}, sync=caps),
    }

    groups = _source_poll_groups(registry)

    assert groups["interval_300s"]["sources"] == ["enabled", "implicit"]


def test_producer_expectations_exclude_explicitly_dormant_sources():
    from okg.substrate.source_sync_status import (
        source_sync_producer_expectations,
    )

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    expected, disabled, warnings = source_sync_producer_expectations({
        "dormant": SimpleNamespace(
            raw={"enabled_by_default": False},
            sync=caps,
        ),
        "active": SimpleNamespace(
            raw={"enabled_by_default": True},
            sync=caps,
        ),
    })

    assert [row["source_name"] for row in expected] == ["active"]
    assert disabled == []
    assert warnings == []


def test_registered_poll_workflow_uses_runtime_backstop_multiplier(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {"docs": SimpleNamespace(sync=caps)}
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    sync_root = tmp_path / "registered-sync-root"
    sync_root.mkdir()
    captured: dict[str, object] = {}
    decorated: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    def fake_run_poll_once(*_args, **kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: dep_dir,
    )
    def fake_load_registry(path, **kwargs):
        captured["registry_path"] = path
        captured["repo_root_override"] = kwargs.get("repo_root_override")
        return registry

    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        fake_load_registry,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        fake_run_poll_once,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
        backstop_multiplier=12,
        backstop_dependent_fanout="none",
        source_registry_repo_root_override=sync_root,
    )

    decorated["okg_source_poll_interval_300s"]()

    assert captured["deployment"] == "demo"
    assert set(captured["registry"]) == {"docs"}
    assert "source" not in captured
    assert "route_via_dbos" not in captured
    assert captured["backstop_multiplier"] == 12
    assert captured["backstop_dependent_fanout"] == "none"
    assert captured["repo_root_override"] == sync_root


def test_registered_poll_workflow_refuses_stale_root_before_source_reads(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.errors import SourceConfigurationAuthorityInvalid
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {"docs": SimpleNamespace(sync=caps)}
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    source_root = tmp_path / "source-root"
    source_root.mkdir()
    decorated: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda *_args, **_kwargs: pytest.fail(
            "poll registry loaded before source-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        lambda *_args, **_kwargs: pytest.fail(
            "poll producer ran before source-root refusal",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
        source_registry_repo_root_override=source_root,
    )
    source_root.rmdir()

    with pytest.raises(SourceConfigurationAuthorityInvalid):
        decorated["okg_source_poll_interval_300s"]()


def test_registered_poll_workflow_groups_sources_by_interval(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {
        "docs": SimpleNamespace(sync=caps),
        "notes": SimpleNamespace(sync=caps),
    }
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    calls: list[set[str]] = []
    decorated: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    def fake_run_poll_once(*_args, **kwargs):
        calls.append(set(kwargs["registry"]))
        return {
            "results": [
                {"source_name": source_name}
                for source_name in sorted(kwargs["registry"])
            ],
            "errors": [],
        }

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda path: registry,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        fake_run_poll_once,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )

    schedule_ids = schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
    )

    assert schedule_ids == ["okg_source_poll_interval_300s"]
    result = decorated["okg_source_poll_interval_300s"]()

    assert calls == [{"docs", "notes"}]
    assert result["sources"] == ["docs", "notes"]
    assert result["active_sources"] == ["docs", "notes"]
    assert [row["source_name"] for row in result["results"]] == [
        "docs",
        "notes",
    ]


def test_registered_poll_reports_absent_source_before_polling_active_sources(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })
    scheduled_registry = {
        name: SimpleNamespace(sync=caps, raw={"enabled_by_default": True})
        for name in ("active", "dormant", "removed")
    }
    current_registry = {
        "active": SimpleNamespace(
            sync=caps,
            raw={"enabled_by_default": True},
            admission_contract="legacy",
        ),
        "dormant": SimpleNamespace(
            sync=caps,
            raw={"enabled_by_default": False},
            admission_contract="legacy",
        ),
    }
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    decorated: dict[str, object] = {}
    events: list[tuple[str, object]] = []

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    def fake_warning(*_args, **kwargs):
        events.append(("warning", kwargs.get("extra")))

    def fake_run_poll_once(*_args, **kwargs):
        events.append(("poll", tuple(kwargs["registry"])))
        return {"results": [{"source_name": "active"}], "errors": []}

    def fake_health(*_args, **_kwargs):
        events.append(("health", None))

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda *_args, **_kwargs: current_registry,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        fake_run_poll_once,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.record_source_sync_producer_health",
        fake_health,
    )
    monkeypatch.setattr(schedules.log, "warning", fake_warning)

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=scheduled_registry,
    )
    events.clear()

    result = decorated["okg_source_poll_interval_300s"]()

    assert events == [
        (
            "warning",
            {"source_disagreements": [{
                "source_name": "removed",
                "reason": "absent_from_source_registry",
            }]},
        ),
        ("poll", ("active",)),
    ]
    assert result["sources"] == ["active", "dormant", "removed"]
    assert result["active_sources"] == ["active"]
    assert result["dormant_sources"] == ["dormant"]
    assert result["source_disagreements"] == [{
        "source_name": "removed",
        "reason": "absent_from_source_registry",
    }]


def test_strict_registered_poll_with_empty_group_does_not_select_registry_peer(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })
    scheduled_registry = {
        "removed": SimpleNamespace(
            sync=caps,
            raw={"enabled_by_default": True},
        ),
    }
    current_registry = {
        "unrelated": SimpleNamespace(
            sync=caps,
            raw={"enabled_by_default": True},
            admission_contract="strict_v1",
        ),
    }
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    decorated: dict[str, object] = {}
    execution_events: list[tuple[str, object]] = []

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda *_args, **_kwargs: current_registry,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.record_source_sync_producer_health",
        lambda *_args, **_kwargs: execution_events.append(("health", None)),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.plan_source_registry_bindings",
        lambda *_args, **_kwargs: SimpleNamespace(plans={"unrelated": object()}),
    )
    for collaborator in (
        "construct_source_adapter_from_plan",
        "construct_source_adapter",
        "_poll_backstop_reconcile_due",
        "_record_producer_health",
        "enqueue_events_for_source",
    ):
        monkeypatch.setattr(
            f"okg.substrate.source_sync_producers.{collaborator}",
            lambda *_args, _name=collaborator, **_kwargs: pytest.fail(
                f"unrelated strict source reached {_name}",
            ),
        )

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=scheduled_registry,
    )
    execution_events.clear()

    result = decorated["okg_source_poll_interval_300s"]()

    assert execution_events == []
    assert result["active_sources"] == []
    assert result["source_disagreements"] == [{
        "source_name": "removed",
        "reason": "absent_from_source_registry",
    }]
    assert result["results"] == []


def test_per_source_cron_override_isolates_schedule_group(
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {
        "docs": SimpleNamespace(sync=caps),
        "notes": SimpleNamespace(sync=caps),
    }
    decorated: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )

    schedule_ids = schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
        schedule_overrides={
            "okg_source_poll_docs": "*/7 * * * *",
        },
    )

    assert "okg_source_poll_interval_300s" in schedule_ids
    assert len(schedule_ids) == 2
    override_schedule = [
        schedule_id for schedule_id in schedule_ids
        if schedule_id != "okg_source_poll_interval_300s"
    ][0]
    assert override_schedule.startswith("okg_source_poll_interval_300s_")
    assert set(decorated) >= {
        "okg_source_poll_interval_300s",
        override_schedule,
    }
    assert not any(name.endswith("_enqueue") for name in decorated)


def test_scheduled_poll_runs_directly_without_queueing_poll_workflow(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registry = {"docs": SimpleNamespace(sync=caps)}
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    decorated: dict[str, object] = {}
    called: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda path: registry,
    )
    def fake_run_poll_once(*_args, **kwargs):
        called["kwargs"] = kwargs
        return {"results": [{"source_name": "docs"}], "errors": []}

    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        fake_run_poll_once,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.enqueue_deployment_workflow",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("enqueue should be skipped")
        ),
    )

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
    )

    result = decorated["okg_source_poll_interval_300s"](
        datetime(2026, 6, 2, 15, 0, tzinfo=timezone.utc),
        None,
    )

    assert called["kwargs"]["registry"] == registry
    assert result["group"] == "interval_300s"
    assert result["scheduled_time"] == "2026-06-02T15:00:00+00:00"


def test_scheduled_poll_rechecks_true_to_false_registry_transition(
    tmp_path,
    monkeypatch,
) -> None:
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules

    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll"],
        "default_event_mode": "scope_complete",
        "poll": {
            "probe": "change_probe",
            "interval_seconds": 300,
        },
    })
    registered = {
        "docs": SimpleNamespace(
            raw={"enabled_by_default": True},
            sync=caps,
        ),
    }
    current = {
        "docs": SimpleNamespace(
            raw={"enabled_by_default": False},
            sync=caps,
        ),
    }
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    decorated: dict[str, object] = {}
    captured: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda path: current,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        lambda *_args, **kwargs: (
            captured.update(kwargs)
            or {"results": [], "errors": []}
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **_kwargs: None,
    )

    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registered,
    )
    result = decorated["okg_source_poll_interval_300s"]()

    assert captured["registry"] == {}
    assert result["sources"] == ["docs"]
    assert result["active_sources"] == []


def test_poll_tick_is_skipped_while_its_previous_poll_is_running(
    tmp_path,
    monkeypatch,
) -> None:
    """A tick arriving mid-poll is skipped and reported, never stacked.

    Lane W1 (worker-keeps-plain-install-current): on the okg repository a busy
    worker started a new poll every 60 s tick and twelve were open at once,
    none finishing.
    """
    import dbos
    from okg.substrate.runtime import source_poll_schedule as schedules
    from okg.substrate.runtime.schedule_overlap import reset_schedule_claims

    reset_schedule_claims()
    caps = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 300},
    })
    registry = {"docs": SimpleNamespace(sync=caps)}
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    sync_root = tmp_path / "registered-sync-root"
    sync_root.mkdir()
    decorated: dict[str, object] = {}
    nested: list[dict] = []
    calls: list[int] = []

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    def fake_run_poll_once(*_args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            # The next cron tick fires while this poll is still running.
            nested.append(decorated["okg_source_poll_interval_300s"]())
        return {"ok": True, "results": [], "errors": []}

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: dep_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda path, **kwargs: registry,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.run_poll_once",
        fake_run_poll_once,
    )
    heartbeats: list[str] = []
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers."
        "record_source_sync_producer_health",
        lambda *_args, **kwargs: heartbeats.append(kwargs["source_name"]),
    )
    schedules.register_source_poll_schedules(
        deployment="demo",
        dsn="postgres://unused",
        registry=registry,
        source_registry_repo_root_override=sync_root,
    )
    assert heartbeats == ["docs"]
    poll = decorated["okg_source_poll_interval_300s"]

    first = poll()
    assert first["ok"] is True
    assert len(calls) == 1
    assert nested[0]["status"] == "skipped_previous_run_active"
    assert nested[0]["skipped_runs"] == 1
    assert nested[0]["sources"] == ["docs"]
    # A skipped tick still says the producer is alive.
    assert heartbeats == ["docs", "docs"]

    # Once the first poll finished, the next tick runs normally.
    assert poll()["ok"] is True
    assert len(calls) == 2
    reset_schedule_claims()
