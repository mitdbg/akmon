"""Positive composition checks for every status-only public surface.

Only observation/config acquisition is replaced.  The owner builder, slicer,
and expectation policy are never patched: distinctive derived fields prove
that each real composition path reaches them.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.source_sync_status import (
    build_source_sync_status_contract,
)
from okg.substrate.sources.registry import SourceEntry


def _health() -> dict[str, object]:
    return {
        "deployment": "demo",
        "producers": [],
        "expected_producers": [],
        "disabled_producers": [],
        "missing_producers": [],
        "stale_producers": [],
        "config_warnings": [],
        "dbos_queue_depth": {"available": True, "active_count": 0},
        "dlq_count": 0,
    }


def _status() -> dict[str, object]:
    return build_source_sync_status_contract(
        {
            **_health(),
            "runtime_services_health": None,
        },
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "docs": {
                    "ok": True,
                    "sources": [{"source": "demo.docs", "status": "fresh"}],
                }
            },
        },
    )


def test_cli_readiness_and_sync_health_execute_owner_builder(tmp_path: Path) -> None:
    from okg.substrate.cli.deployment_readiness import (
        _ready_build_source_sync_status_contract,
        _ready_source_sync_liveness_status,
    )

    # The helper is the actual readiness composition seam.  Its output proves
    # the owner built the schema rather than a test double returning a status.
    readiness = _ready_build_source_sync_status_contract(
        _health(), source_freshness={"ok": True, "missing": [], "stale": [], "groups": {}}, generation=None, mcp=None
    )
    assert readiness["schema"] == "okg.source_sync.status.v1"
    assert readiness["wakeups"]["dbos_workflows"]["available"] is True

    # Exercise the sync-health path's real missing-registry observation branch;
    # no producer-health or status-owner function is patched.
    out = _ready_source_sync_liveness_status(
        dsn="postgresql://unused",
        deployment="demo",
        deployment_dir=tmp_path,
        max_producer_age_seconds=900,
    )
    assert out["source_sync_status"]["schema"] == "okg.source_sync.status.v1"
    assert out["source_sync_status"]["status"] == "blocked"
    assert out["source_sync_status"]["blockers"][0]["code"] == "source_sync_config_warning"


def test_mcp_source_status_executes_owner_slicer(tmp_path: Path, monkeypatch) -> None:
    from okg.substrate.mcp import source_lifecycle

    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text("sources: {}\n", encoding="utf-8")

    class Conn:
        info = SimpleNamespace(
            dsn="postgresql://agent@localhost:5432/okg",
            password="secret",
        )

    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda _path: {},
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(runtime=SimpleNamespace(enabled=True)),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.config.resolve_source_sync",
        lambda _runtime: {},
    )

    # Health is observation acquisition.  The real source slicer receives the
    # resulting status below and must derive source-specific freshness/frontier
    # fallbacks itself.
    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.source_sync_producer_health",
        lambda *_args, **kwargs: {
            "source_sync_status": _status(),
            "deployment": kwargs["deployment"],
        },
    )
    result = source_lifecycle._source_sync_status_for_source(
        Conn(),
        deployment="demo",
        registry_path=registry_path,
        source_name="docs",
    )
    assert result["schema"] == "okg.source_sync.status.v1"
    assert result["source_name"] == "docs"
    assert result["source_freshness"]["status"] == "fresh"
    assert result["runtime_liveness"]["status"] == "not_configured"


def test_console_diagnostics_executes_owner_slicer(monkeypatch) -> None:
    from okg.substrate.console_views import source_diagnostics

    status = _status()
    monkeypatch.setattr(
        source_diagnostics,
        "compose_sources",
        lambda *_args, **_kwargs: {
            "sources": [{
                "name": "docs",
                "source_key": "docs",
                "in_registry": True,
                "records_in_gen": 2,
                "edges_in_gen": 0,
                "fact_counts_available": True,
                "last_sync_age": 3,
            }]
        },
    )
    monkeypatch.setattr(
        source_diagnostics,
        "compose_console_readiness",
        lambda **_kwargs: {"details": {"readiness": {"source_sync_status": status}}},
    )
    monkeypatch.setattr(
        source_diagnostics,
        "_latest_runs_by_source",
        lambda *_args, **_kwargs: ({"docs": []}, {"status": "available"}),
    )
    monkeypatch.setattr(
        source_diagnostics,
        "compose_source_detail",
        lambda *_args, **_kwargs: {"source_name": "docs", "sync_progress_recorded": True, "progress": []},
    )

    result = source_diagnostics.compose_source_diagnostics(
        object(),
        deployment_name="demo",
        deployment_dir=Path("."),
        dsn="postgresql://unused",
        session=SimpleNamespace(generation_id="g1"),
        selected_source="docs",
    )
    assert result["schema"] == "okg.console.source_diagnostics.v1"
    assert result["sources"][0]["freshness"]["status"] == "fresh"
    assert result["selected"]["source_name"] == "docs"


def test_microbench_inventory_executes_owner_expectations() -> None:
    from okg.substrate.source_sync_microbench import build_microbenchmark_inventory

    capabilities = SourceSyncCapabilities.from_dict({
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "reconcile_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": 60},
    })
    entry = SourceEntry(
        name="docs",
        ownership_id="demo.docs",
        source_class="discovery_crawl",
        record_identity_kind="scoped_locator",
        record_identity_fields=("path",),
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        credential_refs=(),
        credential_aliases={},
        required_for_baseline=False,
        raw={"module": "fixture", "class": "FixtureSource"},
        sync=capabilities,
        enabled_by_default=True,
    )
    inventory = build_microbenchmark_inventory(
        deployment="demo",
        registry={"docs": entry},
        runtime_enabled=True,
        runtime_source_sync={"poll": {"enabled": True}},
        include_live=False,
    )
    row = inventory["sources"][0]
    assert inventory["schema"] == "okg.source_sync.microbench.inventory.v1"
    assert row["source_name"] == "docs"
    assert row["expected_producer_count"] == 1
    assert row["expected_producers"][0]["producer"] == "poll"
