"""Characterization tests for the pure source-sync status policy.

These tests deliberately keep the compatibility imports while the extraction
is in flight.  The identity test is the migration seam: once the owner exists,
the facade must expose the exact same objects rather than a second policy.
"""
from __future__ import annotations

import hashlib
import importlib.util
import inspect
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.source_sync_status import (
    _age_seconds,
    _parse_datetime,
    _producer_observed_at,
    _rollup_status,
)
from okg.substrate.source_sync_producers import (
    SOURCE_SYNC_STATUS_SCHEMA,
    build_source_sync_status_contract,
    expected_source_sync_producers,
    source_sync_producer_expectations,
    source_sync_status_for_source,
)


_REQUIREMENT = (
    "req.source-sync-status.behavior test.source-sync-status.characterization"
)

# Keep this list synchronized with the architecture guard's STATUS_SYMBOLS.
# The compatibility facade must retain direct access to every moved symbol,
# including private helpers that existing integrations imported explicitly.
_MOVED_STATUS_SYMBOLS = (
    "DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS",
    "SOURCE_SYNC_STATUS_SCHEMA",
    "_SOURCE_SYNC_STALE_BACKLOG_SECONDS",
    "build_source_sync_status_contract",
    "source_sync_status_for_source",
    "expected_source_sync_producers",
    "source_sync_producer_expectations",
    "_expected_source_sync_producers",
    "_append_expected_or_disabled",
    "_runtime_producer_enabled",
    "_producer_liveness_impact",
    "_readiness_required_source_names",
    "_producer_row_is_required",
    "_desired_frontiers_contract",
    "_runtime_liveness_contract",
    "_producer_kind_contract",
    "_runtime_worker_contract",
    "_runtime_service_view",
    "_runtime_service_down",
    "_runtime_service_status",
    "_source_freshness_contract",
    "_source_freshness_for_source",
    "_list_of_mappings",
    "_optional_number",
    "_rollup_status",
    "_producer_observed_at",
    "_parse_datetime",
    "_age_seconds",
)


def _record_requirement(record_property) -> None:
    record_property("requirements", _REQUIREMENT)


def _canonical_fixture() -> dict[str, object]:
    return build_source_sync_status_contract(
        {
            "deployment": "demo",
            "producers": [
                {"source_name": "docs", "producer": "poll", "age_seconds": 12}
            ],
            "expected_producers": [
                {
                    "source_name": "docs",
                    "producer": "poll",
                    "trigger_kinds": ["poll"],
                    "max_age_seconds": 600,
                    "status": "healthy",
                    "observed": {"age_seconds": 12},
                }
            ],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "dbos_queue_depth": {
                "available": True,
                "active_count": 0,
                "terminal_history_count": 1,
                "failed_terminal_count": 0,
                "rows": [],
                "terminal_history_rows": [{"status": "SUCCESS", "count": 1}],
                "failed_terminal_rows": [],
            },
            "recovery_commands": {"inspect": "uv run okg source status demo"},
        },
        source_freshness={
            "ok": True,
            "missing": [],
            "stale": [],
            "groups": {
                "docs": {
                    "ok": True,
                    "sources": [
                        {"source": "demo.docs", "status": "fresh", "age_seconds": 12}
                    ],
                }
            },
        },
        generation={"status": "published", "generation_id": "g1"},
        mcp={"ok": True, "pinned_generation_id": "g1"},
    )


def _minimal_health(**overrides: object) -> dict[str, object]:
    health: dict[str, object] = {
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
    health.update(overrides)
    return health


def test_canonical_mapping_digest_is_frozen(record_property) -> None:
    _record_requirement(record_property)
    payload = json.dumps(
        _canonical_fixture(), sort_keys=True, separators=(",", ":")
    ).encode()

    assert SOURCE_SYNC_STATUS_SCHEMA == "okg.source_sync.status.v1"
    assert hashlib.sha256(payload).hexdigest() == (
        "1c18631201ec402dcbd2096e2713acf1b54b36ba85ae79b751c2248d5879ba38"
    )


def test_facade_status_exports_match_owner_identity_signatures_and_arities(
    record_property,
) -> None:
    """Compatibility exports are aliases, including both expectation arities."""
    record_property(
        "requirements",
        "req.source-sync-status.compatibility "
        "test.source-sync-status.api-compatibility",
    )
    if importlib.util.find_spec("okg.substrate.source_sync_status") is None:
        pytest.skip("owner does not exist during pre-extraction characterization")
    from okg.substrate import source_sync_status as owner

    missing = object()
    for name in _MOVED_STATUS_SYMBOLS:
        # Access through the module's direct export surface; a missing alias is
        # an API break even when the owner itself still contains the symbol.
        imported_facade = __import__(
            "okg.substrate.source_sync_producers",
            fromlist=[name],
        )
        facade_value = getattr(imported_facade, name, missing)
        assert facade_value is not missing, f"facade export missing: {name}"
        owner_value = getattr(owner, name)
        assert facade_value is owner_value
        if callable(facade_value):
            assert inspect.signature(facade_value) == inspect.signature(owner_value)

    assert len(expected_source_sync_producers({})) == 2
    assert len(source_sync_producer_expectations({})) == 3


def test_public_callable_signatures_and_defaults_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    assert tuple(inspect.signature(build_source_sync_status_contract).parameters) == (
        "health",
        "source_freshness",
        "runtime_services_health",
        "source_inventory",
        "active_work_by_source",
        "generation",
        "mcp",
    )
    assert tuple(inspect.signature(source_sync_status_for_source).parameters) == (
        "status",
        "source_name",
    )
    for function in (expected_source_sync_producers, source_sync_producer_expectations):
        signature = inspect.signature(function)
        assert tuple(signature.parameters) == (
            "registry",
            "max_producer_age_seconds",
            "runtime_source_sync",
        )
        assert signature.parameters["max_producer_age_seconds"].default == 15 * 60
        assert signature.parameters["runtime_source_sync"].default is None


def test_blocker_and_recovery_order_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    status = build_source_sync_status_contract(
        _minimal_health(
            expected_producers=[
                {"source_name": "pact", "producer": "watch", "status": "missing"},
                {"source_name": "docs", "producer": "poll", "status": "stale"},
            ],
            missing_producers=[{"source_name": "pact", "producer": "watch"}],
            stale_producers=[{"source_name": "docs", "producer": "poll"}],
            config_warnings=[{"source_name": "bad", "code": "poll_trigger_without_schedule"}],
            dbos_queue_depth={
                "available": True,
                "active_count": 2,
                "failed_terminal_count": 3,
                "failed_terminal_rows": [{"status": "ERROR", "count": 3}],
                "stale_replaceable_work_present": True,
            },
            recovery_commands={"first": "one", "second": "two"},
            runtime_services_health={
                "ok": False,
                "code": "runtime_services_down",
                "watcher": {"enabled": True, "alive": False},
                "webhook": {"enabled": False, "alive": False},
            },
        ),
        source_inventory=[
            {"source_name": "pact", "freshness_policy": {"readiness_required": True}},
            {"source_name": "docs", "freshness_policy": {"readiness_required": True}},
        ],
        source_freshness={"ok": False, "missing": ["demo.pact"], "stale": ["demo.docs"], "groups": {}},
    )
    assert [row["code"] for row in status["blockers"]] == [
        "source_sync_config_warning",
        "source_sync_producer_missing",
        "source_sync_producer_stale",
        "source_sync_dlq_nonempty",
        "source_sync_backlog_stale_replaceable",
        "source_sync_runtime_unhealthy",
        "source_freshness_not_ready",
        "source_sync_convergence_not_ready",
    ]
    assert list(status["recovery_commands"]) == ["first", "second"]
    assert _rollup_status(["not_configured", "healthy", "warning", "unavailable", "unhealthy", "stale", "missing", "down", "blocked"]) == "blocked"


def test_permissive_malformed_and_empty_defaults_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    original = {
        "deployment": "demo",
        "producers": "malformed",
        "expected_producers": {"malformed": True},
        "disabled_producers": None,
        "missing_producers": ["drop-me", {"source_name": "pact"}],
        "stale_producers": 4,
        "config_warnings": object(),
        "recovery_commands": ["not-a-mapping"],
        "dbos_queue_depth": "not-a-mapping",
        "dlq_count": 0,
        "runtime_services_health": "not-a-mapping",
    }
    before = deepcopy(original)
    status = build_source_sync_status_contract(original)
    assert original["deployment"] == before["deployment"]
    assert original["missing_producers"] == before["missing_producers"]
    assert status["desired_plan"] == {
        "expected_producer_count": 0,
        "disabled_producer_count": 0,
        "expected_producers": [],
        "disabled_producers": [],
        "config_warnings": [],
    }
    assert status["recovery_commands"] == {}
    assert status["wakeups"]["dbos_workflows"] == {}
    assert status["runtime_liveness"]["worker"]["status"] == "not_directly_observed"
    assert status["source_freshness"] == {
        "ok": None,
        "status": "not_evaluated",
        "reason": "source watermarks were not provided to this status view",
        "missing": [],
        "stale": [],
        "groups": {},
    }


def test_healthy_missing_stale_disabled_and_unavailable_states_are_explicit() -> None:
    healthy = build_source_sync_status_contract(_minimal_health())
    assert healthy["ok"] is True
    assert healthy["status"] == "healthy"

    missing = build_source_sync_status_contract(
        _minimal_health(missing_producers=[{"source_name": "docs", "producer": "watch"}])
    )
    assert missing["status"] == "blocked"
    assert "source_sync_producer_missing" in {row["code"] for row in missing["blockers"]}

    stale = build_source_sync_status_contract(
        _minimal_health(stale_producers=[{"source_name": "docs", "producer": "poll"}])
    )
    assert stale["status"] == "blocked"
    assert "source_sync_producer_stale" in {row["code"] for row in stale["blockers"]}

    disabled = build_source_sync_status_contract(
        _minimal_health(disabled_producers=[{"source_name": "legacy", "producer": "poll"}])
    )
    assert disabled["ok"] is True
    assert disabled["desired_plan"]["disabled_producer_count"] == 1

    unavailable = build_source_sync_status_contract(
        _minimal_health(dbos_queue_depth={"available": False, "reason": "unavailable"})
    )
    assert unavailable["backlog"]["dlq_status"] == "unknown"
    assert unavailable["wakeups"]["status"] == "blocked"
    assert unavailable["ok"] is False
    assert unavailable["blockers"] == [{
        "code": "source_sync_dlq_status_unknown",
        "reason": "unavailable",
    }]

    # Preserve the current-dev compatibility rule for callers that explicitly
    # supply the legacy key. This synthetic input is distinct from the public
    # facade's unavailable observation, which sets ``available`` to false above.
    legacy_none = build_source_sync_status_contract(
        _minimal_health(dbos_queue_depth={}, dlq_count=None)
    )
    assert legacy_none["backlog"]["dlq_status"] == "known"
    assert legacy_none["backlog"]["dlq_count"] == 0
    assert legacy_none["ok"] is True


def test_source_slice_shallow_copies_and_input_snapshot_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    status = {
        "schema": SOURCE_SYNC_STATUS_SCHEMA,
        "deployment": "demo",
        "runtime_liveness": "malformed",
        "source_freshness": None,
        "convergence": "malformed",
        "desired_frontiers": {"sources": {"docs": {"status": "advancing"}}},
        "recovery_commands": {"inspect": "command"},
    }
    before = deepcopy(status)
    source = source_sync_status_for_source(status, "docs")
    assert status == before
    assert source["desired_frontier"] == {"status": "advancing"}
    assert source["desired_frontier"] is not status["desired_frontiers"]["sources"]["docs"]
    assert source["recovery_commands"] is not status["recovery_commands"]
    assert source["source_freshness"] == {"status": "not_evaluated", "source_name": "docs"}
    assert source["convergence"]["state"] == "unsupported"


def test_expectation_order_disabled_policy_and_tuple_arities_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    capabilities = SourceSyncCapabilities.from_dict(
        {
            "triggers": ["webhook", "poll", "fs_event", "git_ref_change"],
            "default_event_mode": "cursor",
            "watch": {"roots": ["."]},
            "poll": {"probe": "change_probe", "interval_seconds": 1200},
            "webhook": {"path": "/hook"},
        }
    )
    warning_capabilities = SourceSyncCapabilities.from_dict({"triggers": ["poll"], "default_event_mode": "cursor"})
    registry = {
        "zeta": SimpleNamespace(raw={}, sync=capabilities),
        "alpha": SimpleNamespace(raw={}, sync=capabilities),
        "warning": SimpleNamespace(raw={}, sync=warning_capabilities),
    }
    expected, disabled, warnings = source_sync_producer_expectations(
        registry, max_producer_age_seconds=900, runtime_source_sync={"poll": {"enabled": False}}
    )
    assert [(row["source_name"], row["producer"]) for row in expected] == [
        ("alpha", "watch"), ("alpha", "webhook"), ("zeta", "watch"), ("zeta", "webhook")
    ]
    assert [(row["source_name"], row["producer"]) for row in disabled] == [("alpha", "poll"), ("zeta", "poll")]
    assert [row["max_age_seconds"] for row in disabled] == [2400, 2400]
    assert warnings == [{
        "source_name": "warning",
        "code": "poll_trigger_without_schedule",
        "message": "source declares poll trigger but no sync.poll schedule",
    }]
    assert len(expected_source_sync_producers(registry, runtime_source_sync={"poll": {"enabled": False}})) == 2


def test_timestamp_precedence_naive_time_utc_and_future_clamp_are_frozen(record_property) -> None:
    _record_requirement(record_property)
    updated_at = datetime(2026, 1, 1, 8, 0)
    payload = {
        "last_heartbeat_at": "malformed",
        "last_enqueue_at": "2026-01-01T09:00:00Z",
        "last_event_at": "2026-01-01T10:00:00+00:00",
        "last_run_at": "2026-01-01T11:00:00Z",
    }
    assert _producer_observed_at(payload, updated_at) == datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    assert _producer_observed_at({}, updated_at) == updated_at.replace(tzinfo=timezone.utc)
    assert _parse_datetime("2026-01-01T09:00:00") == datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    assert _parse_datetime("bad") is None
    observed_at = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    assert _age_seconds(observed_at + timedelta(seconds=7), observed_at) == 7.0
    assert _age_seconds(observed_at - timedelta(seconds=7), observed_at) == 0.0


def test_convergence_projection_error_fallback_shape_is_frozen(monkeypatch, record_property) -> None:
    _record_requirement(record_property)
    from okg.substrate import source_sync_convergence

    def _raise_projection_error(**_kwargs):
        raise RuntimeError("projection unavailable")

    monkeypatch.setattr(source_sync_convergence, "build_convergence_status", _raise_projection_error)
    status = build_source_sync_status_contract(_minimal_health())
    assert status["convergence"] == {
        "schema": "okg.source_sync.convergence.v1",
        "deployment": "demo",
        "state": "unsupported",
        "ok": False,
        "reason": "convergence_status_error",
        "error": "RuntimeError",
        "message": "projection unavailable",
    }
    # A convergence projection failure is intentionally only a readable
    # fallback when no source requires convergence readiness.
    assert status["ok"] is True


def test_evaluated_convergence_failure_is_a_blocker() -> None:
    status = build_source_sync_status_contract(
        _minimal_health(),
        source_inventory=[{
            "deployment": "demo",
            "source_name": "docs",
            "runtime": {"enabled": True, "source_sync_enabled": True},
            "sync": {"declared": True, "reconcile_run_mode": "scope_complete"},
            "freshness_policy": {"readiness_required": True, "completed_scope_required_for_readiness": True},
            "constraints": {},
        }],
        source_freshness={"ok": True, "missing": [], "stale": [], "groups": {"docs": {"ok": True, "sources": [{"source": "demo.docs", "status": "fresh"} ]}}},
        generation={"status": "missing", "generation_id": None},
        mcp={"ok": False},
    )
    assert status["ok"] is False
    assert status["status"] == "blocked"
    assert "source_sync_convergence_not_ready" in {row["code"] for row in status["blockers"]}
