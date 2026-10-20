"""Tests for the publisher-workflow-health doctor probe (Phase 1 §1.4).

Pure-logic tests for cron-interval parsing + outcome rules. The live
integration test (probe against an actual `dbos.workflow_status` row)
is covered by the operator-driven cutover gate.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Cron interval parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cron, expected_seconds",
    [
        ("*/30 * * * * *", 30),       # 6-field, seconds
        ("*/5 * * * * *", 5),
        ("*/5 * * * *", 300),          # 5-field, minutes
        ("*/1 * * * *", 60),
        ("0 3 * * *", 86400),          # 5-field fixed daily (hygiene sweeps)
        ("30 3 * * *", 86400),
        ("0 * * * *", 30),             # hourly (wildcard hour) → default
        ("garbage", 30),
        ("", 30),
    ],
)
def test_parse_cron_interval_seconds(cron, expected_seconds):
    from okg.substrate.runtime.publisher_health import (
        _parse_cron_interval_seconds,
    )
    assert _parse_cron_interval_seconds(cron) == expected_seconds


# ---------------------------------------------------------------------------
# Verdict shape (without a live DBOS schema)
# ---------------------------------------------------------------------------


def test_verdict_to_dict_round_trips():
    from datetime import datetime, timezone

    from okg.substrate.runtime.publisher_health import (
        PublisherHealthVerdict,
    )

    v = PublisherHealthVerdict(
        deployment="test",
        ok=True,
        code="publisher_workflow_healthy",
        message="last succeeded 25s ago",
        last_success_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        seconds_since_last_success=25.0,
        threshold_seconds=60.0,
    )
    d = v.to_dict()
    assert d["ok"] is True
    assert d["code"] == "publisher_workflow_healthy"
    assert d["seconds_since_last_success"] == 25.0
    assert d["threshold_seconds"] == 60.0
    assert d["last_success_at"].startswith("2026-01-01")


def test_verdict_handles_none_timestamp():
    from okg.substrate.runtime.publisher_health import (
        PublisherHealthVerdict,
    )

    v = PublisherHealthVerdict(
        deployment="test",
        ok=False,
        code="publisher_workflow_never_ran",
        message="no rows",
        last_success_at=None,
        seconds_since_last_success=None,
        threshold_seconds=60.0,
    )
    d = v.to_dict()
    assert d["last_success_at"] is None
    assert d["seconds_since_last_success"] is None


# ---------------------------------------------------------------------------
# Per-workflow health-check exports
# ---------------------------------------------------------------------------


def test_source_sync_workflow_health_export_exists():
    """The doctor invariant for source-sync depends on this being
    exported from publisher_health (alongside the publisher + embed
    probes). Pin the export so accidental renames don't silently
    break the doctor."""
    from okg.substrate.runtime import publisher_health

    assert hasattr(publisher_health, "check_source_sync_workflow_health")
    assert "check_source_sync_workflow_health" in publisher_health.__all__


def test_embed_workflow_health_export_exists():
    from okg.substrate.runtime import publisher_health

    assert hasattr(publisher_health, "check_embed_workflow_health")
    assert "check_embed_workflow_health" in publisher_health.__all__


def test_embed_workflow_never_ran_is_nonfatal():
    """The embed workflow probe must treat 'never ran' as a non-
    fatal verdict so deployments with embedding disabled don't make
    `okg doctor` fail. Verified by inspecting the helper's
    `never_ran_is_fatal` argument default through the public API."""
    import inspect

    from okg.substrate.runtime.publisher_health import (
        check_embed_workflow_health,
        check_source_sync_workflow_health,
        check_publisher_workflow_health,
    )

    # Publisher: never_ran IS fatal (the cron should always have
    # fired by the time doctor runs against an enabled deployment).
    # Source-sync + embed: never_ran is NOT fatal.
    # Verify by reading the callsite via source inspection.
    src_pub = inspect.getsource(check_publisher_workflow_health)
    src_src = inspect.getsource(check_source_sync_workflow_health)
    src_emb = inspect.getsource(check_embed_workflow_health)
    # Publisher omits never_ran_is_fatal (default True).
    assert "never_ran_is_fatal" not in src_pub
    # Source-sync + embed explicitly set False.
    assert "never_ran_is_fatal=False" in src_src
    assert "never_ran_is_fatal=False" in src_emb


def test_source_sync_stale_is_nonfatal():
    """Source-sync is operator-driven (`okg ingest`), not cron-driven.
    A stale source-sync workflow just means no operator has run
    ingest recently — `okg doctor` must not flag this as a failure.
    Verified by inspecting the helper's `stale_is_fatal` argument
    through the public API."""
    import inspect

    from okg.substrate.runtime.publisher_health import (
        check_embed_workflow_health,
        check_source_sync_workflow_health,
        check_publisher_workflow_health,
    )

    src_pub = inspect.getsource(check_publisher_workflow_health)
    src_src = inspect.getsource(check_source_sync_workflow_health)
    src_emb = inspect.getsource(check_embed_workflow_health)
    # Publisher + embed: stale IS fatal (default True, omitted).
    assert "stale_is_fatal" not in src_pub
    assert "stale_is_fatal" not in src_emb
    # Source-sync explicitly sets False.
    assert "stale_is_fatal=False" in src_src


def test_disabled_conditional_workflow_probe_is_nonfatal(monkeypatch):
    """A conditionally registered workflow whose gate is closed should
    not fail doctor just because historical SUCCESS rows are stale."""
    from okg.substrate.runtime import publisher_health as ph
    from okg.substrate.runtime import publisher_health_workflow_liveness as phl

    def _unexpected_connect(*_args, **_kwargs):
        raise AssertionError("disabled workflow probe should not query DB")

    # Patch the connection seam on the module that actually owns the probe;
    # publisher_health re-exports `check_embed_workflow_health` but
    # the code body lives in publisher_health_workflow_liveness. That module
    # now reaches libpq through the substrate connection factory, so
    # `db_connect` is the name to stub -- stubbing `psycopg` there would
    # patch a name the probe no longer calls and the assertion would pass
    # vacuously.
    monkeypatch.setattr(phl, "db_connect", _unexpected_connect)

    verdict = ph.check_embed_workflow_health(
        deployment="dep-x",
        dsn="postgres://unused",
        expected_to_run=False,
    )

    assert verdict.ok is True
    assert verdict.code == "embed_workflow_not_expected"
    assert verdict.last_success_at is None


# ---------------------------------------------------------------------------
# Queue-depth probe
# ---------------------------------------------------------------------------


def test_queue_depth_verdict_shape():
    from okg.substrate.runtime.publisher_health import QueueDepthVerdict

    v = QueueDepthVerdict(
        deployment="test",
        ok=False,
        code="queue_depth_runaway",
        message="40 ENQUEUED",
        enqueued_count=40,
        pending_count=1,
        threshold=3,
    )
    d = v.to_dict()
    assert d["ok"] is False
    assert d["code"] == "queue_depth_runaway"
    assert d["enqueued_count"] == 40
    assert d["pending_count"] == 1
    assert d["threshold"] == 3


def test_queue_depth_export_exists():
    """Doctor wiring depends on `check_queue_depth` being exported.
    Pin the export so a rename doesn't silently break the runaway
    detection."""
    from okg.substrate.runtime import publisher_health

    assert hasattr(publisher_health, "check_queue_depth")
    assert "check_queue_depth" in publisher_health.__all__
    assert "QueueDepthVerdict" in publisher_health.__all__


# ---------------------------------------------------------------------------
# A deliberately paused publisher: ok, and a warning (issue #2554). The
# verdicts themselves are proven against PostgreSQL in
# test_publisher_paused_health.py; these cover where they are reported.
# ---------------------------------------------------------------------------


def _verdict(code: str, *, ok: bool):
    from okg.substrate.runtime.publisher_health import PublisherHealthVerdict

    return PublisherHealthVerdict(
        deployment="dep-x",
        ok=ok,
        code=code,
        message=f"message for {code}",
        last_success_at=None,
        seconds_since_last_success=1300.0,
        threshold_seconds=600.0,
        schedule_paused=code.startswith("publisher_workflow_paused"),
    )


@pytest.mark.parametrize(
    "code", ["publisher_workflow_paused", "publisher_workflow_paused_long"]
)
def test_doctor_lists_a_paused_publisher_as_a_warning_and_stays_ok(code):
    from okg.substrate.cli.doctor import _record_workflow_health

    report = {"ok": True, "errors": [], "warnings": []}
    _record_workflow_health(
        report, "publisher_workflow_health", _verdict(code, ok=True)
    )

    assert report["ok"] is True
    assert report["errors"] == []
    assert report["warnings"] == [{"code": code, "message": f"message for {code}"}]
    assert report["publisher_workflow_health"]["schedule_paused"] is True


@pytest.mark.parametrize(
    "code",
    ["publisher_workflow_stale", "publisher_workflow_paused_last_cycle_failed"],
)
def test_doctor_fails_on_a_stale_or_failed_publisher(code):
    from okg.substrate.cli.doctor import _record_workflow_health

    report = {"ok": True, "errors": [], "warnings": []}
    _record_workflow_health(
        report, "publisher_workflow_health", _verdict(code, ok=False)
    )

    assert report["ok"] is False
    assert report["errors"] == [{"code": code, "message": f"message for {code}"}]
    assert report["warnings"] == []


def test_doctor_does_not_warn_about_a_healthy_publisher():
    from okg.substrate.cli.doctor import _record_workflow_health

    report = {"ok": True, "errors": [], "warnings": []}
    _record_workflow_health(
        report,
        "publisher_workflow_health",
        _verdict("publisher_workflow_healthy", ok=True),
    )

    assert report == {
        "ok": True,
        "errors": [],
        "warnings": [],
        "publisher_workflow_health": report["publisher_workflow_health"],
    }


def test_readiness_carries_the_paused_warning_and_stays_ready():
    from okg.substrate.cli.deployment import _ready_add_paused_publisher_warnings

    payload: dict = {}
    doctor = {
        "ok": True,
        "json": {
            "ok": True,
            "warnings": [
                {"code": "lens_putback_backlog_warn", "message": "other"},
                {"code": "publisher_workflow_paused", "message": "paused for dep-x"},
            ],
        },
    }
    _ready_add_paused_publisher_warnings(payload, doctor)

    assert "blockers" not in payload
    assert [check["id"] for check in payload["warnings"]] == [
        "publisher.schedule_paused"
    ]
    assert payload["warnings"][0]["status"] == "warning"
    assert payload["warnings"][0]["message"] == "paused for dep-x"
    assert payload["warnings"][0]["details"] == {"code": "publisher_workflow_paused"}


def test_readiness_adds_nothing_when_doctor_has_no_paused_warning():
    from okg.substrate.cli.deployment import _ready_add_paused_publisher_warnings

    payload: dict = {}
    _ready_add_paused_publisher_warnings(payload, {"ok": True, "json": None})
    _ready_add_paused_publisher_warnings(
        payload, {"ok": True, "json": {"warnings": [{"code": "other"}]}}
    )

    assert payload == {}
