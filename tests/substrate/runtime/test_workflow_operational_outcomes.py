"""Fail-closed reader and typed outcome contract tests for #720."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import inspect

import pytest


def test_normalize_outcome_rejects_malformed_and_accepts_partial() -> None:
    from okg.substrate.runtime.workflow_operational_outcomes import (
        WorkflowOperationalEvidenceError,
        normalize_outcome,
    )

    value = normalize_outcome(
        {
            "outcome": "partial_failure",
            "succeeded_count": 2,
            "failed_count": 1,
            "semantic_units": {
                "failed_units": ["n-3|embed_failure", "n-3|embed_failure"],
                "succeeded_node_ids": ["n-1", "n-2"],
            },
        }
    )
    assert value["semantic_units"]["failed_units"] == ["n-3|embed_failure"]

    with pytest.raises(WorkflowOperationalEvidenceError):
        normalize_outcome(
            {
                "outcome": "partial_failure",
                "succeeded_count": 0,
                "failed_count": 1,
                "semantic_units": {},
            }
        )


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Conn:
    def __init__(self, migration, rows):
        self.migration = migration
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _params=()):
        if "schema_migrations" in query:
            return _Result(self.migration)
        if "min(registered_at)" in query:
            # The publisher's first registered outcome (cutover), a minute
            # ago: older than every row the cases below build.
            return _Result([(datetime.now(timezone.utc) - timedelta(minutes=1),)])
        return _Result(self.rows)


def _reader_rows(*, outcome: str = "clean", stale: bool = False, missing: bool = False):
    now = datetime.now(timezone.utc)
    registered = now - timedelta(seconds=1)
    return [{
        "workflow_uuid": "wf-1",
        "status_workflow_name": "okg_embed_batch",
        "status_queue_name": "okg-test",
        "created_at": int((registered - timedelta(milliseconds=1)).timestamp() * 1000),
        "completed_at": int(now.timestamp() * 1000),
        "registration_deployment_name": None if missing else "test",
        "registration_workflow_name": None if missing else "okg_embed_batch",
        "registration_queue_name": None if missing else "okg-test",
        "registration_contract_version": None if missing else 1,
        "registered_at": None if missing else registered,
        "outcome_contract_version": None if missing else 1,
        "outcome": None if missing else outcome,
        "succeeded_count": None if missing else (2 if outcome == "partial_failure" else 1),
        "failed_count": None if missing else (1 if outcome == "partial_failure" else 0),
        "semantic_units": None if missing else {"succeeded_node_ids": ["n-1"]},
        "details": None if missing else {},
        "recorded_at": None if missing else (
            registered - timedelta(seconds=5) if stale else now
        ),
    }]


@pytest.mark.parametrize(
    ("outcome", "stale", "missing", "expected_code"),
    [
        ("clean", False, False, "workflow_outcome_clean"),
        ("partial_failure", False, False, "workflow_outcome_degraded"),
        ("clean", True, False, "workflow_outcome_unknown"),
        ("clean", False, True, "workflow_outcome_unknown"),
    ],
)
def test_reader_is_typed_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    stale: bool,
    missing: bool,
    expected_code: str,
) -> None:
    module = importlib.import_module(
        "okg.substrate.runtime.workflow_operational_outcomes"
    )

    applied_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    migration = [{"applied_at": applied_at, "status": "applied"}]
    conn = _Conn(migration, _reader_rows(outcome=outcome, stale=stale, missing=missing))
    monkeypatch.setattr(module, "db_connect", lambda *_args, **_kwargs: conn)

    verdict = module.check_workflow_operational_outcomes(
        deployment="test", dsn="postgres://stub", lookback_seconds=3600,
    )
    assert verdict.code == expected_code
    assert verdict.ok is (expected_code == "workflow_outcome_clean")


def test_reader_migration_unknown_is_non_green(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module(
        "okg.substrate.runtime.workflow_operational_outcomes"
    )

    conn = _Conn([], [])
    monkeypatch.setattr(module, "db_connect", lambda *_args, **_kwargs: conn)
    verdict = module.check_workflow_operational_outcomes(
        deployment="test", dsn="postgres://stub",
    )
    assert verdict.ok is False
    assert verdict.code == "workflow_outcome_unknown"


def test_reader_never_decodes_dbos_output_marker() -> None:
    module = importlib.import_module(
        "okg.substrate.runtime.workflow_operational_outcomes"
    )

    source = inspect.getsource(module.check_workflow_operational_outcomes)
    assert "workflow_status.output" not in source
    assert "LIKE" not in source


@pytest.mark.parametrize(
    "recorded, completed",
    [
        (datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
         datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        (datetime(2025, 12, 31, 23, 54, 0, tzinfo=timezone.utc),
         datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        (None, datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
    ],
)
def test_reader_rejects_stale_or_future_outcome_timestamp(recorded, completed) -> None:
    from okg.substrate.runtime.workflow_operational_outcomes import _outcome_reason

    row = {
        "outcome_contract_version": 1,
        "registered_at": datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        "recorded_at": recorded,
        "completed_at": completed,
        "outcome": "clean",
        "succeeded_count": 1,
        "failed_count": 0,
        "semantic_units": {"succeeded_node_ids": ["n-1"]},
        "details": {},
    }
    assert _outcome_reason(row) == "stale_outcome"


def test_reader_rejects_malformed_typed_evidence() -> None:
    from okg.substrate.runtime.workflow_operational_outcomes import _outcome_reason

    row = {
        "outcome_contract_version": 1,
        "registered_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        "recorded_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        "completed_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        "outcome": "clean",
        "succeeded_count": 1,
        "failed_count": 0,
        "semantic_units": {"succeeded_node_ids": [1]},
        "details": {},
    }
    assert _outcome_reason(row).startswith("malformed_outcome:")


def test_semantic_replay_equality_ignores_diagnostic_variation_and_conflicts() -> None:
    from okg.substrate.runtime.workflow_operational_outcomes import normalize_outcome

    first = normalize_outcome({
        "outcome": "partial_failure", "succeeded_count": 1,
        "failed_count": 1, "semantic_units": {"failed_units": ["n-2|error"]},
        "details": {"message": "first diagnostic"},
    })
    replay = normalize_outcome({
        "outcome": "partial_failure", "succeeded_count": 1,
        "failed_count": 1, "semantic_units": {"failed_units": ["n-2|error"]},
        "details": {"message": "different diagnostic"},
    })
    assert {key: first[key] for key in ("outcome", "succeeded_count", "failed_count", "semantic_units")} == {
        key: replay[key] for key in ("outcome", "succeeded_count", "failed_count", "semantic_units")
    }
    conflict = normalize_outcome({
        "outcome": "retained_failure", "succeeded_count": 0,
        "failed_count": 1, "semantic_units": {"failed_units": ["n-2|error"]},
    })
    assert conflict["outcome"] != first["outcome"]


def test_source_sync_mapper_uses_report_name_and_rejects_malformed() -> None:
    from okg.substrate.runtime.source_sync_workflow import _source_sync_operational_outcome
    from okg.substrate.runtime.workflow_operational_outcomes import WorkflowOperationalEvidenceError

    outcome = _source_sync_operational_outcome({
        "any_failed": True,
        "superseded": False,
        "malformed_event_units": ["event-7"],
        "report": {
            "results": [
                {"name": "docs", "status": "OK"},
                {"name": "pact", "status": "ERROR", "code": "timeout"},
            ],
            "publishes": [{"status": "published"}],
        },
    })
    assert outcome["outcome"] == "partial_failure"
    assert outcome["semantic_units"]["succeeded_sources"] == ["docs"]
    assert outcome["failed_count"] == 2

    with pytest.raises(WorkflowOperationalEvidenceError):
        _source_sync_operational_outcome({
            "any_failed": False,
            "superseded": False,
            "report": {"results": [{"status": "OK"}], "publishes": []},
        })
