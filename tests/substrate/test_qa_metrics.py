"""Unit tests for QAGateMetrics.

The integration path (live SQL against a real DB) is exercised by
the QA gate runbook; here we cover the dataclass contract:
- the 18 required fields exist with the spec'd names;
- to_dict / from_dict round-trip is byte-stable;
- the `_flatten_invariant_summary` helper correctly projects the
  publisher's `_build_invariant_summary` JSONB shape.
"""
from __future__ import annotations

from dataclasses import fields

import pytest

from okg.substrate.runtime_queue_health import (
    DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
)
from okg.substrate.qa.metrics import (
    QAGateMetrics,
    _dlq_count,
    _flatten_invariant_summary,
    _queue_depth,
)


def _sample() -> QAGateMetrics:
    return QAGateMetrics(
        deployment_name="af_pubs",
        generation_id="okg:af_pubs:branch:default:gen:sample",
        nodes_live_delta=100,
        edges_live_delta=200,
        queue_depth={
            "ingest": 0, "link": 0, "enrich": 0,
            "publish": 0, "embed": 5,
        },
        dlq_count=0,
        cross_source_invariants_total=10,
        cross_source_invariants_failed=0,
        cross_source_invariants_timed_out=0,
        source_local_invariants_total=4,
        source_local_invariants_failed=0,
        substrate_integrity_checks={},
        excluded_sources=[],
        total_invariant_duration_ms=500,
        total_invariant_budget_exceeded=False,
        generation_blocked_count=0,
        doctor_exit_code=0,
        publish_wallclock_seconds=12.5,
    )


def test_qagatemetrics_has_eighteen_fields_with_spec_names() -> None:
    expected = {
        "deployment_name",
        "generation_id",
        "nodes_live_delta",
        "edges_live_delta",
        "queue_depth",
        "dlq_count",
        "cross_source_invariants_total",
        "cross_source_invariants_failed",
        "cross_source_invariants_timed_out",
        "source_local_invariants_total",
        "source_local_invariants_failed",
        "substrate_integrity_checks",
        "excluded_sources",
        "total_invariant_duration_ms",
        "total_invariant_budget_exceeded",
        "generation_blocked_count",
        "doctor_exit_code",
        "publish_wallclock_seconds",
    }
    actual = {f.name for f in fields(QAGateMetrics)}
    # 18 because substrate_integrity_checks was added as a
    # generation-pinned integrity-mode surface.
    # deployment_name, generation_id (2)
    # nodes_live_delta, edges_live_delta (4)
    # queue_depth, dlq_count (6)
    # cross_source: total/failed/timed_out (9)
    # source_local: total/failed (11)
    # substrate_integrity_checks (12)
    # excluded_sources (13)
    # total_invariant_duration_ms, total_invariant_budget_exceeded (14)
    # generation_blocked_count, doctor_exit_code (17)
    # publish_wallclock_seconds (18)
    assert actual == expected, (
        f"missing/extra fields: {expected ^ actual}"
    )


def test_qagatemetrics_round_trip() -> None:
    m = _sample()
    d = m.to_dict()
    assert QAGateMetrics.from_dict(d) == m


def test_flatten_invariant_summary_real_publisher_shape() -> None:
    # Mirror of okg.substrate.publisher.publisher._build_invariant_summary.
    payload = {
        "cross_source": {
            "total": 6,
            "passed": 2,
            "failed": 3,
            "warns": 1,
            "timed_out": 3,
            "fired_warns": [],
        },
        "source_local": {
            "af_pubs.catalog": {
                "total": 2, "passed": 2, "failed": 0,
                "warns": 0, "timed_out": 0, "fired_warns": [],
            },
            "af_pubs.pdf_text": {
                "total": 3, "passed": 2, "failed": 1,
                "warns": 0, "timed_out": 0, "fired_warns": [],
            },
        },
        "total_duration_ms": 12345,
        "total_budget_exceeded": False,
        "substrate_integrity": {
            "checks": {
                "substrate_duplicate_visible_nodes": {
                    "mode": "delta",
                    "sample_mode": "skipped_sample_zero",
                },
            },
        },
    }
    out = _flatten_invariant_summary(payload)
    assert out["cross_source_invariants_total"] == 6
    assert out["cross_source_invariants_failed"] == 3
    assert out["cross_source_invariants_timed_out"] == 3
    assert out["source_local_invariants_total"] == 5  # 2+3
    assert out["source_local_invariants_failed"] == 1  # 0+1
    assert out["substrate_integrity_checks"] == {
        "substrate_duplicate_visible_nodes": {
            "mode": "delta",
            "sample_mode": "skipped_sample_zero",
        },
    }
    assert out["total_invariant_duration_ms"] == 12345
    assert out["total_invariant_budget_exceeded"] is False


def test_flatten_invariant_summary_handles_empty() -> None:
    out = _flatten_invariant_summary({})
    assert out["cross_source_invariants_total"] == 0
    assert out["cross_source_invariants_failed"] == 0
    assert out["cross_source_invariants_timed_out"] == 0
    assert out["source_local_invariants_total"] == 0
    assert out["source_local_invariants_failed"] == 0
    assert out["substrate_integrity_checks"] == {}
    assert out["total_invariant_duration_ms"] == 0
    assert out["total_invariant_budget_exceeded"] is False


def test_queue_depth_can_ignore_optional_job_kinds() -> None:
    cur = _FakeQueueCursor(active_count=4)

    out = _queue_depth(cur, {"embed"})

    assert out["embed"] == 0
    assert out["ingest"] == 4
    joined = "\n".join(cur.queries)
    assert "okg.v_runtime_queue_health" in joined
    assert "okg.v_job_queue" not in joined
    assert "okg.jobs" not in joined


def test_queue_depth_ignores_disabled_embed_runtime_workflow() -> None:
    cur = _FakeQueueCursor(
        active_count=4,
        dlq_count=1,
        workflow_name="okg_embed_batch",
    )

    assert _queue_depth(cur, {"embed"}) == {
        "ingest": 0,
        "link": 0,
        "enrich": 0,
        "publish": 0,
        "embed": 0,
    }
    assert _dlq_count(cur, {"embed"}) == 0


@pytest.mark.parametrize(
    "workflow_name",
    sorted(DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES),
)
def test_queue_depth_ignores_workflows_readiness_delegates(
    workflow_name: str,
) -> None:
    """A running doctor/source-sync workflow is not ingest backlog (#3868)."""
    ignored = set(DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES)
    cur = _FakeQueueCursor(
        active_count=1, dlq_count=1, workflow_name=workflow_name
    )

    assert _queue_depth(cur, None, ignored)["ingest"] == 0
    assert _dlq_count(cur, None, ignored) == 0
    # Without the ignore set the same row still counts: the default is kept.
    assert _queue_depth(cur)["ingest"] == 1


def test_queue_depth_still_counts_other_workflows_when_delegated_ignored() -> None:
    ignored = set(DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES)
    cur = _FakeQueueCursor(
        active_count=2, dlq_count=1, workflow_name="okg_publish_generation"
    )

    assert _queue_depth(cur, None, ignored)["ingest"] == 2
    assert _dlq_count(cur, None, ignored) == 1


def test_queue_depth_projects_embed_runtime_workflow_when_enabled() -> None:
    cur = _FakeQueueCursor(active_count=4, workflow_name="okg_embed_batch")

    assert _queue_depth(cur) == {
        "ingest": 0,
        "link": 0,
        "enrich": 0,
        "publish": 0,
        "embed": 4,
    }


def test_qa_metrics_reports_dbos_runtime_queue_state() -> None:
    cur = _FakeQueueCursor(active_count=5, dlq_count=2)

    assert _queue_depth(cur)["ingest"] == 5
    assert _dlq_count(cur) == 2
    joined = "\n".join(cur.queries)
    assert "okg.v_runtime_queue_health" in joined
    assert "okg.v_job_queue" not in joined
    assert "okg.jobs" not in joined


@pytest.mark.postgres
def test_real_db_queue_helpers_accept_absent_legacy_jobs_table(
    clean_runtime: str,
) -> None:
    import psycopg

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.jobs')")
            assert cur.fetchone()[0] is None
            cur.execute("SELECT to_regclass('dbos.workflow_status')")
            if cur.fetchone()[0] is not None:
                cur.execute("DELETE FROM dbos.workflow_status")
            assert _queue_depth(cur) == {
                "ingest": 0,
                "link": 0,
                "enrich": 0,
                "publish": 0,
                "embed": 0,
            }
            assert _dlq_count(cur) == 0


class _FakeQueueCursor:
    def __init__(
        self,
        *,
        active_count: int = 0,
        dlq_count: int = 0,
        workflow_name: str = "okg_source_sync",
    ) -> None:
        self._active_count = active_count
        self._dlq_count = dlq_count
        self._workflow_name = workflow_name
        self.queries: list[str] = []
        self._result: list[tuple] = []

    def execute(self, query: str, params: tuple | None = None) -> None:
        del params
        statement = str(query)
        self.queries.append(statement)
        if "table_name = 'v_runtime_queue_health'" in statement:
            self._result = [(True,)]
        elif "FROM okg.v_runtime_queue_health" in statement:
            self._result = [(
                "okg-test-source-sync",
                "failed" if self._dlq_count else "waiting",
                "available",
                self._active_count,
                0,
                self._active_count,
                0,
                0,
                self._dlq_count,
                0,
                self._dlq_count,
                self._dlq_count,
                0,
                30.0 if self._active_count else None,
                10.0 if self._active_count else None,
                600,
                False,
                None,
                None,
                self._workflow_name,
            )]
        elif "to_regclass('okg.runtime_dlq_acknowledgement_events')" in statement:
            self._result = [(None,)]
        elif "to_regclass('dbos.workflow_status')" in statement:
            self._result = [("dbos.workflow_status",)]
        elif "table_name = 'workflow_status'" in statement:
            self._result = [
                (column,)
                for column in (
                    "workflow_uuid",
                    "name",
                    "status",
                    "application_version",
                    "queue_name",
                    "inputs",
                    "serialization",
                    "error",
                    "output",
                    "updated_at",
                )
            ]
        elif "FROM dbos.workflow_status" in statement and "GROUP BY" in statement:
            # The dead-letter census: one (name, version, count) group.
            self._result = (
                [(self._workflow_name, None, self._dlq_count)]
                if self._dlq_count
                else []
            )
        elif "FROM dbos.workflow_status" in statement:
            self._result = [
                (
                    f"failed-{index}",
                    self._workflow_name,
                    "ERROR",
                    None,
                    "okg-test-source-sync",
                    None,
                    None,
                    "workflow failed",
                    None,
                    index,
                )
                for index in range(self._dlq_count)
            ]
        elif "to_regclass('dbos.application_versions')" in statement:
            self._result = [(None,)]
        else:
            raise AssertionError(statement)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)


def test_cli_qa_snapshot_passes_the_readiness_delegated_workflows(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg metrics --qa-snapshot` shares readiness's delegated partition."""
    import argparse

    import okg.substrate.qa.metrics as qa_metrics
    from okg.substrate.cli.metrics import _cmd_metrics_qa_snapshot

    seen: dict[str, object] = {}

    def fake_snapshot(**kwargs: object) -> QAGateMetrics:
        seen.update(kwargs)
        return _sample()

    monkeypatch.setattr(qa_metrics, "snapshot", fake_snapshot)
    args = argparse.Namespace(
        deployment="no-such-deployment",
        dsn="postgresql://user@127.0.0.1:1/none",
        generation=None,
    )

    assert _cmd_metrics_qa_snapshot(args, as_json=True) == 0
    capsys.readouterr()
    assert seen["ignored_workflow_names"] == set(
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
    )
