"""The watcher runs a source that is failing its record-set coverage check
ON ITS OWN, instead of bundling it with others (issue #1599).

A watcher-created workflow that dies partway records an EXACT repair contract
naming every source it was running. Bundling a known-failing source into that
contract makes the contract unsatisfiable, and every later workflow then
refuses. On okg-workspace (2026-09-04) a watcher-created four-source workflow
carrying `codex-sessions` (#1318, a known-failing source) had to be cancelled
by hand for exactly this reason.
"""
from __future__ import annotations

import json

import pytest

from okg.substrate.ingest.record_set_coverage_state import (
    COVERAGE_STATE_KEY,
    coverage_state_payload,
)
from okg.substrate.watch import watcher as watcher_module
from okg.substrate.watch.watcher import WatchSource, Watcher


def _capture_enqueues(monkeypatch):
    jobs = []

    def _fake_enqueue(*, deployment, dsn, job, deduplication_id, compaction):
        jobs.append(job)
        return f"wf-{len(jobs)}"

    monkeypatch.setattr(
        watcher_module,
        "enqueue_compacted_source_sync_via_dbos",
        _fake_enqueue,
    )
    return jobs


def _watcher(tmp_path, excluded=frozenset(), monkeypatch=None):
    root = tmp_path / "repo"
    root.mkdir(exist_ok=True)
    scopes = tuple(
        WatchSource(
            source_name=name,
            roots=(str(root),),
            include_globs=(f"{name}/**",),
        )
        for name in ("agent-sessions", "code-structure", "codex-sessions")
    )
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="okg-workspace",
        roots=[root],
        source_scopes=scopes,
        debounce_s=0.0,
    )
    monkeypatch.setattr(
        Watcher, "_sources_to_exclude", lambda self: frozenset(excluded),
    )
    monkeypatch.setattr(
        Watcher, "_record_watch_health", lambda self, *a, **k: None,
    )
    for name in ("agent-sessions", "code-structure", "codex-sessions"):
        (root / name).mkdir(exist_ok=True)
        target = root / name / "file.txt"
        target.write_text("x", encoding="utf-8")
        w._on_event(str(target), event_type="modified")
    return w


def test_a_failing_source_is_split_into_its_own_workflow(
    tmp_path, monkeypatch,
):
    """Out of the bundle, but still enqueued."""
    jobs = _capture_enqueues(monkeypatch)
    w = _watcher(
        tmp_path, excluded={"codex-sessions"}, monkeypatch=monkeypatch,
    )
    assert w._enqueue_ingest() is True

    by_sources = {tuple(job.to_dict()["sources"]): job for job in jobs}
    # The healthy sources travel together; the failing one travels alone, so
    # a failure of its run writes a repair contract naming only itself.
    assert ("agent-sessions", "code-structure") in by_sources
    assert ("codex-sessions",) in by_sources

    bundle = by_sources[("agent-sessions", "code-structure")]
    for event in bundle.to_dict()["events"]:
        assert event["source_name"] != "codex-sessions"

    alone = by_sources[("codex-sessions",)]
    assert [e["source_name"] for e in alone.to_dict()["events"]] == [
        "codex-sessions",
    ]
    assert w.stats.events_isolated_failing_coverage == 1
    assert w.stats.ingest_jobs_enqueued == 2


def test_a_failing_source_can_still_clear_its_own_flag(tmp_path, monkeypatch):
    """The flag clears only on a successful run, and the watcher is what
    enqueues that run — so dropping the events would be self-perpetuating."""
    jobs = _capture_enqueues(monkeypatch)
    w = _watcher(
        tmp_path,
        excluded={"agent-sessions", "code-structure", "codex-sessions"},
        monkeypatch=monkeypatch,
    )
    assert w._enqueue_ingest() is True

    enqueued = sorted(tuple(job.to_dict()["sources"]) for job in jobs)
    assert enqueued == [
        ("agent-sessions",), ("code-structure",), ("codex-sessions",),
    ]
    assert w.stats.events_isolated_failing_coverage == 3
    assert w.stats.ingest_jobs_enqueued == 3


def test_no_exclusions_leaves_the_bundle_unchanged(tmp_path, monkeypatch):
    jobs = _capture_enqueues(monkeypatch)
    w = _watcher(tmp_path, excluded=frozenset(), monkeypatch=monkeypatch)
    assert w._enqueue_ingest() is True
    enqueued_sources = sorted(
        {s for job in jobs for s in job.to_dict()["sources"]}
    )
    assert enqueued_sources == [
        "agent-sessions", "code-structure", "codex-sessions",
    ]
    assert len(jobs) == 1
    assert w.stats.events_isolated_failing_coverage == 0
    assert w.stats.ingest_jobs_enqueued == 1


def test_an_unreadable_coverage_state_enqueues_unfiltered_and_says_so(
    tmp_path, monkeypatch, caplog,
):
    """Fail-open, loudly: a database blip must not stop ingest entirely."""
    def _boom(**kwargs):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(
        "okg.substrate.ingest.record_set_coverage_state."
        "sources_failing_record_set_coverage",
        _boom,
    )
    w = Watcher(
        dsn="postgres://unused",
        deployment_name="okg-workspace",
        roots=[tmp_path],
        source_scopes=(),
        debounce_s=0.0,
    )
    with caplog.at_level("WARNING"):
        assert w._sources_to_exclude() == frozenset()
    assert "WITHOUT the failing-source exclusion" in caplog.text


# ---------------------------------------------------------------------------
# The durable fact the filter reads
# ---------------------------------------------------------------------------


def test_a_coverage_failure_is_recorded_as_failing():
    payload = coverage_state_payload(
        deployment="okg-workspace",
        source_name="agent-sessions",
        result={
            "status": "ERROR",
            "run_id": "r1",
            "error": (
                "record set coverage check failed "
                "[record_set_coverage missing_nodes=0 stale_nodes=10 "
                "class=stale dominant=stale]"
            ),
        },
    )
    assert payload["failing"] is True
    assert payload["source_name"] == "agent-sessions"
    assert payload["run_id"] == "r1"


def test_an_unrelated_failure_is_not_a_coverage_failure():
    payload = coverage_state_payload(
        deployment="okg-workspace",
        source_name="agent-sessions",
        result={"status": "ERROR", "error": "adapter timed out"},
    )
    assert payload["failing"] is False


def test_a_successful_run_clears_the_flag():
    """Recovery is derived from the newest run, not from an operator
    remembering to reset anything."""
    payload = coverage_state_payload(
        deployment="okg-workspace",
        source_name="agent-sessions",
        result={"status": "OK", "run_id": "r2"},
    )
    assert payload["failing"] is False
    assert payload["status"] == "OK"


def test_a_result_with_no_status_records_nothing():
    assert coverage_state_payload(
        deployment="okg-workspace", source_name="x", result={},
    ) is None


@pytest.mark.postgres
def test_failing_sources_round_trip_through_source_progress(clean_runtime):
    from okg.substrate.db.pool import GENERAL, pooled_connection
    from okg.substrate.ingest.record_set_coverage_state import (
        sources_failing_record_set_coverage,
    )

    rows = [
        ("agent-sessions", True),
        ("code-structure", False),
        ("codex-sessions", True),
    ]
    with pooled_connection(clean_runtime, purpose=GENERAL) as conn:
        with conn.cursor() as cur:
            for source_name, failing in rows:
                cur.execute(
                    """
                    INSERT INTO okg.source_progress
                      (deployment, source_name, key, value, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (deployment, source_name, key)
                      DO UPDATE SET value = EXCLUDED.value
                    """,
                    (
                        "okg-workspace",
                        source_name,
                        COVERAGE_STATE_KEY,
                        json.dumps({
                            "schema": "okg.source_sync.record_set_coverage.v1",
                            "failing": failing,
                        }),
                    ),
                )

    failing = sources_failing_record_set_coverage(
        dsn=clean_runtime, deployment="okg-workspace",
    )
    assert failing == {"agent-sessions", "codex-sessions"}

    other = sources_failing_record_set_coverage(
        dsn=clean_runtime, deployment="some-other-deployment",
    )
    assert other == set()
