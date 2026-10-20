"""A source is healthy only when the runtime recorded something that says so.

Issue #2205. Two console reads called a source `healthy` on no recorded
outcome at all. `console/health`'s `source_breakdown` said it whenever an
`okg.source_progress` row existed, and `console/source-diagnostics` -- whose
`health.status` `console/producers` serves to Home and Sources -- said it
whenever no bad signal turned up, so a source with no run, no watermark and
no cadence read as healthy.

The rule these tests hold: `healthy` needs a positive recorded signal -- a
watermark the runtime judged `fresh` against the source's cadence, or a
latest run whose recorded outcome is `OK`. A recorded failure says `failing`.
With neither, and nothing wrong, the answer is `unknown`.

No database. The first group reads rows captured from okg-workspace's live
`console/source-diagnostics` answer on 2026-09-22 (fixture beside this file;
the per-producer liveness list and the stage map are dropped, nothing is
edited) and re-derives each row's health from the fields the rule reads.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from okg.substrate.console_views import health as health_mod
from okg.substrate.console_views import source_diagnostics as sd
from okg.substrate.console_views.producers import _source_row

FIXTURE = (
    Path(__file__).parent / "fixtures" / "okg_workspace_source_rows_20260922.json"
)


def _captured() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _derive(row: dict[str, Any], *, runtime_available: bool = True) -> str:
    incidents = sd._incidents(
        freshness=row["freshness"],
        workflow=row["workflow"],
        latest_run=row["latest_run"],
    )
    return sd._operator_health_label(
        runtime_available=runtime_available,
        freshness=row["freshness"],
        producers=row["producers"],
        workflow=row["workflow"],
        incidents=incidents,
        latest_run=row["latest_run"],
        contribution=row["contribution"],
    )


def _positive_signal(row: dict[str, Any]) -> bool:
    return (
        row["freshness"].get("status") == "fresh"
        or str(row["latest_run"].get("run_status") or "").lower() == "ok"
    )


# --- the captured okg-workspace answer --------------------------------------

def test_captured_fixture_carries_no_dsn_path_or_hostname() -> None:
    text = FIXTURE.read_text(encoding="utf-8").lower()
    for needle in (
        "postgres", "postgresql://", "/users/", "/private/", "/home/",
        "localhost", "127.0.0.1", "password", "dsn",
    ):
        assert needle not in text, needle


def test_okg_workspace_captured_before_state_is_the_defect() -> None:
    """What the read served before this change: 26 sources healthy, none of
    them with a fresh watermark or an OK run behind the word."""
    captured = _captured()
    rows = captured["sources"]
    assert Counter(row["health"]["status"] for row in rows) == {
        "healthy": 26, "stale": 6, "degraded": 3, "missing": 2,
    }
    healthy = [row for row in rows if row["health"]["status"] == "healthy"]
    assert healthy and not any(_positive_signal(row) for row in healthy)


def test_okg_workspace_no_source_is_healthy_without_a_recorded_signal() -> None:
    rows = _captured()["sources"]
    derived = {row["name"]: _derive(row) for row in rows}
    for row in rows:
        if derived[row["name"]] == "healthy":
            assert _positive_signal(row), row["name"]
    # The 26 that read healthy on nothing now say the read does not know.
    assert Counter(derived.values()) == {
        "unknown": 26, "stale": 6, "degraded": 3, "missing": 2,
    }
    # Every other verdict is what the read already recorded.
    for row in rows:
        if row["health"]["status"] != "healthy":
            assert derived[row["name"]] == row["health"]["status"], row["name"]
    assert derived["agent-memory"] == "unknown"
    assert derived["branch-dispositions"] == "degraded"
    assert derived["git-files"] == "stale"


# --- the rule on its own ------------------------------------------------------

def _row(
    *,
    freshness: str = "not_configured",
    producers: str = "not_configured",
    run_status: str | None = None,
    records: int = 0,
) -> dict[str, Any]:
    latest: dict[str, Any] = (
        {"status": "not_observed", "unavailable_reason": "no_source_run_diagnostics_rows"}
        if run_status is None
        else {"status": "available", "run_status": run_status}
    )
    return {
        "freshness": {"status": freshness},
        "producers": {"status": producers},
        "workflow": {"state": "fresh", "active": False},
        "latest_run": latest,
        "contribution": {"records_in_gen": records, "last_sync_age": "3h ago"},
    }


def test_a_fresh_watermark_is_healthy() -> None:
    assert _derive(_row(freshness="fresh")) == "healthy"


def test_an_ok_latest_run_is_healthy() -> None:
    assert _derive(_row(run_status="OK")) == "healthy"


def test_no_outcome_is_unknown_not_healthy() -> None:
    assert _derive(_row()) == "unknown"


def test_a_failed_latest_run_is_failing_and_counted_as_an_incident() -> None:
    row = _row(freshness="fresh", run_status="FAIL")
    assert _derive(row) == "failing"
    incidents = sd._incidents(
        freshness=row["freshness"],
        workflow=row["workflow"],
        latest_run=row["latest_run"],
    )
    assert {"severity": "warn", "code": "latest_run_failed",
            "label": "latest run failed"} in incidents


def test_an_unclassified_outcome_is_no_signal() -> None:
    assert _derive(_row(run_status="skipped")) == "unknown"


def test_without_runtime_graph_records_alone_are_not_health() -> None:
    """Records in the pinned version say the source ran once, not how."""
    assert _derive(_row(records=12), runtime_available=False) == "unknown"
    assert _derive(_row(run_status="OK"), runtime_available=False) == "healthy"
    assert _derive(_row(run_status="FAIL"), runtime_available=False) == "failing"


def test_producers_read_serves_the_derived_status() -> None:
    row = {
        "name": "agent-memory", "source_key": "okg-workspace.agent-memory",
        "in_registry": True, "health": {"status": "unknown", "incident_count": 1},
        "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"},
        "freshness": {"status": "not_configured"}, "latest_run": {},
        "contribution": {},
    }
    assert _source_row(row)["health"]["status"] == "unknown"


# --- the pinned health read ---------------------------------------------------

def test_health_breakdown_does_not_call_a_progress_row_healthy() -> None:
    status, issue, _next = health_mod._source_status(
        spellings={"git-files"}, has_progress=True, excluded=set(),
    )
    assert status == "unknown"
    assert issue
    status, _issue, _next = health_mod._source_status(
        spellings={"git-files"}, has_progress=True, excluded={"git-files"},
    )
    assert status == "excluded"
