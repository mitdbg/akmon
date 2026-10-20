"""Regression: the record-set coverage guard must not send an operator to
a cursor reset when the failure is stale-coverage, not missing-coverage.

Proven empirically on 2026-08-24 against a fresh database with the same
command shape and opposite outcomes:

  * ``git-history``    — ``--reset-cursor`` FIXED it (git refs are a stable
    snapshot, so the failure was missing-dominant).
  * ``agent-sessions`` — ``--reset-cursor`` did NOTHING (live agent
    transcripts are appended DURING the run, so the run's scope grew
    underneath it; the failure was stale-dominant, a live-data race).

The old guard printed one unconditional hint for both, and the source-sync
workflow then flattened the structured counts into a truncated sentence.
"""
from __future__ import annotations

import pickle

from okg.substrate.ingest.runner import (
    record_set_coverage_diagnosis,
    record_set_coverage_marker,
)
from okg.substrate.runtime.source_sync_workflow import (
    SourceSyncWorkflowError,
    _ingest_failure_details,
    _record_set_coverage_fields,
)


def test_missing_dominant_hint_recommends_cursor_reset() -> None:
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=1200,
        missing_edge_count=340,
        stale_node_count=0,
        stale_edge_count=0,
    )
    assert diagnosis["coverage_class"] == "missing"
    assert diagnosis["dominant"] == "missing"
    hint = diagnosis["fix_hint"]
    assert "cursor reset repair" in hint
    assert "NOT the fix" not in hint
    assert "1540 declared identities" in hint


def test_stale_dominant_hint_refuses_cursor_reset() -> None:
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=0,
        missing_edge_count=0,
        stale_node_count=4,
        stale_edge_count=9,
    )
    assert diagnosis["coverage_class"] == "stale"
    assert diagnosis["dominant"] == "stale"
    hint = diagnosis["fix_hint"]
    assert "A cursor reset is NOT the fix here" in hint
    assert "did not cover the scope already in the graph" in hint
    assert "discards backlog progress" in hint
    # The missing-branch recommendation must not appear on this branch.
    assert "Rerun the source with an explicit cursor reset repair" not in hint


def test_capped_stale_sample_is_reported_as_a_lower_bound() -> None:
    """The chronos guard samples stale rows with ``LIMIT 10``.

    A full sample is a lower bound, so it must never read as an exact
    count, and it must never let a large ``missing_*`` count claim
    dominance over an unbounded ``stale_*`` count.
    """
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=5000,
        missing_edge_count=0,
        stale_node_count=10,
        stale_edge_count=10,
        stale_counts_exact=False,
    )
    assert diagnosis["stale_counts_exact"] is False
    assert diagnosis["coverage_class"] == "mixed"
    assert diagnosis["dominant"] == "stale"
    assert "at least 20 source-owned rows" in diagnosis["fix_hint"]
    assert "Do NOT start with a cursor reset" in diagnosis["fix_hint"]


def test_mixed_missing_dominant_hint_names_both_classes() -> None:
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=100,
        missing_edge_count=0,
        stale_node_count=2,
        stale_edge_count=1,
    )
    assert diagnosis["coverage_class"] == "mixed"
    assert diagnosis["dominant"] == "missing"
    hint = diagnosis["fix_hint"]
    assert "mixed coverage, missing-dominant" in hint
    assert "repairs only the missing half" in hint


def test_tie_between_classes_does_not_recommend_a_reset_first() -> None:
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=3,
        missing_edge_count=0,
        stale_node_count=3,
        stale_edge_count=0,
    )
    assert diagnosis["dominant"] == "stale"
    assert "Do NOT start with a cursor reset" in diagnosis["fix_hint"]


def test_workflow_wrapper_preserves_structured_counts() -> None:
    """The structured counts must survive the source-sync wrapper.

    ``run_ingest`` flattens a ``SubstrateError`` to ``f"{type}: {exc}"``
    in the report, so the counts ride inside the message via the marker
    and the workflow lifts them back into structured fields.
    """
    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=0,
        missing_edge_count=0,
        stale_node_count=10,
        stale_edge_count=10,
        stale_counts_exact=False,
    )
    flattened = (
        "SubstrateError: SourceRunner: record_set coverage check failed; "
        "refusing to advance source watermark "
        + record_set_coverage_marker(diagnosis)
    )
    fields = _record_set_coverage_fields(flattened)
    assert fields == {
        "missing_node_count": 0,
        "missing_edge_count": 0,
        "stale_node_count": 10,
        "stale_edge_count": 10,
        "coverage_class": "stale",
        "dominant": "stale",
    }

    details = _ingest_failure_details(
        failed_results=[
            {"name": "agent-sessions", "status": "FAIL",
             "stage": "run", "error": flattened},
        ],
        failed_publishes=[],
    )
    assert details["failed_source"] == "agent-sessions"
    assert details["record_set_coverage"]["stale_node_count"] == 10
    assert details["record_set_coverage"]["dominant"] == "stale"

    err = SourceSyncWorkflowError("boom", data=details)
    assert isinstance(err, RuntimeError)
    assert err.data["record_set_coverage"]["coverage_class"] == "stale"

    # DBOS stores terminal workflow errors as pickles; the structured
    # payload has to survive that round trip too.
    revived = pickle.loads(pickle.dumps(err))
    assert isinstance(revived, RuntimeError)
    assert revived.data["record_set_coverage"]["dominant"] == "stale"


def test_workflow_wrapper_returns_nothing_for_unrelated_errors() -> None:
    assert _record_set_coverage_fields("psycopg.OperationalError: boom") == {}
    details = _ingest_failure_details(
        failed_results=[{"name": "x", "error": "TypeError: bad arg"}],
        failed_publishes=[],
    )
    assert "record_set_coverage" not in details
    assert details["failed_source"] == "x"
