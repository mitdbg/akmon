"""Zero-output source admission accepts idempotent replays (#826).

After the interval-idempotence work, a healthy completed-scope re-sync emits its
full record set but every fact dedupes to a semantic no-op, so nothing new is
appended (``observed_output_count == 0``). Source admission must accept that as
an ``authoritative_replay`` no-op, not reject it as ``unclassified_empty`` and
report the misleading ``zero_output_not_authorized`` reason. These are pure
checks of the classification and rejection-reason helpers, so they run in the
per-PR (no-DB) lane.
"""
from __future__ import annotations

import pytest

from okg.substrate.ingest.service import (
    _admission_run_mode,
    _zero_output_admission_verdict,
    _zero_output_rejection_reason,
)


def _verdict(
    next_cursor,
    *,
    run_mode="reconcile",
    completed_scope=True,
    health="ok",
    rejected_fact_count=0,
    **extra,
):
    return _zero_output_admission_verdict(
        source="cooper.analysis-notes",
        run_id="run-1",
        admission_mode="fast_track",
        completed_scope=completed_scope,
        source_health_status=health,
        run_mode=run_mode,
        next_cursor=next_cursor,
        rejected_fact_count=rejected_fact_count,
        **extra,
    )["producer_policy"]


# 405 records emitted whose facts all deduped to zero new appended output.
_NON_EMPTY_RECORDS = {f"rec-{i}": {"node_id": f"node-{i}"} for i in range(405)}


@pytest.mark.parametrize(
    "next_cursor",
    [
        pytest.param({"record_set": {"records": _NON_EMPTY_RECORDS}}, id="inline"),
        pytest.param(
            {"record_set_summary": {"record_count": 405}, "record_set_ref": "s3://rs"},
            id="external-ref",
        ),
    ],
)
def test_idempotent_replay_zero_output_is_accepted(next_cursor) -> None:
    """A completed-scope authoritative re-sync with a non-empty declared record
    set and zero new output is accepted as an idempotent replay."""
    policy = _verdict(next_cursor, run_mode="reconcile")

    assert policy["status"] == "matched"
    assert policy["terminal_state_authorized"] is True
    assert policy["reason"] == "authoritative_replay"


def test_admission_uses_authorized_effective_mode() -> None:
    from types import SimpleNamespace

    result = SimpleNamespace(
        mode="append",
        effective_mode="scope_complete",
        completed_scope=True,
        next_cursor={"record_set": {"records": {}}},
    )

    assert _admission_run_mode(result, fallback="append") == "scope_complete"


def test_admission_rejects_unproved_effective_mode_upgrade() -> None:
    from types import SimpleNamespace

    result = SimpleNamespace(
        mode="append",
        effective_mode="scope_complete",
        completed_scope=True,
        next_cursor={},
    )

    assert _admission_run_mode(result, fallback="append") == "append"


def test_admission_uses_runner_proven_append_no_change() -> None:
    from types import SimpleNamespace

    result = SimpleNamespace(
        mode="append",
        effective_mode="no_change",
        completed_scope=True,
        runner_proven_no_change=True,
        next_cursor={},
    )

    assert _admission_run_mode(result, fallback="append") == "no_change"


def test_admission_rejects_adapter_claimed_append_no_change() -> None:
    from types import SimpleNamespace

    result = SimpleNamespace(
        mode="append",
        effective_mode="no_change",
        completed_scope=True,
        runner_proven_no_change=False,
        next_cursor={},
    )

    assert _admission_run_mode(result, fallback="append") == "append"


def test_genuinely_empty_snapshot_keeps_its_accepted_classification() -> None:
    """The pre-existing empty-snapshot acceptance is unchanged."""
    policy = _verdict({"record_set": {"records": {}}}, run_mode="reconcile")

    assert policy["status"] == "matched"
    assert policy["terminal_state_authorized"] is True
    assert policy["reason"] == "authoritative_empty_snapshot"


@pytest.mark.parametrize(
    ("next_cursor", "run_mode", "completed_scope", "health", "rejected", "expected"),
    [
        # No record set: nothing declares authoritative coverage -> unclassified.
        ({}, "reconcile", True, "ok", 0, "unclassified_empty"),
        # Cursor mode is not an authoritative full pass, even with a record set.
        (
            {"record_set": {"records": _NON_EMPTY_RECORDS}},
            "cursor",
            True,
            "ok",
            0,
            "unclassified_empty",
        ),
        # Incomplete scope of a source with NO accepted terminal state (a
        # brand-new source) stays refused, and the label says so. #2074 (a)
        # accepts only a source that already has a watermark; see
        # `test_incomplete_healthy_noop_*` below.
        (
            {"record_set": {"records": _NON_EMPTY_RECORDS}},
            "reconcile",
            False,
            "ok",
            0,
            "partial",
        ),
        # ...as does a degraded/failed health signal.
        (
            {"record_set": {"records": _NON_EMPTY_RECORDS}},
            "reconcile",
            True,
            "degraded",
            0,
            "failed_or_degraded",
        ),
        # Silent-failure guard: a reconcile whose facts were ALL rejected looks
        # exactly like a clean replay (record set present, zero appended,
        # healthy, complete) but must NOT advance the watermark past lost facts.
        (
            {"record_set": {"records": _NON_EMPTY_RECORDS}},
            "reconcile",
            True,
            "ok",
            405,
            "facts_rejected",
        ),
    ],
)
def test_zero_output_reject_cases_report_honest_kinds(
    next_cursor, run_mode, completed_scope, health, rejected, expected
) -> None:
    """Runs that must stay rejected are not accepted as replays and carry a
    reason that names why they were rejected."""
    policy = _verdict(
        next_cursor,
        run_mode=run_mode,
        completed_scope=completed_scope,
        health=health,
        rejected_fact_count=rejected,
    )

    assert policy["status"] == "rejected"
    assert policy["terminal_state_authorized"] is False
    assert policy["reason"] == expected
    assert policy["reason"] not in {"authoritative_replay", "authoritative_empty_snapshot"}


def test_run_result_exposes_rejected_fact_count_field() -> None:
    """Plumbing guard: `_finalize_source_admission` reads
    `RunResult.rejected_fact_count` by name to feed the silent-data-loss guard.
    A rename must fail loudly per-PR here, not silently default the guard to 0
    and re-open #826."""
    from dataclasses import fields

    from okg.substrate.ingest.runner import RunResult

    assert "rejected_fact_count" in {f.name for f in fields(RunResult)}


def test_replay_with_rejected_facts_is_not_accepted() -> None:
    """#826 hardening: a record-set-present reconcile that dropped facts is a
    silent-failure signature, not idempotence — it must be rejected."""
    policy = _verdict(
        {"record_set": {"records": _NON_EMPTY_RECORDS}},
        run_mode="reconcile",
        rejected_fact_count=1,
    )

    assert policy["status"] == "rejected"
    assert policy["terminal_state_authorized"] is False
    assert policy["reason"] == "facts_rejected"


def test_structural_rejection_reason_is_not_masked() -> None:
    """When authority passed but the structural verdict rejected the run, the
    rejection reports the structural kind, not the generic authority default."""
    # Authority passed (authority_reason is None), structural rejected: surface
    # the structural kind.
    assert (
        _zero_output_rejection_reason(
            authority_reason=None,
            structural_allowed=False,
            structural_kind="unclassified_empty",
        )
        == "unclassified_empty"
    )

    # An authority denial still owns the reason (authority precedence preserved).
    assert (
        _zero_output_rejection_reason(
            authority_reason="producer_profile_not_writable",
            structural_allowed=True,
            structural_kind="authoritative_replay",
        )
        == "producer_profile_not_writable"
    )

    # The generic default only fills a truly missing reason.
    assert (
        _zero_output_rejection_reason(
            authority_reason=None, structural_allowed=False, structural_kind=None
        )
        == "zero_output_not_authorized"
    )


def test_authority_denial_without_reason_does_not_leak_accept_label() -> None:
    """Hardening: when the structural verdict ACCEPTED (kind
    ``authoritative_replay``) but authority denied the run with no reason string,
    the accept-label must not leak as the rejection reason — the generic default
    stands."""
    assert (
        _zero_output_rejection_reason(
            authority_reason=None,
            structural_allowed=True,
            structural_kind="authoritative_replay",
        )
        == "zero_output_not_authorized"
    )


# --- close-refused-publish-redrive-gap (#828): unpublished dedupe basis ------


def _verdict_with_prior(
    *,
    prior=None,
    check_failed=False,
    rejected_fact_count=0,
):
    return _zero_output_admission_verdict(
        source="cooper.analysis-notes",
        run_id="run-2",
        admission_mode="fast_track",
        completed_scope=True,
        source_health_status="ok",
        run_mode="reconcile",
        next_cursor={"record_set": {"records": _NON_EMPTY_RECORDS}},
        rejected_fact_count=rejected_fact_count,
        prior_unpublished_admissions=prior,
        prior_unpublished_check_failed=check_failed,
    )["producer_policy"]


def test_unpublished_prior_admission_rejects_replay() -> None:
    """close-refused-publish-redrive-gap (#828): a replay-shaped run whose
    dedupe basis is a prior admission that never reached a published
    generation is rejected, naming the blocking admission ids."""
    policy = _verdict_with_prior(prior=["11111111-1111-1111-1111-111111111111"])

    assert policy["status"] == "rejected"
    assert policy["terminal_state_authorized"] is False
    assert policy["reason"] == "prior_emission_unpublished"
    assert policy["unpublished_prior_admissions_sample"] == [
        "11111111-1111-1111-1111-111111111111"
    ]


def test_unpublished_detection_failure_rejects_fail_closed() -> None:
    """A failed detection query must reject with its own reason -- never
    default to 'no unpublished work' (fail-open would silently accept)."""
    policy = _verdict_with_prior(check_failed=True)

    assert policy["status"] == "rejected"
    assert policy["reason"] == "prior_emission_check_unavailable"


def test_rejected_facts_dominate_unpublished_prior() -> None:
    """Precedence: the #826 rejected-facts guard still owns the label when
    both conditions hold (data loss beats stranded work in the report)."""
    policy = _verdict_with_prior(
        prior=["11111111-1111-1111-1111-111111111111"],
        rejected_fact_count=3,
    )

    assert policy["status"] == "rejected"
    assert policy["reason"] == "facts_rejected"


def test_no_prior_unpublished_keeps_replay_acceptance() -> None:
    """Regression: with no unpublished prior work the #826 replay
    acceptance is unchanged."""
    policy = _verdict_with_prior(prior=[])

    assert policy["status"] == "matched"
    assert policy["reason"] == "authoritative_replay"


# --- #2074 (a): a healthy incomplete no-op of a known source is accepted -----
#
# Operator decision on #1859 (2026-09-22). A HEALTHY run that did not finish
# its scan and produced nothing used to be refused as `partial`, which failed
# every idle incremental run. It is now an accepted no-op labelled
# `incomplete_scope_noop` -- but only for a source that already has accepted
# terminal state. Deletion is not at stake: the runner never synthesizes
# retractions from an incomplete run, and `persist_accepted_source_terminal_
# state` advances `last_reconcile` only for a completed scope, so the source
# still reads as stale.

_CARRIED_CURSOR = {"record_set": {"records": _NON_EMPTY_RECORDS}, "page": 7}


@pytest.mark.parametrize("run_mode", ["cursor", "reconcile", "scope_incomplete"])
def test_incomplete_healthy_noop_of_a_known_source_is_accepted(run_mode) -> None:
    policy = _verdict(
        _CARRIED_CURSOR,
        run_mode=run_mode,
        completed_scope=False,
        source_has_published_watermark=True,
    )

    assert policy["status"] == "matched"
    assert policy["terminal_state_authorized"] is True
    assert policy["reason"] == "incomplete_scope_noop"
    assert policy["completed_scope"] is False
    assert policy["source_has_published_watermark"] is True
    # The caller saves no cursor, progress or reconcile time for it.
    assert policy["keeps_source_state"] is True


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # A brand-new source: no prior state to keep.
        pytest.param(
            {"source_has_published_watermark": False},
            "partial",
            id="new-source",
        ),
        pytest.param(
            {"health": "degraded"},
            "failed_or_degraded",
            id="degraded",
        ),
        pytest.param(
            {"health": "acquire_failed"},
            "failed_or_degraded",
            id="failed",
        ),
        pytest.param(
            {"rejected_fact_count": 2},
            "facts_rejected",
            id="rejected-facts",
        ),
        pytest.param(
            {
                "prior_unpublished_admissions": [
                    "22222222-2222-2222-2222-222222222222",
                ],
            },
            "prior_emission_unpublished",
            id="unpublished-prior",
        ),
        pytest.param(
            {"prior_unpublished_check_failed": True},
            "prior_emission_check_unavailable",
            id="unpublished-check-failed",
        ),
    ],
)
def test_incomplete_noop_stays_refused_without_a_clean_basis(
    overrides, expected,
) -> None:
    arguments = {
        "run_mode": "cursor",
        "completed_scope": False,
        "source_has_published_watermark": True,
        **overrides,
    }
    policy = _verdict(_CARRIED_CURSOR, **arguments)

    assert policy["status"] == "rejected"
    assert policy["terminal_state_authorized"] is False
    assert policy["reason"] == expected
    assert "keeps_source_state" not in policy


def test_completed_accepted_zero_output_run_still_saves_source_state() -> None:
    """Only the incomplete no-op withholds terminal state."""
    policy = _verdict(_CARRIED_CURSOR, run_mode="no_change")

    assert policy["terminal_state_authorized"] is True
    assert "keeps_source_state" not in policy


@pytest.mark.parametrize(
    ("completed_scope", "advances_reconcile"),
    [(False, False), (True, True)],
)
def test_incomplete_noop_does_not_advance_last_reconcile(
    monkeypatch, completed_scope, advances_reconcile,
) -> None:
    """The accepted no-op still reads as stale.

    The admission path saves no terminal state for the no-op at all (the
    Postgres proof is in test_source_runner_retraction_atomicity.py); this
    is the backstop below it. The terminal state an accepted zero-output
    run persists is built by
    `source_terminal_state_evidence` and written by
    `persist_accepted_source_terminal_state`. The watermark write is captured
    rather than executed, so this runs without a database; the completed arm
    is the control that proves the capture sees `last_reconcile` at all.
    """
    from datetime import datetime, timezone

    from okg.substrate.ingest import source_terminal_state

    captured: dict = {}

    def _capture(conn, **kwargs):
        del conn
        captured.update(kwargs)

    monkeypatch.setattr(
        source_terminal_state, "persist_source_watermark", _capture,
    )
    observed_at = datetime(2026, 9, 23, tzinfo=timezone.utc)
    terminal_state = source_terminal_state.source_terminal_state_evidence(
        next_cursor=_CARRIED_CURSOR,
        observed_at=observed_at,
        completed_scope=completed_scope,
        deployment="default",
        source_progress=None,
    )
    source_terminal_state.persist_accepted_source_terminal_state(
        None,
        source="cooper.analysis-notes",
        terminal_state=terminal_state,
    )

    assert captured["next_cursor"] == _CARRIED_CURSOR
    assert captured["last_reconcile"] == (
        observed_at if advances_reconcile else None
    )
