"""A source run may report `partial` health without claiming full scope.

Issue #1284. A source that produced usable facts but did not observe its whole
scope — a truncated crawl, a page-limited API sweep, a cancelled walk — had no
truthful status: `ok` overstates it and `endpoint_failed` understates it.

Two properties matter:

* the status is new to `SourceHealth` only. `PREFLIGHT_STATUSES` is unchanged,
  because a preflight answers "can this source run at all?", which has no
  partial answer.
* a partial run may not claim `completed_scope`. Absence in a partial run is
  not evidence of deletion, so the pairing is refused at construction rather
  than left to the runner to notice.

The runner needs no change: `_completed_scope_health_is_acceptable` already
withholds deletion authority from every status outside
`{ok, not_applicable, skipped_optional}`.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import (
    PREFLIGHT_STATUSES,
    SOURCE_HEALTH_STATUSES,
    SOURCE_RUN_PARTIAL_STATUS,
    SourceHealth,
    SourcePreflightResult,
    SourceRun,
)


def test_source_health_accepts_partial() -> None:
    health = SourceHealth(
        status=SOURCE_RUN_PARTIAL_STATUS,
        reason="crawl stopped at the page limit",
        record_count=120,
    )
    assert health.status == "partial"
    assert health.as_dict()["status"] == "partial"


def test_preflight_vocabulary_is_unchanged() -> None:
    assert SOURCE_RUN_PARTIAL_STATUS not in PREFLIGHT_STATUSES
    assert SOURCE_HEALTH_STATUSES == PREFLIGHT_STATUSES | {"partial"}
    assert PREFLIGHT_STATUSES == frozenset({
        "ok",
        "skipped_optional",
        "missing_credential",
        "auth_failed",
        "tls_failed",
        "endpoint_failed",
        "cache_missing",
        "not_applicable",
        "acquire_stale",
        "acquire_failed",
    })


def test_preflight_result_refuses_the_run_only_status() -> None:
    with pytest.raises(ValueError, match="unknown source preflight status"):
        SourcePreflightResult(
            source_name="issue_api", status=SOURCE_RUN_PARTIAL_STATUS
        )


def test_preflight_result_still_accepts_its_own_vocabulary() -> None:
    for status in sorted(PREFLIGHT_STATUSES):
        assert (
            SourcePreflightResult(source_name="issue_api", status=status).status
            == status
        )


def test_unknown_status_still_raises() -> None:
    with pytest.raises(ValueError, match="unknown source health status"):
        SourceHealth(status="mostly_ok")


def test_partial_run_may_not_claim_completed_scope() -> None:
    with pytest.raises(ValueError, match="completed_scope"):
        SourceRun(
            facts=[],
            completed_scope=True,
            health=SourceHealth(status=SOURCE_RUN_PARTIAL_STATUS),
        )


def test_partial_run_without_completed_scope_is_accepted() -> None:
    run = SourceRun(
        facts=[],
        completed_scope=False,
        health=SourceHealth(status=SOURCE_RUN_PARTIAL_STATUS),
    )
    assert run.health is not None and run.health.status == "partial"
    assert run.completed_scope is False


def test_ok_run_may_still_claim_completed_scope() -> None:
    run = SourceRun(
        facts=[], completed_scope=True, health=SourceHealth(status="ok")
    )
    assert run.completed_scope is True


def test_empty_scope_assertion_is_off_by_default() -> None:
    """#1859 rule (b): no reader asserts an empty scope unless it says so."""
    assert SourceRun(facts=[], completed_scope=True).asserts_empty_scope is False


def test_empty_scope_assertion_needs_a_completed_scope() -> None:
    with pytest.raises(ValueError, match="assert its scope is empty"):
        SourceRun(facts=[], asserts_empty_scope=True)


def test_empty_scope_assertion_contradicts_preserved_authority() -> None:
    with pytest.raises(ValueError, match="both assert an empty scope"):
        SourceRun(
            facts=[],
            completed_scope=True,
            preserves_prior_record_set=True,
            asserts_empty_scope=True,
        )


@pytest.mark.parametrize(
    "authority",
    ["record_set", "record_set_replacement_keys", "record_authority_stream"],
)
def test_empty_scope_assertion_carries_no_record_authority(authority) -> None:
    from okg.substrate.library.sources.base import (
        RecordAuthorityStream,
        RecordSet,
    )

    value = {
        "record_set": RecordSet(),
        "record_set_replacement_keys": {"r-gone"},
        "record_authority_stream": RecordAuthorityStream(
            operations=iter(()),
            logical_sha256="0" * 64,
            record_count=0,
            operation_count=0,
            component_count=0,
        ),
    }[authority]
    with pytest.raises(ValueError, match="asserts an empty scope declares"):
        SourceRun(
            facts=[],
            completed_scope=True,
            asserts_empty_scope=True,
            **{authority: value},
        )


def test_completed_empty_scope_assertion_is_accepted() -> None:
    run = SourceRun(facts=[], completed_scope=True, asserts_empty_scope=True)
    assert run.asserts_empty_scope is True


def test_runner_withholds_deletion_authority_for_partial() -> None:
    """The status is honored without a runner change: the existing acceptance
    set excludes it, so a partial run cannot authorize retraction-by-absence.
    """
    from okg.substrate.ingest.runner import SourceRunner

    acceptable = SourceRunner._completed_scope_health_is_acceptable
    assert acceptable(SourceHealth(status="ok")) is True
    assert acceptable(SourceHealth(status=SOURCE_RUN_PARTIAL_STATUS)) is False
