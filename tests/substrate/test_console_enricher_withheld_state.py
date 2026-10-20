"""Pure-function tests for the withheld-enricher state in the Health view.

These test `okg.substrate.console_views.enricher_diagnostics` helpers
directly. They touch no database, so they live under `tests/substrate/`
where the fast pull-request lane owns them; the endpoint-level tests for the
same view live in `tests/okg-workspace/` behind the `postgres` marker.
"""

from __future__ import annotations

from okg.substrate.console_views.enricher_diagnostics import (
    _health_status,
    _withheld_detail,
    _withheld_next_check,
)


def test_a_withheld_enricher_is_named_not_reported_as_unknown() -> None:
    """Issue #1528 F4: the Health view names a withheld enricher.

    Before: the publisher wrote `withheld:...` to
    `enricher_progress.last_status`, and this view -- which knew only
    `failed:`, `success` and `never` -- fell through to `unknown`, with a null
    `latest_issue` and "No action needed." for `next_check`. An operator
    looking at a correctly withheld enricher saw an unexplained blank and no
    way to clear it.

    After: the state reads `withheld`, and the held source is recoverable from
    the reason so the view can name it and point at `okg admission decide`.
    The state is read only from the reason the publisher wrote -- never
    inferred from silence, which is the #1330 confusion this whole change
    exists to prevent.

    A pure-function test on purpose: `_health_status` and `_withheld_detail`
    ARE the change, and the shared seed fixture used by the endpoint tests in
    `tests/okg-workspace/test_console_enricher_diagnostics.py` is broken on
    the branch base (see issue #1753), so routing through `compose` would test
    someone else's defect rather than this one.
    """
    held = "withheld:source_held_for_admission:codex_sessions"

    assert _health_status({"last_status": held}, None) == "withheld"
    # Not the states it must never be confused with.
    assert _health_status({"last_status": held}, None) != "unknown"
    assert _health_status({"last_status": held}, None) != "failing"
    # A withheld row is still withheld when a stale successful run from an
    # earlier cycle sits beside it: the reason wins over the run.
    assert (
        _health_status({"last_status": held}, {"status": "ok"}) == "withheld"
    )
    # The reason names the held source, which is what makes the next check
    # actionable rather than generic.
    assert _withheld_detail({"last_status": held})[1] == "codex_sessions"

    # The neighbouring states are untouched.
    assert _health_status({"last_status": "failed:boom"}, None) == "failing"
    assert _health_status({"last_status": "never"}, None) == "unknown"
    assert (
        _health_status(
            {"last_status": "success", "lifetime_edges_emitted": 3}, None,
        )
        == "healthy"
    )
    # A non-withheld row must not fabricate a source name.
    assert _withheld_detail({"last_status": "success"}) == ("", "(unknown)")


def test_next_check_does_not_send_the_operator_to_a_command_that_cannot_work(
) -> None:
    """Issue #1528 F2: the release path must match the envelope's real state.

    Before: every withheld enricher was told to run `okg admission decide`.
    For most holds that command CANNOT move the envelope --
    `runtime/admission_workflows.py:948-985` short-circuits both accept and
    reject to `already_decided` on any non-`pending` envelope (before issue
    #1752 a `quarantine`-mode source run never reached `pending` at all:
    ingest's fence held only `review` and let it fall through to
    `accepted`). Sending an operator to a no-op is worse than saying
    nothing, because it reads as a fix that was tried and failed.

    After: a decidable hold gets the command; a wedged one gets the truth
    about what actually clears it. Both are asserted, in both directions.
    """
    held = "withheld:source_held_for_admission:codex_sessions"
    reason, detail = _withheld_detail({"last_status": held})

    # Decidable: a pending envelope in either held mode. The command is
    # offered. Issue #1752: ingest finalizes a quarantine-mode run pending,
    # like a review-mode run, so `okg admission decide` applies it.
    for state in (("review", "pending"), ("quarantine", "pending")):
        decidable = _withheld_next_check(
            reason, detail, {"codex_sessions": state},
        )
        assert "okg admission decide" in decidable, state
        assert "not decidable" not in decidable.lower(), state

    # Wedged: any held-mode envelope that is no longer pending. The command
    # is NOT offered as the fix, and the real release path is named.
    for state in (
        ("quarantine", "accepted"),
        ("quarantine", "published"),
        ("quarantine", "blocked"),
        ("review", "quarantined"),
    ):
        wedged = _withheld_next_check(
            reason, detail, {"codex_sessions": state},
        )
        assert "already_decided" in wedged, state
        assert "non-held admission mode" in wedged, state

    # A declaration error is a different problem with a different fix, and
    # must never be reported as an admission hold.
    unknown = _withheld_next_check(
        "unknown_declared_source", "not_a_source", {},
    )
    assert "source_registry.yaml" in unknown
    assert "okg admission decide" not in unknown
