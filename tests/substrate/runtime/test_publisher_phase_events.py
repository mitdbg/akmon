"""Tests for the publisher cycle's per-phase DBOS event emission.

Covers the `_emit_phase_event` helper's safety contract (no crash
outside a workflow context) and the 8 normative phase events the
spec declares. The deep integration test (full cycle inside a DBOS
workflow) is exercised by the cutover-time verification step
(`decompose-publisher-cycle-into-dbos-steps` task 13), not here.
"""
from __future__ import annotations

from unittest.mock import patch


from okg.substrate.publisher.publisher import _emit_phase_event


def test_emit_phase_event_swallows_no_workflow_context() -> None:
    """Outside a DBOS workflow context, the helper MUST NOT raise.

    This is the contract that lets the cycle run from unit tests,
    `--inline` CLI paths, and other non-workflow callers.
    """
    # No DBOS singleton, no workflow context.
    _emit_phase_event("test_phase", {"key": "value"})
    # If we got here, the helper didn't raise. That's the assertion.


def test_emit_phase_event_calls_set_event_when_available() -> None:
    """When `dbos.DBOS.set_event` is reachable, the helper invokes
    it with the given name + payload."""
    with (
        patch("dbos.DBOS.workflow_id", "wf-test", create=True),
        patch("dbos.DBOS.set_event") as mock_set,
    ):
        _emit_phase_event("phase_a_complete", {"converged": True})

    mock_set.assert_called_once_with(
        "phase_a_complete", {"converged": True},
    )


def test_emit_phase_event_swallows_set_event_errors() -> None:
    """If `set_event` raises (e.g. workflow context missing despite
    DBOS being importable), the helper still doesn't crash the cycle."""
    with (
        patch("dbos.DBOS.workflow_id", "wf-test", create=True),
        patch(
            "dbos.DBOS.set_event",
            side_effect=RuntimeError("no current workflow"),
        ),
    ):
        # No assertion needed — the test passes if no exception
        # escapes the helper.
        _emit_phase_event("phase_b_complete", {"blocked": False})


# ---------------------------------------------------------------------------
# The 8 normative phase event names are pinned by the spec; this
# test surfaces a regression if they're renamed without updating the
# spec delta + the operator-facing `okg runtime debug-workflow`
# timeline rendering.
# ---------------------------------------------------------------------------


def test_normative_phase_event_names() -> None:
    """Read publisher.py source and confirm the spec-listed phase
    event names are referenced as ``_emit_phase_event("<name>", ...)``.
    """
    import inspect

    from okg.substrate.publisher import publisher

    source = inspect.getsource(publisher)

    expected = [
        "reserve_complete",
        "phase_a_complete",
        "phase_b_complete",
        "commit_complete",
        "post_publish_views_complete",
        "post_publish_lenses_complete",
        "post_publish_consistency_complete",
        "cycle_complete",
    ]

    for name in expected:
        # Match the event-name string literal; the surrounding
        # _emit_phase_event(...) call may wrap to multiple lines.
        assert f'"{name}"' in source, (
            f"missing event-name string literal for normative "
            f"phase name {name!r}"
        )
