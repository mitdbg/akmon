"""cursor_support_status churn-measured promotion (#323, task churn-microbench).

The status is no longer a declared-mode guess: a probe short-circuit (or a
measured no-change run that scanned zero files) promotes it to a measured
admission verdict.
"""
from __future__ import annotations

from okg.substrate.source_sync_microbench import _cursor_support_status


def test_unsupported_when_no_sync():
    assert _cursor_support_status(
        sync_declared=False, default_event_mode="cursor", reconcile_mode="",
    ) == "unsupported"


def test_declared_mode_fallback_without_measurement():
    # No measured signal => the original declared-mode label.
    assert _cursor_support_status(
        sync_declared=True, default_event_mode="cursor", reconcile_mode="",
    ) == "declared_cursor_mode"
    assert _cursor_support_status(
        sync_declared=True, default_event_mode="scope_complete",
        reconcile_mode="scope_complete",
    ) == "completed_scope_requires_audit"


def test_probe_short_circuit_promotes_to_verified():
    # Even a completed-scope source becomes verified once a probe short-circuit
    # is measured.
    assert _cursor_support_status(
        sync_declared=True,
        default_event_mode="scope_complete",
        reconcile_mode="scope_complete",
        probe_short_circuited=True,
    ) == "incremental_verified"


def test_measured_zero_scan_promotes_to_verified():
    assert _cursor_support_status(
        sync_declared=True,
        default_event_mode="cursor",
        reconcile_mode="",
        measured_files_scanned=0,
    ) == "incremental_verified"


def test_measured_nonzero_scan_does_not_promote():
    assert _cursor_support_status(
        sync_declared=True,
        default_event_mode="scope_complete",
        reconcile_mode="scope_complete",
        measured_files_scanned=5,
    ) == "completed_scope_requires_audit"
