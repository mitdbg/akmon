"""The worker-ack gate must say WHY it refused, without changing WHEN it refuses.

The gate is correct to fail closed. What it lacked was a reason: a bare False
surfaced ten minutes later as `helm --wait` deadline exceeded, and tracing the
real cause -- a containerised runtime cannot satisfy a host-supervisor identity
-- took hours. These tests pin both halves: the boolean verdict is unchanged,
and the refusal now names the disagreeing fields.
"""

from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.deployment_apply import (
    _worker_ack_matches,
    _worker_ack_mismatches,
)

FINGERPRINTS = {
    "desired_state_digest": "d",
    "source_registry_fingerprint": "s",
    "runtime_fingerprint": "r",
    "worker_config_fingerprint": "w",
    "manifest_fingerprint": "m",
    "catalog_fingerprint": "c",
    "schema_fingerprint": "sc",
    "writer_fingerprint": "wr",
    "schedule_fingerprint": "sh",
}

IDENTITY = {
    "working_directory": "/srv/okg",
    "process_executable": "/srv/okg/.venv/bin/python3",
    "interpreter": "/srv/okg/.venv/bin/python3",
    "import_root": "/srv/okg/src",
    "code_revision": "abc123",
    "database": {"host": "h", "port": 5432, "dbname": "d"},
}


def _desired(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        **FINGERPRINTS,
        "source_fingerprints": {"git": "g1"},
        "source_names": ["git"],
        "source_schedule_ids": ["git-daily"],
        "runtime_enabled": True,
        "service_authority": dict(IDENTITY),
    }
    base.update(over)
    return base


def _ack(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        **FINGERPRINTS,
        "source_fingerprints": {"git": "g1"},
        "active_sources": ["git"],
        "active_schedules": ["git-daily"],
        "observed_runtime_authority": dict(IDENTITY),
    }
    base.update(over)
    return base


def test_matching_ack_is_accepted_with_no_reasons() -> None:
    assert _worker_ack_mismatches(_desired(), _ack()) == []
    assert _worker_ack_matches(_desired(), _ack()) is True


def test_absent_ack_is_refused_with_a_reason() -> None:
    assert _worker_ack_mismatches(_desired(), None) == [
        "no worker acknowledgment recorded"
    ]
    assert _worker_ack_matches(_desired(), None) is False


@pytest.mark.parametrize("field", sorted(FINGERPRINTS))
def test_each_fingerprint_mismatch_is_named(field: str) -> None:
    reasons = _worker_ack_mismatches(_desired(), _ack(**{field: "changed"}))
    assert any(field in reason for reason in reasons), reasons
    assert _worker_ack_matches(_desired(), _ack(**{field: "changed"})) is False


def test_source_and_schedule_drift_are_named() -> None:
    assert any(
        "source_fingerprints" in r
        for r in _worker_ack_mismatches(_desired(), _ack(source_fingerprints={"git": "g2"}))
    )
    assert any(
        "active_sources" in r
        for r in _worker_ack_mismatches(_desired(), _ack(active_sources=["git", "docs"]))
    )
    assert any(
        "active_schedules" in r
        for r in _worker_ack_mismatches(_desired(), _ack(active_schedules=[]))
    )


def test_containerised_runtime_reports_every_disagreeing_identity_field() -> None:
    """The real failure shape: a container judged by a host-supervisor identity.

    working_directory, process_executable, interpreter and import_root all
    describe a launchd/systemd checkout, and code_revision is None because the
    image carries no .git. The refusal must say so rather than return False.
    """

    container = {
        "working_directory": "/app",
        "process_executable": "/usr/local/bin/python3",
        "interpreter": "/usr/local/bin/python3",
        "import_root": "/app/src",
        "code_revision": None,
        "database": {"host": "h", "port": 5432, "dbname": "d"},
    }
    reasons = _worker_ack_mismatches(
        _desired(), _ack(observed_runtime_authority=container)
    )
    assert _worker_ack_matches(_desired(), _ack(observed_runtime_authority=container)) is False
    joined = " | ".join(reasons)
    assert "code_revision" in joined
    assert "no .git in the image?" in joined


def test_missing_observed_identity_still_refuses_when_runtime_enabled() -> None:
    """Absent identity is refused, and named -- never treated as agreement."""

    blank = {**IDENTITY, "code_revision": None}
    reasons = _worker_ack_mismatches(_desired(), _ack(observed_runtime_authority=blank))
    assert reasons
    assert any("code_revision" in r for r in reasons)


def test_identity_is_not_enforced_when_runtime_is_disabled() -> None:
    """Unchanged semantics: the presence check is gated on runtime_enabled.

    A blank identity with runtime disabled falls through to the equality check,
    which still refuses on a real difference but does not demand presence.
    """

    desired = _desired(runtime_enabled=False, service_authority={})
    ack = _ack(observed_runtime_authority={})
    assert _worker_ack_mismatches(desired, ack) == []
    assert _worker_ack_matches(desired, ack) is True


def test_fingerprint_drift_short_circuits_before_identity() -> None:
    """Ordering is preserved: identity is not reported over a stale fingerprint."""

    reasons = _worker_ack_mismatches(
        _desired(),
        _ack(runtime_fingerprint="stale", observed_runtime_authority={}),
    )
    assert reasons == ["fingerprint runtime_fingerprint differs"]
