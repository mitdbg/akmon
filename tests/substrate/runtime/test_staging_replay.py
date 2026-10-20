"""Hermetic witnesses for the staging-replay governance guard (PACT
``bound-host-storage-pressure``, req.host.staging-governance).

The okg-workspace staging Postgres replayed 58 -> 77.9 GB in ~30 unattended
minutes after a Docker daemon restart, twice zeroing the host. These witnesses
prove the guard closes that: staging replay is explicit opt-in per session,
budgeted, and headroom-bounded, and a restart never silently re-arms it.

NO Docker, NO Postgres: every ``pg_database_size`` reading, Postgres server
identity (``pg_postmaster_start_time``), host-headroom verdict, and clock
reading is injected. The acceptance behaviors covered:

* an unarmed invocation refuses loudly, stating how to arm;
* arming without a byte budget is refused (no silent default -- P0);
* an armed replay halts at its declared byte budget;
* it halts on a host-headroom blocker;
* a restart does not re-arm -- the arm is consumed on budget exhaustion / a
  Postgres restart, and expires on its TTL.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okg.substrate.runtime.staging_replay import (
    MAX_ARM_TTL_SECONDS,
    STAGING_REPLAY_ARM_SCHEMA,
    StagingReplayArmRefused,
    arm_staging_replay,
    disarm_staging_replay,
    guard_iteration,
    guard_publisher_cycle,
    read_arm,
    resolve_arm_marker_path,
)

DEPLOYMENT = "okg-workspace-staging"
GIB = 1024**3
BASE = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)


def _headroom(status: str, message: str = "host headroom") -> SimpleNamespace:
    return SimpleNamespace(status=status, message=message)


def _ok_headroom() -> SimpleNamespace:
    return _headroom("ok", "plenty of headroom")


class _Sizes:
    """Injectable pg_database_size: returns the current ``.value`` each call."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value


class _Identity:
    """Injectable pg_postmaster_start_time identity; mutate ``.value`` to
    simulate a Postgres restart (the daemon/container-restart shape)."""

    def __init__(self, value: str = "pg-start-A") -> None:
        self.value = value

    def __call__(self) -> str:
        return self.value


def _iterate(marker, *, now, sizes, identity, headroom=_ok_headroom):
    return guard_iteration(
        deployment=DEPLOYMENT,
        marker_path=marker,
        now=now,
        db_size_fn=sizes,
        headroom_fn=headroom,
        server_identity_fn=identity,
    )


# ---------------------------------------------------------------------------
# 1. Unarmed invocation refuses loudly.
# ---------------------------------------------------------------------------


def test_unarmed_iteration_refuses_loudly(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    assert not marker.exists()

    verdict = _iterate(
        marker, now=BASE, sizes=_Sizes(58 * GIB), identity=_Identity(),
    )

    assert verdict.halted
    assert verdict.reason == "not_armed"
    # The refusal names the opt-in contract, how to arm, and the deployment.
    assert "opt-in" in verdict.message
    assert "--budget-bytes" in verdict.message
    assert DEPLOYMENT in verdict.message


def test_guard_publisher_cycle_unarmed_is_hermetic(tmp_path) -> None:
    # A valid data root with no marker refuses BEFORE touching Postgres, so the
    # wiring helper is exercisable without a DB.
    verdict = guard_publisher_cycle(
        deployment=DEPLOYMENT,
        dsn="postgresql://unused:unused@127.0.0.1:1/none",
        now=BASE,
        environ={"OKG_ENVIRONMENT_DATA_ROOT": str(tmp_path)},
    )
    assert verdict.halted
    assert verdict.reason == "not_armed"


def test_guard_publisher_cycle_missing_data_root_fails_closed() -> None:
    verdict = guard_publisher_cycle(
        deployment=DEPLOYMENT,
        dsn="postgresql://unused:unused@127.0.0.1:1/none",
        now=BASE,
        environ={},
    )
    assert verdict.halted
    assert verdict.reason == "misconfigured"
    assert "OKG_ENVIRONMENT_DATA_ROOT" in verdict.message


# ---------------------------------------------------------------------------
# 2. Arming without a byte budget is refused (no silent default).
# ---------------------------------------------------------------------------


def test_arm_without_budget_is_refused(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    with pytest.raises(StagingReplayArmRefused) as excinfo:
        arm_staging_replay(
            deployment=DEPLOYMENT,
            marker_path=marker,
            budget_bytes=None,
            now=BASE,
        )
    assert "budget" in str(excinfo.value).lower()
    assert not marker.exists()  # nothing was armed


@pytest.mark.parametrize("bad_budget", [0, -1])
def test_arm_with_nonpositive_budget_is_refused(tmp_path, bad_budget) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    with pytest.raises(StagingReplayArmRefused):
        arm_staging_replay(
            deployment=DEPLOYMENT,
            marker_path=marker,
            budget_bytes=bad_budget,
            now=BASE,
        )
    assert not marker.exists()


@pytest.mark.parametrize("bad_ttl", [0, -5, MAX_ARM_TTL_SECONDS + 1])
def test_arm_with_out_of_bounds_ttl_is_refused(tmp_path, bad_ttl) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    with pytest.raises(StagingReplayArmRefused):
        arm_staging_replay(
            deployment=DEPLOYMENT,
            marker_path=marker,
            budget_bytes=GIB,
            now=BASE,
            ttl_seconds=bad_ttl,
        )
    assert not marker.exists()


def test_arm_writes_a_schema_marker(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=10 * GIB,
        now=BASE,
        ttl_seconds=1800,
    )
    stored = read_arm(marker)
    assert stored is not None
    assert stored.schema == STAGING_REPLAY_ARM_SCHEMA
    assert stored.deployment == DEPLOYMENT
    assert stored.budget_bytes == 10 * GIB
    assert stored.state == "armed"
    assert stored.baseline_bytes is None


# ---------------------------------------------------------------------------
# 3. Armed replay halts at its byte budget.
# ---------------------------------------------------------------------------


def test_budget_halt_fires_and_consumes_the_arm(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    sizes = _Sizes(58 * GIB)
    identity = _Identity()
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=10 * GIB,
        now=BASE,
        ttl_seconds=MAX_ARM_TTL_SECONDS,
    )

    # First cycle activates the session at the 58 GiB baseline; under budget.
    v1 = _iterate(marker, now=BASE, sizes=sizes, identity=identity)
    assert v1.action == "continue"
    activated = read_arm(marker)
    assert activated.state == "active"
    assert activated.baseline_bytes == 58 * GIB
    assert activated.server_identity == "pg-start-A"

    # Grow +5 GiB: still under the 10 GiB budget.
    sizes.value = 63 * GIB
    v2 = _iterate(
        marker, now=BASE + timedelta(minutes=5), sizes=sizes, identity=identity,
    )
    assert v2.action == "continue"
    assert v2.consumed_bytes == 5 * GIB

    # Grow +11 GiB from baseline: over budget -> loud halt, arm consumed.
    sizes.value = 69 * GIB
    v3 = _iterate(
        marker, now=BASE + timedelta(minutes=10), sizes=sizes, identity=identity,
    )
    assert v3.halted
    assert v3.reason == "budget"
    assert v3.consumed_bytes == 11 * GIB
    assert v3.budget_bytes == 10 * GIB
    assert v3.marker_consumed
    assert not marker.exists()

    # A restart / next cycle finds no marker -> not armed (no re-arm).
    v4 = _iterate(
        marker, now=BASE + timedelta(minutes=11), sizes=sizes, identity=identity,
    )
    assert v4.reason == "not_armed"


# ---------------------------------------------------------------------------
# 4. Host-headroom blocker halts (arm left intact -- pressure is transient).
# ---------------------------------------------------------------------------


def test_blocker_headroom_halts_without_consuming(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    sizes = _Sizes(58 * GIB)
    identity = _Identity()
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=100 * GIB,  # generous budget: headroom is the binding halt
        now=BASE,
        ttl_seconds=MAX_ARM_TTL_SECONDS,
    )
    # Activate under OK headroom.
    assert _iterate(marker, now=BASE, sizes=sizes, identity=identity).action == (
        "continue"
    )

    blocker = lambda: _headroom(  # noqa: E731 - inline injected probe
        "blocker", "7.33% available on / , blocker floor is 8%"
    )
    v = _iterate(
        marker,
        now=BASE + timedelta(minutes=1),
        sizes=sizes,
        identity=identity,
        headroom=blocker,
    )
    assert v.halted
    assert v.reason == "headroom"
    assert v.headroom_status == "blocker"
    assert "7.33%" in v.message  # the sentinel's numbers are surfaced
    assert not v.marker_consumed
    assert marker.exists()  # transient pressure -- the arm survives

    # Once headroom recovers the same arm resumes (still under budget).
    v2 = _iterate(
        marker, now=BASE + timedelta(minutes=2), sizes=sizes, identity=identity,
    )
    assert v2.action == "continue"


# ---------------------------------------------------------------------------
# 5. A restart does not re-arm: server-restart + TTL expiry consume the arm.
# ---------------------------------------------------------------------------


def test_postgres_restart_consumes_the_arm(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    sizes = _Sizes(58 * GIB)
    identity = _Identity("pg-start-A")
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=50 * GIB,
        now=BASE,
        ttl_seconds=MAX_ARM_TTL_SECONDS,
    )
    # Activate against server identity A.
    assert _iterate(marker, now=BASE, sizes=sizes, identity=identity).action == (
        "continue"
    )
    assert read_arm(marker).server_identity == "pg-start-A"

    # The Docker daemon restart bounces the container -> new postmaster start.
    identity.value = "pg-start-B"
    v = _iterate(
        marker, now=BASE + timedelta(minutes=1), sizes=sizes, identity=identity,
    )
    assert v.halted
    assert v.reason == "server_restarted"
    assert v.marker_consumed
    assert not marker.exists()

    # The next cycle after the restart is not armed -> no silent resume.
    v2 = _iterate(
        marker, now=BASE + timedelta(minutes=2), sizes=sizes, identity=identity,
    )
    assert v2.reason == "not_armed"


def test_expired_arm_before_activation_halts_and_consumes(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=50 * GIB,
        now=BASE,
        ttl_seconds=600,  # expires at BASE + 10 min
    )
    v = _iterate(
        marker,
        now=BASE + timedelta(minutes=11),
        sizes=_Sizes(58 * GIB),
        identity=_Identity(),
    )
    assert v.halted
    assert v.reason == "expired"
    assert v.marker_consumed
    assert not marker.exists()


def test_expiry_after_activation_halts_and_consumes(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    sizes = _Sizes(58 * GIB)
    identity = _Identity()
    arm_staging_replay(
        deployment=DEPLOYMENT,
        marker_path=marker,
        budget_bytes=50 * GIB,
        now=BASE,
        ttl_seconds=600,
    )
    assert _iterate(marker, now=BASE, sizes=sizes, identity=identity).action == (
        "continue"
    )
    v = _iterate(
        marker,
        now=BASE + timedelta(minutes=11),
        sizes=sizes,
        identity=identity,
    )
    assert v.reason == "expired"
    assert not marker.exists()


# ---------------------------------------------------------------------------
# Defensive: an arm for another deployment is not honored.
# ---------------------------------------------------------------------------


def test_arm_for_other_deployment_is_not_honored(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    arm_staging_replay(
        deployment="some-other-staging",
        marker_path=marker,
        budget_bytes=GIB,
        now=BASE,
    )
    v = _iterate(marker, now=BASE, sizes=_Sizes(GIB), identity=_Identity())
    assert v.halted
    assert v.reason == "wrong_deployment"


def test_disarm_consumes_marker(tmp_path) -> None:
    marker = resolve_arm_marker_path(tmp_path)
    arm_staging_replay(
        deployment=DEPLOYMENT, marker_path=marker, budget_bytes=GIB, now=BASE,
    )
    assert disarm_staging_replay(marker) is True
    assert not marker.exists()
    assert disarm_staging_replay(marker) is False
