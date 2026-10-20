"""Governance for the okg-workspace staging replay (PACT
``bound-host-storage-pressure``, req.host.staging-governance).

On 2026-07-17 the okg-workspace staging Postgres replayed 58 -> 77.9 GB in
~30 unattended minutes after a Docker daemon restart, twice driving the host
filesystem to zero. Root cause: two auto-resume surfaces conspired -- the
staging container carried ``restart: unless-stopped`` (it came back the
instant the daemon did) and the supervised worker relaunched and resumed its
publish loop -- so a *daemon restart alone* re-armed an unbounded replay.

This module is the ``arm-check entrypoint`` half that makes staging replay
EXPLICIT OPT-IN, BUDGETED, and HEADROOM-BOUNDED. The complementary
``restart: no`` (container) and no-auto-restart (worker service) half lives in
:mod:`okg.substrate.environment_services`.

There is no dedicated staging-replay *driver* in this repo: the replay is the
generic DBOS publisher cycle (:func:`okg.substrate.runtime.publisher_workflow
._run_one_cycle`) running under a worker whose ``OKG_ENVIRONMENT_TIER`` is
``staging``. This module does not invent a driver; it gates the existing one
at the workflow's entry via :func:`guard_publisher_cycle`.

Contract
--------
* An operator arms ONE replay session with a REQUIRED byte budget
  (:func:`arm_staging_replay`). Arming without a positive budget is refused --
  there is no silent default (P0). The arm is a marker file with a bounded TTL.
* Every publisher cycle on a staging-tier worker calls
  :func:`guard_iteration` at the workflow's entry. It HALTS the cycle (skips
  the publish) loudly when:
    - no marker exists (not armed) -- the message states how to arm;
    - the marker's TTL has expired;
    - the Postgres server restarted since the arm was activated
      (``pg_postmaster_start_time`` changed) -- this is exactly the
      daemon/container-restart shape, so a restart never silently resumes;
    - the staging DB grew past the declared budget (``pg_database_size``
      delta from the session baseline);
    - the host-headroom sentinel reports ``blocker``.
* On budget exhaustion / expiry / server restart the marker is CONSUMED
  (deleted) so a restart cannot re-arm it -- the operator must arm a fresh
  session. A blocker-headroom halt does NOT consume the marker (the pressure
  is transient; a later cycle re-checks once headroom returns).

Everything here takes injected clock / size / identity / headroom callables so
the witnesses run hermetically (no Docker, no Postgres).
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

__all__ = [
    "ARM_MARKER_FILENAME",
    "DATA_ROOT_ENV",
    "DEFAULT_ARM_TTL_SECONDS",
    "MAX_ARM_TTL_SECONDS",
    "STAGING_REPLAY_ARM_SCHEMA",
    "STAGING_TIER",
    "TIER_ENV",
    "StagingReplayArm",
    "StagingReplayArmRefused",
    "StagingReplayError",
    "StagingReplayVerdict",
    "arm_staging_replay",
    "disarm_staging_replay",
    "guard_iteration",
    "guard_publisher_cycle",
    "read_arm",
    "resolve_arm_marker_path",
]

STAGING_REPLAY_ARM_SCHEMA = "okg.staging_replay_arm/v1"
ARM_MARKER_FILENAME = "staging-replay-arm.json"

# TTL is a safety bound on how long a single arm stays valid, not a budget:
# a bounded default is allowed (unlike the byte budget, which has no default).
DEFAULT_ARM_TTL_SECONDS = 3600
MAX_ARM_TTL_SECONDS = 86_400  # 24h hard ceiling on any single arm

DATA_ROOT_ENV = "OKG_ENVIRONMENT_DATA_ROOT"
TIER_ENV = "OKG_ENVIRONMENT_TIER"
STAGING_TIER = "staging"

# Callable shapes injected by witnesses (all default to real Postgres/probe
# reads inside :func:`guard_publisher_cycle`).
SizeFn = Callable[[], int]
IdentityFn = Callable[[], str]
HeadroomFn = Callable[[], Any]


def _how_to_arm(deployment: str) -> str:
    return (
        "staging replay is NOT armed -- it is explicit opt-in per session and "
        "is never auto-resumed by a daemon/container restart. Arm one session "
        "with a byte budget: `python -m okg.substrate.runtime.staging_replay "
        f"arm --deployment {deployment} --data-root <OKG_ENVIRONMENT_DATA_ROOT> "
        "--budget-bytes <N>`"
    )


class StagingReplayError(Exception):
    """A staging-replay marker is present but unusable (corrupt / wrong
    schema / wrong deployment). Reported loudly, never papered over."""


class StagingReplayArmRefused(StagingReplayError):
    """Refusal to arm -- e.g. no byte budget was declared (P0: no silent
    default), or the requested TTL is out of bounds."""


@dataclass(frozen=True)
class StagingReplayVerdict:
    """Outcome of one :func:`guard_iteration` call.

    ``action`` is ``"continue"`` (the cycle may publish) or ``"halt"`` (the
    cycle MUST skip). ``reason`` names why on a halt (``not_armed`` /
    ``expired`` / ``server_restarted`` / ``budget`` / ``headroom`` /
    ``misconfigured`` / ``probe_failed`` / ``wrong_deployment``)."""

    action: str
    reason: str
    message: str
    deployment: str
    consumed_bytes: int | None = None
    budget_bytes: int | None = None
    headroom_status: str | None = None
    marker_consumed: bool = False

    @property
    def halted(self) -> bool:
        return self.action == "halt"

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "message": self.message,
            "deployment": self.deployment,
            "consumed_bytes": self.consumed_bytes,
            "budget_bytes": self.budget_bytes,
            "headroom_status": self.headroom_status,
            "marker_consumed": self.marker_consumed,
        }


@dataclass(frozen=True)
class StagingReplayArm:
    """The persisted arm marker.

    ``state`` is ``"armed"`` between :func:`arm_staging_replay` and the first
    guarded cycle, then ``"active"`` once a baseline DB size and the Postgres
    server identity have been captured."""

    schema: str
    deployment: str
    budget_bytes: int
    armed_at: str
    ttl_seconds: int
    expires_at: str
    state: str = "armed"
    baseline_bytes: int | None = None
    activated_at: str | None = None
    server_identity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "deployment": self.deployment,
            "budget_bytes": self.budget_bytes,
            "armed_at": self.armed_at,
            "ttl_seconds": self.ttl_seconds,
            "expires_at": self.expires_at,
            "state": self.state,
            "baseline_bytes": self.baseline_bytes,
            "activated_at": self.activated_at,
            "server_identity": self.server_identity,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "StagingReplayArm":
        if not isinstance(raw, Mapping) or raw.get("schema") != (
            STAGING_REPLAY_ARM_SCHEMA
        ):
            raise StagingReplayError(
                "staging replay arm marker has the wrong schema"
            )
        try:
            budget = int(raw["budget_bytes"])
            ttl = int(raw["ttl_seconds"])
            deployment = str(raw["deployment"])
            armed_at = str(raw["armed_at"])
            expires_at = str(raw["expires_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise StagingReplayError(
                f"staging replay arm marker is missing/invalid fields: {exc}"
            ) from exc
        if budget <= 0:
            raise StagingReplayError(
                "staging replay arm marker declares a non-positive budget"
            )
        baseline = raw.get("baseline_bytes")
        return cls(
            schema=STAGING_REPLAY_ARM_SCHEMA,
            deployment=deployment,
            budget_bytes=budget,
            armed_at=armed_at,
            ttl_seconds=ttl,
            expires_at=expires_at,
            state=str(raw.get("state") or "armed"),
            baseline_bytes=int(baseline) if baseline is not None else None,
            activated_at=(
                str(raw["activated_at"])
                if raw.get("activated_at") is not None
                else None
            ),
            server_identity=(
                str(raw["server_identity"])
                if raw.get("server_identity") is not None
                else None
            ),
        )


def resolve_arm_marker_path(data_root: str | Path) -> Path:
    """The marker path for a staging environment's data root."""
    return Path(data_root) / ARM_MARKER_FILENAME


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def read_arm(marker_path: str | Path) -> StagingReplayArm | None:
    """Read the arm marker. Returns ``None`` when absent; raises
    :class:`StagingReplayError` on a present-but-corrupt marker (loud)."""
    path = Path(marker_path)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise StagingReplayError(
            f"staging replay arm marker is unreadable ({exc})"
        ) from exc
    return StagingReplayArm.from_dict(raw)


def disarm_staging_replay(marker_path: str | Path) -> bool:
    """Consume the marker (single-use). Returns True if one was removed."""
    path = Path(marker_path)
    existed = path.is_file()
    path.unlink(missing_ok=True)
    return existed


def arm_staging_replay(
    *,
    deployment: str,
    marker_path: str | Path,
    budget_bytes: int | None,
    now: datetime,
    ttl_seconds: int = DEFAULT_ARM_TTL_SECONDS,
) -> StagingReplayArm:
    """Arm ONE staging-replay session.

    Refuses (raises :class:`StagingReplayArmRefused`) when ``budget_bytes`` is
    missing or non-positive -- a byte budget has no silent default (P0). The
    TTL is a bounded safety window; it must be positive and within
    :data:`MAX_ARM_TTL_SECONDS`.
    """
    if budget_bytes is None:
        raise StagingReplayArmRefused(
            "refuse to arm staging replay without a byte budget: declare "
            "--budget-bytes (pg_database_size the replay may add before it "
            "halts). There is no default budget (P0)."
        )
    try:
        budget = int(budget_bytes)
    except (TypeError, ValueError) as exc:
        raise StagingReplayArmRefused(
            f"staging replay budget must be an integer byte count, got "
            f"{budget_bytes!r}"
        ) from exc
    if budget <= 0:
        raise StagingReplayArmRefused(
            f"staging replay budget must be a positive byte count, got {budget}"
        )
    try:
        ttl = int(ttl_seconds)
    except (TypeError, ValueError) as exc:
        raise StagingReplayArmRefused(
            f"staging replay TTL must be an integer seconds, got {ttl_seconds!r}"
        ) from exc
    if ttl <= 0 or ttl > MAX_ARM_TTL_SECONDS:
        raise StagingReplayArmRefused(
            f"staging replay TTL must be in (0, {MAX_ARM_TTL_SECONDS}] seconds, "
            f"got {ttl}"
        )
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    expires_at = now + timedelta(seconds=ttl)
    arm = StagingReplayArm(
        schema=STAGING_REPLAY_ARM_SCHEMA,
        deployment=deployment,
        budget_bytes=budget,
        armed_at=now.isoformat(),
        ttl_seconds=ttl,
        expires_at=expires_at.isoformat(),
        state="armed",
    )
    _atomic_write_json(Path(marker_path), arm.to_dict())
    return arm


def _halt(
    reason: str,
    message: str,
    *,
    deployment: str,
    consumed: int | None = None,
    budget: int | None = None,
    headroom_status: str | None = None,
    consumed_marker: bool = False,
) -> StagingReplayVerdict:
    return StagingReplayVerdict(
        action="halt",
        reason=reason,
        message=message,
        deployment=deployment,
        consumed_bytes=consumed,
        budget_bytes=budget,
        headroom_status=headroom_status,
        marker_consumed=consumed_marker,
    )


def _parse_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def guard_iteration(
    *,
    deployment: str,
    marker_path: str | Path,
    now: datetime,
    db_size_fn: SizeFn,
    headroom_fn: HeadroomFn,
    server_identity_fn: IdentityFn,
) -> StagingReplayVerdict:
    """Gate one replay iteration (one publisher cycle).

    Returns a ``continue`` verdict only when the session is armed, unexpired,
    on the same Postgres server it was activated against, under budget, and
    the host is above the blocker floor. Otherwise returns a loud ``halt``.
    The first call on a freshly-armed marker ACTIVATES it, capturing the
    baseline DB size and server identity.
    """
    path = Path(marker_path)
    arm = read_arm(path)
    if arm is None:
        return _halt(
            "not_armed", _how_to_arm(deployment), deployment=deployment,
        )
    if arm.deployment != deployment:
        return _halt(
            "wrong_deployment",
            (
                f"staging replay arm marker at {path} is for deployment "
                f"{arm.deployment!r}, not {deployment!r}; refusing to replay"
            ),
            deployment=deployment,
        )
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if now >= _parse_ts(arm.expires_at):
        disarm_staging_replay(path)
        return _halt(
            "expired",
            (
                f"staging replay arm expired at {arm.expires_at} (now {now.isoformat()}); "
                "the arm is consumed -- re-arm a fresh session to resume. "
                + _how_to_arm(deployment)
            ),
            deployment=deployment,
            budget=arm.budget_bytes,
            consumed_marker=True,
        )

    identity = server_identity_fn()
    current = db_size_fn()

    if arm.state != "active":
        arm = replace(
            arm,
            state="active",
            baseline_bytes=int(current),
            activated_at=now.isoformat(),
            server_identity=identity,
        )
        _atomic_write_json(path, arm.to_dict())
        baseline = int(current)
    else:
        if arm.server_identity != identity:
            disarm_staging_replay(path)
            return _halt(
                "server_restarted",
                (
                    "staging Postgres restarted since the replay was armed "
                    f"(armed against {arm.server_identity!r}, now {identity!r}) "
                    "-- this is the daemon/container-restart shape; the arm is "
                    "consumed so a restart never silently resumes. "
                    + _how_to_arm(deployment)
                ),
                deployment=deployment,
                budget=arm.budget_bytes,
                consumed_marker=True,
            )
        baseline = int(arm.baseline_bytes or 0)

    consumed = max(0, int(current) - baseline)
    if consumed >= arm.budget_bytes:
        disarm_staging_replay(path)
        return _halt(
            "budget",
            (
                f"staging replay hit its byte budget: consumed {consumed} bytes "
                f">= budget {arm.budget_bytes} bytes (baseline {baseline}, now "
                f"{int(current)} on pg_database_size); halting and consuming the "
                "arm -- re-arm with a larger budget to continue."
            ),
            deployment=deployment,
            consumed=consumed,
            budget=arm.budget_bytes,
            consumed_marker=True,
        )

    headroom = headroom_fn()
    status = getattr(headroom, "status", None)
    if status == "blocker":
        detail = getattr(headroom, "message", "host headroom below blocker floor")
        return _halt(
            "headroom",
            (
                "staging replay halted on host-headroom blocker (arm left "
                f"intact -- pressure is transient): {detail}"
            ),
            deployment=deployment,
            consumed=consumed,
            budget=arm.budget_bytes,
            headroom_status=status,
        )

    return StagingReplayVerdict(
        action="continue",
        reason="armed",
        message=(
            f"staging replay armed and within bounds: consumed {consumed}/"
            f"{arm.budget_bytes} bytes, headroom status "
            f"{status or 'unknown'}"
        ),
        deployment=deployment,
        consumed_bytes=consumed,
        budget_bytes=arm.budget_bytes,
        headroom_status=status,
    )


def _pg_scalar(dsn: str, query: str) -> Any:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
    return row[0] if row else None


def guard_publisher_cycle(
    *,
    deployment: str,
    dsn: str,
    now: datetime | None = None,
    environ: Mapping[str, str] | None = None,
) -> StagingReplayVerdict:
    """Workflow-entry gate for the DBOS publisher cycle on a staging worker.

    Resolves the arm marker under ``OKG_ENVIRONMENT_DATA_ROOT`` and builds the
    real Postgres / headroom callables, then delegates to
    :func:`guard_iteration`. Fails closed (``halt``) on a missing data root or
    an unreachable Postgres -- a big write with unmeasurable state is the
    incident shape.
    """
    env = os.environ if environ is None else environ
    when = now or datetime.now(tz=timezone.utc)
    data_root = env.get(DATA_ROOT_ENV)
    if not data_root:
        return _halt(
            "misconfigured",
            (
                f"{DATA_ROOT_ENV} is unset for staging deployment "
                f"{deployment!r}; cannot locate the staging replay arm marker "
                "-- refusing to replay"
            ),
            deployment=deployment,
        )
    marker_path = resolve_arm_marker_path(data_root)

    def _db_size() -> int:
        value = _pg_scalar(dsn, "SELECT pg_database_size(current_database())")
        return int(value or 0)

    def _server_identity() -> str:
        value = _pg_scalar(dsn, "SELECT pg_postmaster_start_time()")
        return str(value)

    def _headroom() -> Any:
        from okg.substrate.runtime.host_headroom import (
            check_host_disk_headroom,
        )

        return check_host_disk_headroom(deployment=deployment)

    try:
        return guard_iteration(
            deployment=deployment,
            marker_path=marker_path,
            now=when,
            db_size_fn=_db_size,
            headroom_fn=_headroom,
            server_identity_fn=_server_identity,
        )
    except StagingReplayError as exc:
        return _halt(
            "marker_invalid",
            f"staging replay arm marker is unusable: {exc}",
            deployment=deployment,
        )
    except Exception as exc:  # noqa: BLE001 - unmeasurable state fails closed.
        return _halt(
            "probe_failed",
            (
                f"staging replay guard could not measure staging state "
                f"({exc}); refusing to replay (fail closed)"
            ),
            deployment=deployment,
        )


# ---------------------------------------------------------------------------
# Operator CLI: the explicit per-session arm entrypoint.
# ---------------------------------------------------------------------------


def _cli(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m okg.substrate.runtime.staging_replay",
        description=(
            "Arm / inspect / disarm the okg-workspace staging replay. Replay "
            "is explicit opt-in per session and is never auto-resumed by a "
            "daemon/container restart."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    arm = sub.add_parser("arm", help="arm one replay session (needs a budget)")
    arm.add_argument("--deployment", required=True)
    arm.add_argument("--data-root", required=True)
    arm.add_argument(
        "--budget-bytes",
        type=int,
        required=True,
        help="bytes the replay may add to pg_database_size before it halts",
    )
    arm.add_argument(
        "--ttl-seconds", type=int, default=DEFAULT_ARM_TTL_SECONDS,
    )

    status = sub.add_parser("status", help="show the current arm state")
    status.add_argument("--data-root", required=True)

    disarm = sub.add_parser("disarm", help="consume the arm marker")
    disarm.add_argument("--data-root", required=True)

    args = parser.parse_args(argv)
    if args.command == "arm":
        marker = resolve_arm_marker_path(args.data_root)
        try:
            result = arm_staging_replay(
                deployment=args.deployment,
                marker_path=marker,
                budget_bytes=args.budget_bytes,
                now=datetime.now(tz=timezone.utc),
                ttl_seconds=args.ttl_seconds,
            )
        except StagingReplayArmRefused as exc:
            print(f"error: {exc}")
            return 2
        print(
            json.dumps(
                {"ok": True, "armed": result.to_dict(), "marker": str(marker)},
                indent=2,
            )
        )
        return 0
    if args.command == "status":
        marker = resolve_arm_marker_path(args.data_root)
        arm_state = read_arm(marker)
        print(
            json.dumps(
                {
                    "marker": str(marker),
                    "armed": arm_state.to_dict() if arm_state else None,
                },
                indent=2,
            )
        )
        return 0
    # disarm
    marker = resolve_arm_marker_path(args.data_root)
    removed = disarm_staging_replay(marker)
    print(json.dumps({"marker": str(marker), "consumed": removed}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(_cli())
