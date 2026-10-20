"""Lens-putback sweep as a DBOS workflow (deepen-dbos-integration Phase 0).

Drains stale ``okg.lens_putback_requests`` rows that got stuck in
``status='prepared'`` because the caller deferred commit and never came
back. Two policies, operator-chosen via
``manifest.runtime.lens_putback.policy``:

  * ``sweep_to_failed`` (default) — transitions stale ``prepared`` rows
    to ``status='failed'`` with ``error.type='stale_prepared_request'``.
    Safer default: the reader has to call ``prepare_lens_putback`` again
    if they want to retry. Avoids surprising callers who deferred commit
    intentionally.

  * ``auto_commit`` (opt-in) — calls ``commit_lens_overlay`` on each
    stale ``prepared`` row. Useful for batch-prepare-then-commit flows
    where the substrate is the committer.

The workflow captures ``now`` once via a ``@DBOS.step`` at workflow
start so DBOS recovery (e.g. after kill -9) sees the same timestamp
and produces the same sweep set, not an expanded one drawn from the
wall clock during downtime.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any


__all__ = [
    "LENS_PUTBACK_WORKFLOW_NAME",
    "lens_putback_sweep_workflow",
    "register_lens_putback_workflow",
]


log = logging.getLogger(__name__)


LENS_PUTBACK_WORKFLOW_NAME = "okg_lens_putback_sweep"


# Module-level handle to the decorated workflow so the scheduled
# enqueue can find it.
_DECORATED: dict[str, Any] = {}


def register_lens_putback_workflow(*, deployment: str) -> None:
    """Decorate the lens-putback sweep workflow.

    Called from the worker entry point AFTER ``DBOS()`` is constructed
    but BEFORE ``DBOS.launch()``. Clears the cache on entry so a fresh
    worker boot doesn't pick up a stale closure.
    """
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.step()
    def _capture_now() -> str:
        """Persist a single ``now`` timestamp at workflow start.

        Step-wrapped so DBOS replay sees the same value; otherwise a
        recovered workflow would compute a different sweep set drawn
        from the wall clock during downtime.

        Returns an ISO-8601 string (UTC) because DBOS step outputs
        must be JSON-serializable.
        """
        return datetime.now(timezone.utc).isoformat()

    @DBOS.workflow(
        name=LENS_PUTBACK_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def lens_putback_sweep_workflow() -> dict:
        """Run one sweep of stale prepared putback requests.

        Returns the sweep result dict. DBOS persists this as the
        workflow's output; operators read it via
        ``okg runtime debug-workflow <id>``.
        """
        now_iso = _capture_now()
        return _run_one_sweep(deployment=deployment, now_iso=now_iso)

    _DECORATED[LENS_PUTBACK_WORKFLOW_NAME] = lens_putback_sweep_workflow
    log.info(
        "registered lens-putback sweep workflow: deployment=%s name=%s",
        deployment, LENS_PUTBACK_WORKFLOW_NAME,
    )


def lens_putback_sweep_workflow() -> Any:
    """Public accessor for the decorated workflow function."""
    if LENS_PUTBACK_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "lens-putback sweep workflow not registered; call "
            "register_lens_putback_workflow() before DBOS.launch()"
        )
    return _DECORATED[LENS_PUTBACK_WORKFLOW_NAME]


# ---------------------------------------------------------------------------
# Sweep runner
# ---------------------------------------------------------------------------


def _run_one_sweep(*, deployment: str, now_iso: str) -> dict:
    """Resolve manifest + DSN, open a connection, call the sweep.

    Manifest resolution at exec time matches the publisher workflow's
    pattern — the workflow signature is arg-less so DBOS recovery doesn't
    need to re-serialize the manifest.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.deployments import (
        require_postgres_dsn, resolve_deployment,
    )
    from okg.substrate.lenses import sweep_stale_putback_requests

    manifest = resolve_deployment(deployment)
    dsn = require_postgres_dsn(manifest, deployment=deployment)

    config = _resolve_lens_putback_config(manifest)
    now = datetime.fromisoformat(now_iso)

    # `restore-publisher-perf-and-profiling` §B
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )
    with workflow_profiling_context(
        manifest=manifest,
        deployment=deployment,
        target_kind="lens_putback_sweep",
        command="okg_lens_putback_sweep",
    ), db_connect(dsn) as conn:
        result = sweep_stale_putback_requests(
            conn,
            deployment=deployment,
            now=now,
            stale_after=config["stale_after"],
            policy=config["policy"],
        )

    log.info(
        "lens-putback sweep complete: deployment=%s policy=%s "
        "considered=%s swept=%s committed=%s skipped=%s",
        deployment, config["policy"],
        result["considered"], result["swept"],
        result["committed"], result["skipped"],
    )
    return result


def _resolve_lens_putback_config(manifest) -> dict:
    """Read ``runtime.lens_putback`` from the manifest with defaults.

    Defaults:
      * ``stale_after``: 1 hour.
      * ``policy``: ``sweep_to_failed``.

    The manifest's nested config is optional; deployments that don't
    care can omit the block entirely.
    """
    runtime = getattr(manifest, "runtime", None)
    raw = {}
    if runtime is not None:
        raw = getattr(runtime, "lens_putback", None) or {}
        if not isinstance(raw, dict):
            raw = {}

    stale_after_str = raw.get("stale_after", "PT1H")
    stale_after = _parse_iso8601_duration(stale_after_str)

    policy = raw.get("policy", "sweep_to_failed")
    if policy not in ("sweep_to_failed", "auto_commit"):
        raise ValueError(
            f"runtime.lens_putback.policy must be "
            f"'sweep_to_failed' or 'auto_commit', got {policy!r}"
        )

    return {"stale_after": stale_after, "policy": policy}


def _parse_iso8601_duration(s: str) -> timedelta:
    """Minimal ISO-8601 duration parser for the subset we use.

    Supports ``PT<n>S``, ``PT<n>M``, ``PT<n>H``, ``P<n>D``, and any
    combination of those (e.g. ``P1DT12H``). Not a full RFC 3339
    implementation — months/years intentionally rejected because they
    don't have a fixed duration in seconds.
    """
    if not s.startswith("P"):
        raise ValueError(f"not an ISO-8601 duration: {s!r}")
    body = s[1:]
    days = hours = minutes = seconds = 0
    date_part, _, time_part = body.partition("T")
    # Date part: only days allowed.
    if date_part:
        for token, unit in _scan_tokens(date_part):
            if unit == "D":
                days = token
            else:
                raise ValueError(
                    f"unsupported date-part unit in {s!r}: {unit!r}"
                )
    if time_part:
        for token, unit in _scan_tokens(time_part):
            if unit == "H":
                hours = token
            elif unit == "M":
                minutes = token
            elif unit == "S":
                seconds = token
            else:
                raise ValueError(
                    f"unsupported time-part unit in {s!r}: {unit!r}"
                )
    return timedelta(
        days=days, hours=hours, minutes=minutes, seconds=seconds,
    )


def _scan_tokens(part: str):
    """Yield (int, unit-char) pairs from an ISO-8601 duration sub-part."""
    num = ""
    for c in part:
        if c.isdigit():
            num += c
        elif c.isalpha():
            if not num:
                raise ValueError(
                    f"unit {c!r} with no preceding digits in {part!r}"
                )
            yield int(num), c
            num = ""
        else:
            raise ValueError(f"unexpected character {c!r} in {part!r}")
    if num:
        raise ValueError(f"trailing digits with no unit in {part!r}")
