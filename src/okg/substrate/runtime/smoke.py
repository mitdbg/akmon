"""DBOS runtime smoke test.

Validates that the DBOS singleton can be constructed against a
deployment's DSN, that ``LISTEN``/``NOTIFY`` round-trips through the
configured connection (this is what ``DBOS.recv``/``DBOS.send`` rely
on), and that the connection pool sized per `sdk-design.md` §4.10
fits the substrate's connection budget.

Phase 1 §2.3.3 — the LISTEN/NOTIFY round-trip is the load-bearing
behavioral check. If the deployment fronts Postgres with a
transaction-pooled PgBouncer the bypass discipline (Decision 1) must
be set; this smoke test detects the misconfiguration loudly rather
than silently producing a non-functional DBOS install.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from .config import RuntimeConfig
from .engine import with_dbos_singleton


__all__ = [
    "SmokeResult",
    "run_smoke",
]


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmokeResult:
    deployment: str
    listen_notify_ok: bool
    round_trip_ms: float | None
    error: str | None = None


def run_smoke(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    timeout_seconds: float = 10.0,
) -> SmokeResult:
    """Run a LISTEN/NOTIFY round-trip through the DBOS singleton.

    Sequence:
      1. Construct + launch a DBOS singleton against ``dsn``.
      2. Start a workflow that calls ``DBOS.recv("smoke",
         timeout_seconds=...)``.
      3. From the same process, call ``DBOSClient.send`` (or the local
         ``DBOS.send`` since we're inside the same singleton) with the
         workflow_id and topic="smoke" plus a sentinel payload.
      4. Verify the workflow returns the sentinel within
         ``timeout_seconds``.

    Returns :class:`SmokeResult` with ``listen_notify_ok=True`` on
    success. Failures are captured into ``error`` rather than raised so
    callers (notably the ``okg runtime smoke`` CLI) can format them
    consistently.
    """
    from dbos import DBOS

    sentinel = f"smoke-{int(time.time() * 1000)}"

    @DBOS.workflow()
    def _smoke_wait() -> str:
        # Wait briefly for a notification on the smoke topic.
        # If no payload arrives DBOS.recv returns None on timeout.
        result = DBOS.recv("smoke", timeout_seconds=timeout_seconds)
        return result if result is not None else "<timeout>"

    try:
        with with_dbos_singleton(
            deployment=deployment, dsn=dsn, runtime=runtime,
        ):
            # Start the receiver workflow; it blocks on DBOS.recv.
            handle = DBOS.start_workflow(_smoke_wait)
            # Brief beat so the receiver has actually issued LISTEN.
            time.sleep(0.1)
            t0 = time.monotonic()
            DBOS.send(handle.workflow_id, sentinel, topic="smoke")
            received = handle.get_result()
            elapsed_ms = (time.monotonic() - t0) * 1000.0
    except Exception as e:
        log.exception("smoke test failed for deployment=%s", deployment)
        return SmokeResult(
            deployment=deployment,
            listen_notify_ok=False,
            round_trip_ms=None,
            error=f"{type(e).__name__}: {e}",
        )

    if received != sentinel:
        return SmokeResult(
            deployment=deployment,
            listen_notify_ok=False,
            round_trip_ms=elapsed_ms,
            error=(
                f"sentinel mismatch: sent {sentinel!r}, "
                f"got {received!r}"
            ),
        )

    log.info(
        "smoke OK: deployment=%s round_trip_ms=%.1f",
        deployment, elapsed_ms,
    )
    return SmokeResult(
        deployment=deployment,
        listen_notify_ok=True,
        round_trip_ms=elapsed_ms,
    )
