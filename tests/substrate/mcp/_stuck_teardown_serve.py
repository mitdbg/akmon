"""Test-only launcher: real ``okg mcp-serve`` whose shutdown never finishes.

Used only by ``test_mcp_serve_stop_guard.py``; nothing in ``src/`` imports
or reads it. It patches this process (and only this process) before calling
the real CLI entry point, ``okg.substrate.cli.main.main``, with the arguments
it was given.

The patch reproduces the hang lane P captured on 2026-10-01: asyncio.run()
teardown cancels every task and waits for them, and one task is never woken,
so ``_cancel_all_tasks`` never returns. Here the stuck task is one that
catches every cancellation and goes back to sleep, which is deterministic
where the real lost wakeup happens about 1 stop in 50-100.

Two switches, read only by this file:

* ``OKG_TEST_STOP_GUARD_DISABLED=1`` raises the stop guard's bound to a value
  no test waits for, which shows the process stays hung without the guard.
* ``OKG_TEST_STOP_GUARD_NO_STUCK_TASK=1`` leaves the teardown unpatched, so
  the launcher is the plain CLI.
* ``OKG_TEST_STOP_GUARD_BLOCKING_RELEASE=1`` makes the connection-claim
  release sleep far past the guard's bound, as a paused database would, to
  show the deadline does not wait for it.
"""

from __future__ import annotations

import asyncio
import os
import sys

from mcp.server.fastmcp import FastMCP

import okg.substrate.mcp.server as server_mod
from okg.substrate.cli.main import main

_original_run_stdio_async = FastMCP.run_stdio_async


async def _ignore_cancellation_forever() -> None:
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            continue


async def _run_stdio_async_with_stuck_task(self: FastMCP) -> None:
    asyncio.get_running_loop().create_task(
        _ignore_cancellation_forever(),
        name="okg-test-ignores-cancellation",
    )
    await _original_run_stdio_async(self)


if os.environ.get("OKG_TEST_STOP_GUARD_NO_STUCK_TASK") != "1":
    FastMCP.run_stdio_async = _run_stdio_async_with_stuck_task  # type: ignore[method-assign]

if os.environ.get("OKG_TEST_STOP_GUARD_BLOCKING_RELEASE") == "1":
    import time

    from okg.substrate.db import capacity

    def _release_that_blocks(*, application_name: str) -> None:
        del application_name
        time.sleep(600)

    capacity.release_process_reservations = _release_that_blocks

if os.environ.get("OKG_TEST_STOP_GUARD_DISABLED") == "1":
    server_mod.MCP_STOP_GUARD_SECONDS = 1_000_000.0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
