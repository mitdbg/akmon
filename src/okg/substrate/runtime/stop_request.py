"""Process-wide "the worker was asked to stop" flag for the publisher.

The runtime worker sets it from its SIGTERM/SIGINT handler. The publisher
reads it at the one safe point it has between whole publish attempts: after
an attempt was refused and rolled back, and every lock is released. A set
flag means "do not start another attempt"; it never interrupts an attempt
that is already running, so no publish is cut off mid-transaction.
"""

from __future__ import annotations

import threading

__all__ = ["clear_stop", "request_stop", "stop_requested"]

_STOP = threading.Event()


def request_stop() -> None:
    """Record that the process was asked to stop. Safe from a signal handler."""
    _STOP.set()


def stop_requested() -> bool:
    return _STOP.is_set()


def clear_stop() -> None:
    """Reset the flag (tests; a worker never un-requests a stop)."""
    _STOP.clear()
