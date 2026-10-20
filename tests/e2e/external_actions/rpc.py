"""Tiny authenticated RPC helpers for the deterministic e2e fixtures.

The acceptance harness deliberately uses ``multiprocessing.connection`` rather
than an HTTP framework for process control.  The review API itself is still
exercised through its real ASGI callable; these helpers only keep the fixture
processes isolated and make every request explicit.
"""

from __future__ import annotations

from multiprocessing.connection import Client, Connection, Listener
from typing import Any, Callable, Mapping


AUTHKEY = b"okg-external-action-review-e2e-v1"


def serve(listener: Listener, handler: Callable[[Mapping[str, Any]], Any]) -> None:
    """Serve one command per client until a handler returns ``_STOP``."""

    while True:
        connection = listener.accept()
        try:
            command = connection.recv()
            result = handler(command)
            if result is _STOP:
                connection.send({"ok": True})
                return
            connection.send({"ok": True, "result": result})
        except BaseException as exc:  # transport failures must reach the test
            connection.send(
                {
                    "ok": False,
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            )
        finally:
            connection.close()


def call(address: Any, command: Mapping[str, Any]) -> Any:
    connection = Client(address, authkey=AUTHKEY)
    try:
        connection.send(dict(command))
        reply = connection.recv()
    finally:
        connection.close()
    if not isinstance(reply, Mapping) or reply.get("ok") is not True:
        error = reply if isinstance(reply, Mapping) else {"message": repr(reply)}
        raise RuntimeError(f"fixture RPC failed: {error.get('error')}: {error.get('message')}")
    return reply.get("result")


def ready_listener() -> Listener:
    return Listener(("127.0.0.1", 0), authkey=AUTHKEY)


class _Stop:
    pass


_STOP = _Stop()


def stop(address: Any) -> None:
    call(address, {"op": "shutdown"})


__all__ = ["AUTHKEY", "Client", "Connection", "Listener", "call", "ready_listener", "serve", "stop", "_STOP"]
