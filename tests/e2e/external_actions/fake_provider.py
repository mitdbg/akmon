"""Separate-process fake provider for external-action acceptance tests."""

from __future__ import annotations

import hashlib
import multiprocessing
from typing import Any, Mapping

from .rpc import _STOP, call, ready_listener, serve, stop


def _receipt(request: Mapping[str, Any]) -> str:
    # The receipt is deterministic and contains only the canonical request
    # tuple.  It is intentionally not a real provider credential or URL.
    raw = "|".join(
        str(request.get(field, ""))
        for field in ("operation_id", "method", "target_ref", "media_type", "body", "idempotency_key")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(b"fake-provider-receipt-v1\0" + raw).hexdigest()


def _main(ready, mode: str) -> None:
    listener = ready_listener()
    ready.send(listener.address)
    observations: list[dict[str, Any]] = []
    current_mode = mode

    def handle(command: Mapping[str, Any]) -> Any:
        nonlocal current_mode
        operation = command.get("op")
        if operation == "shutdown":
            return _STOP
        if operation == "set_mode":
            current_mode = command.get("mode", "success")
            return {"mode": current_mode}
        if operation == "reset":
            observations.clear()
            return {"count": 0}
        if operation == "snapshot":
            return {"mode": current_mode, "count": len(observations), "observations": list(observations)}
        if operation != "invoke" or not isinstance(command.get("request"), Mapping):
            raise ValueError("unknown fake-provider command")
        request = dict(command["request"])
        if current_mode == "unavailable":
            return {"accepted": False, "error": "provider_unavailable"}
        receipt = _receipt(request)
        observation = {"request": request, "receipt_digest": receipt}
        # Durable acceptance is recorded even when the response is dropped.
        observations.append(observation)
        if current_mode == "explicit_failure":
            return {"accepted": False, "error": "provider_explicit_failure", "receipt_digest": receipt}
        if current_mode == "response_loss":
            return {"accepted": True, "response_lost": True, "receipt_digest": None}
        if current_mode != "success":
            raise ValueError("unknown fake-provider mode")
        return {"accepted": True, "receipt_digest": receipt}

    serve(listener, handle)
    listener.close()


class FakeProviderProcess:
    """Process wrapper exposing only deterministic provider observations."""

    def __init__(self, *, mode: str = "success") -> None:
        self._ready_parent, ready_child = multiprocessing.Pipe(False)
        context = multiprocessing.get_context("spawn")
        self.process = context.Process(target=_main, args=(ready_child, mode), name="fake-ticket-provider")
        self.process.daemon = True
        self.process.start()
        ready_child.close()
        self.address = self._ready_parent.recv()
        self._ready_parent.close()

    def command(self, command: Mapping[str, Any]) -> Any:
        return call(self.address, command)

    def snapshot(self) -> Mapping[str, Any]:
        return self.command({"op": "snapshot"})

    def set_mode(self, mode: str) -> None:
        self.command({"op": "set_mode", "mode": mode})

    def close(self) -> None:
        if self.process.is_alive():
            try:
                stop(self.address)
            finally:
                self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()


__all__ = ["FakeProviderProcess"]
