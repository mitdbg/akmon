"""External effect ledger process used before fake-provider I/O."""

from __future__ import annotations

import multiprocessing
from typing import Any, Mapping

from .rpc import _STOP, call, ready_listener, serve, stop


def _main(ready) -> None:
    listener = ready_listener()
    ready.send(listener.address)
    current_epoch = 17
    mode = "normal"
    entries: dict[str, dict[str, Any]] = {}

    def handle(command: Mapping[str, Any]) -> Any:
        nonlocal current_epoch, mode
        operation = command.get("op")
        if operation == "shutdown":
            return _STOP
        if operation == "set_mode":
            mode = command["mode"]
            return {"mode": mode}
        if operation == "rotate_epoch":
            current_epoch = int(command["epoch"])
            return {"current_epoch": current_epoch}
        if operation == "snapshot":
            return {"current_epoch": current_epoch, "mode": mode, "entries": list(entries.values())}
        if operation != "begin" or not isinstance(command.get("entry"), Mapping):
            raise ValueError("unknown fake-ledger command")
        if mode == "unavailable":
            return {"accepted": False, "error": "effect_ledger_unavailable"}
        entry = dict(command["entry"])
        key = entry.get("effect_instance_key")
        if not isinstance(key, str):
            raise ValueError("effect instance key is required")
        if int(entry.get("execution_epoch", -1)) != current_epoch:
            return {"accepted": False, "error": "execution_epoch_fenced"}
        existing = entries.get(key)
        if existing is not None:
            if existing == entry:
                return {"accepted": True, "replay": True, "entry": existing}
            return {"accepted": False, "error": "effect_binding_conflict"}
        if mode == "corrupt":
            return {"accepted": False, "error": "effect_ledger_corrupt"}
        entries[key] = entry
        return {"accepted": True, "replay": False, "entry": entry}

    serve(listener, handle)
    listener.close()


class FakeEffectLedgerProcess:
    """A tiny atomic begin ledger outside the review API process."""

    def __init__(self) -> None:
        parent, child = multiprocessing.Pipe(False)
        context = multiprocessing.get_context("spawn")
        self.process = context.Process(target=_main, args=(child,), name="fake-effect-ledger")
        self.process.daemon = True
        self.process.start()
        child.close()
        self.address = parent.recv()
        parent.close()

    def begin(self, entry: Mapping[str, Any]) -> Mapping[str, Any]:
        return self.command({"op": "begin", "entry": dict(entry)})

    def command(self, command: Mapping[str, Any]) -> Any:
        return call(self.address, command)

    def rotate_epoch(self, epoch: int) -> None:
        self.command({"op": "rotate_epoch", "epoch": epoch})

    def set_mode(self, mode: str) -> None:
        self.command({"op": "set_mode", "mode": mode})

    def snapshot(self) -> Mapping[str, Any]:
        return self.command({"op": "snapshot"})

    def close(self) -> None:
        if self.process.is_alive():
            try:
                stop(self.address)
            finally:
                self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()


__all__ = ["FakeEffectLedgerProcess"]
