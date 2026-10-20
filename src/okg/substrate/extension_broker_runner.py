"""Child-process half of the extension broker JSON protocol."""

from __future__ import annotations

import builtins
import json
import resource
import socket
import sys
import types
from collections.abc import Mapping
from typing import Any

from okg.deployment import (
    ExtensionBatch,
    ExtensionRequest,
)


class _SandboxViolation(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _audit(event: str, _args: tuple[Any, ...]) -> None:
    if event == "open":
        raise _SandboxViolation(
            "extension_filesystem_contained",
            "direct filesystem access was contained by the extension broker",
        )
    if event.startswith("socket."):
        raise _SandboxViolation(
            "extension_network_contained",
            "direct network access was contained by the extension broker",
        )
    if event in {
        "ctypes.dlopen",
        "os.exec",
        "os.fork",
        "os.forkpty",
        "os.posix_spawn",
        "os.system",
        "subprocess.Popen",
    }:
        raise _SandboxViolation(
            "extension_process_contained",
            "child process launch was contained by the extension broker",
        )


def _apply_resource_limits() -> None:
    limits = [
        (resource.RLIMIT_CORE, 0),
        (resource.RLIMIT_CPU, 10),
        # Leave enough room for Python to flush an oversized response so the
        # parent can classify it with its stricter 1 MB protocol limit.
        (resource.RLIMIT_FSIZE, 2_000_000),
        (resource.RLIMIT_NOFILE, 32),
    ]
    address_space = getattr(resource, "RLIMIT_AS", None)
    if address_space is not None:
        try:
            extension_budget = 512 * 1024 * 1024
            baseline = _linux_virtual_memory_bytes()
            target = (
                extension_budget if baseline is None else baseline + extension_budget
            )
            _soft, hard = resource.getrlimit(address_space)
            if hard != resource.RLIM_INFINITY:
                target = min(target, hard)
            if baseline is None or target > baseline:
                resource.setrlimit(address_space, (target, target))
        except (OSError, ValueError):
            # Darwin exposes RLIMIT_AS but rejects every finite value. Linux,
            # the deployment runtime, enforces an extension-only 512 MB budget
            # above the framework's already-loaded address space. The parent
            # still bounds time and file-backed output without this primitive.
            pass
    for resource_id, limit in limits:
        resource.setrlimit(resource_id, (limit, limit))


def _linux_virtual_memory_bytes() -> int | None:
    """Return current Linux address space before the extension audit starts."""

    try:
        with open("/proc/self/statm", encoding="ascii") as status:
            pages = int(status.read().split()[0])
    except (IndexError, OSError, ValueError):
        return None
    return pages * resource.getpagesize()


def _extension_builtins() -> dict[str, Any]:
    allowed_imports = frozenset(
        {"json", "math", "okg.deployment", "pathlib", "re", "socket", "typing"}
    )
    normal_import = builtins.__import__

    def admitted_import(
        name: str,
        globals: Mapping[str, Any] | None = None,
        locals: Mapping[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if level != 0 or name not in allowed_imports:
            raise _SandboxViolation(
                "extension_import_contained",
                f"import {name!r} was contained by the extension broker",
            )
        if name == "socket":
            # The module is loaded before installing the audit hook. Calls
            # made through it still emit socket.* events and are refused.
            return socket
        return normal_import(name, globals, locals, fromlist, level)

    values = dict(vars(builtins))
    for name in ("breakpoint", "compile", "eval", "exec", "input", "open"):
        values.pop(name, None)
    values["__import__"] = admitted_import
    return values


def _batch_payload(batch: ExtensionBatch) -> dict[str, Any]:
    return {
        "nodes": [
            {
                "node_id": item.node_id,
                "subtype": item.subtype,
                "attrs": _json_value(item.attrs),
            }
            for item in batch.nodes
        ],
        "edges": [
            {
                "src": item.src,
                "dst": item.dst,
                "edge_type": item.edge_type,
                "attrs": _json_value(item.attrs),
            }
            for item in batch.edges
        ],
        "observations": _json_value(batch.observations),
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(child) for child in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(
        f"extension output contains unsupported value {type(value).__name__}"
    )


def _run(request: dict[str, Any]) -> dict[str, Any]:
    source = request["source"]
    filename = request["filename"]
    module_name = request["module_name"]
    attribute = request["attribute"]
    module = types.ModuleType(module_name)
    module.__file__ = filename
    module.__package__ = module_name.rpartition(".")[0]
    module.__dict__["__builtins__"] = _extension_builtins()
    _apply_resource_limits()
    sys.addaudithook(_audit)
    try:
        exec(compile(source, filename, "exec"), module.__dict__)
        function = getattr(module, attribute)
        if getattr(function, "__okg_extension_id__", None) != request["extension_id"]:
            raise _SandboxViolation(
                "extension_entrypoint_metadata_mismatch",
                "public SDK extension id does not match admitted package metadata",
            )
        if getattr(function, "__okg_extension_kind__", None) != request["kind"]:
            raise _SandboxViolation(
                "extension_entrypoint_metadata_mismatch",
                "public SDK extension kind does not match admitted package metadata",
            )
        declared = tuple(request["declared_capabilities"])
        if getattr(function, "__okg_extension_capabilities__", None) != declared:
            raise _SandboxViolation(
                "extension_entrypoint_metadata_mismatch",
                "public SDK capabilities do not match admitted package metadata",
            )
        extension_request = ExtensionRequest(
            extension_id=request["extension_id"],
            binding_id=request["binding_id"],
            capabilities=tuple(request["granted_capabilities"]),
            payload=request["payload"],
        )
        batch = function(extension_request)
        if not isinstance(batch, ExtensionBatch):
            raise TypeError("extension entrypoint must return ExtensionBatch")
        return {"ok": True, "batch": _batch_payload(batch)}
    except _SandboxViolation as exc:
        return {"ok": False, "code": exc.code, "message": str(exc)}
    # The child protocol returns only the type, never a raw secret-bearing repr.
    except Exception as exc:
        return {
            "ok": False,
            "code": "extension_execution_failed",
            "message": f"extension execution failed with {type(exc).__name__}",
        }


def main() -> int:
    try:
        request = json.loads(sys.stdin.read())
        response = _run(request)
    except Exception as exc:
        response = {
            "ok": False,
            "code": "extension_protocol_invalid",
            "message": f"broker request failed with {type(exc).__name__}",
        }
    sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")))
    return 0 if response.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
